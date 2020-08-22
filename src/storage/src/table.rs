use crate::StorageError;
use data::encoding_core::SortableEncoding;
use data::{DataType, Datum, LogicalTimestamp, SortOrder, TupleIter};
use rocksdb::prelude::*;
use rocksdb::{DBRawIterator, WriteBatch, WriteBatchWithIndex};
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// A Table is at this level is a collection of rows, identified by an id.
/// We'll expose all of these tables by id in some special schema but in general not all of these
/// are "tables" from the users perspective, some may be indexes..
/// The primary key isn't exactly a primary key as freq doesn't always = 1. It's more to give KV
/// semantics(and the performance that comes with it) for system/streaming state tables.
/// It's not really defined what it means for a user table as of yet, At least for a start we'll
/// consider all columns of a user table to be primary, if we wanted to expose it at the user level
/// then we'd have to detect when the tuple-rest didn't match and throw an error.
#[derive(Clone)]
pub struct Table {
    db: Arc<DB>,
    id: u32,
    columns: Vec<(String, DataType)>,
    pk: Vec<SortOrder>,
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Table {}

impl Debug for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Table({})", self.id))
    }
}

impl Table {
    /// Creates a new table. The pk represents the number of columns in the pk and their sort
    /// orders
    pub(crate) fn new(
        db: Arc<DB>,
        id: u32,
        columns: Vec<(String, DataType)>,
        pk: Vec<SortOrder>,
    ) -> Self {
        assert!(columns.len() >= pk.len());
        Table {
            db,
            id,
            columns,
            pk,
        }
    }

    /// Returns the id of the table.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Forces a rocks db compaction of the table, we'll expose this out in sql as it may be useful
    /// after bulk loads or for benchmark tests as it blocks until compaction is done
    pub fn force_rocks_compaction(&self) {
        // id + 2 as id->id+1 is the index portion of the table and id+1->id+2 is the log portion
        self.db.compact_range(
            Some(self.id.to_be_bytes()),
            Some((self.id + 2).to_be_bytes()),
        );
    }

    /// Performs an atomic write, This semantically is done at the storage level so writes to any
    /// tables can appear in here
    pub fn atomic_write<F, E: From<StorageError>>(&self, batch: F) -> Result<(), E>
    where
        F: FnOnce(&mut Writer) -> Result<(), E>,
    {
        let mut writer = Writer::new();
        batch(&mut writer)?;
        let mut write_options = WriteOptions::new();
        write_options.set_sync(true);
        write_options.set_low_pri(true);
        self.db
            .write_opt(writer.write_batch, &write_options)
            .map_err(StorageError::from)?;
        Ok(())
    }

    /// Lower level atomic write without read after write support, used to work around some
    /// unsupported rocks db functionality
    pub fn atomic_write_without_index<F, E: From<StorageError>>(&self, batch: F) -> Result<(), E>
    where
        F: FnOnce(&mut WriteBatch) -> Result<(), E>,
    {
        let mut write_batch = WriteBatch::default();
        batch(&mut write_batch)?;
        let mut write_options = WriteOptions::new();
        write_options.set_sync(true);
        write_options.set_low_pri(true);
        self.db
            .write_opt(write_batch, &write_options)
            .map_err(StorageError::from)?;
        Ok(())
    }

    /// Looks up the current value for pk without any MVCC semantics, useful for system
    /// tables and streaming state tables and as it doesn't iter under the covers it can
    /// make use of the bloom filters for way better perf.
    /// Requires a buffer to be passed in, populates rest tuple with the rest of the
    /// tuple
    pub fn system_point_lookup<'a>(
        &'a self,
        pk: &[Datum<'a>],
        key_buf: &mut Vec<u8>,
        rest_tuple: &mut Vec<Datum<'a>>,
    ) -> Result<Option<i64>, StorageError> {
        write_index_header_key(self, pk, key_buf);

        if let Some(value_slice) = self.db.get_pinned(key_buf)? {
            rest_tuple.clear();

            let mut tuple_rest_len = 0_u64;
            // We skip 8 bytes over the timestamp, then read in the freq
            let mut freq = 0_i64;
            let mut value_buf = freq.read_sortable_bytes(SortOrder::Asc, &value_slice[8..]);
            // Not the tuple
            value_buf = tuple_rest_len.read_sortable_bytes(SortOrder::Asc, value_buf);
            rest_tuple.extend((0..tuple_rest_len).map(|_| Datum::default()));
            for datum in rest_tuple {
                value_buf = datum.from_sortable_bytes(value_buf);
            }
            Ok(Some(freq))
        } else {
            Ok(None)
        }
    }

    /// Full scan of the table, all returned record timestamps are guaranteed to be *less*
    /// than the passed in timestamp
    pub fn full_scan(&self, timestamp: LogicalTimestamp) -> impl TupleIter<E = StorageError> + '_ {
        self.range_scan(None, None, timestamp)
    }

    /// Range scan of the table, all returned record timestamps are guaranteed to be *less*
    /// than the passed in timestamp.
    /// The ranges here are inclusive(but based on the prefixes) so...
    /// from: 1, to: 1
    /// will include anything prefixed with 1.
    /// The from:to must be ordered as per the pk ordering.
    /// ie if the first col is sorted desc then the correct call here would be
    /// from: 5 to: 1.
    pub fn range_scan(
        &self,
        from: Option<&[Datum]>,
        to: Option<&[Datum]>,
        timestamp: LogicalTimestamp,
    ) -> impl TupleIter<E = StorageError> + '_ {
        let mut iter_options = ReadOptions::default();
        iter_options.set_prefix_same_as_start(true);

        if let Some(to_datum) = to {
            let mut buf = vec![];
            write_range_key(self, to_datum, &mut buf, true);
            iter_options.set_iterate_upper_bound(buf);
        } else {
            iter_options.set_iterate_upper_bound((self.id + 1).to_be_bytes());
        }

        let mut iter = self.db.raw_iterator_opt(iter_options);

        // Seek to start.
        if let Some(from_datum) = from {
            let mut buf = vec![];
            write_range_key(self, from_datum, &mut buf, false);
            iter.seek(&buf);
        } else {
            iter.seek(&self.id.to_be_bytes());
        }

        IndexIter::new(iter, timestamp, self.columns.len())
    }

    pub fn columns(&self) -> &[(String, DataType)] {
        &self.columns
    }
}

/// TupleIter implementation for iterating over the index section of tables
struct IndexIter<'a> {
    iter: DBRawIterator<'a>,
    timestamp: LogicalTimestamp,
    /// Rocks db iters start already positioned on the first item
    /// so we want the first call to advance to not advance the underlying
    /// rocksdb iter
    first: bool,
    tuple_buffer: Vec<Datum<'static>>,
    freq: Option<i64>,
}

impl<'a> IndexIter<'a> {
    fn new(iter: DBRawIterator<'a>, timestamp: LogicalTimestamp, column_count: usize) -> Self {
        let tuple_buffer = right_size_new_to(column_count);
        IndexIter {
            iter,
            timestamp,
            first: true,
            tuple_buffer,
            freq: None,
        }
    }
}

impl TupleIter for IndexIter<'_> {
    type E = StorageError;

    fn advance(&mut self) -> Result<(), StorageError> {
        // Once we emit a record we need to skip to the header of the next.
        // When true this seeks to the next header record
        let mut seek_next_header = true;

        loop {
            if self.first {
                self.first = false;
            } else {
                self.iter.next();
            }

            if self.iter.valid() {
                // key = <prefix as u32 be>:<tuple-pk as sorted>:<0>
                // value = <timestamp as u64 le><freq as i64 varint><tuple-rest as sorted>

                // Chop prefix
                let mut key_buf = &self.iter.key().unwrap()[4..];
                let mut value_buf = self.iter.value().unwrap();
                // Tuple Pk
                let mut tuple_pk_len = 0_u64;
                key_buf = tuple_pk_len.read_sortable_bytes(SortOrder::Asc, &key_buf);
                for idx in 0..tuple_pk_len {
                    key_buf = self.tuple_buffer[idx as usize].from_sortable_bytes(key_buf);
                }

                // Timestamp
                let mut tuple_timestamp = LogicalTimestamp::default();
                if key_buf[0] == 0 {
                    // "Header" record
                    tuple_timestamp.ms =
                        u64::from_le_bytes(value_buf[..8].as_ref().try_into().unwrap());
                    value_buf = &value_buf[8..];
                    seek_next_header = false;
                } else if seek_next_header {
                    continue;
                } else {
                    tuple_timestamp.ms =
                        u64::MAX - u64::from_be_bytes(key_buf[..8].as_ref().try_into().unwrap());
                };

                // Check to make sure the tuple isn't in the future, if so loop to the next record
                if tuple_timestamp >= self.timestamp {
                    continue;
                }

                // freq
                let mut freq = 0_i64;
                value_buf = freq.read_sortable_bytes(SortOrder::Asc, value_buf);

                // We've found the correct record for the pk at this time, but its zero...
                // Skip to the next
                if freq == 0 {
                    seek_next_header = true;
                    continue;
                }

                self.freq = Some(freq);

                // non-pk part of the tuple
                let mut datum_count = 0_u64;
                value_buf = datum_count.read_sortable_bytes(SortOrder::Asc, value_buf);
                for idx in 0..datum_count {
                    value_buf = self.tuple_buffer[(tuple_pk_len + idx) as usize]
                        .from_sortable_bytes(value_buf);
                }
                break;
            } else {
                self.freq = None;
                self.iter.status()?;
                break;
            }
        }
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum<'_>], i64)> {
        if let Some(freq) = self.freq {
            Some((&self.tuple_buffer, freq))
        } else {
            None
        }
    }

    fn column_count(&self) -> usize {
        self.tuple_buffer.len()
    }
}

/// Abstraction through which all writes happens, allows some degree of
/// read after write functionality which is not offered by rocksdb.
pub struct Writer {
    write_batch: WriteBatchWithIndex,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Writer {
            write_batch: WriteBatchWithIndex::default(),
            key_buf: Vec::with_capacity(64),
            value_buf: Vec::with_capacity(64),
        }
    }

    /// Writes the tuple into the table
    pub fn write_tuple(
        &mut self,
        table: &Table,
        tuple: &[Datum],
        timestamp: LogicalTimestamp,
        mut freq: i64,
    ) -> Result<(), StorageError> {
        assert_eq!(tuple.len(), table.columns.len());
        // create rocksdb key
        write_index_header_key(table, tuple, &mut self.key_buf);

        // TODO investigate holding onto slice as rocksdb may reuse it if we pass it back in.
        if let Some(value_bytes) = self.write_batch.get(&table.db, &self.key_buf)? {
            // There's an existing record..
            // We need to bump it down from the header.
            let last_timestamp = u64::from_le_bytes(value_bytes.as_ref()[..8].try_into().unwrap());
            // We need to update the freqs here.
            let mut last_freq = 0_i64;
            last_freq.read_sortable_bytes(SortOrder::Asc, &value_bytes.as_ref()[8..]);
            freq += last_freq;

            if last_timestamp != timestamp.ms {
                self.key_buf.pop();
                self.key_buf
                    .extend_from_slice(&(u64::MAX - last_timestamp).to_be_bytes());

                self.write_batch
                    .put(&self.key_buf, &value_bytes.as_ref()[8..]);

                // Restore the key
                self.key_buf.truncate(self.key_buf.len() - 8);
                self.key_buf.push(0);
            }
        }
        write_index_header_value(table, tuple, timestamp, freq, &mut self.value_buf);

        self.write_batch.put(&self.key_buf, &self.value_buf);
        Ok(())
    }

    /// Writes the tuple into the table without any real mvcc or logging semantics.
    /// This should really only be used as an optimisation mechanism for the storing
    /// state for streaming etc, it shouldn't be used on user facing tables.
    /// Will overwrite the latest version of a tuple for the same primary key
    pub fn system_write_tuple(&mut self, table: &Table, tuple: &[Datum], freq: i64) {
        self.write_index_header(table, tuple, LogicalTimestamp::default(), freq);
    }

    /// Deletes tuples but should only be used for tuples written with system_write_tuple.
    /// Only the pk parts of the tuple are needed but passing in more wont hurt but will delete
    /// according to that pk...
    pub fn system_delete_tuple(&mut self, table: &Table, pk: &[Datum]) {
        write_index_header_key(table, pk, &mut self.key_buf);
        self.write_batch.delete(&self.key_buf);
    }

    fn write_index_header(
        &mut self,
        table: &Table,
        tuple: &[Datum],
        timestamp: LogicalTimestamp,
        freq: i64,
    ) {
        write_index_header_key(table, tuple, &mut self.key_buf);
        write_index_header_value(table, tuple, timestamp, freq, &mut self.value_buf);

        self.write_batch.put(&self.key_buf, &self.value_buf);
    }
}

fn write_index_header_key(table: &Table, tuple: &[Datum], key_buf: &mut Vec<u8>) {
    // It turns out the the index_header_key is the same as our starting range keys
    assert!(tuple.len() >= table.pk.len());
    write_range_key(table, tuple, key_buf, false);
}

/// Used to write to create the from/to byte keys to feed into our iterator
fn write_range_key(table: &Table, tuple: &[Datum], key_buf: &mut Vec<u8>, end: bool) {
    // Index header:
    // key = <prefix as u32 be>:<tuple-pk as sorted>:<0(timestamp)>
    key_buf.clear();
    // Prefix
    key_buf.extend_from_slice(&table.id.to_be_bytes());

    // Tuple-PK
    (table.pk.len() as u64).write_sortable_bytes(SortOrder::Asc, key_buf);

    for (sort_order, datum) in table.pk.iter().zip(tuple) {
        datum.as_sortable_bytes(*sort_order, key_buf);
    }
    if end {
        key_buf.push(255);
    } else {
        key_buf.push(0);
    }
}

fn write_index_header_value(
    table: &Table,
    tuple: &[Datum],
    timestamp: LogicalTimestamp,
    freq: i64,
    value_buf: &mut Vec<u8>,
) {
    // Index header:
    // value = <timestamp as u64 le><freq as i64 varint><tuple-rest as sorted>
    value_buf.clear();

    ////////// VALUE
    // Actual Timestamp
    value_buf.extend_from_slice(&timestamp.ms.to_le_bytes());

    // Freq
    freq.write_sortable_bytes(SortOrder::Asc, value_buf);

    // Tuple-rest
    let rest = &tuple[(table.pk.len())..];
    (rest.len() as u64).write_sortable_bytes(SortOrder::Asc, value_buf);
    for datum in rest {
        datum.as_sortable_bytes(SortOrder::Asc, value_buf);
    }
}

fn right_size_new_to<T: Default>(size: usize) -> Vec<T> {
    (0..size).map(|_| T::default()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Storage, StorageError};
    use data::{Datum, LogicalTimestamp, SortOrder};

    /// Hard to functionally test this, so this is more just a smoke test that anything else!
    #[test]
    fn test_force_rocks_compaction() -> Result<(), StorageError> {
        let storage = Storage::new_in_mem()?;
        let table = storage.table(
            1234,
            vec![("col1".to_string(), DataType::Text)],
            vec![SortOrder::Asc],
        );
        table.force_rocks_compaction();
        Ok(())
    }

    #[test]
    fn test_system_write_tuple() -> Result<(), StorageError> {
        let storage = Storage::new_in_mem()?;
        let columns = vec![
            ("col1".to_string(), DataType::Integer),
            ("col2".to_string(), DataType::Integer),
            ("col3".to_string(), DataType::Text),
        ];
        let table = storage.table(1234, columns, vec![SortOrder::Asc]);
        let tuple1 = vec![
            Datum::from(123),
            Datum::Null,
            Datum::from("abc".to_string()),
        ];
        let tuple2 = vec![
            Datum::from(456),
            Datum::Null,
            Datum::from("efg".to_string()),
        ];
        let freq: i64 = 3;

        table.atomic_write::<_, StorageError>(|writer| {
            writer.system_write_tuple(&table, &tuple1, freq);
            writer.system_write_tuple(&table, &tuple2, freq);

            Ok(())
        })?;

        // Iter with whatever timestamp
        let mut iter = table.full_scan(LogicalTimestamp::new(1));
        assert_eq!(iter.next()?, Some((tuple1.as_ref(), 3)));
        assert_eq!(iter.next()?, Some((tuple2.as_ref(), 3)));
        assert_eq!(iter.next()?, None);

        // Delete a tuple and see if it takes
        table.atomic_write::<_, StorageError>(|writer| {
            writer.system_delete_tuple(&table, &[Datum::from(123)]);
            Ok(())
        })?;

        let mut iter = table.full_scan(LogicalTimestamp::new(1));
        assert_eq!(iter.next()?, Some((tuple2.as_ref(), 3)));
        assert_eq!(iter.next()?, None);

        // Try a point lookup
        let mut buf = vec![];
        let mut target_buf = vec![];
        let freq = table.system_point_lookup(&[Datum::from(456)], &mut buf, &mut target_buf)?;

        assert_eq!(freq, Some(3));
        assert_eq!(
            target_buf,
            vec![Datum::Null, Datum::from("efg".to_string())]
        );
        Ok(())
    }

    #[test]
    fn test_write_tuple() -> Result<(), StorageError> {
        let storage = Storage::new_in_mem()?;
        let columns = vec![
            ("col1".to_string(), DataType::Integer),
            ("col3".to_string(), DataType::Text),
        ];
        let table = storage.table(1234, columns, vec![SortOrder::Asc]);
        let tuple = vec![Datum::from(123), Datum::from("abc".to_string())];

        table.atomic_write::<_, StorageError>(|writer| {
            writer.write_tuple(&table, &tuple, LogicalTimestamp::new(10), 1)?;
            writer.write_tuple(&table, &tuple, LogicalTimestamp::new(20), 1)?;
            writer.write_tuple(&table, &tuple, LogicalTimestamp::new(20), 1)?;
            writer.write_tuple(&table, &tuple, LogicalTimestamp::new(30), 1)?;
            Ok(())
        })?;

        // Iter at different times
        let mut iter = table.full_scan(LogicalTimestamp::new(5));
        assert_eq!(iter.next()?, None);

        let mut iter = table.full_scan(LogicalTimestamp::new(15));
        assert_eq!(iter.next()?, Some((tuple.as_ref(), 1)));
        assert_eq!(iter.next()?, None);

        let mut iter = table.full_scan(LogicalTimestamp::new(25));
        assert_eq!(iter.next()?, Some((tuple.as_ref(), 3)));
        assert_eq!(iter.next()?, None);

        let mut iter = table.full_scan(LogicalTimestamp::new(35));
        assert_eq!(iter.next()?, Some((tuple.as_ref(), 4)));
        assert_eq!(iter.next()?, None);

        Ok(())
    }

    #[test]
    fn test_right_size_new_to() {
        let to: Vec<bool> = right_size_new_to(5);

        assert_eq!(to, vec![false, false, false, false, false])
    }

    #[test]
    fn test_range_scan_full_key_forward() -> Result<(), StorageError> {
        let storage = Storage::new_in_mem()?;
        let columns = vec![("col1".to_string(), DataType::Integer)];
        let table = storage.table(1234, columns, vec![SortOrder::Asc]);

        table.atomic_write::<_, StorageError>(|writer| {
            writer.write_tuple(&table, &[Datum::from(1)], LogicalTimestamp::new(10), 1)?;
            writer.write_tuple(&table, &[Datum::from(2)], LogicalTimestamp::new(10), 1)?;
            writer.write_tuple(&table, &[Datum::from(3)], LogicalTimestamp::new(10), 1)?;
            writer.write_tuple(&table, &[Datum::from(4)], LogicalTimestamp::new(10), 1)?;
            Ok(())
        })?;

        // Empty to
        let mut iter = table.range_scan(Some(&[Datum::from(3)]), None, LogicalTimestamp::MAX);
        assert_eq!(iter.next()?, Some(([Datum::from(3)].as_ref(), 1)));
        assert_eq!(iter.next()?, Some(([Datum::from(4)].as_ref(), 1)));
        assert_eq!(iter.next()?, None);

        // Empty from
        let mut iter = table.range_scan(None, Some(&[Datum::from(2)]), LogicalTimestamp::MAX);
        assert_eq!(iter.next()?, Some(([Datum::from(1)].as_ref(), 1)));
        assert_eq!(iter.next()?, Some(([Datum::from(2)].as_ref(), 1)));
        assert_eq!(iter.next()?, None);

        // Both populated
        let mut iter = table.range_scan(
            Some(&[Datum::from(2)]),
            Some(&[Datum::from(3)]),
            LogicalTimestamp::MAX,
        );
        assert_eq!(iter.next()?, Some(([Datum::from(2)].as_ref(), 1)));
        assert_eq!(iter.next()?, Some(([Datum::from(3)].as_ref(), 1)));
        assert_eq!(iter.next()?, None);

        Ok(())
    }

    #[test]
    fn test_range_scan_full_key_reverse() -> Result<(), StorageError> {
        let storage = Storage::new_in_mem()?;
        let columns = vec![("col1".to_string(), DataType::Integer)];
        let table = storage.table(1234, columns, vec![SortOrder::Desc]);

        table.atomic_write::<_, StorageError>(|writer| {
            writer.write_tuple(&table, &[Datum::from(1)], LogicalTimestamp::new(10), 1)?;
            writer.write_tuple(&table, &[Datum::from(2)], LogicalTimestamp::new(10), 1)?;
            writer.write_tuple(&table, &[Datum::from(3)], LogicalTimestamp::new(10), 1)?;
            writer.write_tuple(&table, &[Datum::from(4)], LogicalTimestamp::new(10), 1)?;
            Ok(())
        })?;

        // Empty to
        let mut iter = table.range_scan(Some(&[Datum::from(2)]), None, LogicalTimestamp::MAX);
        assert_eq!(iter.next()?, Some(([Datum::from(2)].as_ref(), 1)));
        assert_eq!(iter.next()?, Some(([Datum::from(1)].as_ref(), 1)));
        assert_eq!(iter.next()?, None);

        // Empty from
        let mut iter = table.range_scan(None, Some(&[Datum::from(3)]), LogicalTimestamp::MAX);
        assert_eq!(iter.next()?, Some(([Datum::from(4)].as_ref(), 1)));
        assert_eq!(iter.next()?, Some(([Datum::from(3)].as_ref(), 1)));
        assert_eq!(iter.next()?, None);

        // Both populated
        let mut iter = table.range_scan(
            Some(&[Datum::from(3)]),
            Some(&[Datum::from(2)]),
            LogicalTimestamp::MAX,
        );
        assert_eq!(iter.next()?, Some(([Datum::from(3)].as_ref(), 1)));
        assert_eq!(iter.next()?, Some(([Datum::from(2)].as_ref(), 1)));
        assert_eq!(iter.next()?, None);

        Ok(())
    }
}
