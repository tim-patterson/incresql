use crate::StorageError;
use data::encoding_core::SortableEncoding;
use data::{Datum, LogicalTimestamp, SortOrder, TupleIter};
use rocksdb::prelude::*;
use rocksdb::{DBRawIterator, WriteBatchWithIndex};
use std::convert::TryInto;
use std::sync::Arc;

/// A Table is at this level is a collection of rows, identified by an id.
/// We'll expose all of these tables by id in some special schema but in general not all of these
/// are "tables" from the users perspective, some may be indexes.
/// There's no "real" typing or naming of columns at this level either but we do need to know the
/// column count and sort orders for the primary key columns(which must come first in the tuples)
pub struct Table {
    db: Arc<DB>,
    id: u32,
    pk: Vec<SortOrder>,
    column_count: usize,
}

impl Table {
    /// Creates a new table. The pk represents the number of columns in the pk and their sort
    /// orders
    pub(crate) fn new(db: Arc<DB>, id: u32, pk: Vec<SortOrder>, column_count: usize) -> Self {
        Table {
            db,
            id,
            pk,
            column_count,
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
            Some(self.id.to_le_bytes()),
            Some((self.id + 2).to_le_bytes()),
        );
    }

    /// Performs an atomic write, This semantically is done at the storage level so writes to any
    /// tables can appear in here
    pub fn atomic_write<F>(&self, batch: F) -> Result<(), StorageError>
    where
        F: FnOnce(&mut Writer) -> Result<(), StorageError>,
    {
        let mut writer = Writer::new();
        batch(&mut writer)?;
        self.db.write(writer.write_batch)?;
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
    ) -> Result<Option<()>, StorageError> {
        write_index_header_key(self, pk, key_buf);

        if let Some(value_slice) = self.db.get_pinned(key_buf)? {
            rest_tuple.clear();

            let mut tuple_rest_len = 0_u64;
            // We skip 8 bytes over the timestamp..
            let mut value_buf =
                tuple_rest_len.read_sortable_bytes(SortOrder::Asc, &value_slice[8..]);
            rest_tuple.extend((0..tuple_rest_len).map(|_| Datum::default()));
            for datum in rest_tuple {
                value_buf = datum.from_sortable_bytes(value_buf);
            }
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    /// Full scan of the table, all returned record timestamps are guaranteed to be *less*
    /// than the passed in timestamp
    pub fn full_scan(&self, timestamp: LogicalTimestamp) -> impl TupleIter<StorageError> + '_ {
        let mut iter_options = ReadOptions::default();
        iter_options.set_prefix_same_as_start(true);
        iter_options.set_iterate_upper_bound((self.id + 1).to_be_bytes());
        let mut iter = self.db.raw_iterator_opt(iter_options);
        // Seek to start.
        iter.seek(&self.id.to_be_bytes());

        IndexIter::new(iter, timestamp, self.column_count)
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

impl TupleIter<StorageError> for IndexIter<'_> {
    fn advance(&mut self) -> Result<(), StorageError> {
        loop {
            if self.first {
                self.first = false;
            } else {
                self.iter.next();
            }

            if self.iter.valid() {
                // key = <prefix as u32 be>:<tuple-pk as sorted>:<ff>
                // value = <tuple-rest as sorted><timestamp as u64 le><freq as i64 varint>

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
                if key_buf[0] == 0xff {
                    // "Header" record
                    tuple_timestamp.ms =
                        u64::from_le_bytes(value_buf[..8].as_ref().try_into().unwrap());
                    value_buf = &value_buf[8..];
                } else {
                    tuple_timestamp.ms =
                        u64::from_le_bytes(key_buf[..8].as_ref().try_into().unwrap());
                };

                // Check to make sure the tuple isn't in the future, if so loop to the next record
                if tuple_timestamp >= self.timestamp {
                    continue;
                }

                // Populate the non-pk part of the tuple
                let mut datum_count = 0_u64;
                value_buf = datum_count.read_sortable_bytes(SortOrder::Asc, value_buf);
                for idx in 0..datum_count {
                    value_buf = self.tuple_buffer[(tuple_pk_len + idx) as usize]
                        .from_sortable_bytes(value_buf);
                }
                // And freq
                let mut freq = 0_i64;
                freq.read_sortable_bytes(SortOrder::Asc, value_buf);
                self.freq = Some(freq);

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

        // Index header:
        // value = <timestamp as u64 le><tuple-rest as sorted><freq as i64 varint>
        let value_buf = &mut self.value_buf;
        value_buf.clear();

        ////////// VALUE
        // Actual Timestamp
        value_buf.extend_from_slice(&timestamp.ms.to_le_bytes());

        // Tuple-rest
        let rest = &tuple[(table.pk.len())..];
        (rest.len() as u64).write_sortable_bytes(SortOrder::Asc, value_buf);
        for datum in rest {
            datum.as_sortable_bytes(SortOrder::Asc, value_buf);
        }

        // Freq
        freq.write_sortable_bytes(SortOrder::Asc, value_buf);

        self.write_batch.put(&self.key_buf, value_buf);
    }
}

fn write_index_header_key(table: &Table, tuple: &[Datum], key_buf: &mut Vec<u8>) {
    // Index header:
    // key = <prefix as u32 be>:<tuple-pk as sorted>:<ff(timestamp)>
    key_buf.clear();
    // Prefix
    key_buf.extend_from_slice(&table.id.to_be_bytes());

    // Tuple-PK
    (table.pk.len() as u64).write_sortable_bytes(SortOrder::Asc, key_buf);
    for (sort_order, datum) in table.pk.iter().zip(tuple) {
        datum.as_sortable_bytes(*sort_order, key_buf);
    }
    // 0 Timestamp
    key_buf.push(0xFF);
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
        let table = storage.table(1234, vec![SortOrder::Asc], 1);
        table.force_rocks_compaction();
        Ok(())
    }

    #[test]
    fn test_system_write_tuple() -> Result<(), StorageError> {
        let storage = Storage::new_in_mem()?;
        let table = storage.table(1234, vec![SortOrder::Asc], 3);
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

        table.atomic_write(|writer| {
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
        table.atomic_write(|writer| {
            writer.system_delete_tuple(&table, &[Datum::from(123)]);
            Ok(())
        })?;

        let mut iter = table.full_scan(LogicalTimestamp::new(1));
        assert_eq!(iter.next()?, Some((tuple2.as_ref(), 3)));
        assert_eq!(iter.next()?, None);

        // Try a point lookup
        let mut buf = vec![];
        let mut target_buf = vec![];
        let res = table.system_point_lookup(&[Datum::from(456)], &mut buf, &mut target_buf)?;

        assert_eq!(res, Some(()));
        assert_eq!(
            target_buf,
            vec![Datum::Null, Datum::from("efg".to_string())]
        );
        Ok(())
    }

    #[test]
    fn test_right_size_new_to() {
        let to: Vec<bool> = right_size_new_to(5);

        assert_eq!(to, vec![false, false, false, false, false])
    }
}
