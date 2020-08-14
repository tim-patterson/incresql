use crate::StorageError;
use data::encoding_core::SortableEncoding;
use data::{Datum, LogicalTimestamp, SortOrder};
use rocksdb::{ReadOptions, WriteBatch, DB};
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

    /// Full scan of the table
    pub fn full_scan(&self, _timestamp: LogicalTimestamp) {
        // TODO snapshot
        let mut iter_options = ReadOptions::default();
        iter_options.set_prefix_same_as_start(true);
        iter_options.set_iterate_upper_bound((self.id + 1).to_be_bytes());
        let mut iter = self.db.raw_iterator_opt(iter_options);
        // Seek to start.
        iter.seek(&self.id.to_be_bytes());
        unimplemented!()
    }
}

/// Abstraction through which all writes happens, allows some degree of
/// read after write functionality which is not offered by rocksdb.
pub struct Writer {
    write_batch: WriteBatch,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Writer {
            write_batch: WriteBatch::default(),
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
        freq: i64,
    ) {
        self.write_index_header(table, tuple, timestamp, freq);
    }

    fn write_index_header(
        &mut self,
        table: &Table,
        tuple: &[Datum],
        timestamp: LogicalTimestamp,
        freq: i64,
    ) {
        // Index header value:
        // key = <prefix as u32 be>:<tuple-pk as sorted>:<0 as u64 be desc>
        // value = <tuple-rest as sorted><timestamp as u64 le><freq as i64 varint>
        let key_buf = &mut self.key_buf;
        let value_buf = &mut self.value_buf;

        key_buf.clear();
        value_buf.clear();
        ////////// KEY
        // Prefix
        key_buf.extend_from_slice(&table.id.to_be_bytes());

        // Tuple-PK
        (table.pk.len() as u64).write_sortable_bytes(SortOrder::Asc, key_buf);
        for (sort_order, datum) in table.pk.iter().zip(tuple) {
            datum.as_sortable_bytes(*sort_order, key_buf);
        }

        // 0 Timestamp
        key_buf.extend_from_slice(&u64::MAX.to_be_bytes());

        ////////// VALUE
        // Tuple-rest
        let rest = &tuple[(table.pk.len())..];
        (rest.len() as u64).write_sortable_bytes(SortOrder::Asc, value_buf);
        for datum in rest {
            datum.as_sortable_bytes(SortOrder::Asc, key_buf);
        }

        // Actual Timestamp
        value_buf.extend_from_slice(&timestamp.ms().to_le_bytes());
        // Freq
        freq.write_sortable_bytes(SortOrder::Asc, value_buf);

        self.write_batch.put(key_buf, value_buf);
    }
}

#[cfg(test)]
mod tests {
    use crate::{Storage, StorageError};
    use data::{Datum, LogicalTimestamp, SortOrder};

    /// Hard to functionally test this, so this is more just a smoke test that anything else!
    #[test]
    fn test_force_rocks_compaction() -> Result<(), StorageError> {
        let path = "../../target/unittest_dbs/storage/table/force_rocks_compaction";
        std::fs::remove_dir_all(path).ok();
        {
            let storage = Storage::new_with_path(path)?;
            let table = storage.table(1234, vec![SortOrder::Asc], 1);
            table.force_rocks_compaction();
        }
        std::fs::remove_dir_all(path).ok();
        Ok(())
    }

    #[test]
    /// TODO just a smoke test until we get a read path working!
    fn test_write_tuple() -> Result<(), StorageError> {
        let path = "../../target/unittest_dbs/storage/table/write_tuple";
        std::fs::remove_dir_all(path).ok();
        {
            let storage = Storage::new_with_path(path)?;
            let table = storage.table(1234, vec![SortOrder::Asc], 3);
            let tuple = vec![Datum::from(123), Datum::Null, Datum::from("abc")];
            let timestamp = LogicalTimestamp::new(89732893);
            let freq = 1;

            table.atomic_write(|writer| {
                writer.write_tuple(&table, &tuple, timestamp, freq);

                Ok(())
            })?;
        }
        std::fs::remove_dir_all(path).ok();
        Ok(())
    }
}
