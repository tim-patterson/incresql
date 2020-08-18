use crate::ExecutionError;
use data::{Datum, LogicalTimestamp, TupleIter};
use storage::{StorageError, Table};

pub struct TableScanExecutor {
    // We must drop scan_iter first
    scan_iter: Box<dyn TupleIter<StorageError>>,
    #[allow(dead_code)]
    table: Table,
}

impl TableScanExecutor {
    pub fn new(table: Table, timestamp: LogicalTimestamp) -> Self {
        // The lifetime of an rocksdb iter is tied to the underlying rocksdb.
        // In our case table holds an Arc<db> so if we keep that alive we're ok.
        // so below we fudge the lifetimes to make it work
        let scan_iter = Box::from(table.full_scan(timestamp));
        let scan_iter = unsafe {
            std::mem::transmute::<Box<dyn TupleIter<StorageError>>, Box<dyn TupleIter<StorageError>>>(
                scan_iter,
            )
        };

        TableScanExecutor { scan_iter, table }
    }
}

impl TupleIter<ExecutionError> for TableScanExecutor {
    fn advance(&mut self) -> Result<(), ExecutionError> {
        self.scan_iter.advance()?;
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        self.scan_iter.get()
    }

    fn column_count(&self) -> usize {
        self.scan_iter.column_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::Catalog;
    use storage::Storage;

    #[test]
    fn test_table_scan_executor() -> Result<(), ExecutionError> {
        let storage = Storage::new_in_mem()?;
        let catalog = Catalog::new(storage).unwrap();
        let table = catalog.table("incresql", "databases").unwrap();

        let mut executor = TableScanExecutor::new(table, LogicalTimestamp::MAX);
        assert_eq!(
            executor.next()?,
            Some(([Datum::from("default")].as_ref(), 1))
        );
        assert_eq!(
            executor.next()?,
            Some(([Datum::from("incresql")].as_ref(), 1))
        );
        assert_eq!(executor.next()?, None);
        Ok(())
    }
}
