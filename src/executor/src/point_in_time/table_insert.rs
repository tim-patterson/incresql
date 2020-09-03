use crate::point_in_time::BoxedExecutor;
use crate::ExecutionError;
use data::{Datum, LogicalTimestamp, PeekableIter, TupleIter};
use storage::Table;

/// When advance is called this simply inserts all tuples
/// into the table
pub struct TableInsertExecutor {
    source: PeekableIter<dyn TupleIter<E = ExecutionError>>,
    table: Table,
}

impl TableInsertExecutor {
    pub fn new(source: BoxedExecutor, table: Table) -> Self {
        TableInsertExecutor {
            source: PeekableIter::from(source),
            table,
        }
    }
}

impl TupleIter for TableInsertExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), ExecutionError> {
        let iter = &mut self.source;
        let table = &self.table;

        while iter.peek()?.is_some() {
            table.atomic_write::<_, ExecutionError>(|batch| {
                // Chunk our write batches as we don't want to blow out our memory.
                // We'll lose atomicity but tables are only really meant for lookup
                // data etc not for etl type workloads
                let mut c = 10000;
                while let Some((tuple, freq)) = iter.next()? {
                    batch.write_tuple(table, tuple, LogicalTimestamp::now(), freq)?;
                    c -= 1;
                    if c == 0 {
                        break;
                    }
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        None
    }

    fn column_count(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point_in_time::values::ValuesExecutor;
    use crate::ExecutionError;
    use catalog::Catalog;
    use data::DataType;

    #[test]
    fn test_insert_executor() -> Result<(), ExecutionError> {
        let mut catalog = Catalog::new_for_test().unwrap();

        catalog
            .create_table("default", "test", &[("a".to_string(), DataType::Integer)])
            .unwrap();
        let table = catalog.item("default", "test").unwrap().table;

        let values = vec![
            vec![Datum::from(1)],
            vec![Datum::from(2)],
            vec![Datum::from(3)],
        ];
        let source = Box::from(ValuesExecutor::new(Box::from(values.into_iter()), 2));

        let mut executor = TableInsertExecutor::new(source, table);
        assert_eq!(executor.next()?, None);

        let table = catalog.item("default", "test").unwrap().table;
        let mut table_iter = table.full_scan(LogicalTimestamp::MAX);

        assert_eq!(table_iter.next()?, Some(([Datum::from(1)].as_ref(), 1)));
        assert_eq!(table_iter.next()?, Some(([Datum::from(2)].as_ref(), 1)));

        Ok(())
    }
}
