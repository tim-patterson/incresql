mod bootstrap;
use data::json::JsonBuilder;
use data::{DataType, Datum, LogicalTimestamp, SortOrder, TupleIter};
use storage::{Storage, StorageError, Table};

/// The catalog is responsible for the lifecycles and naming of all the
/// database objects.
pub struct Catalog {
    // The lowest level of metadata stored by the catalog.
    // a table of
    // table_id:bigint(pk), column_len:int, pks_sorts:[bool]:json
    prefix_metadata_table: Table,
    // Table listing databases
    // name:text(pk)
    databases_table: Table,
    // Table listing tables
    // database_id:text(pk), table_name:text(pk), table_id:bigint, columns:json
    tables_table: Table,
}

const PREFIX_METADATA_TABLE_ID: u32 = 0;
const DATABASES_TABLE_ID: u32 = 2;
const TABLES_TABLE_ID: u32 = 4;

impl Catalog {
    /// Creates a catalog, wrapping the passed in storage
    pub fn new(storage: Storage) -> Result<Self, StorageError> {
        let prefix_metadata_table =
            storage.table(PREFIX_METADATA_TABLE_ID, vec![SortOrder::Asc], 3);
        let databases_table = storage.table(DATABASES_TABLE_ID, vec![SortOrder::Asc], 1);
        let tables_table = storage.table(TABLES_TABLE_ID, vec![SortOrder::Asc, SortOrder::Asc], 4);
        let mut catalog = Catalog {
            prefix_metadata_table,
            databases_table,
            tables_table,
        };
        catalog.bootstrap()?;
        Ok(catalog)
    }

    pub fn list_databases(&self) -> Result<Vec<String>, StorageError> {
        let mut iter = self.databases_table.full_scan(LogicalTimestamp::MAX);
        let mut results = vec![];
        while let Some((tuple, _freq)) = iter.next()? {
            results.push(tuple[0].as_text().unwrap().to_string());
        }
        Ok(results)
    }

    /// Creates a database, doesn't do any checks to see if the database already exists etc.
    fn create_database_impl(&mut self, database_name: &str) -> Result<(), StorageError> {
        self.databases_table.atomic_write(|batch| {
            batch.write_tuple(
                &self.databases_table,
                &[Datum::from(database_name)],
                LogicalTimestamp::now(),
                1,
            )
        })
    }

    /// Creates a table but doesn't do any checks around the database, table, or id.
    fn create_table_impl(
        &mut self,
        database_name: &str,
        table_name: &str,
        table_id: u32,
        columns: &[(&str, DataType)],
        pks: &[SortOrder],
    ) -> Result<(), StorageError> {
        let timestamp = LogicalTimestamp::now();

        let columns_datum = Datum::from(JsonBuilder::default().array(|array| {
            for (alias, datatype) in columns {
                array.push_array(|col_array| {
                    col_array.push_string(alias);
                    col_array.push_string(&datatype.to_string());
                })
            }
        }));

        let pks = Datum::from(JsonBuilder::default().array(|array| {
            for pk in pks {
                array.push_bool(pk.is_desc());
            }
        }));

        self.tables_table.atomic_write(|batch| {
            let tuple = [
                Datum::from(database_name),
                Datum::from(table_name),
                Datum::from(table_id as i64),
                columns_datum,
            ];
            batch.write_tuple(&self.tables_table, &tuple, timestamp, 1)?;

            let tuple = [
                Datum::from(table_id as i64),
                Datum::from(columns.len() as i32),
                pks,
            ];
            batch.write_tuple(&self.prefix_metadata_table, &tuple, timestamp, 1)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap() -> Result<(), StorageError> {
        let storage = Storage::new_in_mem()?;
        let catalog = Catalog::new(storage)?;
        assert_eq!(
            catalog.list_databases().unwrap(),
            vec!["default".to_string(), "incresql".to_string()]
        );
        Ok(())
    }
}
