use data::json::JsonBuilder;
use data::{DataType, Datum, LogicalTimestamp, SortOrder};
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
    // database_name:text(pk), table_name:text(pk), table_id:bigint, columns:json
    tables_table: Table,
}

const PREFIX_METADATA_TABLE_ID: u32 = 0;
const DATABASES_TABLE_ID: u32 = 2;
const TABLES_TABLE_ID: u32 = 4;

impl Catalog {
    /// Creates a catalog from the passed in storage
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

    /// Function used on first boot to initialize system tables
    fn bootstrap(&mut self) -> Result<(), StorageError> {
        let mut key_buf = vec![];
        let mut value_buf = vec![];
        // Initialization check
        if self
            .prefix_metadata_table
            .system_point_lookup(
                &[Datum::from(PREFIX_METADATA_TABLE_ID as i64)],
                &mut key_buf,
                &mut value_buf,
            )?
            .is_some()
        {
            return Ok(());
        }

        self.create_database("incresql")?;
        self.create_table(
            "incresql",
            "prefix_tables",
            PREFIX_METADATA_TABLE_ID,
            &[
                ("table_id", DataType::BigInt),
                ("column_len", DataType::Integer),
                ("pk_sort", DataType::Json),
            ],
            &[SortOrder::Asc],
        )?;
        self.create_table(
            "incresql",
            "databases",
            DATABASES_TABLE_ID,
            &[("name", DataType::Text)],
            &[SortOrder::Asc],
        )?;
        self.create_table(
            "incresql",
            "tables",
            TABLES_TABLE_ID,
            &[
                ("database_name", DataType::Text),
                ("name", DataType::Text),
                ("table_id", DataType::BigInt),
                ("columns", DataType::Json),
            ],
            &[SortOrder::Asc, SortOrder::Asc],
        )
    }

    fn create_database(&self, database_name: &str) -> Result<(), StorageError> {
        self.databases_table.atomic_write(|batch| {
            batch.write_tuple(
                &self.databases_table,
                &[Datum::from(database_name)],
                LogicalTimestamp::now(),
                1,
            )
        })
    }

    fn create_table(
        &self,
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
        let _catalog = Catalog::new(storage)?;
        Ok(())
    }
}
