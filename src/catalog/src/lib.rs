mod bootstrap;
use data::json::JsonBuilder;
use data::{DataType, Datum, LogicalTimestamp, SortOrder};
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use storage::{Storage, StorageError, Table};

#[derive(Debug)]
pub enum CatalogError {
    StorageError(StorageError),
    TableNotFound(String, String),
}

impl Display for CatalogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::StorageError(err) => Display::fmt(err, f),
            CatalogError::TableNotFound(db, table) => {
                f.write_fmt(format_args!("Table {}.{} not found", db, table))
            }
        }
    }
}

impl From<StorageError> for CatalogError {
    fn from(err: StorageError) -> Self {
        CatalogError::StorageError(err)
    }
}

/// The catalog is responsible for the lifecycles and naming of all the
/// database objects.
pub struct Catalog {
    storage: Storage,
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
    pub fn new(storage: Storage) -> Result<Self, CatalogError> {
        let prefix_metadata_table = storage.table(
            PREFIX_METADATA_TABLE_ID,
            vec![
                ("table_id".to_string(), DataType::BigInt),
                ("column_len".to_string(), DataType::Integer),
                ("pks_sorts".to_string(), DataType::Json),
            ],
            vec![SortOrder::Asc],
        );
        let databases_table = storage.table(
            DATABASES_TABLE_ID,
            vec![("name".to_string(), DataType::Text)],
            vec![SortOrder::Asc],
        );
        let tables_table = storage.table(
            TABLES_TABLE_ID,
            vec![
                ("database_name".to_string(), DataType::Text),
                ("name".to_string(), DataType::Text),
                ("table_id".to_string(), DataType::BigInt),
                ("columns".to_string(), DataType::Json),
            ],
            vec![SortOrder::Asc, SortOrder::Asc],
        );
        let mut catalog = Catalog {
            storage,
            prefix_metadata_table,
            databases_table,
            tables_table,
        };
        catalog.bootstrap()?;
        Ok(catalog)
    }

    pub fn table(&self, database: &str, table: &str) -> Result<Table, CatalogError> {
        let tables_pk = [Datum::from(database), Datum::from(table)];
        let mut key_buf = vec![];
        let mut value = vec![];

        self.tables_table
            .system_point_lookup(&tables_pk, &mut key_buf, &mut value)?
            .ok_or_else(|| CatalogError::TableNotFound(database.to_string(), table.to_string()))?;

        let id = value[0].as_bigint().unwrap() as u32;
        let columns: Vec<_> = value[1]
            .as_json()
            .unwrap()
            .iter_array()
            .unwrap()
            .map(|col| {
                let mut iter = col.iter_array().unwrap();
                let col_name = iter.next().unwrap().get_string().unwrap();
                let col_type =
                    DataType::try_from(iter.next().unwrap().get_string().unwrap()).unwrap();
                (col_name.to_string(), col_type)
            })
            .collect();

        let prefix_pk = [value[0].clone()];
        self.prefix_metadata_table
            .system_point_lookup(&prefix_pk, &mut key_buf, &mut value)?
            .unwrap();

        let pk = value[1]
            .as_json()
            .unwrap()
            .iter_array()
            .unwrap()
            .map(|b| {
                if b.get_boolean().unwrap() {
                    SortOrder::Desc
                } else {
                    SortOrder::Asc
                }
            })
            .collect();

        Ok(self.storage.table(id, columns, pk))
    }

    /// Creates a database, doesn't do any checks to see if the database already exists etc.
    fn create_database_impl(&mut self, database_name: &str) -> Result<(), CatalogError> {
        self.databases_table.atomic_write(|batch| {
            batch.write_tuple(
                &self.databases_table,
                &[Datum::from(database_name)],
                LogicalTimestamp::now(),
                1,
            )
        })?;
        Ok(())
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
                    col_array.push_string(&format!("{:#}", datatype));
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
    use data::TupleIter;

    #[test]
    fn test_get_table() -> Result<(), CatalogError> {
        let storage = Storage::new_in_mem()?;
        let catalog = Catalog::new(storage)?;
        let table = catalog.table("incresql", "databases")?;

        assert_eq!(table.columns(), catalog.databases_table.columns());

        let mut iter = table.full_scan(LogicalTimestamp::MAX);
        assert_eq!(iter.next()?, Some(([Datum::from("default")].as_ref(), 1)));
        Ok(())
    }
}
