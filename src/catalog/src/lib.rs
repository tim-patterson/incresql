mod bootstrap;
use data::json::JsonBuilder;
use data::{DataType, Datum, LogicalTimestamp, SortOrder, TupleIter};
use std::convert::TryFrom;
use storage::{Storage, StorageError, Table};

mod error;
pub use error::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// The catalog is responsible for the lifecycles and naming of all the
/// database objects.
#[derive(Debug)]
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
    // database_id:text(pk), table_name:text(pk), type:text, sql:text, table_id:bigint, columns:json, system:bool
    tables_table: Table,
}

/// Represents an item returned by the catalog
#[derive(Debug, Eq, PartialEq)]
pub struct CatalogItem {
    pub columns: Vec<(String, DataType)>,
    pub item: TableOrView,
}

#[derive(Debug, Eq, PartialEq)]
pub enum TableOrView {
    Table(Table),
    View(String),
}

const PREFIX_METADATA_TABLE_ID: u32 = 0;
const DATABASES_TABLE_ID: u32 = 2;
const TABLES_TABLE_ID: u32 = 4;

impl Catalog {
    /// Creates a catalog, wrapping the passed in storage
    pub fn new(storage: Storage) -> Result<Self, CatalogError> {
        let prefix_metadata_table =
            storage.table(PREFIX_METADATA_TABLE_ID, 3, vec![SortOrder::Asc]);
        let databases_table = storage.table(DATABASES_TABLE_ID, 1, vec![SortOrder::Asc]);
        let tables_table = storage.table(TABLES_TABLE_ID, 7, vec![SortOrder::Asc, SortOrder::Asc]);
        let mut catalog = Catalog {
            storage,
            prefix_metadata_table,
            databases_table,
            tables_table,
        };
        catalog.bootstrap()?;
        Ok(catalog)
    }

    /// Creates a new catalog backed by in-memory storage
    pub fn new_for_test() -> Result<Self, CatalogError> {
        Catalog::new(Storage::new_in_mem()?)
    }

    /// Returns the catalog item with the given name
    pub fn item(&self, database: &str, table: &str) -> Result<CatalogItem, CatalogError> {
        let tables_pk = [Datum::from(database), Datum::from(table)];
        let mut key_buf = vec![];
        let mut value = vec![];

        let freq = self
            .tables_table
            .system_point_lookup(&tables_pk, &mut key_buf, &mut value)?
            .unwrap_or(0);
        if freq == 0 {
            return Err(CatalogError::TableNotFound(
                database.to_string(),
                table.to_string(),
            ));
        }
        let table_type = value[0].as_text();

        let columns: Vec<_> = value[3]
            .as_json()
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

        let item = match table_type {
            "table" => {
                let id = value[2].as_bigint() as u32;

                let prefix_pk = [value[2].clone()];
                self.prefix_metadata_table
                    .system_point_lookup(&prefix_pk, &mut key_buf, &mut value)?
                    .unwrap();

                let pk = value[1]
                    .as_json()
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

                TableOrView::Table(self.storage.table(id, columns.len(), pk))
            }
            "view" => TableOrView::View(value[1].as_text().to_string()),
            tt => panic!("Unknown table type {}", tt),
        };

        Ok(CatalogItem { columns, item })
    }

    /// Called to create a database
    pub fn create_database(&mut self, database_name: &str) -> Result<(), CatalogError> {
        self.check_db_not_exists(database_name)?;
        self.create_database_impl(database_name)
    }

    /// Called to drop a database
    pub fn drop_database(&mut self, database_name: &str) -> Result<(), CatalogError> {
        self.check_db_exists(database_name)?;
        self.check_db_empty(database_name)?;
        // Write with freq -1
        self.databases_table.atomic_write(|batch| {
            batch.write_tuple(
                &self.databases_table,
                &[Datum::from(database_name)],
                LogicalTimestamp::now(),
                -1,
            )
        })?;
        Ok(())
    }

    /// Creates a new table
    pub fn create_table(
        &mut self,
        database_name: &str,
        table_name: &str,
        columns: &[(String, DataType)],
    ) -> Result<(), CatalogError> {
        self.check_db_exists(database_name)?;
        self.check_table_not_exists(database_name, table_name)?;
        let id = self.generate_table_id(table_name)?;
        let pk: Vec<_> = columns.iter().map(|_| SortOrder::Asc).collect();

        self.create_table_impl(database_name, table_name, id, columns, &pk, false)
    }

    /// Creates a new view
    pub fn create_view(
        &mut self,
        database_name: &str,
        table_name: &str,
        columns: &[(String, DataType)],
        view_sql: &str,
    ) -> Result<(), CatalogError> {
        self.check_db_exists(database_name)?;
        self.check_table_not_exists(database_name, table_name)?;
        self.create_view_impl(database_name, table_name, columns, view_sql, false)
    }

    /// Drops a table or a view
    pub fn drop_table(
        &mut self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), CatalogError> {
        self.check_table_exists(database_name, table_name)?;
        self.drop_table_impl(database_name, table_name)
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

    /// Check database empty.
    fn check_db_empty(&mut self, database_name: &str) -> Result<(), CatalogError> {
        let db_datum = [Datum::from(database_name)];
        let mut iter =
            self.tables_table
                .range_scan(Some(&db_datum), Some(&db_datum), LogicalTimestamp::MAX);
        if iter.next()?.is_some() {
            Err(CatalogError::DatabaseNotEmpty(database_name.to_string()))
        } else {
            Ok(())
        }
    }

    /// Check for if a database with that name exists
    fn check_db_exists(&mut self, database_name: &str) -> Result<(), CatalogError> {
        if !self.db_exists(database_name)? {
            Err(CatalogError::DatabaseNotFound(database_name.to_string()))
        } else {
            Ok(())
        }
    }

    fn check_db_not_exists(&mut self, database_name: &str) -> Result<(), CatalogError> {
        if self.db_exists(database_name)? {
            Err(CatalogError::DatabaseAlreadyExists(
                database_name.to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn db_exists(&mut self, database_name: &str) -> Result<bool, CatalogError> {
        let db_datum = [Datum::from(database_name)];
        let mut iter = self.databases_table.range_scan(
            Some(&db_datum),
            Some(&db_datum),
            LogicalTimestamp::MAX,
        );
        Ok(iter.next()?.is_some())
    }

    /// Check for if a database with that name exists
    fn check_table_exists(
        &mut self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), CatalogError> {
        if !self.table_exists(database_name, table_name)? {
            Err(CatalogError::TableNotFound(
                database_name.to_string(),
                table_name.to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn check_table_not_exists(
        &mut self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), CatalogError> {
        if self.table_exists(database_name, table_name)? {
            Err(CatalogError::TableAlreadyExists(
                database_name.to_string(),
                table_name.to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn table_exists(
        &mut self,
        database_name: &str,
        table_name: &str,
    ) -> Result<bool, CatalogError> {
        let table_datum = [Datum::from(database_name), Datum::from(table_name)];
        let mut iter = self.tables_table.range_scan(
            Some(&table_datum),
            Some(&table_datum),
            LogicalTimestamp::MAX,
        );
        Ok(iter.next()?.is_some())
    }

    fn generate_table_id(&mut self, table_name: &str) -> Result<u32, CatalogError> {
        let mut hasher = DefaultHasher::new();
        table_name.hash(&mut hasher);
        let mut id = hasher.finish() as u32;
        // Make sure table_id is even
        if id & 1 == 1 {
            id -= 1;
        }
        loop {
            let proposed = [Datum::from(id as i64)];
            let mut iter = self.prefix_metadata_table.range_scan(
                Some(&proposed),
                Some(&proposed),
                LogicalTimestamp::MAX,
            );
            if iter.next()?.is_none() {
                return Ok(id);
            } else {
                id += 2;
            }
        }
    }

    /// Creates a table but doesn't do any checks around the database, table, or id.
    fn create_table_impl(
        &mut self,
        database_name: &str,
        table_name: &str,
        table_id: u32,
        columns: &[(String, DataType)],
        pks: &[SortOrder],
        system: bool,
    ) -> Result<(), CatalogError> {
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
                Datum::from("table"),
                Datum::Null,
                Datum::from(table_id as i64),
                columns_datum,
                Datum::from(system),
            ];
            batch.write_tuple(&self.tables_table, &tuple, timestamp, 1)?;

            let tuple = [
                Datum::from(table_id as i64),
                Datum::from(columns.len() as i32),
                pks,
            ];
            batch.write_tuple(&self.prefix_metadata_table, &tuple, timestamp, 1)
        })?;
        Ok(())
    }

    /// Creates a view but doesn't do any checks around name clashes etc
    fn create_view_impl(
        &mut self,
        database_name: &str,
        table_name: &str,
        columns: &[(String, DataType)],
        sql: &str,
        system: bool,
    ) -> Result<(), CatalogError> {
        let timestamp = LogicalTimestamp::now();

        let columns_datum = Datum::from(JsonBuilder::default().array(|array| {
            for (alias, datatype) in columns {
                array.push_array(|col_array| {
                    col_array.push_string(alias);
                    col_array.push_string(&format!("{:#}", datatype));
                })
            }
        }));

        self.tables_table.atomic_write(|batch| {
            let tuple = [
                Datum::from(database_name),
                Datum::from(table_name),
                Datum::from("view"),
                Datum::from(sql),
                Datum::Null,
                columns_datum,
                Datum::from(system),
            ];
            batch.write_tuple(&self.tables_table, &tuple, timestamp, 1)
        })?;
        Ok(())
    }

    /// Drops a table or view but doesn't do any of the pre checks
    fn drop_table_impl(
        &mut self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), CatalogError> {
        let now = LogicalTimestamp::now();
        let table_key = [Datum::from(database_name), Datum::from(table_name)];
        let mut tables_iter =
            self.tables_table
                .range_scan(Some(&table_key), Some(&table_key), LogicalTimestamp::MAX);

        let (table_tuple, table_freq) = tables_iter.next()?.unwrap();
        self.tables_table.atomic_write::<_, StorageError>(|batch| {
            match table_tuple[2].as_text() {
                "table" => {
                    // first drop the data, then the meta data
                    // TODO we should be able to genericise write batch and write batch WI so we can choose
                    // to opt into/outof read after write vs higher perf(and delete range support!)
                    let prefix_key = &table_tuple[4..5];
                    let mut prefix_iter = self.prefix_metadata_table.range_scan(
                        Some(&prefix_key),
                        Some(&prefix_key),
                        LogicalTimestamp::MAX,
                    );

                    let table_id = prefix_key[0].as_bigint() as u32;

                    let (prefix_tuple, prefix_freq) = prefix_iter.next()?.unwrap();

                    self.tables_table
                        .atomic_write_without_index::<_, StorageError>(|write_batch| {
                            write_batch
                                .delete_range(table_id.to_be_bytes(), (table_id + 2).to_be_bytes());
                            Ok(())
                        })?;
                    batch.write_tuple(
                        &self.prefix_metadata_table,
                        prefix_tuple,
                        now,
                        -prefix_freq,
                    )?;
                }
                "view" => {}
                tt => panic!("Unknown table type {}", tt),
            }

            batch.write_tuple(&self.tables_table, table_tuple, now, -table_freq)?;

            Ok(())
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_table() -> Result<(), CatalogError> {
        let catalog = Catalog::new_for_test()?;
        let item = catalog.item("incresql", "databases")?;
        if let TableOrView::Table(table) = item.item {
            let mut iter = table.full_scan(LogicalTimestamp::MAX);
            assert_eq!(iter.next()?, Some(([Datum::from("default")].as_ref(), 1)));
        } else {
            panic!()
        }
        Ok(())
    }

    #[test]
    fn test_create_database() -> Result<(), CatalogError> {
        let mut catalog = Catalog::new_for_test()?;
        let dbs_table = catalog.item("incresql", "databases")?;

        catalog.create_database("abc")?;

        if let TableOrView::Table(table) = &dbs_table.item {
            let mut iter = table.full_scan(LogicalTimestamp::MAX);
            assert_eq!(iter.next()?, Some(([Datum::from("abc")].as_ref(), 1)));
        } else {
            panic!()
        }

        assert_eq!(
            catalog.create_database("abc"),
            Err(CatalogError::DatabaseAlreadyExists("abc".to_string()))
        );

        catalog.drop_database("abc")?;
        if let TableOrView::Table(table) = &dbs_table.item {
            let mut iter = table.full_scan(LogicalTimestamp::MAX);
            assert_eq!(iter.next()?, Some(([Datum::from("default")].as_ref(), 1)));
        } else {
            panic!()
        }

        Ok(())
    }

    #[test]
    fn test_create_table() -> Result<(), CatalogError> {
        let mut catalog = Catalog::new_for_test()?;
        let columns = vec![("a".to_string(), DataType::Integer)];

        catalog.create_table("default", "test", &columns)?;

        let item = catalog.item("default", "test")?;
        assert_eq!(item.columns, columns.as_slice());

        catalog.drop_table("default", "test")?;
        assert!(catalog.item("default", "test").is_err());
        Ok(())
    }

    #[test]
    fn test_create_view() -> Result<(), CatalogError> {
        let mut catalog = Catalog::new_for_test()?;
        let columns = vec![("a".to_string(), DataType::Integer)];

        catalog.create_view("default", "test", &columns, "hello world")?;

        let item = catalog.item("default", "test")?;
        assert_eq!(item.columns, columns.as_slice());
        assert_eq!(item.item, TableOrView::View("hello world".to_string()));

        catalog.drop_table("default", "test")?;
        assert!(catalog.item("default", "test").is_err());
        Ok(())
    }
}
