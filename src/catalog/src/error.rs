use std::fmt::{Display, Formatter};
use storage::StorageError;

#[derive(Debug, Eq, PartialEq)]
pub enum CatalogError {
    StorageError(StorageError),
    TableAlreadyExists(String, String),
    TableNotFound(String, String),
    DatabaseAlreadyExists(String),
    DatabaseNotFound(String),
    DatabaseNotEmpty(String),
}

impl Display for CatalogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::StorageError(err) => Display::fmt(err, f),
            CatalogError::TableAlreadyExists(db, table) => {
                f.write_fmt(format_args!("Table {}.{} already exists", db, table))
            }
            CatalogError::TableNotFound(db, table) => {
                f.write_fmt(format_args!("Table {}.{} not found", db, table))
            }
            CatalogError::DatabaseAlreadyExists(db) => {
                f.write_fmt(format_args!("Database {} already exists", db))
            }
            CatalogError::DatabaseNotFound(db) => {
                f.write_fmt(format_args!("Database {} not found", db))
            }
            CatalogError::DatabaseNotEmpty(db) => f.write_fmt(format_args!(
                "Database {} is not empty, please remote all contained tables first",
                db
            )),
        }
    }
}

impl std::error::Error for CatalogError {}

impl From<StorageError> for CatalogError {
    fn from(err: StorageError) -> Self {
        CatalogError::StorageError(err)
    }
}
