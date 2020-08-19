use rocksdb::Error;
use std::fmt::{Display, Formatter};

/// An error from the storage layer
#[derive(Debug, Eq, PartialEq)]
pub enum StorageError {
    RocksDbError(String),
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::RocksDbError(err) => f.write_str(err),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<rocksdb::Error> for StorageError {
    fn from(err: Error) -> Self {
        StorageError::RocksDbError(err.to_string())
    }
}
