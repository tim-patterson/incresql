use rocksdb::Error;
use std::fmt::{Display, Formatter};

/// An error from the storage layer
#[derive(Debug)]
pub enum StorageError {
    RocksDbError(rocksdb::Error),
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::RocksDbError(err) => Display::fmt(err, f),
        }
    }
}

impl From<rocksdb::Error> for StorageError {
    fn from(err: Error) -> Self {
        StorageError::RocksDbError(err)
    }
}
