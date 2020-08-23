use std::error::Error;
use std::fmt::{Display, Formatter};
use storage::StorageError;

mod aggregate_expression;
pub mod point_in_time;
mod scalar_expression;
mod utils;

#[derive(Debug)]
pub enum ExecutionError {
    StorageError(StorageError),
    IOError(String),
}

impl Error for ExecutionError {}

impl Display for ExecutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionError::StorageError(err) => Display::fmt(err, f),
            ExecutionError::IOError(err) => f.write_str(err),
        }
    }
}

impl From<StorageError> for ExecutionError {
    fn from(err: StorageError) -> Self {
        ExecutionError::StorageError(err)
    }
}

impl From<std::io::Error> for ExecutionError {
    fn from(err: std::io::Error) -> Self {
        ExecutionError::IOError(err.to_string())
    }
}
