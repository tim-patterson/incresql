use std::error::Error;
use std::fmt::{Display, Formatter};
use storage::StorageError;

mod expression;
pub mod point_in_time;
mod utils;

#[derive(Debug)]
pub enum ExecutionError {
    StorageError(StorageError),
}

impl Error for ExecutionError {}

impl Display for ExecutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionError::StorageError(err) => Display::fmt(err, f),
        }
    }
}

impl From<StorageError> for ExecutionError {
    fn from(err: StorageError) -> Self {
        ExecutionError::StorageError(err)
    }
}
