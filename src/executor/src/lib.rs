use std::error::Error;
use std::fmt::{Display, Formatter};

mod expression;
pub mod point_in_time;
mod utils;

#[derive(Debug)]
pub struct ExecutionError {}

impl Display for ExecutionError {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl Error for ExecutionError {}
