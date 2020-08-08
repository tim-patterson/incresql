use data::DataType;

mod common;
mod normalize;
mod point_in_time;
mod validate;
use functions::registry::{FunctionResolutionError, Registry};
pub use point_in_time::PointInTimePlan;
use std::fmt::{Display, Formatter};

#[derive(Debug, Default)]
pub struct Planner {
    function_registry: Registry,
}

impl Planner {
    pub fn new(function_registry: Registry) -> Self {
        Planner { function_registry }
    }
}

#[derive(Debug)]
pub enum PlannerError {
    FunctionResolutionError(FunctionResolutionError),
}

impl From<FunctionResolutionError> for PlannerError {
    fn from(err: FunctionResolutionError) -> Self {
        PlannerError::FunctionResolutionError(err)
    }
}

impl Display for PlannerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PlannerError::FunctionResolutionError(err) => Display::fmt(err, f),
        }
    }
}

/// A Field is simply a column name and a type.
/// While this is sort of a property of a logical operator it does require resolving functions
/// etc to calculate, this is part of what the planner does
#[derive(Debug, Eq, PartialEq)]
pub struct Field {
    pub alias: String,
    pub data_type: DataType,
}
