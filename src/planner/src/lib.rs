use data::DataType;

mod common;
mod explain;
mod normalize;
mod optimize;
mod point_in_time;
mod validate;
use ast::expr::ColumnReference;
use functions::registry::{FunctionResolutionError, Registry};
pub use point_in_time::PointInTimePlan;
use std::fmt::{Display, Formatter};

#[derive(Debug, Default)]
pub struct Planner {
    pub function_registry: Registry,
}

impl Planner {
    pub fn new(function_registry: Registry) -> Self {
        Planner { function_registry }
    }
}

/// An error from the planning phase
#[derive(Debug)]
pub enum PlannerError {
    FunctionResolutionError(FunctionResolutionError),
    FieldResolutionError(FieldResolutionError),
}

impl From<FunctionResolutionError> for PlannerError {
    fn from(err: FunctionResolutionError) -> Self {
        PlannerError::FunctionResolutionError(err)
    }
}

impl From<FieldResolutionError> for PlannerError {
    fn from(err: FieldResolutionError) -> Self {
        PlannerError::FieldResolutionError(err)
    }
}

impl Display for PlannerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PlannerError::FunctionResolutionError(err) => Display::fmt(err, f),
            PlannerError::FieldResolutionError(err) => Display::fmt(err, f),
        }
    }
}

/// An error during field resolution (aka column references)
#[derive(Debug)]
pub enum FieldResolutionError {
    Ambiguous(ColumnReference, Vec<Field>),
    NotFound(ColumnReference, Vec<Field>),
}

impl Display for FieldResolutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldResolutionError::Ambiguous(col, fields) => {
                let field_list = fields
                    .iter()
                    .map(|f| {
                        ColumnReference {
                            qualifier: f.qualifier.clone(),
                            alias: f.alias.clone(),
                        }
                        .to_string()
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                f.write_fmt(format_args!("Field {} is ambiguous, matched on ({}), maybe you need to fully qualify the field", col, field_list))
            }
            FieldResolutionError::NotFound(col, fields) => {
                let field_list = fields
                    .iter()
                    .map(|f| {
                        ColumnReference {
                            qualifier: f.qualifier.clone(),
                            alias: f.alias.clone(),
                        }
                        .to_string()
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                f.write_fmt(format_args!(
                    "Field {} not found, possible fields are ({})",
                    col, field_list
                ))
            }
        }
    }
}

/// A Field is simply a column name and a type.
/// While this is sort of a property of a logical operator it does require resolving functions
/// etc to calculate, this is part of what the planner does
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Field {
    pub qualifier: Option<String>,
    pub alias: String,
    pub data_type: DataType,
}
