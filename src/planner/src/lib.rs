mod utils;

use data::DataType;

mod common;
mod explain;
mod normalize;
mod optimize;
mod point_in_time;
mod validate;
use ast::expr::{ColumnReference, Expression};
use catalog::{Catalog, CatalogError};
use functions::registry::{FunctionResolutionError, Registry};
pub use point_in_time::PointInTimePlan;
use std::fmt::{Display, Formatter};
use std::sync::RwLock;

#[derive(Debug)]
pub struct Planner {
    pub function_registry: Registry,
    pub catalog: RwLock<Catalog>,
}

impl Planner {
    pub fn new(function_registry: Registry, catalog: Catalog) -> Self {
        Planner {
            function_registry,
            catalog: RwLock::new(catalog),
        }
    }

    /// Creates a new planner wrapping the default register and a new
    /// catalog backed by in-memory storage
    pub fn new_for_test() -> Self {
        Planner::new(Registry::default(), Catalog::new_for_test().unwrap())
    }
}

/// An error from the planning phase
#[derive(Debug)]
pub enum PlannerError {
    FunctionResolutionError(FunctionResolutionError),
    FieldResolutionError(FieldResolutionError),
    CatalogError(CatalogError),
    PredicateNotBoolean(DataType, Expression),
    UnionAllMismatch(Vec<DataType>, Vec<DataType>, usize),
    InsertMismatch(Vec<DataType>, Vec<DataType>),
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

impl From<CatalogError> for PlannerError {
    fn from(err: CatalogError) -> Self {
        PlannerError::CatalogError(err)
    }
}

impl Display for PlannerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PlannerError::FunctionResolutionError(err) => Display::fmt(err, f),
            PlannerError::FieldResolutionError(err) => Display::fmt(err, f),
            PlannerError::CatalogError(err) => Display::fmt(err, f),
            PlannerError::PredicateNotBoolean(datatype, expr) => f.write_fmt(format_args!(
                "Predicate returns {} not BOOLEAN - {}",
                datatype, expr
            )),
            PlannerError::UnionAllMismatch(first, other, other_idx) => {
                if first.len() != other.len() {
                    f.write_fmt(format_args!("Union all mismatch, first sub expression has {} rows while the subexpr {} has {} rows", first.len(), *other_idx + 1, other.len()) )
                } else {
                    let first_str = first
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    let other_str = other
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    f.write_fmt(format_args!(
                        "Union all types mismatch\nfirst datatypes: {}\nsubexpr {} datatypes: {}",
                        first_str,
                        other_idx + 1,
                        other_str
                    ))
                }
            }
            PlannerError::InsertMismatch(table, source) => {
                let table_str = table
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let source_str = source
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                f.write_fmt(format_args!(
                    "Insert mismatch, table expects row of:\n  {}\nsource is:\n  {}",
                    table_str, source_str
                ))
            }
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
                            star: false,
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
                            star: false,
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
