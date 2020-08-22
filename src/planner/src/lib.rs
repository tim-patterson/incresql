mod p1_validation;
mod p2_optimization;
mod p3_pit_planning;
mod utils;

use data::{DataType, Session};

mod error;
mod explain;
use crate::utils::logical::fields_for_operator;
use ast::rel::logical::LogicalOperator;
use catalog::Catalog;
pub use error::*;
use functions::registry::Registry;
pub use p3_pit_planning::PointInTimePlan;
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

    /// Runs the validation and optimization phases
    pub(crate) fn plan_common(
        &self,
        query: LogicalOperator,
        session: &Session,
    ) -> Result<(Vec<Field>, LogicalOperator), PlannerError> {
        let query = self.validate(query, session)?;
        let query = self.optimize(query, session)?;
        let fields = fields_for_operator(&query).collect();
        Ok((fields, query))
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
