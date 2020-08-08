use crate::{Planner, PlannerError};
use ast::rel::logical::LogicalOperator;

/// Validate the query, as part of the process of validating the query we will actually end up
/// doing all the catalog and function lookups and subbing them in.
impl Planner {
    pub fn validate(&self, query: LogicalOperator) -> Result<LogicalOperator, PlannerError> {
        Ok(query)
    }
}
