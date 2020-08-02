use crate::PlannerError;
use ast::rel::logical::LogicalOperator;

/// Validate the query, as part of the process of validating the query we will actually end up
/// doing all the catalog lookups and subbing them in.
pub fn validate(query: LogicalOperator) -> Result<LogicalOperator, PlannerError> {
    Ok(query)
}
