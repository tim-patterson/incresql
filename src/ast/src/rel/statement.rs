use crate::rel::logical::LogicalOperator;

/// The top level structure parsed, could be a query or DDL statement.
#[derive(Debug, Eq, PartialEq)]
pub enum Statement {
    Query(LogicalOperator),
}
