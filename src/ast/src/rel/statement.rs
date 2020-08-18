use crate::rel::logical::LogicalOperator;

/// The top level structure parsed, could be a query or DDL statement.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Statement {
    Query(LogicalOperator),
    ShowFunctions,
    ShowDatabases,
    ShowTables,
    UseDatabase(String),
    Explain(Explain),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Explain {
    pub operator: LogicalOperator,
}
