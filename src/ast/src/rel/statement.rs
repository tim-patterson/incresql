use crate::rel::logical::LogicalOperator;

/// The top level structure parsed, could be a query or DDL statement.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Statement {
    Query(LogicalOperator),
    ShowFunctions,
    ShowDatabases,
    ShowTables,
    CreateDatabase(CreateDatabase),
    DropDatabase(String),
    UseDatabase(String),
    Explain(Explain),
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Explain {
    pub operator: LogicalOperator,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CreateDatabase {
    pub name: String,
}
