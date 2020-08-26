use crate::rel::logical::LogicalOperator;
use data::DataType;

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
    CreateTable(CreateTable),
    CompactTable(CompactTable),
    DropTable(DropTable),
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

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CreateTable {
    pub database: Option<String>,
    pub name: String,
    pub columns: Vec<(String, DataType)>,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DropTable {
    pub database: Option<String>,
    pub name: String,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CompactTable {
    pub database: Option<String>,
    pub name: String,
}
