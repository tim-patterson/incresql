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
    CreateView(CreateView),
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

/// Create view we grab the raw text as well as the logical operator.
/// once we've validated the operator is good we actually throw it
/// away and just store the sql.  This may change in the future tho.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CreateView {
    pub database: Option<String>,
    pub name: String,
    pub sql: String,
    pub query: LogicalOperator,
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
