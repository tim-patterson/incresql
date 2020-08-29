use crate::expr::{Expression, SortExpression};
use crate::rel::logical::SerdeOptions;
use data::{Datum, LogicalTimestamp};
use storage::Table;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PointInTimeOperator {
    Single, // No from clause, ie select 1 + 1
    Project(Project),
    Values(Values),
    Filter(Filter),
    Limit(Limit),
    Sort(Sort),
    UnionAll(UnionAll),
    TableScan(TableScan),
    TableInsert(TableInsert),
    NegateFreq(Box<PointInTimeOperator>),
    SortedGroup(Group),
    HashGroup(Group),
    FileScan(FileScan),
}

impl Default for PointInTimeOperator {
    fn default() -> Self {
        PointInTimeOperator::Single
    }
}

/// An operator that just feeds up a fixed set of values.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Values {
    pub data: Vec<Vec<Datum<'static>>>,
    pub column_count: usize,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Project {
    pub expressions: Vec<Expression>,
    pub source: Box<PointInTimeOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Filter {
    pub predicate: Expression,
    pub source: Box<PointInTimeOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Limit {
    pub offset: i64,
    pub limit: i64,
    pub source: Box<PointInTimeOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnionAll {
    pub sources: Vec<PointInTimeOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TableScan {
    pub table: Table,
    pub timestamp: LogicalTimestamp,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TableInsert {
    pub table: Table,
    pub source: Box<PointInTimeOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Sort {
    pub sort_expressions: Vec<SortExpression>,
    pub source: Box<PointInTimeOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Group {
    pub source: Box<PointInTimeOperator>,
    pub expressions: Vec<Expression>,
    pub key_len: usize,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FileScan {
    pub directory: String,
    pub serde_options: SerdeOptions,
}
