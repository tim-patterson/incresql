use crate::expr::Expression;
use data::Datum;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PointInTimeOperator {
    Single, // No from clause, ie select 1 + 1
    Project(Project),
    Values(Values),
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
