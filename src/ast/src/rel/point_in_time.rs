use crate::expr::Expression;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PointInTimeOperator {
    Single, // No from clause, ie select 1 + 1
    Project(Project),
}

impl Default for PointInTimeOperator {
    fn default() -> Self {
        PointInTimeOperator::Single
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Project {
    pub expressions: Vec<Expression>,
    pub source: Box<PointInTimeOperator>,
}
