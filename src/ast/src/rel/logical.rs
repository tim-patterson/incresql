use crate::expr::NamedExpression;

/// Represents a query in the generic sense, generated from the parser, and validated and
/// modified by the planner.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum LogicalOperator {
    // These may appear anywhere in a logical operator at anytime
    Single, // No from clause, ie select 1 + 1
    Project(Project<LogicalOperator>),
}

impl Default for LogicalOperator {
    fn default() -> Self {
        LogicalOperator::Single
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Project<S> {
    pub distinct: bool, // Comes from parser, planner will rewrite to a group by
    pub expressions: Vec<NamedExpression>,
    pub source: Box<S>,
}
