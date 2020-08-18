use executor::ExecutionError;
use parser::ParseError;
use planner::PlannerError;
use std::fmt::{Debug, Display, Formatter};

pub enum QueryError {
    ParseError(ParseError),
    PlannerError(PlannerError),
    ExecutionError(ExecutionError),
}

impl Display for QueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::ParseError(err) => Display::fmt(err, f),
            QueryError::PlannerError(err) => Display::fmt(err, f),
            QueryError::ExecutionError(err) => Display::fmt(err, f),
        }
    }
}

impl Debug for QueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl From<ParseError> for QueryError {
    fn from(parse_error: ParseError) -> Self {
        QueryError::ParseError(parse_error)
    }
}

impl From<ExecutionError> for QueryError {
    fn from(execution_error: ExecutionError) -> Self {
        QueryError::ExecutionError(execution_error)
    }
}

impl From<PlannerError> for QueryError {
    fn from(planner_error: PlannerError) -> Self {
        QueryError::PlannerError(planner_error)
    }
}
