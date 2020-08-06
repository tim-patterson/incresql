use executor::ExecutionError;
use parser::ParseError;
use planner::PlannerError;

#[derive(Debug)]
pub enum QueryError {
    ParseError(ParseError),
    PlannerError(PlannerError),
    ExecutionError(ExecutionError),
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
