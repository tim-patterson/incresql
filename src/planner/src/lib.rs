use ast::rel::logical::LogicalOperator;
use ast::rel::point_in_time::PointInTimeOperator;

mod point_in_time;
mod validate;

pub fn plan_for_point_in_time(query: LogicalOperator) -> Result<PointInTimeOperator, PlannerError> {
    Ok(point_in_time::plan_for_point_in_time(plan_common(query)?))
}

fn plan_common(query: LogicalOperator) -> Result<LogicalOperator, PlannerError> {
    validate::validate(query)
}

pub struct PlannerError {}
