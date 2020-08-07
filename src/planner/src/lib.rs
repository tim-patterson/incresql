use ast::rel::logical::LogicalOperator;
use data::DataType;

mod common;
mod normalize;
mod point_in_time;
mod validate;
pub use point_in_time::PointInTimePlan;
use std::fmt::{Display, Formatter};

/// Plan a point in time query, this optimizes the logical operator tree and then transforms into
/// a physical plan for point in time
pub fn plan_for_point_in_time(query: LogicalOperator) -> Result<PointInTimePlan, PlannerError> {
    let (fields, operator) = common::plan_common(query)?;
    Ok(point_in_time::plan_for_point_in_time(fields, operator))
}

#[derive(Debug)]
pub struct PlannerError {}

impl Display for PlannerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Planner Error")
    }
}

/// A Field is simply a column name and a type.
/// While this is sort of a property of a logical operator it does require resolving functions
/// etc to calculate, this is part of what the planner does
#[derive(Debug, Eq, PartialEq)]
pub struct Field {
    pub alias: String,
    pub data_type: DataType,
}
