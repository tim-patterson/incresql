use crate::{Field, Planner, PlannerError};
use ast::rel::logical::{LogicalOperator, Project};
use ast::rel::point_in_time::{self, PointInTimeOperator};
use ast::expr::Expression;

pub struct PointInTimePlan {
    pub fields: Vec<Field>,
    pub operator: PointInTimeOperator,
}

impl Planner {
    /// Plan a point in time query, this optimizes the logical operator tree and then transforms into
    /// a physical plan for point in time
    pub fn plan_for_point_in_time(
        &self,
        query: LogicalOperator,
    ) -> Result<PointInTimePlan, PlannerError> {
        let (fields, operator) = self.plan_common(query)?;
        let operator = build_operator(operator);

        Ok(PointInTimePlan { fields, operator })
    }
}

fn build_operator(query: LogicalOperator) -> PointInTimeOperator {
    match query {
        LogicalOperator::Single => PointInTimeOperator::Single,
        LogicalOperator::Project(Project {
            distinct,
            expressions,
            source,
        }) => {
            assert!(!distinct, "Distinct should not be true at this point!");
            PointInTimeOperator::Project(point_in_time::Project {
                expressions: expressions.into_iter().map(|ne| ne.expression).collect(),
                source: Box::new(build_operator(*source)),
            })
        }
        LogicalOperator::Values(values) => {
            let data = values.data.into_iter().map(|row| {
                row.into_iter().map(|expr| {
                    if let Expression::Constant(datum, _datatype) = expr {
                        datum
                    } else {
                        panic!("Planner should have already have validated that all values exprs are constants - {:?}", expr)
                    }
                }).collect()
            }).collect();

            PointInTimeOperator::Values(point_in_time::Values {
                data,
                column_count: values.fields.len()
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Planner, PlannerError};
    use ast::expr::{Expression, NamedExpression};
    use data::{DataType, Datum};
    use functions::registry::Registry;

    #[test]
    fn test_plan_for_point_in_time() -> Result<(), PlannerError> {
        let planner = Planner::new(Registry::new(false));
        let raw_query = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::Constant(Datum::Null, DataType::Null),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let expected = PointInTimeOperator::Project(point_in_time::Project {
            expressions: vec![Expression::Constant(Datum::Null, DataType::Null)],
            source: Box::new(PointInTimeOperator::Single),
        });

        assert_eq!(
            planner.plan_for_point_in_time(raw_query)?.operator,
            expected
        );
        Ok(())
    }
}
