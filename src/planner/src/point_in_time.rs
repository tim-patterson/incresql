use crate::Field;
use ast::rel::logical::{LogicalOperator, Project};
use ast::rel::point_in_time::{self, PointInTimeOperator};

pub struct PointInTimePlan {
    pub fields: Vec<Field>,
    pub operator: PointInTimeOperator,
}

/// Takes a planned logical operator and performs point in time optimizations and transforms
/// to a physical operator tree
pub fn plan_for_point_in_time(fields: Vec<Field>, query: LogicalOperator) -> PointInTimePlan {
    let operator = build_operator(query);

    PointInTimePlan { fields, operator }
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PlannerError;
    use ast::expr::{Expression, NamedExpression};
    use data::Datum;

    #[test]
    fn test_plan_for_point_in_time() -> Result<(), PlannerError> {
        let raw_query = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::Literal(Datum::Null),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let expected = PointInTimeOperator::Project(point_in_time::Project {
            expressions: vec![Expression::Literal(Datum::Null)],
            source: Box::new(PointInTimeOperator::Single),
        });

        assert_eq!(crate::plan_for_point_in_time(raw_query)?.operator, expected);
        Ok(())
    }
}
