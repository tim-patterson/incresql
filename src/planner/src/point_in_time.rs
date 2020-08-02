use ast::rel::logical::{LogicalOperator, Project};
use ast::rel::point_in_time::{self, PointInTimeOperator};

pub fn plan_for_point_in_time(query: LogicalOperator) -> PointInTimeOperator {
    match query {
        LogicalOperator::Single => PointInTimeOperator::Single,
        LogicalOperator::Project(Project {
            distinct,
            expressions,
            source,
        }) => {
            assert!(!distinct, "Distinct should not be true at this point!");
            PointInTimeOperator::Project(point_in_time::Project {
                expressions,
                source: Box::new(plan_for_point_in_time(*source)),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use data::Datum;

    #[test]
    fn test_plan_for_point_in_time() {
        let raw_query = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::Literal(Datum::Null),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let expected = PointInTimeOperator::Project(point_in_time::Project {
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::Literal(Datum::Null),
            }],
            source: Box::new(PointInTimeOperator::Single),
        });

        assert_eq!(plan_for_point_in_time(raw_query), expected);
    }
}
