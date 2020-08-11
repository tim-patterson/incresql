use crate::{Field, Planner, PlannerError};
use ast::expr::Expression;
use ast::rel::logical::{Filter, LogicalOperator, Project, UnionAll};
use ast::rel::point_in_time::{self, PointInTimeOperator};
use data::Session;

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
        session: &Session,
    ) -> Result<PointInTimePlan, PlannerError> {
        let (fields, operator) = self.plan_common(query, session)?;
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
        LogicalOperator::Filter(Filter { predicate, source }) => {
            PointInTimeOperator::Filter(point_in_time::Filter {
                predicate,
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
                column_count: values.fields.len(),
            })
        }
        LogicalOperator::UnionAll(UnionAll { sources }) => {
            PointInTimeOperator::UnionAll(point_in_time::UnionAll {
                sources: sources.into_iter().map(build_operator).collect(),
            })
        }
        LogicalOperator::TableAlias(table_alias) => build_operator(*table_alias.source),
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
        let session = Session::new(1);
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
            planner
                .plan_for_point_in_time(raw_query, &session)?
                .operator,
            expected
        );
        Ok(())
    }
}
