use crate::{Field, Planner, PlannerError};
use ast::expr::Expression;
use ast::rel::logical::{
    Filter, Limit, LogicalOperator, Project, ResolvedTable, TableInsert, UnionAll,
};
use ast::rel::point_in_time::{self, PointInTimeOperator};
use data::{LogicalTimestamp, Session};

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
        LogicalOperator::Limit(Limit {
            offset,
            limit,
            source,
        }) => PointInTimeOperator::Limit(point_in_time::Limit {
            offset,
            limit,
            source: Box::new(build_operator(*source)),
        }),
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
        LogicalOperator::ResolvedTable(ResolvedTable { table }) => {
            PointInTimeOperator::TableScan(point_in_time::TableScan {
                table,
                // Having a timestamp in the future gives us read after write within the same ms
                // Rockdb already gives us atomic writes so I can't think of any downsides with this
                timestamp: LogicalTimestamp::MAX,
            })
        }
        LogicalOperator::TableInsert(TableInsert { table, source }) => {
            let actual_table =
                if let LogicalOperator::ResolvedTable(ResolvedTable { table }) = *table {
                    table
                } else {
                    panic!("Can not insert into anything other than a resolved table")
                };

            PointInTimeOperator::TableInsert(point_in_time::TableInsert {
                table: actual_table,
                source: Box::new(build_operator(*source)),
            })
        }
        LogicalOperator::TableAlias(table_alias) => build_operator(*table_alias.source),
        LogicalOperator::TableReference(_) => panic!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Planner, PlannerError};
    use ast::expr::{Expression, NamedExpression};
    use data::{DataType, Datum};

    #[test]
    fn test_plan_for_point_in_time() -> Result<(), PlannerError> {
        let planner = Planner::new_for_test();
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
