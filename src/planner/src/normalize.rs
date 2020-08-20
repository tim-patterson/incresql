use crate::{Planner, PlannerError};
use ast::expr::{ColumnReference, Expression};
use ast::rel::logical::LogicalOperator;

impl Planner {
    /// Normalize the query, adding in missing aliases etc so the rest of the planning doesn't need
    /// to work around all of that
    pub(crate) fn normalize(
        &self,
        mut query: LogicalOperator,
    ) -> Result<LogicalOperator, PlannerError> {
        normalize_column_aliases(&mut query);
        Ok(query)
    }
}

/// This just created dummy _col1 style column aliases for expressions
/// where they aren't specified in the queries
fn normalize_column_aliases(query: &mut LogicalOperator) {
    for child in query.children_mut() {
        normalize_column_aliases(child);
    }

    // Column Aliases
    for (idx, ne) in query.named_expressions_mut().enumerate() {
        if ne.alias.is_none() {
            ne.alias = if let Expression::ColumnReference(ColumnReference { alias, .. }) =
                &ne.expression
            {
                Some(alias.clone())
            } else {
                Some(format!("_col{}", idx + 1))
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::Project;
    use data::{DataType, Datum};

    #[test]
    fn test_normalize_column_aliases() -> Result<(), PlannerError> {
        let planner = Planner::new_for_test();
        let operator = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![
                NamedExpression {
                    alias: Some(String::from("1")),
                    expression: Expression::Constant(Datum::Null, DataType::Null),
                },
                NamedExpression {
                    alias: None,
                    expression: Expression::Constant(Datum::Null, DataType::Null),
                },
            ],
            source: Box::new(LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![
                    NamedExpression {
                        alias: None,
                        expression: Expression::Constant(Datum::Null, DataType::Null),
                    },
                    NamedExpression {
                        alias: None,
                        expression: Expression::Constant(Datum::Null, DataType::Null),
                    },
                ],
                source: Box::new(LogicalOperator::Single),
            })),
        });

        let mut normalized = planner.normalize(operator)?;
        let top_aliases: Vec<_> = normalized
            .named_expressions_mut()
            .map(|ne| ne.alias.as_ref().unwrap())
            .collect();

        assert_eq!(top_aliases, vec!["1", "_col2"]);

        let lower_operator = normalized.children_mut().next().unwrap();
        let lower_aliases: Vec<_> = lower_operator
            .named_expressions_mut()
            .map(|ne| ne.alias.as_ref().unwrap())
            .collect();

        assert_eq!(lower_aliases, vec!["_col1", "_col2"]);

        Ok(())
    }
}
