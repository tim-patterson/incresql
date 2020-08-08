use crate::{Planner, PlannerError};
use ast::rel::logical::LogicalOperator;

/// Normalize the query, adding in missing aliases etc so the rest of the planning doesn't need
/// to work around all of that
impl Planner {
    pub(crate) fn normalize(
        &self,
        mut query: LogicalOperator,
    ) -> Result<LogicalOperator, PlannerError> {
        normalize_impl(&mut query);
        Ok(query)
    }
}

fn normalize_impl(query: &mut LogicalOperator) {
    for child in query.children_mut() {
        normalize_impl(child);
    }

    // Column Aliases
    for (idx, ne) in query.named_expressions_mut().enumerate() {
        if ne.alias.is_none() {
            ne.alias = Some(format!("_col{}", idx + 1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::Project;
    use data::Datum;
    use functions::registry::Registry;

    #[test]
    fn test_normalize_column_aliases() -> Result<(), PlannerError> {
        let planner = Planner::new(Registry::new(false));
        let operator = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![
                NamedExpression {
                    alias: Some(String::from("1")),
                    expression: Expression::Literal(Datum::Null),
                },
                NamedExpression {
                    alias: None,
                    expression: Expression::Literal(Datum::Null),
                },
            ],
            source: Box::new(LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![
                    NamedExpression {
                        alias: None,
                        expression: Expression::Literal(Datum::Null),
                    },
                    NamedExpression {
                        alias: None,
                        expression: Expression::Literal(Datum::Null),
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
