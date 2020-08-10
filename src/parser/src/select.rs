use crate::atoms::{as_clause, kw};
use crate::expression::named_expression;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::expr::NamedExpression;
use ast::rel::logical::{LogicalOperator, Project, TableAlias};
use nom::bytes::complete::tag;
use nom::combinator::{cut, map, opt};
use nom::multi::separated_list;
use nom::sequence::{delimited, pair, preceded, tuple};

/// Parses a select statement
pub fn select(input: &str) -> ParserResult<LogicalOperator> {
    map(
        preceded(
            kw("SELECT"),
            cut(tuple((
                preceded(ws_0, comma_sep_named_expressions),
                opt(preceded(ws_0, from_clause)),
            ))),
        ),
        |(expressions, from_option)| {
            let query = from_option.unwrap_or(LogicalOperator::Single);

            LogicalOperator::Project(Project {
                distinct: false,
                expressions,
                source: Box::from(query),
            })
        },
    )(input)
}

fn comma_sep_named_expressions(input: &str) -> ParserResult<Vec<NamedExpression>> {
    separated_list(tuple((ws_0, tag(","), ws_0)), named_expression)(input)
}

/// Parse the from clause of a query.
fn from_clause(input: &str) -> ParserResult<LogicalOperator> {
    preceded(kw("FROM"), cut(preceded(ws_0, from_item)))(input)
}

fn from_item(input: &str) -> ParserResult<LogicalOperator> {
    // sub query
    map(
        pair(
            delimited(pair(tag("("), ws_0), select, pair(tag(")"), ws_0)),
            as_clause,
        ),
        |(sub_query, alias_opt)| {
            if let Some(alias) = alias_opt {
                LogicalOperator::TableAlias(TableAlias {
                    alias,
                    source: Box::from(sub_query),
                })
            } else {
                sub_query
            }
        },
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::Expression;

    #[test]
    fn test_select() {
        assert_eq!(
            select("SELECT 1,2 foo , 3 as bar").unwrap().1,
            LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![
                    NamedExpression {
                        expression: Expression::from(1),
                        alias: None
                    },
                    NamedExpression {
                        expression: Expression::from(2),
                        alias: Some(String::from("foo"))
                    },
                    NamedExpression {
                        expression: Expression::from(3),
                        alias: Some(String::from("bar"))
                    },
                ],
                source: Box::from(LogicalOperator::Single)
            })
        );
    }

    #[test]
    fn test_from_simple() {
        let sql = "SELECT 1 FROM (SELECT 1)";

        let inner = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                expression: Expression::from(1),
                alias: None,
            }],
            source: Box::from(LogicalOperator::Single),
        });

        let expected = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                expression: Expression::from(1),
                alias: None,
            }],
            source: Box::from(inner),
        });

        assert_eq!(select(sql).unwrap().1, expected);
    }

    #[test]
    fn test_from_aliased() {
        let sql1 = "SELECT 1 FROM (SELECT 1) as foo";
        let sql2 = "SELECT 1 FROM (SELECT 1) foo";

        let inner = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                expression: Expression::from(1),
                alias: None,
            }],
            source: Box::from(LogicalOperator::Single),
        });

        let alias = LogicalOperator::TableAlias(TableAlias {
            alias: "foo".to_string(),
            source: Box::new(inner),
        });

        let expected = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                expression: Expression::from(1),
                alias: None,
            }],
            source: Box::from(alias),
        });

        assert_eq!(&select(sql1).unwrap().1, &expected);

        assert_eq!(&select(sql2).unwrap().1, &expected);
    }
}
