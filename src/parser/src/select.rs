use crate::atoms::{as_clause, integer, kw};
use crate::expression::{expression, named_expression};
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::expr::{Expression, NamedExpression};
use ast::rel::logical::{Filter, Limit, LogicalOperator, Project, TableAlias, UnionAll};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{cut, map, opt};
use nom::multi::{many0, separated_list};
use nom::sequence::{delimited, pair, preceded, separated_pair, tuple};

/// Parses a select statement, a select statement consists of potentially multiple
/// select expressions unioned together
pub fn select(input: &str) -> ParserResult<LogicalOperator> {
    map(
        pair(
            select_expr,
            many0(preceded(
                tuple((ws_0, kw("UNION"), ws_0, kw("ALL"), ws_0)),
                select_expr,
            )),
        ),
        |(first, mut rest)| {
            if rest.is_empty() {
                first
            } else {
                rest.insert(0, first);
                LogicalOperator::UnionAll(UnionAll { sources: rest })
            }
        },
    )(input)
}

/// Parses a singular select expression
fn select_expr(input: &str) -> ParserResult<LogicalOperator> {
    map(
        preceded(
            kw("SELECT"),
            cut(tuple((
                preceded(ws_0, comma_sep_named_expressions),
                opt(preceded(ws_0, from_clause)),
                opt(preceded(ws_0, where_clause)),
                opt(preceded(ws_0, limit_clause)),
            ))),
        ),
        |(expressions, from_option, where_option, limit_option)| {
            let mut query = from_option.unwrap_or(LogicalOperator::Single);

            if let Some(predicate) = where_option {
                query = LogicalOperator::Filter(Filter {
                    predicate,
                    source: Box::new(query),
                });
            }

            query = LogicalOperator::Project(Project {
                distinct: false,
                expressions,
                source: Box::from(query),
            });

            if let Some((offset, limit)) = limit_option {
                query = LogicalOperator::Limit(Limit {
                    offset,
                    limit,
                    source: Box::new(query),
                });
            }

            query
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
            delimited(pair(tag("("), ws_0), select, pair(ws_0, tag(")"))),
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

/// Parse the where clause of a query.
fn where_clause(input: &str) -> ParserResult<Expression> {
    preceded(kw("WHERE"), cut(preceded(ws_0, expression)))(input)
}

/// Limit clause, returns (offset, limit)
fn limit_clause(input: &str) -> ParserResult<(i64, i64)> {
    // Theres 3 forms for limit
    // LIMIT offset, limit
    // LIMIT limit
    // LIMIT limit OFFSET offset
    preceded(
        kw("LIMIT"),
        cut(preceded(
            ws_0,
            alt((
                // LIMIT offset, limit
                separated_pair(integer, tuple((ws_0, tag(","), ws_0)), integer),
                // LIMIT limit OFFSET offset
                map(
                    separated_pair(integer, tuple((ws_0, kw("OFFSET"), ws_0)), integer),
                    |(limit, offset)| (offset, limit),
                ),
                // LIMIT limit
                map(integer, |limit| (0, limit)),
            )),
        )),
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

    #[test]
    fn test_where() {
        assert_eq!(
            select("SELECT 1 WHERE true").unwrap().1,
            LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![NamedExpression {
                    expression: Expression::from(1),
                    alias: None
                },],
                source: Box::from(LogicalOperator::Filter(Filter {
                    predicate: Expression::from(true),
                    source: Box::new(LogicalOperator::Single)
                }))
            })
        );
    }

    #[test]
    fn test_limit() {
        let expected = LogicalOperator::Limit(Limit {
            offset: 0,
            limit: 10,
            source: Box::new(LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![NamedExpression {
                    expression: Expression::from(1),
                    alias: None,
                }],
                source: Box::new(LogicalOperator::Single),
            })),
        });

        assert_eq!(select("SELECT 1 LIMIT 10").unwrap().1, expected);

        let expected = LogicalOperator::Limit(Limit {
            offset: 2,
            limit: 10,
            source: Box::new(LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![NamedExpression {
                    expression: Expression::from(1),
                    alias: None,
                }],
                source: Box::new(LogicalOperator::Single),
            })),
        });

        assert_eq!(&select("SELECT 1 LIMIT 10 OFFSET 2").unwrap().1, &expected);

        assert_eq!(select("SELECT 1 LIMIT 2, 10").unwrap().1, expected);
    }

    #[test]
    fn test_union_all() {
        assert_eq!(
            select("SELECT 1 UNION ALL SELECT 2").unwrap().1,
            LogicalOperator::UnionAll(UnionAll {
                sources: vec![
                    LogicalOperator::Project(Project {
                        distinct: false,
                        expressions: vec![NamedExpression {
                            expression: Expression::from(1),
                            alias: None
                        },],
                        source: Box::from(LogicalOperator::Single)
                    }),
                    LogicalOperator::Project(Project {
                        distinct: false,
                        expressions: vec![NamedExpression {
                            expression: Expression::from(2),
                            alias: None
                        },],
                        source: Box::from(LogicalOperator::Single)
                    })
                ]
            })
        );
    }
}
