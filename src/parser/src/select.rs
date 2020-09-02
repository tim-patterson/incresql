use crate::atoms::{as_clause, integer, kw, qualified_reference, quoted_string};
use crate::expression::{comma_sep_expressions, expression, named_expression, sort_expression};
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::expr::{Expression, NamedExpression, SortExpression};
use ast::rel::logical::{
    FileScan, Filter, GroupBy, Join, JoinType, Limit, LogicalOperator, Project, SerdeOptions, Sort,
    TableAlias, TableReference, UnionAll,
};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{cut, map, opt};
use nom::multi::{many0, separated_list, separated_nonempty_list};
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
                opt(preceded(ws_0, group_by_clause)),
                opt(preceded(ws_0, order_clause)),
                opt(preceded(ws_0, limit_clause)),
            ))),
        ),
        |(expressions, from_option, where_option, group_option, order_option, limit_option)| {
            let mut query = from_option.unwrap_or(LogicalOperator::Single);

            if let Some(predicate) = where_option {
                query = LogicalOperator::Filter(Filter {
                    predicate,
                    source: Box::new(query),
                });
            }

            query = if let Some(group_keys) = group_option {
                LogicalOperator::GroupBy(GroupBy {
                    expressions,
                    key_expressions: group_keys,
                    source: Box::from(query),
                })
            } else {
                LogicalOperator::Project(Project {
                    distinct: false,
                    expressions,
                    source: Box::from(query),
                })
            };

            if let Some(sort_expressions) = order_option {
                query = LogicalOperator::Sort(Sort {
                    sort_expressions,
                    source: Box::new(query),
                })
            }

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

// The from clause of a query can get a bit tricky...
// Consider the following
// FROM
//  a,
//  b as foo,
//  (select ...) as bar,
//  c join d as e on c.id = d.id
//  join f on ...
//
//
// So we can consider the from to be made up of a bunch of
// comma separated entries, these are cross-joined together
// Each of these entries is a "join", the join parser takes
// many "join_item"'s that are aliased entries from the
// unaliased_join_item which is the lowest atom building block.

/// Parse the from clause of a query.
fn from_clause(input: &str) -> ParserResult<LogicalOperator> {
    map(
        preceded(
            kw("FROM"),
            cut(separated_nonempty_list(
                tuple((ws_0, tag(","), ws_0)),
                preceded(ws_0, join),
            )),
        ),
        |items| {
            // Grab the first and then cross join with each join as we go.
            let mut iter = items.into_iter();
            let first = iter.next().unwrap();
            iter.fold(first, |left, right| {
                LogicalOperator::Join(Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on: Expression::from(true),
                    join_type: JoinType::Inner,
                })
            })
        },
    )(input)
}

fn join(input: &str) -> ParserResult<LogicalOperator> {
    map(
        pair(
            join_item,
            many0(pair(
                preceded(
                    tuple((ws_0, opt(pair(kw("INNER"), ws_0)), kw("JOIN"), ws_0)),
                    join_item,
                ),
                preceded(tuple((ws_0, kw("ON"), ws_0)), expression),
            )),
        ),
        |(first, joins)| {
            joins.into_iter().fold(first, |left, (right, condition)| {
                LogicalOperator::Join(Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on: condition,
                    join_type: JoinType::Inner,
                })
            })
        },
    )(input)
}

fn join_item(input: &str) -> ParserResult<LogicalOperator> {
    map(
        pair(unaliased_join_item, as_clause),
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

fn unaliased_join_item(input: &str) -> ParserResult<LogicalOperator> {
    alt((
        // sub query
        directory_source,
        delimited(pair(tag("("), ws_0), select, pair(ws_0, tag(")"))),
        table_reference_with_alias,
    ))(input)
}

/// Parse the where clause of a query.
pub(crate) fn where_clause(input: &str) -> ParserResult<Expression> {
    preceded(kw("WHERE"), cut(preceded(ws_0, expression)))(input)
}

/// Parse the group by clause of a query.
pub(crate) fn group_by_clause(input: &str) -> ParserResult<Vec<Expression>> {
    preceded(
        kw("GROUP"),
        cut(preceded(
            tuple((ws_0, kw("BY"), ws_0)),
            comma_sep_expressions,
        )),
    )(input)
}

/// Parse the order by clause of a query.
pub(crate) fn order_clause(input: &str) -> ParserResult<Vec<SortExpression>> {
    preceded(
        tuple((kw("ORDER"), ws_0, kw("BY"))),
        cut(preceded(
            ws_0,
            separated_list(tuple((ws_0, tag(","), ws_0)), sort_expression),
        )),
    )(input)
}

/// Limit clause, returns (offset, limit)
pub(crate) fn limit_clause(input: &str) -> ParserResult<(i64, i64)> {
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

/// Parse as a table AND wrap in a Table Alias
fn table_reference_with_alias(input: &str) -> ParserResult<LogicalOperator> {
    map(qualified_reference, |(database, table)| {
        let table_source = LogicalOperator::TableReference(TableReference {
            database,
            table: table.clone(),
        });
        LogicalOperator::TableAlias(TableAlias {
            alias: table,
            source: Box::new(table_source),
        })
    })(input)
}

/// Parse a file source
fn directory_source(input: &str) -> ParserResult<LogicalOperator> {
    map(
        preceded(
            kw("DIRECTORY"),
            cut(preceded(
                ws_0,
                pair(quoted_string, opt(preceded(ws_0, serde_options))),
            )),
        ),
        |(directory, serde_options)| {
            LogicalOperator::FileScan(FileScan {
                directory,
                serde_options: serde_options.unwrap_or_default(),
            })
        },
    )(input)
}

fn serde_options(input: &str) -> ParserResult<SerdeOptions> {
    map(
        delimited(
            tuple((kw("WITH"), ws_0, tag("("), ws_0)),
            delimiter_option,
            tuple((ws_0, tag(")"))),
        ),
        |delimiter| SerdeOptions { delimiter },
    )(input)
}

fn delimiter_option(input: &str) -> ParserResult<u8> {
    map(
        preceded(
            tuple((kw("DELIMITER"), ws_0, tag("="), ws_0)),
            quoted_string,
        ),
        |s| s.as_bytes()[0],
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{ColumnReference, Expression};
    use data::SortOrder;

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
    fn test_group_by() {
        assert_eq!(
            select("SELECT 1 GROUP BY a").unwrap().1,
            LogicalOperator::GroupBy(GroupBy {
                expressions: vec![NamedExpression {
                    expression: Expression::from(1),
                    alias: None
                },],
                key_expressions: vec![Expression::ColumnReference(ColumnReference {
                    qualifier: None,
                    alias: "a".to_string(),
                    star: false
                })],
                source: Box::new(LogicalOperator::Single)
            })
        );
    }

    #[test]
    fn test_old_style_join() {
        assert_eq!(
            select("SELECT 1 FROM a, b").unwrap().1,
            LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![NamedExpression {
                    expression: Expression::from(1),
                    alias: None
                },],
                source: Box::new(LogicalOperator::Join(Join {
                    left: Box::new(LogicalOperator::TableAlias(TableAlias {
                        alias: "a".to_string(),
                        source: Box::new(LogicalOperator::TableReference(TableReference {
                            database: None,
                            table: "a".to_string()
                        }))
                    })),
                    right: Box::new(LogicalOperator::TableAlias(TableAlias {
                        alias: "b".to_string(),
                        source: Box::new(LogicalOperator::TableReference(TableReference {
                            database: None,
                            table: "b".to_string()
                        }))
                    })),
                    on: Expression::from(true),
                    join_type: JoinType::Inner
                }))
            })
        );
    }

    #[test]
    fn test_new_style_join() {
        assert_eq!(
            select("SELECT 1 FROM a join b on 3").unwrap().1,
            LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![NamedExpression {
                    expression: Expression::from(1),
                    alias: None
                },],
                source: Box::new(LogicalOperator::Join(Join {
                    left: Box::new(LogicalOperator::TableAlias(TableAlias {
                        alias: "a".to_string(),
                        source: Box::new(LogicalOperator::TableReference(TableReference {
                            database: None,
                            table: "a".to_string()
                        }))
                    })),
                    right: Box::new(LogicalOperator::TableAlias(TableAlias {
                        alias: "b".to_string(),
                        source: Box::new(LogicalOperator::TableReference(TableReference {
                            database: None,
                            table: "b".to_string()
                        }))
                    })),
                    on: Expression::from(3),
                    join_type: JoinType::Inner
                }))
            })
        );
    }

    #[test]
    fn test_order_by() {
        let project = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                expression: Expression::from(1),
                alias: None,
            }],
            source: Box::new(LogicalOperator::Single),
        });

        assert_eq!(
            select("SELECT 1 ORDER BY 1 desc").unwrap().1,
            LogicalOperator::Sort(Sort {
                sort_expressions: vec![SortExpression {
                    ordering: SortOrder::Desc,
                    expression: Expression::from(1)
                }],
                source: Box::new(project)
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

    #[test]
    fn test_table_reference() {
        assert_eq!(
            table_reference_with_alias("foo").unwrap().1,
            LogicalOperator::TableAlias(TableAlias {
                alias: "foo".to_string(),
                source: Box::new(LogicalOperator::TableReference(TableReference {
                    database: None,
                    table: "foo".to_string()
                })),
            })
        );

        assert_eq!(
            table_reference_with_alias("foo.bar").unwrap().1,
            LogicalOperator::TableAlias(TableAlias {
                alias: "bar".to_string(),
                source: Box::new(LogicalOperator::TableReference(TableReference {
                    database: Some("foo".to_string()),
                    table: "bar".to_string()
                })),
            })
        );
    }

    #[test]
    fn test_directory_src() {
        assert_eq!(
            select(r#"SELECT 1 FROM DIRECTORY "test""#).unwrap().1,
            LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![NamedExpression {
                    expression: Expression::from(1),
                    alias: None,
                }],
                source: Box::new(LogicalOperator::FileScan(FileScan {
                    directory: "test".to_string(),
                    serde_options: SerdeOptions::default()
                })),
            })
        );
    }

    #[test]
    fn test_directory_src_serde_option() {
        assert_eq!(
            select(r#"SELECT 1 FROM DIRECTORY "test" WITH (delimiter="|")"#)
                .unwrap()
                .1,
            LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![NamedExpression {
                    expression: Expression::from(1),
                    alias: None,
                }],
                source: Box::new(LogicalOperator::FileScan(FileScan {
                    directory: "test".to_string(),
                    serde_options: SerdeOptions { delimiter: b'|' }
                })),
            })
        );
    }
}
