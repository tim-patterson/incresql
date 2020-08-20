use crate::atoms::{kw, qualified_reference};
use crate::literals::literal;
use crate::select::select;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::expr::Expression;
use ast::rel::logical::{LogicalOperator, TableInsert, TableReference, Values};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{cut, map};
use nom::multi::separated_list;
use nom::sequence::{pair, preceded, tuple};

/// Parses an insert statement
pub fn insert(input: &str) -> ParserResult<LogicalOperator> {
    map(
        preceded(
            kw("INSERT"),
            pair(
                cut(preceded(
                    tuple((ws_0, kw("INTO"), ws_0)),
                    qualified_reference,
                )),
                cut(preceded(ws_0, alt((select, values)))),
            ),
        ),
        |((database, table_name), select)| {
            LogicalOperator::TableInsert(TableInsert {
                table: Box::new(LogicalOperator::TableReference(TableReference {
                    database,
                    table: table_name,
                })),
                source: Box::new(select),
            })
        },
    )(input)
}

/// Parses a values clause.
fn values(input: &str) -> ParserResult<LogicalOperator> {
    map(
        preceded(
            alt((kw("VALUES"), kw("VALUE"))),
            cut(preceded(
                ws_0,
                separated_list(tuple((ws_0, tag(","), ws_0)), values_row),
            )),
        ),
        |data| {
            LogicalOperator::Values(Values {
                fields: vec![],
                data,
            })
        },
    )(input)
}

/// Parses a single values row, ie "(1,false,...)"
fn values_row(input: &str) -> ParserResult<Vec<Expression>> {
    map(
        tuple((
            tag("("),
            ws_0,
            separated_list(tuple((ws_0, tag(","), ws_0)), literal),
            ws_0,
            tag(")"),
        )),
        |(_, _, list, _, _)| list,
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::Project;

    #[test]
    fn test_insert_from() {
        assert_eq!(
            insert("insert into foo select 1").unwrap().1,
            LogicalOperator::TableInsert(TableInsert {
                table: Box::new(LogicalOperator::TableReference(TableReference {
                    database: None,
                    table: "foo".to_string()
                })),
                source: Box::new(LogicalOperator::Project(Project {
                    distinct: false,
                    expressions: vec![NamedExpression {
                        alias: None,
                        expression: Expression::from(1)
                    }],
                    source: Box::new(LogicalOperator::Single)
                }))
            })
        );
    }

    #[test]
    fn test_insert_values() {
        assert_eq!(
            insert("insert into foo values (1,2), (3,4)").unwrap().1,
            LogicalOperator::TableInsert(TableInsert {
                table: Box::new(LogicalOperator::TableReference(TableReference {
                    database: None,
                    table: "foo".to_string()
                })),
                source: Box::new(LogicalOperator::Values(Values {
                    fields: vec![],
                    data: vec![
                        vec![Expression::from(1), Expression::from(2)],
                        vec![Expression::from(3), Expression::from(4)]
                    ]
                }))
            })
        );
    }
}
