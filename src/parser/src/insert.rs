use crate::atoms::{kw, qualified_reference};
use crate::select::select;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::rel::logical::{LogicalOperator, TableInsert, TableReference};
use nom::combinator::{cut, map};
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
                cut(preceded(ws_0, select)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::Project;

    #[test]
    fn test_insert() {
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
}
