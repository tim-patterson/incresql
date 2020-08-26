use crate::atoms::{identifier_str, kw, qualified_reference};
use crate::create::create;
use crate::delete::delete;
use crate::drop::drop_;
use crate::insert::insert;
use crate::select::select;
use crate::show::show;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::rel::logical::LogicalOperator;
use ast::statement::{CompactTable, Explain, Statement};
use nom::branch::alt;
use nom::combinator::{cut, map};
use nom::sequence::{preceded, tuple};

pub fn statement(input: &str) -> ParserResult<Statement> {
    alt((
        map(logical_operator, Statement::Query),
        show,
        explain,
        use_,
        create,
        drop_,
        compact,
    ))(input)
}

/// The logical operator statements, these can be used both as a standalone
/// statement and as input to the explain operator
fn logical_operator(input: &str) -> ParserResult<LogicalOperator> {
    alt((select, insert, delete))(input)
}

fn explain(input: &str) -> ParserResult<Statement> {
    map(
        preceded(kw("EXPLAIN"), cut(preceded(ws_0, logical_operator))),
        |query| Statement::Explain(Explain { operator: query }),
    )(input)
}

fn use_(input: &str) -> ParserResult<Statement> {
    map(
        preceded(kw("USE"), cut(preceded(ws_0, identifier_str))),
        Statement::UseDatabase,
    )(input)
}

fn compact(input: &str) -> ParserResult<Statement> {
    map(
        preceded(
            kw("COMPACT"),
            cut(preceded(
                tuple((ws_0, kw("TABLE"), ws_0)),
                qualified_reference,
            )),
        ),
        |(database, name)| Statement::CompactTable(CompactTable { database, name }),
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::{LogicalOperator, Project};

    #[test]
    fn test_statement_select() {
        assert_eq!(
            statement("SELECT 1").unwrap().1,
            Statement::Query(LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![NamedExpression {
                    expression: Expression::from(1),
                    alias: None
                },],
                source: Box::from(LogicalOperator::Single)
            }))
        );
    }

    #[test]
    fn test_statement_show() {
        assert_eq!(
            statement("SHOW functions").unwrap().1,
            Statement::ShowFunctions
        );
    }

    #[test]
    fn test_explain_select() {
        assert_eq!(
            statement("EXPLAIN SELECT 1").unwrap().1,
            Statement::Explain(Explain {
                operator: LogicalOperator::Project(Project {
                    distinct: false,
                    expressions: vec![NamedExpression {
                        expression: Expression::from(1),
                        alias: None
                    },],
                    source: Box::from(LogicalOperator::Single)
                }),
            })
        );
    }

    #[test]
    fn test_use() {
        assert_eq!(
            statement("USE foobar").unwrap().1,
            Statement::UseDatabase("foobar".to_string())
        );
    }

    #[test]
    fn test_compact() {
        assert_eq!(
            statement("Compact table foo.bar").unwrap().1,
            Statement::CompactTable(CompactTable {
                database: Some("foo".to_string()),
                name: "bar".to_string()
            })
        );
    }
}
