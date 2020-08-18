use crate::atoms::{identifier_str, kw};
use crate::select::select;
use crate::show::show;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::rel::statement::{Explain, Statement};
use nom::branch::alt;
use nom::combinator::{cut, map};
use nom::sequence::preceded;

pub fn statement(input: &str) -> ParserResult<Statement> {
    alt((map(select, Statement::Query), show, explain, use_))(input)
}

fn explain(input: &str) -> ParserResult<Statement> {
    map(
        preceded(kw("EXPLAIN"), cut(preceded(ws_0, select))),
        |query| Statement::Explain(Explain { operator: query }),
    )(input)
}

fn use_(input: &str) -> ParserResult<Statement> {
    map(
        preceded(kw("USE"), cut(preceded(ws_0, identifier_str))),
        Statement::UseDatabase,
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
}
