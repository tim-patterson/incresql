use crate::select::select;
use crate::show::show;
use crate::ParserResult;
use ast::rel::statement::Statement;
use nom::branch::alt;
use nom::combinator::map;

pub fn statement(input: &str) -> ParserResult<Statement> {
    alt((map(select, Statement::Query), show))(input)
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
}
