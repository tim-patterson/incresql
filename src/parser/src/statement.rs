use crate::select::select;
use crate::ParserResult;
use ast::rel::statement::Statement;
use nom::combinator::map;

pub fn statement(input: &str) -> ParserResult<Statement> {
    map(select, Statement::Query)(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::{LogicalOperator, Project};

    #[test]
    fn test_statement() {
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
}
