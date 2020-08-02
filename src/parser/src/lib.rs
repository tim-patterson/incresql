use ast::rel::statement::Statement;
use nom::combinator::all_consuming;
use nom::error::{convert_error, VerboseError};
use nom::IResult;

mod atoms;
mod expression;
mod literals;
mod select;
mod statement;
mod whitespace;

type ParserResult<'a, T> = IResult<&'a str, T, VerboseError<&'a str>>;

// The top level entry to parse a sql statement.
// By forming sub parsers into a tree with branches in the tree being common prefixes it allows us
// To give better contextual error messages in the future.
pub fn parse(input: &str) -> Result<Statement, String> {
    let parser_result = all_consuming(statement::statement)(input);

    parser_result.map(|(_, command)| command).map_err(|err| {
        match err {
            nom::Err::Error(e) => convert_error(input, e),
            nom::Err::Failure(e) => convert_error(input, e),
            // We should only get an incomplete if we used the streaming parsers
            nom::Err::Incomplete(_) => panic!(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::{LogicalOperator, Project};
    use data::Datum;

    #[test]
    fn test_statement_select() {
        assert_eq!(
            parse("SELECT 1").unwrap(),
            Statement::Query(LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![NamedExpression {
                    expression: Expression::Literal(Datum::from(1)),
                    alias: None
                },],
                source: Box::from(LogicalOperator::Single)
            }))
        );
    }

    #[test]
    fn test_statement_err() {
        assert_eq!(
            parse("SELECT !!").unwrap_err(),
            "0: at line 1, in Eof:\nSELECT !!\n       ^\n\n"
        );
    }
}
