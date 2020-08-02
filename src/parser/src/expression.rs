use crate::atoms::{identifier_str, kw};
use crate::literals::literal;
use crate::whitespace::{ws_0, ws_1};
use crate::ParserResult;
use ast::expr::{Expression, NamedExpression};
use nom::combinator::{map, opt};
use nom::sequence::{pair, preceded};

/// Parses a bog standard expression, ie 1 + 2
pub fn expression(input: &str) -> ParserResult<Expression> {
    literal_expression(input)
}

/// Parses a named expression, ie 1 as one
pub fn named_expression(input: &str) -> ParserResult<NamedExpression> {
    map(
        pair(
            expression,
            opt(preceded(
                pair(opt(pair(ws_0, kw("AS"))), ws_1),
                identifier_str,
            )),
        ),
        |(expression, alias)| NamedExpression { expression, alias },
    )(input)
}

fn literal_expression(input: &str) -> ParserResult<Expression> {
    map(literal, Expression::Literal)(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::Datum;

    #[test]
    fn test_literal_expression() {
        assert_eq!(
            expression("NuLl").unwrap().1,
            Expression::Literal(Datum::Null)
        );
    }

    #[test]
    fn test_named_expression() {
        let expression = Expression::Literal(Datum::Null);
        assert_eq!(
            named_expression("NuLl").unwrap().1,
            NamedExpression {
                expression,
                alias: None
            }
        );

        let expression = Expression::Literal(Datum::Null);
        assert_eq!(
            named_expression("NuLl foobar").unwrap().1,
            NamedExpression {
                expression,
                alias: Some(String::from("foobar"))
            }
        );

        let expression = Expression::Literal(Datum::Null);
        assert_eq!(
            named_expression("NuLl as foobar").unwrap().1,
            NamedExpression {
                expression,
                alias: Some(String::from("foobar"))
            }
        );
    }
}
