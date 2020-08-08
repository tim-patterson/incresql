use crate::atoms::{identifier_str, kw};
use crate::literals::literal;
use crate::whitespace::{ws_0, ws_1};
use crate::ParserResult;
use ast::expr::{Expression, FunctionCall, NamedExpression};
use nom::branch::{alt, Alt};
use nom::bytes::complete::tag;
use nom::combinator::{map, opt};
use nom::error::VerboseError;
use nom::multi::{many0, separated_list};
use nom::sequence::{pair, preceded, tuple};

/// Parses a bog standard expression, ie 1 + 2
pub fn expression(input: &str) -> ParserResult<Expression> {
    expression_0(input)
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

/// Parse a comma separated list of expressions ie 1,2+2
pub fn comma_sep_expressions(input: &str) -> ParserResult<Vec<Expression>> {
    separated_list(tuple((ws_0, tag(","), ws_0)), expression)(input)
}

fn expression_0(input: &str) -> ParserResult<Expression> {
    infix_many((tag("+"), tag("-")), expression_1)(input)
}

fn expression_1(input: &str) -> ParserResult<Expression> {
    infix_many((tag("*"), tag("/")), expression_2)(input)
}

fn expression_2(input: &str) -> ParserResult<Expression> {
    alt((function_call, literal_expression))(input)
}

fn literal_expression(input: &str) -> ParserResult<Expression> {
    map(literal, Expression::Literal)(input)
}

/// Used to reduce boilerplate at each precedence level for infix operators
/// Takes a tuple of operator tags, and the parser function for the higher precedence layer
fn infix_many<'a, List: Alt<&'a str, &'a str, VerboseError<&'a str>>>(
    operators: List,
    higher: fn(&'a str) -> ParserResult<Expression>,
) -> impl Fn(&'a str) -> ParserResult<Expression> {
    map(
        // Basically for an expression like
        // 1 + 2 * 3 + 5 + 6
        // we decompose into <higher> op <higher> op <higher> ...
        // in this case assuming we're at the +/- level then that's...
        // (1) + (2 * 3) + (5) + (6)
        // These are then left folded together to form
        // (((1 + (2 * 3)) + 5) + 6)
        tuple((higher, many0(tuple((ws_0, alt(operators), ws_0, higher))))),
        |(start, ops)| {
            ops.into_iter().fold(start, |acc, (_, op, _, exp2)| {
                Expression::FunctionCall(FunctionCall {
                    function_name: op.to_lowercase(),
                    args: vec![acc, exp2],
                })
            })
        },
    )
}

fn function_call(input: &str) -> ParserResult<Expression> {
    map(
        tuple((
            identifier_str,
            tuple((ws_0, tag("("), ws_0)),
            comma_sep_expressions,
            ws_0,
            tag(")"),
        )),
        |(function_name, _, params, _, _)| {
            Expression::FunctionCall(FunctionCall {
                function_name,
                args: params,
            })
        },
    )(input)
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
    fn test_function_expression() {
        assert_eq!(
            expression("foo()").unwrap().1,
            Expression::FunctionCall(FunctionCall {
                function_name: "foo".to_string(),
                args: vec![]
            })
        );

        assert_eq!(
            expression("foo(1,2)").unwrap().1,
            Expression::FunctionCall(FunctionCall {
                function_name: "foo".to_string(),
                args: vec![
                    Expression::Literal(Datum::from(1)),
                    Expression::Literal(Datum::from(2)),
                ]
            })
        );
    }

    #[test]
    fn test_bedmath_expression() {
        assert_eq!(
            expression("1 + 2 * 3 + 4 - 5").unwrap().1,
            // Should be (((1 + (2 * 3)) + 4) - 5)
            Expression::FunctionCall(FunctionCall {
                function_name: "-".to_string(),
                args: vec![
                    Expression::FunctionCall(FunctionCall {
                        function_name: "+".to_string(),
                        args: vec![
                            Expression::FunctionCall(FunctionCall {
                                function_name: "+".to_string(),
                                args: vec![
                                    Expression::Literal(Datum::from(1)),
                                    Expression::FunctionCall(FunctionCall {
                                        function_name: "*".to_string(),
                                        args: vec![
                                            Expression::Literal(Datum::from(2)),
                                            Expression::Literal(Datum::from(3)),
                                        ]
                                    })
                                ]
                            }),
                            Expression::Literal(Datum::from(4)),
                        ]
                    }),
                    Expression::Literal(Datum::from(5)),
                ]
            })
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
