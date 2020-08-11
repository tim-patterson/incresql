use crate::atoms::{as_clause, identifier_str, kw};
use crate::literals::{datatype, literal};
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::expr::{Cast, ColumnReference, Expression, FunctionCall, NamedExpression};
use nom::branch::{alt, Alt};
use nom::bytes::complete::tag;
use nom::combinator::{cut, map};
use nom::error::VerboseError;
use nom::multi::{many0, separated_list};
use nom::sequence::{pair, preceded, tuple};

/// Parses a bog standard expression, ie 1 + 2
/// operators precedence according to https://dev.mysql.com/doc/refman/8.0/en/operator-precedence.html
pub fn expression(input: &str) -> ParserResult<Expression> {
    expression_0(input)
}

/// Parses a named expression, ie 1 as one
pub fn named_expression(input: &str) -> ParserResult<NamedExpression> {
    map(pair(expression, as_clause), |(expression, alias)| {
        NamedExpression { expression, alias }
    })(input)
}

/// Parse a comma separated list of expressions ie 1,2+2
pub fn comma_sep_expressions(input: &str) -> ParserResult<Vec<Expression>> {
    separated_list(tuple((ws_0, tag(","), ws_0)), expression)(input)
}

fn expression_0(input: &str) -> ParserResult<Expression> {
    infix_many((tag("="), tag("!=")), expression_1)(input)
}

fn expression_1(input: &str) -> ParserResult<Expression> {
    infix_many((tag("+"), tag("-")), expression_2)(input)
}

fn expression_2(input: &str) -> ParserResult<Expression> {
    infix_many((tag("*"), tag("/")), expression_3)(input)
}

fn expression_3(input: &str) -> ParserResult<Expression> {
    alt((function_call, cast, literal, column_reference))(input)
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

fn cast(input: &str) -> ParserResult<Expression> {
    preceded(
        kw("CAST"),
        cut(map(
            tuple((
                tuple((ws_0, tag("("), ws_0)),
                expression,
                tuple((ws_0, kw("AS"), ws_0)),
                datatype,
                pair(ws_0, tag(")")),
            )),
            |(_, expr, _, datatype, _)| {
                Expression::Cast(Cast {
                    expr: Box::new(expr),
                    datatype,
                })
            },
        )),
    )(input)
}

fn column_reference(input: &str) -> ParserResult<Expression> {
    alt((
        map(
            tuple((identifier_str, tag("."), identifier_str)),
            |(qualifier, _, alias)| {
                Expression::ColumnReference(ColumnReference {
                    qualifier: Some(qualifier),
                    alias,
                })
            },
        ),
        map(identifier_str, |alias| {
            Expression::ColumnReference(ColumnReference {
                qualifier: None,
                alias,
            })
        }),
    ))(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::{DataType, Datum};

    #[test]
    fn test_literal_expression() {
        assert_eq!(
            expression("NuLl").unwrap().1,
            Expression::Constant(Datum::Null, DataType::Null)
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
                args: vec![Expression::from(1), Expression::from(2),]
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
                                    Expression::from(1),
                                    Expression::FunctionCall(FunctionCall {
                                        function_name: "*".to_string(),
                                        args: vec![Expression::from(2), Expression::from(3),]
                                    })
                                ]
                            }),
                            Expression::from(4),
                        ]
                    }),
                    Expression::from(5),
                ]
            })
        );
    }

    #[test]
    fn test_named_expression() {
        let expression = Expression::Constant(Datum::Null, DataType::Null);
        assert_eq!(
            named_expression("NuLl").unwrap().1,
            NamedExpression {
                expression,
                alias: None
            }
        );

        let expression = Expression::Constant(Datum::Null, DataType::Null);
        assert_eq!(
            named_expression("NuLl foobar").unwrap().1,
            NamedExpression {
                expression,
                alias: Some(String::from("foobar"))
            }
        );

        let expression = Expression::Constant(Datum::Null, DataType::Null);
        assert_eq!(
            named_expression("NuLl as foobar").unwrap().1,
            NamedExpression {
                expression,
                alias: Some(String::from("foobar"))
            }
        );
    }

    #[test]
    fn test_cast() {
        let expr = Expression::Constant(Datum::Null, DataType::Null);
        assert_eq!(
            expression("cast( null as decimal(1,2))").unwrap().1,
            Expression::Cast(Cast {
                expr: Box::new(expr),
                datatype: DataType::Decimal(1, 2)
            })
        );
    }

    #[test]
    fn test_column_reference() {
        assert_eq!(
            expression("foo").unwrap().1,
            Expression::ColumnReference(ColumnReference {
                qualifier: None,
                alias: "foo".to_string()
            })
        );

        assert_eq!(
            expression("foo.bar").unwrap().1,
            Expression::ColumnReference(ColumnReference {
                qualifier: Some("foo".to_string()),
                alias: "bar".to_string()
            })
        );

        assert_eq!(
            expression("`foo`").unwrap().1,
            Expression::ColumnReference(ColumnReference {
                qualifier: None,
                alias: "foo".to_string()
            })
        );

        assert_eq!(
            expression("`foo`.`bar`").unwrap().1,
            Expression::ColumnReference(ColumnReference {
                qualifier: Some("foo".to_string()),
                alias: "bar".to_string()
            })
        );
    }
}
