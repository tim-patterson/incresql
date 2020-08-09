use crate::atoms::kw;
use crate::expression::named_expression;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::expr::NamedExpression;
use ast::rel::logical::{LogicalOperator, Project};
use nom::bytes::complete::tag;
use nom::combinator::{cut, map};
use nom::multi::separated_list;
use nom::sequence::{preceded, tuple};

/// Parses a select statement
pub fn select(input: &str) -> ParserResult<LogicalOperator> {
    map(
        preceded(
            kw("SELECT"),
            cut(preceded(ws_0, comma_sep_named_expressions)),
        ),
        |expressions| {
            LogicalOperator::Project(Project {
                distinct: false,
                expressions,
                source: Box::from(LogicalOperator::Single),
            })
        },
    )(input)
}

fn comma_sep_named_expressions(input: &str) -> ParserResult<Vec<NamedExpression>> {
    separated_list(tuple((ws_0, tag(","), ws_0)), named_expression)(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::Expression;

    #[test]
    fn test_select() {
        assert_eq!(
            select("SELECT 1,2 foo , 3 as bar").unwrap().1,
            LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![
                    NamedExpression {
                        expression: Expression::from(1),
                        alias: None
                    },
                    NamedExpression {
                        expression: Expression::from(2),
                        alias: Some(String::from("foo"))
                    },
                    NamedExpression {
                        expression: Expression::from(3),
                        alias: Some(String::from("bar"))
                    },
                ],
                source: Box::from(LogicalOperator::Single)
            })
        );
    }
}
