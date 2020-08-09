use data::rust_decimal::prelude::ToPrimitive;
use data::rust_decimal::Decimal;
use data::{DataType, Datum};
use functions::{Function, FunctionSignature};
use std::cmp::max;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expression {
    Literal(Datum<'static>, DataType),
    FunctionCall(FunctionCall),
    Cast(Cast),
    CompiledFunctionCall(CompiledFunctionCall),
}

/// Represents a function call straight from the parser.
/// Ie the function isn't actually resolved by this point
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FunctionCall {
    pub function_name: String,
    pub args: Vec<Expression>,
}

/// Represents a sql cast, gets compiled to a function
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Cast {
    pub expr: Box<Expression>,
    pub datatype: DataType,
}

/// Represents a function call once its been resolved and type
/// checked
#[derive(Debug, Clone)]
pub struct CompiledFunctionCall {
    pub function: &'static dyn Function,
    pub args: Vec<Expression>,
    // Used to store the evaluation results of the sub expressions
    pub expr_buffer: Vec<Datum<'static>>,
    // Boxed to keep size of expression down
    pub signature: Box<FunctionSignature<'static>>,
}

impl PartialEq for CompiledFunctionCall {
    fn eq(&self, other: &Self) -> bool {
        self.args == other.args && self.signature == other.signature
    }
}

impl Eq for CompiledFunctionCall {}

/// Named expression, ie select foo as bar
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NamedExpression {
    pub alias: Option<String>,
    pub expression: Expression,
}

// Convenience helpers to construct expression literals
impl From<bool> for Expression {
    fn from(b: bool) -> Self {
        Expression::Literal(Datum::from(b), DataType::Boolean)
    }
}

impl From<i32> for Expression {
    fn from(i: i32) -> Self {
        Expression::Literal(Datum::from(i), DataType::Integer)
    }
}

impl From<i64> for Expression {
    fn from(i: i64) -> Self {
        Expression::Literal(Datum::from(i), DataType::BigInt)
    }
}

impl From<Decimal> for Expression {
    fn from(d: Decimal) -> Self {
        let s = d.scale() as u8;
        // A bit yuk, there's no integer log10 yet
        let mut p = 0;
        let mut temp = d.to_i128().unwrap().abs();
        while temp != 0 {
            p += 1;
            temp /= 10;
        }
        p = max(p + s, 1);
        Expression::Literal(Datum::from(d), DataType::Decimal(p, s))
    }
}

impl From<&'static str> for Expression {
    fn from(s: &'static str) -> Self {
        Expression::Literal(Datum::from(s), DataType::Text)
    }
}

impl From<String> for Expression {
    fn from(s: String) -> Self {
        Expression::Literal(Datum::from(s), DataType::Text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr_from_boolean() {
        assert_eq!(
            Expression::from(true),
            Expression::Literal(Datum::Boolean(true), DataType::Boolean)
        );
        assert_eq!(
            Expression::from(false),
            Expression::Literal(Datum::Boolean(false), DataType::Boolean)
        );
    }

    #[test]
    fn test_expr_from_integer() {
        assert_eq!(
            Expression::from(1234),
            Expression::Literal(Datum::Integer(1234), DataType::Integer)
        );
    }

    #[test]
    fn test_expr_from_bigint() {
        assert_eq!(
            Expression::from(1234_i64),
            Expression::Literal(Datum::BigInt(1234), DataType::BigInt)
        );
    }

    #[test]
    fn test_expr_from_decimal() {
        assert_eq!(
            Expression::from(Decimal::new(12345, 2)),
            Expression::Literal(
                Datum::Decimal(Decimal::new(12345, 2)),
                DataType::Decimal(5, 2)
            )
        );

        assert_eq!(
            Expression::from(Decimal::new(-12345, 2)),
            Expression::Literal(
                Datum::Decimal(Decimal::new(-12345, 2)),
                DataType::Decimal(5, 2)
            )
        );

        assert_eq!(
            Expression::from(Decimal::new(0, 1)),
            Expression::Literal(Datum::Decimal(Decimal::new(0, 1)), DataType::Decimal(1, 1))
        );

        assert_eq!(
            Expression::from(Decimal::new(1234, 4)),
            Expression::Literal(
                Datum::Decimal(Decimal::new(1234, 4)),
                DataType::Decimal(4, 4)
            )
        );
    }

    #[test]
    fn test_expr_from_string() {
        assert_eq!(
            Expression::from(String::from("Hello world")),
            Expression::Literal(
                Datum::TextOwned(String::from("Hello world").into_boxed_str()),
                DataType::Text
            )
        );
    }
}
