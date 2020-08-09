use crate::atoms::{decimal, integer, kw, quoted_string};
use crate::ParserResult;
use data::Datum;
use nom::branch::alt;
use nom::combinator::{map, value};

pub fn literal(input: &str) -> ParserResult<Datum<'static>> {
    alt((null_literal, boolean_literal, number_literal, text_literal))(input)
}

fn null_literal(input: &str) -> ParserResult<Datum<'static>> {
    value(Datum::Null, kw("NULL"))(input)
}

fn boolean_literal(input: &str) -> ParserResult<Datum<'static>> {
    alt((
        value(Datum::from(true), kw("TRUE")),
        value(Datum::from(false), kw("FALSE")),
    ))(input)
}

fn number_literal(input: &str) -> ParserResult<Datum<'static>> {
    // Our casts will promote ints -> bigints -> decimals -> floats so that should be the preference
    // for parsing numbers, we may not actually need to parse floats unless we support NaN/Inf etc
    alt((
        map(integer, |i| {
            if std::i32::MIN as i64 <= i && i <= std::i32::MAX as i64 {
                Datum::from(i as i32)
            } else {
                Datum::from(i)
            }
        }),
        map(decimal, Datum::from),
    ))(input)
}

fn text_literal(input: &str) -> ParserResult<Datum<'static>> {
    map(quoted_string, Datum::from)(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_null_literal() {
        assert_eq!(literal("NuLl").unwrap().1, Datum::Null);
    }

    #[test]
    fn test_boolean_literal() {
        assert_eq!(literal("true").unwrap().1, Datum::from(true));

        assert_eq!(literal("false").unwrap().1, Datum::from(false));
    }

    #[test]
    fn test_number_literal() {
        assert_eq!(literal("123").unwrap().1, Datum::from(123));
        assert_eq!(
            literal("3000000000").unwrap().1,
            Datum::from(3000000000_i64)
        );
        assert_eq!(
            literal("123.456").unwrap().1,
            Datum::from(Decimal::from_str("123.456").unwrap())
        );
    }

    #[test]
    fn test_text_literal() {
        assert_eq!(
            literal("'Hello world'").unwrap().1,
            Datum::from("Hello world".to_string())
        );
    }
}
