use crate::atoms::{decimal, integer, kw, quoted_string};
use crate::whitespace::ws_0;
use crate::ParserResult;
use data::DataType::Decimal;
use data::{DataType, Datum, DECIMAL_MAX_PRECISION};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{map, value};
use nom::sequence::tuple;

pub fn literal(input: &str) -> ParserResult<Datum<'static>> {
    alt((null_literal, boolean_literal, number_literal, text_literal))(input)
}

pub fn datatype(input: &str) -> ParserResult<DataType> {
    alt((
        value(DataType::Boolean, kw("BOOLEAN")),
        value(DataType::Integer, kw("INTEGER")),
        value(DataType::Integer, kw("INT")),
        value(DataType::BigInt, kw("BIGINT")),
        map(
            tuple((
                tuple((kw("DECIMAL"), ws_0, tag("("), ws_0)),
                integer,
                tuple((ws_0, tag(","), ws_0)),
                integer,
                ws_0,
                tag(")"),
            )),
            |(_, p, _, s, _, _)| Decimal(p as u8, s as u8),
        ),
        map(
            tuple((
                tuple((kw("DECIMAL"), ws_0, tag("("), ws_0)),
                integer,
                ws_0,
                tag(")"),
            )),
            |(_, p, _, _)| Decimal(p as u8, 0),
        ),
        value(DataType::Decimal(DECIMAL_MAX_PRECISION, 0), kw("DECIMAL")),
        value(DataType::Text, kw("TEXT")),
    ))(input)
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

    #[test]
    fn test_simple_datatype_literals() {
        assert_eq!(datatype("boolean").unwrap().1, DataType::Boolean);

        assert_eq!(datatype("int").unwrap().1, DataType::Integer);

        assert_eq!(datatype("integer").unwrap().1, DataType::Integer);

        assert_eq!(datatype("bigint").unwrap().1, DataType::BigInt);

        assert_eq!(datatype("text").unwrap().1, DataType::Text);
    }

    #[test]
    fn test_decimal_datatype_literals() {
        assert_eq!(
            datatype("decimal").unwrap().1,
            DataType::Decimal(DECIMAL_MAX_PRECISION, 0)
        );

        assert_eq!(datatype("decimal(10)").unwrap().1, DataType::Decimal(10, 0));

        assert_eq!(
            datatype("decimal ( 10 , 2 )").unwrap().1,
            DataType::Decimal(10, 2)
        );
    }
}
