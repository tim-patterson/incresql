use crate::ParserResult;
use data::Decimal;
use nom::branch::alt;
use nom::bytes::complete::{
    escaped_transform, is_not, tag, tag_no_case, take, take_while, take_while1,
};
use nom::character::complete::alphanumeric1;
use nom::combinator::{cut, map, map_res, not, opt, peek, recognize, value};
use nom::error::{context, ErrorKind, VerboseError, VerboseErrorKind};
use nom::sequence::{delimited, pair, terminated, tuple};
use nom::AsChar;
use std::str::FromStr;

/// String's are double or single quoted
pub fn quoted_string(input: &str) -> ParserResult<String> {
    // function required to convert escaped char to what it represents
    fn trans(input: &str) -> ParserResult<&str> {
        alt((
            map(tag("n"), |_| "\n"),
            map(tag("r"), |_| "\r"),
            map(tag("t"), |_| "\t"),
            take(1_usize), // covers " \ etc, bogus escapes will just be replaced with the literal letter
        ))(input)
    }

    alt((
        // is_not wont return anything for zero length string :(
        value(String::new(), tag_no_case("\"\"")),
        value(String::new(), tag_no_case("''")),
        delimited(
            tag("\""),
            escaped_transform(is_not("\"\\"), '\\', trans),
            cut(context("Missing closing double quote", tag("\""))),
        ),
        delimited(
            tag("'"),
            escaped_transform(is_not("\'\\"), '\\', trans),
            cut(context("Missing closing quote", tag("'"))),
        ),
    ))(input)
}

/// Parse an integer
pub fn integer(input: &str) -> ParserResult<i64> {
    map_res(
        pair(
            opt(tag("-")),
            // Take .'s and then fail in result so we don't wrongly parse just the start of a float
            take_while(|c: char| c.is_dec_digit() || c == '.'),
        ),
        |(neg, digits)| (neg.unwrap_or("").to_string() + digits).parse(),
    )(input)
}

/// Parse a decimal
pub fn decimal(input: &str) -> ParserResult<Decimal> {
    map(
        recognize(tuple((
            opt(tag("-")),
            alt((
                // optionally a whole number followed by point and frac(required)
                recognize(tuple((
                    take_while(|c: char| c.is_dec_digit()),
                    tag("."),
                    take_while1(|c: char| c.is_dec_digit()),
                ))),
                // Just an integer
                take_while1(|c: char| c.is_dec_digit()),
            )),
        ))),
        |s| Decimal::from_str(s).unwrap(),
    )(input)
}

/// Eof parser
fn eof(input: &str) -> ParserResult<()> {
    if input.is_empty() {
        Ok((input, ()))
    } else {
        Err(nom::Err::Error(VerboseError {
            errors: vec![(input, VerboseErrorKind::Nom(ErrorKind::NonEmpty))],
        }))
    }
}

/// A wrapper around tag_no_case that also ensures that we don't just
/// take half a word, ie to ensure we don't parse "nulls_removed" as NULL
/// and then have our parser blow up with some error about "s_removed"
pub fn kw(keyword: &'static str) -> impl Fn(&str) -> ParserResult<&str> {
    move |input| {
        terminated(
            tag_no_case(keyword),
            peek(alt((
                not(alt((alphanumeric1, tag("_"), tag("$"), tag("@")))),
                eof,
            ))),
        )(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_double_quoted_string() {
        assert_eq!(
            quoted_string(r#""My String \"\t \n\a""#).unwrap().1,
            "My String \"\t \na".to_string()
        );

        quoted_string("").expect_err("Expected to fail");
    }

    #[test]
    fn test_quoted_string_no_input() {
        quoted_string("").expect_err("Expected to fail");
    }

    #[test]
    fn test_empty_quoted_string() {
        assert_eq!(quoted_string(r#""""#).unwrap().1, "".to_string());
    }

    #[test]
    fn test_single_quoted_string() {
        assert_eq!(
            quoted_string(r#"'My String "\'\t \n\a'"#).unwrap().1,
            "My String \"'\t \na".to_string()
        );
    }

    #[test]
    fn test_unclosed_string() {
        assert!(quoted_string(r#""My String \"\t \n\a"#)
            .unwrap_err()
            .to_string()
            .contains("Missing closing double quote"));
    }

    #[test]
    fn test_unclosed_empty_string() {
        assert!(quoted_string(r#"""#)
            .unwrap_err()
            .to_string()
            .contains("Missing closing double quote"));
    }

    #[test]
    fn test_integer() {
        assert_eq!(integer("123").unwrap().1, 123);

        integer("").expect_err("Expected to fail");
    }

    #[test]
    fn test_integer_negative() {
        assert_eq!(integer("-123").unwrap().1, -123);
    }

    #[test]
    fn test_integer_float() {
        integer("123.1").expect_err("Expected to fail");
    }

    #[test]
    fn test_decimal() {
        assert_eq!(decimal("123").unwrap().1, Decimal::from_str("123").unwrap());
        assert_eq!(
            decimal("123.456").unwrap().1,
            Decimal::from_str("123.456").unwrap()
        );
        assert_eq!(
            decimal("-123.456").unwrap().1,
            Decimal::from_str("-123.456").unwrap()
        );

        assert_eq!(
            decimal(".456").unwrap().1,
            Decimal::from_str(".456").unwrap()
        );

        decimal("").expect_err("Expected to fail");
    }

    #[test]
    fn test_eof() {
        assert_eq!(eof("").unwrap().1, ());

        eof("123.1").expect_err("Expected to fail");
    }

    #[test]
    fn test_kw() {
        assert_eq!(kw("null")("null").unwrap().1, "null");

        assert_eq!(kw("null")("null foo").unwrap().1, "null");

        kw("null")("nulls_removed").expect_err("Expected to fail");
    }
}
