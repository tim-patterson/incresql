use crate::whitespace::ws_0;
use crate::ParserResult;
use data::rust_decimal::Decimal;
use nom::branch::alt;
use nom::bytes::complete::{
    escaped_transform, is_not, tag, tag_no_case, take, take_until, take_while, take_while1,
    take_while_m_n,
};
use nom::character::complete::alphanumeric1;
use nom::combinator::{cut, map, map_res, not, opt, peek, recognize, value};
use nom::error::{context, ErrorKind, VerboseError, VerboseErrorKind};
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
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
        recognize(pair(
            opt(tag("-")),
            // Take .'s and then fail in result so we don't wrongly parse just the start of a float
            take_while(|c: char| c.is_dec_digit() || c == '.'),
        )),
        |s: &str| s.parse(),
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

/// Parse an identifier string, to avoid ambiguity a non quoted identifier must not have any
/// embedded mathematical operators etc in it.
/// A purely numeric identifier would also cause ambiguity so we're enforce that the first char
/// should be non-numeric, while we will allow using some keywords as identifiers in some cases we
/// need to exclude these to allow unambiguous parsing.
/// Alternatively backticks can be used to quote the identifiers, will lowercase all identifiers
pub fn identifier_str(input: &str) -> ParserResult<String> {
    map(
        alt((
            recognize(preceded(
                // These basically need to be the list of valid keywords that can appear
                // after a table name
                not(peek(alt((
                    kw("FROM"),
                    kw("WHERE"),
                    kw("ORDER"),
                    kw("UNION"),
                    kw("LIMIT"),
                    kw("GROUP"),
                    kw("JOIN"),
                    kw("LEFT"),
                    kw("RIGHT"),
                    kw("INNER"),
                    kw("OUTER"),
                    kw("FULL"),
                    kw("ON"),
                    kw("IS"),
                )))),
                pair(
                    take_while_m_n(1, 1, |c: char| {
                        c.is_alpha() || c == '_' || c == '$' || c == '@'
                    }),
                    take_while(|c: char| c.is_alphanumeric() || c == '_' || c == '$' || c == '@'),
                ),
            )),
            delimited(
                tag("`"),
                take_until("`"),
                cut(context("Missing closing backtick on identifier", tag("`"))),
            ),
        )),
        |s| s.to_lowercase(),
    )(input)
}

/// The as clause for expressions, tables etc.
/// Consumes leading white space if there's a successful match
pub fn as_clause(input: &str) -> ParserResult<Option<String>> {
    opt(preceded(
        pair(opt(pair(ws_0, kw("AS"))), ws_0),
        identifier_str,
    ))(input)
}

pub fn qualified_reference(input: &str) -> ParserResult<(Option<String>, String)> {
    alt((
        map(
            tuple((identifier_str, tag("."), identifier_str)),
            |(qualifier, _, alias)| (Some(qualifier), alias),
        ),
        map(identifier_str, |alias| (None, alias)),
    ))(input)
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

    #[test]
    fn test_identifier_string() {
        assert_eq!(identifier_str("abcC123").unwrap().1, "abcc123");

        assert_eq!(identifier_str("abcC123 fsd").unwrap().1, "abcc123");

        assert!(identifier_str("1bcC123 fsd").is_err());

        assert_eq!(identifier_str("`1bcC123 fsd`").unwrap().1, "1bcc123 fsd");
    }

    #[test]
    fn test_as_clause() {
        assert_eq!(as_clause("").unwrap().1, None);

        assert_eq!(as_clause("foo").unwrap().1, Some(String::from("foo")));

        assert_eq!(as_clause("as foo").unwrap().1, Some(String::from("foo")));

        assert_eq!(as_clause("as `foo`").unwrap().1, Some(String::from("foo")));
    }

    #[test]
    fn test_qualified_reference() {
        assert_eq!(
            qualified_reference("foo").unwrap().1,
            (None, "foo".to_string())
        );

        assert_eq!(
            qualified_reference("foo.bar").unwrap().1,
            (Some("foo".to_string()), "bar".to_string())
        );
    }
}
