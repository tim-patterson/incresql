use nom::branch::alt;
use nom::bytes::complete::escaped_transform;
use nom::bytes::complete::{is_not, tag, tag_no_case, take, take_while};
use nom::combinator::{all_consuming, cut, map, map_res, opt, recognize, value};
use nom::error::context;
use nom::multi::many0;
use nom::sequence::{delimited, pair, preceded};
use nom::{AsChar, IResult};

/// Jsonpath utils.
/// Jsonpath expressions start at a single root and with each path section the expression
/// can return 0-* elements.  Due it not really forming a tree, we'll represent it with a
/// vector of these selectors with the root node being implicit.

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct JsonPathExpression {
    selectors: Vec<JsonPathSelector>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
enum JsonPathSelector {
    Wildcard,
    StringIdentifier(String),
    NumericIdentifier(i64),
}

impl JsonPathExpression {
    /// Parse the given expression and if valid returns the "compiled"
    /// expression
    pub(crate) fn parse(expression: &str) -> Option<JsonPathExpression> {
        parse_expression(expression)
            .ok()
            .map(|(_rest, selectors)| JsonPathExpression { selectors })
    }
}

type ParserResult<'a, T> = IResult<&'a str, T>;

fn parse_expression(input: &str) -> ParserResult<Vec<JsonPathSelector>> {
    all_consuming(preceded(tag("$"), many0(parse_selector)))(input)
}

fn parse_selector(input: &str) -> ParserResult<JsonPathSelector> {
    // Its always either dot or bracket notation, numbers only seem to be able to work via
    // bracket notation
    alt((
        value(JsonPathSelector::Wildcard, alt((tag(".*"), tag("[*]")))),
        map(delimited(tag("["), integer, tag("]")), |i| {
            JsonPathSelector::NumericIdentifier(i)
        }),
        map(delimited(tag("["), quoted_string, tag("]")), |s| {
            JsonPathSelector::StringIdentifier(s)
        }),
        map(preceded(tag("."), is_not(".[")), |s: &str| {
            JsonPathSelector::StringIdentifier(s.to_string())
        }),
    ))(input)
}

// Quoted String and Integer functions are lifted from the parser with some tweaks.

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
        delimited(
            tag("\""),
            escaped_transform(is_not("\"\\"), '\\', trans),
            cut(context("Missing closing double quote", tag("\""))),
        ),
    ))(input)
}

/// Parse an integer
pub fn integer(input: &str) -> ParserResult<i64> {
    map_res(
        recognize(pair(opt(tag("-")), take_while(|c: char| c.is_dec_digit()))),
        |s: &str| s.parse(),
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_only() {
        assert_eq!(parse_expression("$").unwrap().1, vec![])
    }

    #[test]
    fn test_numeric_selector() {
        assert_eq!(
            parse_expression("$[1]").unwrap().1,
            vec![JsonPathSelector::NumericIdentifier(1)]
        )
    }

    #[test]
    fn test_string_dot_selector() {
        assert_eq!(
            parse_expression("$.hello").unwrap().1,
            vec![JsonPathSelector::StringIdentifier("hello".to_string())]
        )
    }

    #[test]
    fn test_string_bracket_selector() {
        assert_eq!(
            parse_expression(r#"$["hello"]"#).unwrap().1,
            vec![JsonPathSelector::StringIdentifier("hello".to_string())]
        )
    }

    #[test]
    fn test_wildcard_dot_selector() {
        assert_eq!(
            parse_expression("$.*").unwrap().1,
            vec![JsonPathSelector::Wildcard]
        )
    }

    #[test]
    fn test_wildcard_bracket_selector() {
        assert_eq!(
            parse_expression(r#"$[*]"#).unwrap().1,
            vec![JsonPathSelector::Wildcard]
        )
    }

    #[test]
    fn test_compound_expression() {
        assert_eq!(
            parse_expression(r#"$.one["two"][2].*[*]"#).unwrap().1,
            vec![
                JsonPathSelector::StringIdentifier("one".to_string()),
                JsonPathSelector::StringIdentifier("two".to_string()),
                JsonPathSelector::NumericIdentifier(2),
                JsonPathSelector::Wildcard,
                JsonPathSelector::Wildcard
            ]
        )
    }
}
