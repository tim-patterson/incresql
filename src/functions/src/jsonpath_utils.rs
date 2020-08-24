use data::json::{Json, JsonType};
use nom::branch::alt;
use nom::bytes::complete::escaped_transform;
use nom::bytes::complete::{is_not, tag, tag_no_case, take, take_while};
use nom::combinator::{all_consuming, cut, map, map_res, opt, recognize, value};
use nom::error::context;
use nom::lib::std::iter::once;
use nom::multi::many0;
use nom::sequence::{delimited, pair, preceded};
use nom::{AsChar, IResult};
use std::iter::empty;

/// Jsonpath utils.
/// Jsonpath expressions start at a single root and with each path section the expression
/// can return 0-* elements.  Due it not really forming a tree, we'll represent it with a
/// vector of these selectors with the root node being implicit.

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct JsonPathExpression {
    selectors: Vec<JsonPathSelector>,
}

impl JsonPathExpression {
    /// Parse the given expression and if valid returns the "compiled"
    /// expression
    pub(crate) fn parse(expression: &str) -> Option<JsonPathExpression> {
        parse_expression(expression)
            .ok()
            .map(|(_rest, selectors)| JsonPathExpression { selectors })
    }

    /// If the json path expression could return more than one value when evaluated then
    /// this will return true. Needed as the mysql behaviour for functions like json_extract
    /// is to return values wrapped in a json array if this is true, otherwise to return
    /// the singular value (or null)
    pub(crate) fn could_return_many(&self) -> bool {
        self.selectors
            .iter()
            .any(|selector| selector == &JsonPathSelector::Wildcard)
    }

    /// Evaluates the given jsonpath and returns an iterator over the matches
    pub(crate) fn evaluate<'a, 'b: 'a>(
        &'a self,
        json: Json<'b>,
    ) -> Box<dyn Iterator<Item = Json<'b>> + 'a> {
        let root: Box<dyn Iterator<Item = Json>> = Box::from(once(json));
        self.selectors.iter().fold(root, |input, selector| {
            Box::from(input.flat_map(move |node| selector.evaluate(node)))
        })
    }

    /// Returns the first match if one exists
    pub(crate) fn evaluate_single<'b>(&self, json: Json<'b>) -> Option<Json<'b>> {
        self.evaluate(json).next()
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
enum JsonPathSelector {
    Wildcard,
    StringIdentifier(String),
    NumericIdentifier(i64),
}

impl JsonPathSelector {
    pub fn evaluate<'a, 'b: 'a>(
        &'a self,
        input: Json<'b>,
    ) -> Box<dyn Iterator<Item = Json<'b>> + 'a> {
        match input.json_type() {
            JsonType::Object => {
                let kv_iter = input.iter_object().unwrap();
                match self {
                    JsonPathSelector::Wildcard => Box::from(kv_iter.map(|(_k, v)| v)),
                    JsonPathSelector::StringIdentifier(str) => {
                        Box::from(kv_iter.filter_map(move |(k, v)| {
                            if k.eq_ignore_ascii_case(str) {
                                Some(v)
                            } else {
                                None
                            }
                        }))
                    }
                    JsonPathSelector::NumericIdentifier(idx) => {
                        // This seems to match the behaviour of of other jsonpath implementations.
                        // I think its because in JS arrays are semantically objects with the indexes
                        // as keys
                        Box::from(kv_iter.filter_map(move |(k, v)| {
                            if k.eq(&idx.to_string()) {
                                Some(v)
                            } else {
                                None
                            }
                        }))
                    }
                }
            }
            JsonType::Array => {
                let v_iter = input.iter_array().unwrap();

                match self {
                    JsonPathSelector::Wildcard => Box::from(v_iter),
                    JsonPathSelector::StringIdentifier(s) => {
                        if let Ok(i) = s.parse::<i64>() {
                            if i < 0 {
                                Box::from(empty())
                            } else {
                                Box::from(v_iter.enumerate().filter_map(move |(idx, v)| {
                                    if idx == i as usize {
                                        Some(v)
                                    } else {
                                        None
                                    }
                                }))
                            }
                        } else {
                            Box::from(empty())
                        }
                    }
                    JsonPathSelector::NumericIdentifier(i) => {
                        if *i < 0 {
                            Box::from(empty())
                        } else {
                            Box::from(v_iter.enumerate().filter_map(move |(idx, v)| {
                                if idx == *i as usize {
                                    Some(v)
                                } else {
                                    None
                                }
                            }))
                        }
                    }
                }
            }
            _ => Box::from(empty()),
        }
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
        map(preceded(tag("."), integer), |i: i64| {
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
    use data::json::OwnedJson;

    #[test]
    fn test_root_only() {
        let expr = JsonPathExpression::parse("$").unwrap();
        assert_eq!(expr, JsonPathExpression { selectors: vec![] });
        assert_eq!(expr.could_return_many(), false);

        let input = OwnedJson::parse("123").unwrap();
        assert_eq!(expr.evaluate_single(input.as_json()), Some(input.as_json()))
    }

    #[test]
    fn test_numeric_selector() {
        let expr = JsonPathExpression::parse("$[1]").unwrap();
        assert_eq!(
            expr,
            JsonPathExpression {
                selectors: vec![JsonPathSelector::NumericIdentifier(1)]
            }
        );
        assert_eq!(expr.could_return_many(), false);

        // Test index into array
        let input = OwnedJson::parse("[1,2,3]").unwrap();
        let expected = OwnedJson::parse("2").unwrap();
        assert_eq!(
            expr.evaluate_single(input.as_json()),
            Some(expected.as_json())
        );
    }

    #[test]
    fn test_numeric_dot_selector() {
        let expr = JsonPathExpression::parse("$.1").unwrap();
        assert_eq!(
            expr,
            JsonPathExpression {
                selectors: vec![JsonPathSelector::NumericIdentifier(1)]
            }
        );
        assert_eq!(expr.could_return_many(), false);
        // Test index into object
        let input = OwnedJson::parse(r#"{"k": "v", "1": "foo"}"#).unwrap();
        let expected = OwnedJson::parse(r#""foo""#).unwrap();
        assert_eq!(
            expr.evaluate_single(input.as_json()),
            Some(expected.as_json())
        );
    }

    #[test]
    fn test_string_dot_selector() {
        let expr = JsonPathExpression::parse("$.hello").unwrap();
        assert_eq!(
            expr,
            JsonPathExpression {
                selectors: vec![JsonPathSelector::StringIdentifier("hello".to_string())]
            }
        );
        assert_eq!(expr.could_return_many(), false);
        // Test index into object
        let input = OwnedJson::parse(r#"{"1": "foo", "hello": "world"}"#).unwrap();
        let expected = OwnedJson::parse(r#""world""#).unwrap();
        assert_eq!(
            expr.evaluate_single(input.as_json()),
            Some(expected.as_json())
        );

        // Test index into array
        let expr2 = JsonPathExpression::parse(r#"$["1"]"#).unwrap();
        let input = OwnedJson::parse(r#"["a","b","c"]"#).unwrap();
        let expected = OwnedJson::parse(r#""b""#).unwrap();
        assert_eq!(
            expr2.evaluate_single(input.as_json()),
            Some(expected.as_json())
        );
    }

    #[test]
    fn test_string_bracket_selector() {
        let expr = JsonPathExpression::parse(r#"$["hello"]"#).unwrap();
        assert_eq!(
            expr,
            JsonPathExpression {
                selectors: vec![JsonPathSelector::StringIdentifier("hello".to_string())]
            }
        );
        assert_eq!(expr.could_return_many(), false);
    }

    #[test]
    fn test_wildcard_dot_selector() {
        let expr = JsonPathExpression::parse("$.*").unwrap();
        assert_eq!(
            expr,
            JsonPathExpression {
                selectors: vec![JsonPathSelector::Wildcard]
            }
        );
        assert_eq!(expr.could_return_many(), true);
        // Test object
        let input = OwnedJson::parse(r#"{"1": "foo"}"#).unwrap();
        let expected = OwnedJson::parse(r#""foo""#).unwrap();
        assert_eq!(
            expr.evaluate_single(input.as_json()),
            Some(expected.as_json())
        );
    }

    #[test]
    fn test_wildcard_bracket_selector() {
        let expr = JsonPathExpression::parse(r#"$[*]"#).unwrap();
        assert_eq!(
            expr,
            JsonPathExpression {
                selectors: vec![JsonPathSelector::Wildcard]
            }
        );
        assert_eq!(expr.could_return_many(), true);

        // Test array
        let input = OwnedJson::parse(r#"["foo"]"#).unwrap();
        let expected = OwnedJson::parse(r#""foo""#).unwrap();
        assert_eq!(
            expr.evaluate_single(input.as_json()),
            Some(expected.as_json())
        );
    }

    #[test]
    fn test_compound_expression() {
        let expr = JsonPathExpression::parse(r#"$.one["two"][2].1.*[*]"#).unwrap();
        assert_eq!(
            expr,
            JsonPathExpression {
                selectors: vec![
                    JsonPathSelector::StringIdentifier("one".to_string()),
                    JsonPathSelector::StringIdentifier("two".to_string()),
                    JsonPathSelector::NumericIdentifier(2),
                    JsonPathSelector::NumericIdentifier(1),
                    JsonPathSelector::Wildcard,
                    JsonPathSelector::Wildcard
                ]
            }
        );
        assert_eq!(expr.could_return_many(), true);

        let input = OwnedJson::parse(r#"{"one": {"two": [0,1,[0,[["nested"]]]]} }"#).unwrap();
        let expected = OwnedJson::parse(r#""nested""#).unwrap();
        assert_eq!(
            expr.evaluate_single(input.as_json()),
            Some(expected.as_json())
        );
    }
}
