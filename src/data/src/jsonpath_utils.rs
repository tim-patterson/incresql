use crate::json::{Json, JsonType};
use nom::branch::alt;
use nom::bytes::complete::escaped_transform;
use nom::bytes::complete::{is_not, tag, tag_no_case, take, take_while};
use nom::combinator::{all_consuming, cut, map, map_res, opt, recognize, value};
use nom::error::context;
use nom::lib::std::cmp::Ordering;
use nom::lib::std::fmt::Formatter;
use nom::multi::many0;
use nom::sequence::{delimited, pair, preceded};
use nom::{AsChar, IResult};
use std::fmt::Display;

/// Jsonpath utils.
/// Jsonpath expressions start at a single root and with each path section the expression
/// can return 0-* elements.  Due it not really forming a tree, we'll represent it with a
/// vector of these selectors with the root node being implicit.

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct JsonPathExpression {
    selectors: Vec<JsonPathSelector>,
    original: String,
}

impl JsonPathExpression {
    /// Parse the given expression and if valid returns the "compiled"
    /// expression
    pub fn parse(expression: &str) -> Option<JsonPathExpression> {
        parse_expression(expression)
            .ok()
            .map(|(_rest, selectors)| JsonPathExpression {
                selectors,
                original: expression.to_string(),
            })
    }

    /// If the json path expression could return more than one value when evaluated then
    /// this will return true. Needed as the mysql behaviour for functions like json_extract
    /// is to return values wrapped in a json array if this is true, otherwise to return
    /// the singular value (or null)
    pub fn could_return_many(&self) -> bool {
        self.selectors
            .iter()
            .any(|selector| selector == &JsonPathSelector::Wildcard)
    }

    /// Evaluates the given jsonpath and calls a call back for each match.
    pub fn evaluate<'a, 'b: 'a, F: FnMut(Json<'b>)>(&'a self, json: Json<'b>, f: &mut F) {
        if self.selectors.is_empty() {
            f(json)
        } else {
            self.selectors[0].evaluate(json, &self.selectors[1..], f);
        }
    }

    /// Returns the first match if one exists
    pub fn evaluate_single<'b>(&self, json: Json<'b>) -> Option<Json<'b>> {
        let mut result = None;
        self.evaluate(json, &mut (|j| result = Some(j)));
        result
    }

    pub fn original(&self) -> &str {
        &self.original
    }
}

impl Display for JsonPathExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("'{}'", self.original))
    }
}

impl Ord for JsonPathExpression {
    fn cmp(&self, other: &Self) -> Ordering {
        self.original.cmp(&other.original)
    }
}

impl PartialOrd for JsonPathExpression {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
enum JsonPathSelector {
    Wildcard,
    StringIdentifier(String),
    NumericIdentifier(i64),
}

impl JsonPathSelector {
    /// Evaluate the given selector, calling the call back function on any matches.
    pub fn evaluate<'a, 'b: 'a, F: FnMut(Json<'b>)>(
        &'a self,
        input: Json<'b>,
        rest: &[JsonPathSelector],
        f: &mut F,
    ) {
        match input.json_type() {
            JsonType::Object => {
                let kv_iter = input.iter_object().unwrap();
                match self {
                    JsonPathSelector::Wildcard => {
                        for (_, v) in kv_iter {
                            if rest.is_empty() {
                                f(v);
                            } else {
                                rest[0].evaluate(v, &rest[1..], f);
                            }
                        }
                    }
                    JsonPathSelector::StringIdentifier(str) => {
                        for (k, v) in kv_iter {
                            if k.eq_ignore_ascii_case(str) {
                                if rest.is_empty() {
                                    f(v);
                                } else {
                                    rest[0].evaluate(v, &rest[1..], f);
                                }
                            }
                        }
                    }
                    JsonPathSelector::NumericIdentifier(idx) => {
                        // This seems to match the behaviour of of other jsonpath implementations.
                        // I think its because in JS arrays are semantically objects with the indexes
                        // as keys
                        for (k, v) in kv_iter {
                            if k.eq(&idx.to_string()) {
                                if rest.is_empty() {
                                    f(v);
                                } else {
                                    rest[0].evaluate(v, &rest[1..], f);
                                }
                            }
                        }
                    }
                }
            }
            JsonType::Array => {
                let v_iter = input.iter_array().unwrap();

                match self {
                    JsonPathSelector::Wildcard => {
                        for v in v_iter {
                            if rest.is_empty() {
                                f(v);
                            } else {
                                rest[0].evaluate(v, &rest[1..], f);
                            }
                        }
                    }
                    JsonPathSelector::StringIdentifier(s) => {
                        if let Ok(i) = s.parse::<i64>() {
                            if i >= 0 {
                                for (idx, v) in v_iter.enumerate() {
                                    if idx == i as usize {
                                        if rest.is_empty() {
                                            f(v);
                                        } else {
                                            rest[0].evaluate(v, &rest[1..], f);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    JsonPathSelector::NumericIdentifier(i) => {
                        if *i >= 0 {
                            for (idx, v) in v_iter.enumerate() {
                                if idx == *i as usize {
                                    if rest.is_empty() {
                                        f(v);
                                    } else {
                                        rest[0].evaluate(v, &rest[1..], f);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
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
    use crate::json::OwnedJson;

    #[test]
    fn test_root_only() {
        let expr = JsonPathExpression::parse("$").unwrap();
        assert_eq!(
            expr,
            JsonPathExpression {
                selectors: vec![],
                original: "$".to_string()
            }
        );
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
                selectors: vec![JsonPathSelector::NumericIdentifier(1)],
                original: "$[1]".to_string()
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
                selectors: vec![JsonPathSelector::NumericIdentifier(1)],
                original: "$.1".to_string()
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
                selectors: vec![JsonPathSelector::StringIdentifier("hello".to_string())],
                original: "$.hello".to_string()
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
                selectors: vec![JsonPathSelector::StringIdentifier("hello".to_string())],
                original: r#"$["hello"]"#.to_string()
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
                selectors: vec![JsonPathSelector::Wildcard],
                original: "$.*".to_string()
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
                selectors: vec![JsonPathSelector::Wildcard],
                original: "$[*]".to_string()
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
                ],
                original: r#"$.one["two"][2].1.*[*]"#.to_string()
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
