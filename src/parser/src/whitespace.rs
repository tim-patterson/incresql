use crate::ParserResult;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_until};
use nom::character::complete::{multispace1, not_line_ending};
use nom::combinator::value;
use nom::multi::many0;
use nom::sequence::{pair, tuple};

/// Like multispace 0 but also handles comments etc
pub fn ws_0(input: &str) -> ParserResult<()> {
    value(
        (),
        many0(alt((line_comment, block_comment, value((), multispace1)))),
    )(input)
}

pub fn line_comment(input: &str) -> ParserResult<()> {
    value((), pair(tag("--"), not_line_ending))(input)
}

pub fn block_comment(input: &str) -> ParserResult<()> {
    value((), tuple((tag("/*"), take_until("*/"), tag("*/"))))(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_line_comment() {
        assert_eq!(
            line_comment("-- this is a comment\n abc").unwrap().0,
            "\n abc"
        );
    }

    #[test]
    fn test_block_comment() {
        assert_eq!(
            block_comment("/* this * is / a /* comment */abc")
                .unwrap()
                .0,
            "abc"
        );
    }

    #[test]
    fn test_ws_0() {
        assert_eq!(ws_0("abc").unwrap().0, "abc");

        assert_eq!(ws_0(" abc").unwrap().0, "abc");

        assert_eq!(ws_0(" \nabc").unwrap().0, "abc");

        assert_eq!(ws_0("-- some comment\nabc").unwrap().0, "abc");
    }
}
