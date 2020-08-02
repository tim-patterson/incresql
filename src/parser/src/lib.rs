use nom::error::VerboseError;
use nom::IResult;

mod atoms;
mod expression;
mod literals;
mod whitespace;

type ParserResult<'a, T> = IResult<&'a str, T, VerboseError<&'a str>>;
