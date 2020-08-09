use crate::atoms::kw;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::rel::statement::Statement;
use nom::combinator::{cut, value};
use nom::sequence::preceded;

/// Parses a show statement
pub fn show(input: &str) -> ParserResult<Statement> {
    value(
        Statement::ShowFunctions,
        preceded(kw("SHOW"), cut(preceded(ws_0, kw("FUNCTIONS")))),
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_functions() {
        assert_eq!(show("Show Functions").unwrap().1, Statement::ShowFunctions);
    }
}
