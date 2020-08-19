use crate::atoms::{identifier_str, kw};
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::rel::statement::Statement;
use nom::combinator::{cut, map};
use nom::sequence::{preceded, tuple};

/// Parses a drop statement
pub fn drop_(input: &str) -> ParserResult<Statement> {
    preceded(
        kw("DROP"),
        cut(map(
            tuple((ws_0, kw("DATABASE"), ws_0, identifier_str)),
            |(_, _, _, database)| Statement::DropDatabase(database),
        )),
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_database() {
        assert_eq!(
            drop_("drop database foo").unwrap().1,
            Statement::DropDatabase("foo".to_string())
        );
    }
}
