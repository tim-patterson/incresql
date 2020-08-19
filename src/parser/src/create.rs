use crate::atoms::{identifier_str, kw};
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::rel::statement::{CreateDatabase, Statement};
use nom::combinator::{cut, map};
use nom::sequence::{preceded, tuple};

/// Parses a create statement
pub fn create(input: &str) -> ParserResult<Statement> {
    preceded(
        kw("CREATE"),
        cut(map(
            tuple((ws_0, kw("DATABASE"), ws_0, identifier_str)),
            |(_, _, _, database)| Statement::CreateDatabase(CreateDatabase { name: database }),
        )),
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_database() {
        assert_eq!(
            create("Create database foo").unwrap().1,
            Statement::CreateDatabase(CreateDatabase {
                name: "foo".to_string()
            })
        );
    }
}
