use crate::atoms::{identifier_str, kw, qualified_reference};
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::statement::{DropTable, Statement};
use nom::branch::alt;
use nom::combinator::{cut, map};
use nom::sequence::{preceded, tuple};

/// Parses a drop statement
pub fn drop_(input: &str) -> ParserResult<Statement> {
    preceded(kw("DROP"), cut(alt((database, table))))(input)
}

fn database(input: &str) -> ParserResult<Statement> {
    map(
        tuple((ws_0, kw("DATABASE"), ws_0, identifier_str)),
        |(_, _, _, database)| Statement::DropDatabase(database),
    )(input)
}

fn table(input: &str) -> ParserResult<Statement> {
    map(
        tuple((ws_0, kw("TABLE"), ws_0, qualified_reference)),
        |(_, _, _, (database, table))| {
            Statement::DropTable(DropTable {
                database,
                name: table,
            })
        },
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drop_database() {
        assert_eq!(
            drop_("drop database foo").unwrap().1,
            Statement::DropDatabase("foo".to_string())
        );
    }

    #[test]
    fn test_drop_table() {
        assert_eq!(
            drop_("drop table foo").unwrap().1,
            Statement::DropTable(DropTable {
                database: None,
                name: "foo".to_string()
            })
        );

        assert_eq!(
            drop_("drop table foo.bar").unwrap().1,
            Statement::DropTable(DropTable {
                database: Some("foo".to_string()),
                name: "bar".to_string()
            })
        );
    }
}
