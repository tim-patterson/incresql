use crate::atoms::{identifier_str, kw, qualified_reference};
use crate::literals::datatype;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::statement::{CreateDatabase, CreateTable, Statement};
use data::DataType;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{cut, map};
use nom::multi::separated_list;
use nom::sequence::{pair, preceded, separated_pair, tuple};

/// Parses a create statement
pub fn create(input: &str) -> ParserResult<Statement> {
    preceded(kw("CREATE"), cut(alt((create_database, create_table))))(input)
}

fn create_database(input: &str) -> ParserResult<Statement> {
    map(
        tuple((ws_0, kw("DATABASE"), ws_0, identifier_str)),
        |(_, _, _, database)| Statement::CreateDatabase(CreateDatabase { name: database }),
    )(input)
}

fn create_table(input: &str) -> ParserResult<Statement> {
    map(
        preceded(
            pair(ws_0, kw("TABLE")),
            cut(tuple((
                ws_0,
                qualified_reference,
                tuple((ws_0, tag("("), ws_0)),
                separated_list(tuple((ws_0, tag(","), ws_0)), column_spec),
                tuple((ws_0, tag(")"))),
            ))),
        ),
        |(_, (db_name, table_name), _, columns, _)| {
            Statement::CreateTable(CreateTable {
                database: db_name,
                name: table_name,
                columns,
            })
        },
    )(input)
}

fn column_spec(input: &str) -> ParserResult<(String, DataType)> {
    separated_pair(identifier_str, ws_0, datatype)(input)
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

    #[test]
    fn test_create_table() {
        assert_eq!(
            create("Create table foo.bar ( c1 INT, c2 BOOLEAN )")
                .unwrap()
                .1,
            Statement::CreateTable(CreateTable {
                database: Some("foo".to_string()),
                name: "bar".to_string(),
                columns: vec![
                    ("c1".to_string(), DataType::Integer),
                    ("c2".to_string(), DataType::Boolean)
                ]
            })
        );
    }
}
