use crate::atoms::{and_recognise, identifier_str, kw, qualified_reference};
use crate::literals::datatype;
use crate::select::select;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::statement::{CreateDatabase, CreateTable, CreateView, Statement};
use data::DataType;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{cut, map};
use nom::multi::separated_list0;
use nom::sequence::{pair, preceded, separated_pair, tuple};

/// Parses a create statement
pub fn create(input: &str) -> ParserResult<Statement> {
    preceded(
        kw("CREATE"),
        cut(alt((create_database, create_table, create_view))),
    )(input)
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
                separated_list0(tuple((ws_0, tag(","), ws_0)), column_spec),
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

fn create_view(input: &str) -> ParserResult<Statement> {
    map(
        preceded(
            pair(ws_0, kw("VIEW")),
            cut(tuple((
                ws_0,
                qualified_reference,
                ws_0,
                kw("AS"),
                ws_0,
                and_recognise(select),
            ))),
        ),
        |(_, (db_name, table_name), _, _, _, (query, query_sql))| {
            Statement::CreateView(CreateView {
                database: db_name,
                name: table_name,
                sql: query_sql.to_string(),
                query,
            })
        },
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::{LogicalOperator, Project};

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

    #[test]
    fn test_create_view() {
        assert_eq!(
            create("Create view foo.bar as select 1").unwrap().1,
            Statement::CreateView(CreateView {
                database: Some("foo".to_string()),
                name: "bar".to_string(),
                sql: "select 1".to_string(),
                query: LogicalOperator::Project(Project {
                    distinct: false,
                    expressions: vec![NamedExpression {
                        alias: None,
                        expression: Expression::from(1)
                    }],
                    source: Box::new(Default::default())
                })
            })
        );
    }
}
