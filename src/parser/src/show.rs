use crate::atoms::kw;
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::statement::Statement;
use nom::branch::alt;
use nom::combinator::{cut, value};
use nom::sequence::preceded;

/// Parses a show statement
pub fn show(input: &str) -> ParserResult<Statement> {
    preceded(
        kw("SHOW"),
        cut(alt((
            value(Statement::ShowFunctions, preceded(ws_0, kw("FUNCTIONS"))),
            value(Statement::ShowDatabases, preceded(ws_0, kw("DATABASES"))),
            value(Statement::ShowTables, preceded(ws_0, kw("TABLES"))),
        ))),
    )(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_functions() {
        assert_eq!(show("Show Functions").unwrap().1, Statement::ShowFunctions);
    }

    #[test]
    fn test_show_tables() {
        assert_eq!(show("Show tables").unwrap().1, Statement::ShowTables);
    }

    #[test]
    fn test_show_databases() {
        assert_eq!(show("Show databases").unwrap().1, Statement::ShowDatabases);
    }
}
