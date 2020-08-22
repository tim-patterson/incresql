use crate::runner::*;

#[test]
fn test_auto_convert_project() {
    with_connection(|connection| {
        connection.query(
            r#"explain select count() from incresql.databases"#,
            "
            |GROUP|||
            | |exprs:|||
            | |  _col1 <BIGINT>|0|count()|
            | |group keys:|||
            | |source:|||
            | |  TABLE(databases)|||
            | |   |cols:|||
            | |   |  name <TEXT>|0||
        ",
        );

        // Test zero rows
        connection.query(
            r#"select count() from incresql.databases where false"#,
            "
            |0|
        ",
        );
    });
}
