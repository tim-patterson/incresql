use crate::runner::*;

#[test]
fn test_inner_joins() {
    with_connection(|connection| {
        // Create a table in the default schema
        connection.query(r#"CREATE TABLE test (c TEXT)"#, "");

        connection.query(
            r#"INSERT INTO test VALUES
        ("tables")"#,
            "",
        );

        // Create a view in the default schema but with context in incresql
        connection.query(r#"use incresql"#, "");
        connection.query(
            r#"create view default.test_view as select name as table_name from tables"#,
            "",
        );

        // Move back to default and query with a join to the view
        connection.query(r#"use default"#, "");
        connection.query(
            r#"SELECT * FROM test JOIN test_view ON c=table_name"#,
            "\
        |tables|tables|
        ",
        );
    });
}
