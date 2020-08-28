use crate::runner::*;

#[test]
fn test_auto_convert_project() {
    with_connection(|connection| {
        connection.query(
            r#"explain select count(*) from incresql.databases"#,
            "
        |GROUP||||
        | |output_exprs:||||
        | |  _col1|0|BIGINT|count()|
        | |source:||||
        | |  TABLE(databases)||||
        | |   |columns:||||
        | |   |  name|0|TEXT||
        ",
        );

        // Test zero rows
        connection.query(
            r#"select count(*), sum(1) from incresql.databases where false"#,
            "
            |0|NULL|
        ",
        );

        connection.query(r#"Create table test (c1 TEXT, c2 INT)"#, "");
        connection.query(
            r#"INSERT INTO test VALUES
        ("a", 1), ("a", 2), ("b", 3), ("b", NULL), ("c", NULL)"#,
            "",
        );

        connection.query(
            r#"select c1, count(*), count(c2), sum(c2) from test group by c1 order by c1"#,
            "
            |a|2|2|3|
            |b|2|1|3|
            |c|1|0|NULL|
        ",
        );
    });
}
