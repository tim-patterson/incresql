use crate::runner::*;

#[test]
fn create_tables() {
    with_connection(|connection| {
        connection.query(
            r#"CREATE TABLE t1 (a INT, b TEXT, c DECIMAL(4,2), d BOOLEAN)"#,
            "",
        );

        connection.query(
            r#"SELECT database_name, name FROM incresql.tables where name = "t1""#,
            "
                |default|t1|
            ",
        );

        // Drop test
        connection.query(r#"INSERT INTO t1 SELECT 1, "a", 12.34, false"#, "");

        connection.query(r#"DROP TABLE t1"#, "");

        connection.query(
            r#"SELECT database_name, name FROM incresql.tables where name = "t1""#,
            "",
        );

        // Recreate and make sure data doesn't reappear
        connection.query(
            r#"CREATE TABLE t1 (a INT, b TEXT, c DECIMAL(4,2), d BOOLEAN)"#,
            "",
        );
        connection.query(r#"SELECT * FROM t1"#, "");
    });
}
