use crate::runner::*;

#[test]
fn test_insert_from_select() {
    with_connection(|connection| {
        connection.query(r#"CREATE TABLE t1 (a INT, b TEXT)"#, "");

        connection.query(r#"INSERT INTO t1 SELECT 1, "abc""#, "");

        connection.query(r#"INSERT INTO t1 SELECT 2, "def""#, "");

        connection.query(
            r#"SELECT * FROM t1"#,
            "
                |1|abc|
                |2|def|
            ",
        );

        connection.query(r#"CREATE TABLE t2 (a INT, b TEXT)"#, "");

        connection.query(r#"INSERT INTO default.t2 SELECT * FROM t1"#, "");

        connection.query(
            r#"SELECT * FROM t2"#,
            "
                |1|abc|
                |2|def|
            ",
        );
    });
}
