use crate::runner::*;

#[test]
fn test_delete() {
    with_connection(|connection| {
        connection.query(r#"CREATE TABLE t1 (a INT, b TEXT)"#, "");

        connection.query(r#"INSERT INTO t1 SELECT 1, "abc""#, "");
        connection.query(r#"INSERT INTO t1 SELECT 2, "def""#, "");
        connection.query(r#"INSERT INTO t1 SELECT 3, "ghi""#, "");

        connection.query(
            r#"SELECT * FROM t1"#,
            "
            |1|abc|
            |2|def|
            |3|ghi|
        ",
        );

        connection.query(r#"DELETE FROM t1 WHERE t1.b="def""#, "");

        connection.query(
            r#"SELECT * FROM t1"#,
            "
            |1|abc|
            |3|ghi|
        ",
        );

        connection.query(r#"DELETE FROM t1 LIMIT 1"#, "");

        connection.query(
            r#"SELECT * FROM t1"#,
            "
            |3|ghi|
        ",
        );
    });
}
