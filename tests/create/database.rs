use crate::runner::*;

#[test]
fn show_databases() {
    with_connection(|connection| {
        connection.query(r#"CREATE DATABASE foobar"#, "");

        connection.query(
            r#"SELECT * FROM incresql.databases where name = "foobar""#,
            "
                |foobar|
            ",
        );

        connection.query(r#"use foobar"#, "");
    });
}
