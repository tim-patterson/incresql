use crate::runner::query;

#[test]
fn select_from_table() {
    query(
        r#"SELECT name FROM incresql.databases"#,
        "
        |default|
        |incresql|
        |information_schema|
        ",
    );

    query(
        r#"SELECT databases.name FROM incresql.databases"#,
        "
        |default|
        |incresql|
        |information_schema|
        ",
    );

    query(
        r#"SELECT name FROM incresql.databases as foo"#,
        "
        |default|
        |incresql|
        |information_schema|
        ",
    );

    query(
        r#"SELECT foo.name FROM incresql.databases foo"#,
        "
        |default|
        |incresql|
        |information_schema|
        ",
    );
}
