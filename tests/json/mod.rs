use crate::runner::*;

#[test]
fn test_json_extract() {
    with_connection(|connection| {
        connection.query(
            r#"select cast("[1,2,3,4]" as json)->"$.1""#,
            "
        |2|
        ",
        );

        connection.query(
            r#"select cast("[[1,2],[3,4],[5,6]]" as json)->"$.*.0""#,
            "
        |[1,3,5]|
        ",
        );
    });
}

#[test]
fn test_json_unquote() {
    with_connection(|connection| {
        connection.query(
            r#"select json_unquote(cast("null" as json))"#,
            "
        |null|
        ",
        );

        // I don't 100% agree with this but it does match the mysql functionality
        connection.query(
            r#"select type_of(json_unquote(cast(NULL as json)))"#,
            "
        |TEXT|
        ",
        );

        connection.query(
            r#"select json_unquote(cast("\"test\"" as json))"#,
            "
        |test|
        ",
        );

        connection.query(
            r#"select json_unquote(cast("[\"test\"]" as json))"#,
            r#"
        |["test"]|
        "#,
        );

        connection.query(
            r#"select json_unquote(cast("123" as json))"#,
            "
        |123|
        ",
        );
    });
}
