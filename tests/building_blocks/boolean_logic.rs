use crate::runner::*;

#[test]
fn and_or() {
    with_connection(|connection| {
        connection.query(
            r#"SELECT null and true"#,
            "\
        |NULL|
        ",
        );

        connection.query(
            r#"SELECT true and true"#,
            "\
        |TRUE|
        ",
        );

        connection.query(
            r#"SELECT true and false"#,
            "\
        |FALSE|
        ",
        );

        connection.query(
            r#"SELECT null or true"#,
            "\
        |NULL|
        ",
        );

        connection.query(
            r#"SELECT true or false"#,
            "\
        |TRUE|
        ",
        );

        connection.query(
            r#"SELECT false or false"#,
            "\
        |FALSE|
        ",
        );
    });
}

#[test]
fn and_or_not_precedence() {
    with_connection(|connection| {
        // This is how it should be parsed
        connection.query(
            r#"SELECT (true and false) or false, false or (false and true)"#,
            "
        |FALSE|FALSE|
        ",
        );

        // Checking without brackets.
        connection.query(
            r#"SELECT true and false or false, false or false and true"#,
            "
        |FALSE|FALSE|
        ",
        );

        // check not
        connection.query(
            r#"SELECT not true, not not true"#,
            "
        |FALSE|TRUE|
        ",
        );

        // check "not" binds tighter than "or"
        connection.query(
            r#"SELECT not true or true"#,
            "
        |TRUE|
        ",
        );

        // Check "not" binds tighter than "and"
        connection.query(
            r#"SELECT not false and false"#,
            "
        |FALSE|
        ",
        );
    });
}
