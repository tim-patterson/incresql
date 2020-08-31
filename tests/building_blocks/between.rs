use crate::runner::{with_connection, TestQuery};

#[test]
fn select_between() {
    with_connection(|connection| {
        connection.query(
            r#"SELECT 1 between 1 and 2.0"#,
            "
        |TRUE|
        ",
        );

        connection.query(
            r#"SELECT 1 + 0 between 1 + 0 and 2.0 + 0"#,
            "
        |TRUE|
        ",
        );
    });
}
