use crate::runner::*;

#[test]
fn test_to_from_date() {
    with_connection(|connection| {
        connection.query(
            r#"select cast("2010-10-23" as date)"#,
            "
        |2010-10-23|
        ",
        );

        connection.query(
            r#"select cast(cast("2010-10-23" as date) as text)"#,
            "
        |2010-10-23|
        ",
        );
    });
}
