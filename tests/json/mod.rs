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
