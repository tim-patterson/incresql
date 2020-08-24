use crate::runner::*;

#[test]
fn test_select_from_csv_file() {
    with_connection(|connection| {
        connection.query(
            r#"select * from directory "test_data/csv""#,
            r#"
        |["123","abc","12.1"]|
        |["456","d,ef","13.2"]|
        "#,
        );
    });
}
