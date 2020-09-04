use crate::runner::query;

#[test]
fn show_databases() {
    query(
        r#"SHOW DATABASES"#,
        "
        |default|
        |incresql|
        |information_schema|
        ",
    );
}
