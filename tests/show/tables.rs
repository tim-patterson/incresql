use crate::runner::*;

#[test]
fn show_tables() {
    with_connection(|connection| {
        connection.query(r#"SHOW TABLES"#, "");

        connection.query(r#"USE incresql"#, "");

        connection.query(
            r#"SHOW TABLES"#,
            "
            |databases|
            |prefix_tables|
            |tables|
       ",
        );
    })
}
