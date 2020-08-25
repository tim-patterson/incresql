use crate::runner::query;

#[test]
fn show_functions() {
    query(
        r#"SHOW FUNCTIONS"#,
        "
        |!=|
        |*|
        |+|
        |-|
        |->|
        |->>|
        |/|
        |=|
        |count|
        |database|
        |json_extract|
        |json_unquote|
        |sum|
        |to_bigint|
        |to_bool|
        |to_date|
        |to_decimal|
        |to_int|
        |to_json|
        |to_text|
        |type_of|
        ",
    );
}
