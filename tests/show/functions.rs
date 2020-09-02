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
        |<|
        |<=|
        |=|
        |>|
        |>=|
        |and|
        |avg|
        |between|
        |coalesce|
        |count|
        |database|
        |date_sub|
        |isfalse|
        |isnull|
        |istrue|
        |json_extract|
        |json_unquote|
        |not|
        |or|
        |sum|
        |to_bigint|
        |to_bool|
        |to_date|
        |to_decimal|
        |to_int|
        |to_json|
        |to_jsonpath|
        |to_text|
        |type_of|
        ",
    );
}
