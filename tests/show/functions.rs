use crate::runner::query;

#[test]
fn show_functions() {
    query(
        r#"SHOW FUNCTIONS"#,
        "
        |*|
        |+|
        |-|
        |/|
        |=|
        |to_bigint|
        |to_bool|
        |to_decimal|
        |to_int|
        |to_text|
        |type_of|
        ",
    );
}
