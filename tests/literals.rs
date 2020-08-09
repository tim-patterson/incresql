use runner::query;

mod runner;

#[test]
fn select_literal_null() {
    query(
        r#"SELECT NULL, type_of(NULL)"#,
        "
        NULL|NULL
        ",
    );
}

#[test]
fn select_literal_int() {
    query(
        r#"SELECT 123, type_of(123)"#,
        "
        123|INTEGER
        ",
    );
}

#[test]
fn select_literal_bigint() {
    query(
        r#"SELECT 9123123123, type_of(9123123123)"#,
        "
        9123123123|BIGINT
        ",
    );
}

#[test]
fn select_literal_decimal() {
    query(
        r#"SELECT 12.34, type_of(12.34)"#,
        "
        12.34|DECIMAL(28,2)
        ",
    );
}

#[test]
fn select_literal_text() {
    query(
        r#"SELECT "abc", type_of("abc")"#,
        "
        abc|TEXT
        ",
    );
}
