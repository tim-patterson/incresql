use crate::runner::query;

#[test]
fn select_literal_null() {
    query(
        r#"SELECT NULL, type_of(NULL)"#,
        "
        |NULL|NULL|
        ",
    );
}

#[test]
fn select_literal_int() {
    query(
        r#"SELECT 123, type_of(123)"#,
        "
        |123|INTEGER|
        ",
    );
}

#[test]
fn select_literal_bigint() {
    query(
        r#"SELECT 9123123123, type_of(9123123123)"#,
        "
        |9123123123|BIGINT|
        ",
    );
}

#[test]
fn select_literal_decimal() {
    query(
        r#"SELECT 200000000000000000000, type_of(200000000000000000000)"#,
        "
        |200000000000000000000|DECIMAL(21,0)|
        ",
    );
    query(
        r#"SELECT 12.34, type_of(12.34)"#,
        "
        |12.34|DECIMAL(4,2)|
        ",
    );

    query(
        r#"SELECT 1.00, type_of(1.00)"#,
        "
        |1.00|DECIMAL(3,2)|
        ",
    );

    query(
        r#"SELECT .12, type_of(.12)"#,
        "
        |0.12|DECIMAL(2,2)|
        ",
    );
}

#[test]
fn select_literal_text() {
    query(
        r#"SELECT "abc", type_of("abc")"#,
        "
        |abc|TEXT|
        ",
    );
}
