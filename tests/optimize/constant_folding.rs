use crate::runner::query;

#[test]
fn simple_math() {
    query(
        r#"SELECT 1 + 2 * 3 - 4"#,
        "
        |3|
        ",
    );

    query(
        r#"EXPLAIN SELECT 1 + 2 * 3 - 4"#,
        "
        |PROJECT||||
        | |output_exprs:||||
        | |  _col1|0|INTEGER|3|
        | |source:||||
        | |  SINGLE||||
        ",
    );
}

#[test]
fn test_types() {
    query(
        r#"SELECT 1.0 + 2.0 * 3.0 - 4.0, type_of(1.0 + 2.0 * 3.0 - 4.0)"#,
        "
        |3.00|DECIMAL(6,2)|
        ",
    );

    query(
        r#"EXPLAIN SELECT 1.0 + 2.0 * 3.0 - 4.0"#,
        "
        |PROJECT||||
        | |output_exprs:||||
        | |  _col1|0|DECIMAL(6,2)|3.00|
        | |source:||||
        | |  SINGLE||||
        ",
    );
}
