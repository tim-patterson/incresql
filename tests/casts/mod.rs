use crate::runner::*;

#[test]
fn test_to_from_date() {
    with_connection(|connection| {
        connection.query(
            r#"select cast("2010-10-23" as date)"#,
            "
        |2010-10-23|
        ",
        );

        connection.query(
            r#"select cast(cast("2010-10-23" as date) as text)"#,
            "
        |2010-10-23|
        ",
        );
    });
}

#[test]
fn test_auto_cast() {
    with_connection(|connection| {
        connection.query(
            r#"create table t(i INTEGER, b BIGINT, d DECIMAL(10,2))"#,
            "",
        );

        connection.query(
            r#"EXPLAIN SELECT i + b, i + d, b + d from t"#,
            "
        |PROJECT||||
        | |output_exprs:||||
        | |  _col1|0|BIGINT|`+`(to_bigint(<OFFSET 0>), <OFFSET 1>)|
        | |  _col2|1|DECIMAL(11,2)|`+`(to_decimal(<OFFSET 0>), <OFFSET 2>)|
        | |  _col3|2|DECIMAL(21,2)|`+`(to_decimal(<OFFSET 1>), <OFFSET 2>)|
        | |source:||||
        | |  TABLE(t)||||
        | |   |columns:||||
        | |   |  i|0|INTEGER||
        | |   |  b|1|BIGINT||
        | |   |  d|2|DECIMAL(10,2)||
        ",
        );
    });
}
