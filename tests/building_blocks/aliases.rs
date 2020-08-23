use crate::runner::query;

#[test]
fn select_subquery_no_alias() {
    query(
        r#"SELECT foo FROM (SELECT 1 as foo)"#,
        "
        |1|
        ",
    );

    query(
        r#"EXPLAIN SELECT foo FROM (SELECT 1 as foo)"#,
        "
        |PROJECT||||
        | |output_exprs:||||
        | |  foo|0|INTEGER|<OFFSET 0>|
        | |source:||||
        | |  PROJECT||||
        | |   |output_exprs:||||
        | |   |  foo|0|INTEGER|1|
        | |   |source:||||
        | |   |  SINGLE||||
        ",
    );
}

#[test]
fn select_subquery_with_alias() {
    query(
        r#"SELECT foo FROM (SELECT 1 as foo) as bar"#,
        "
        |1|
        ",
    );

    query(
        r#"SELECT foo FROM (SELECT 1 as foo) bar"#,
        "
        |1|
        ",
    );

    query(
        r#"SELECT bar.foo FROM (SELECT 1 as foo) bar"#,
        "
        |1|
        ",
    );

    query(
        r#"SELECT `bar`.`foo` FROM (SELECT 1 as foo) bar"#,
        "
        |1|
        ",
    );

    query(
        r#"EXPLAIN SELECT foo FROM (SELECT 1 as foo) as bar"#,
        "
        |PROJECT||||
        | |output_exprs:||||
        | |  foo|0|INTEGER|<OFFSET 0>|
        | |source:||||
        | |  PROJECT(bar)||||
        | |   |output_exprs:||||
        | |   |  foo|0|INTEGER|1|
        | |   |source:||||
        | |   |  SINGLE||||
        ",
    );
}
