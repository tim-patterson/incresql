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
        |PROJECT|||
        | |exprs:|||
        | |  foo <INTEGER>|0|<OFFSET 0>|
        | |source:|||
        | |  PROJECT|||
        | |   |exprs:|||
        | |   |  foo <INTEGER>|0|1|
        | |   |source:|||
        | |   |  SINGLE|||
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
        |PROJECT|||
        | |exprs:|||
        | |  foo <INTEGER>|0|<OFFSET 0>|
        | |source:|||
        | |  PROJECT(bar)|||
        | |   |exprs:|||
        | |   |  foo <INTEGER>|0|1|
        | |   |source:|||
        | |   |  SINGLE|||
        ",
    );
}
