use crate::runner::query;

#[test]
fn select_subquery_no_alias() {
    query(
        r#"SELECT 1 FROM (SELECT 1)"#,
        "
        |1|
        ",
    );

    query(
        r#"EXPLAIN SELECT 1 FROM (SELECT 1)"#,
        "
        |PROJECT||
        | |exprs:||
        | |  _col1 <INTEGER>|1|
        | |source:||
        | |  PROJECT||
        | |   |exprs:||
        | |   |  _col1 <INTEGER>|1|
        | |   |source:||
        | |   |  SINGLE||
        ",
    );
}

#[test]
fn select_subquery_with_alias() {
    query(
        r#"SELECT 1 FROM (SELECT 1) as foo"#,
        "
        |1|
        ",
    );

    query(
        r#"EXPLAIN SELECT 1 FROM (SELECT 1) as foo"#,
        "
        |PROJECT||
        | |exprs:||
        | |  _col1 <INTEGER>|1|
        | |source:||
        | |  PROJECT(foo)||
        | |   |exprs:||
        | |   |  _col1 <INTEGER>|1|
        | |   |source:||
        | |   |  SINGLE||
        ",
    );
}
