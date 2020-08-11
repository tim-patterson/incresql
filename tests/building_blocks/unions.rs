use crate::runner::query;

#[test]
fn select_union_toplevel() {
    query(
        r#"SELECT 1, "a" UNION ALL SELECT 2, "b""#,
        "
        |1|a|
        |2|b|
        ",
    );

    query(
        r#"SELECT 1, "a" where false UNION ALL SELECT 2, "b""#,
        "
        |2|b|
        ",
    );

    query(
        r#"SELECT c1, c2 FROM (SELECT 1 as c1, "a" as c2) UNION ALL SELECT 2, "b""#,
        "
        |1|a|
        |2|b|
        ",
    );
}

#[test]
fn select_union_subquery() {
    query(
        r#"SELECT c1, c2 FROM (SELECT 1 as c1, "a" as c2 UNION ALL SELECT 2, "b")"#,
        "
        |1|a|
        |2|b|
        ",
    );
}

#[test]
fn select_union_explain() {
    query(
        r#"EXPLAIN SELECT c1 FROM (SELECT 1 as c1 UNION ALL SELECT 2)"#,
        "
        |PROJECT|||
        | |exprs:|||
        | |  _col1 <INTEGER>|0|<OFFSET 0>|
        | |source:|||
        | |  UNION_ALL|||
        | |   |sources:|||
        | |   |  PROJECT|||
        | |   |   |exprs:|||
        | |   |   |  c1 <INTEGER>|0|1|
        | |   |   |source:|||
        | |   |   |  SINGLE|||
        | |   |  PROJECT|||
        | |   |   |exprs:|||
        | |   |   |  _col1 <INTEGER>|0|2|
        | |   |   |source:|||
        | |   |   |  SINGLE|||
        ",
    );
}
