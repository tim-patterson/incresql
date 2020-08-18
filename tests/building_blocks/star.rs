use crate::runner::query;

#[test]
fn select_star_from_table() {
    query(
        r#"SELECT * FROM (SELECT 1 as c1, 2 as c2) foo"#,
        "
        |1|2|
        ",
    );

    query(
        r#"EXPLAIN SELECT * FROM (SELECT 1 as c1, 2 as c2) foo"#,
        "
        |PROJECT|||
        | |exprs:|||
        | |  c1 <INTEGER>|0|<OFFSET 0>|
        | |  c2 <INTEGER>|1|<OFFSET 1>|
        | |source:|||
        | |  PROJECT(foo)|||
        | |   |exprs:|||
        | |   |  c1 <INTEGER>|0|1|
        | |   |  c2 <INTEGER>|1|2|
        | |   |source:|||
        | |   |  SINGLE|||
        ",
    );

    query(
        r#"SELECT foo.* FROM (SELECT 1 as c1, 2 as c2) foo"#,
        "
        |1|2|
        ",
    );

    query(
        r#"SELECT foo.`*` FROM (SELECT 1 as c1, 2 as `*`) foo"#,
        "
        |2|
        ",
    );
}
