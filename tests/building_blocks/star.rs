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
        |PROJECT||||
        | |output_exprs:||||
        | |  c1|0|INTEGER|<OFFSET 0>|
        | |  c2|1|INTEGER|<OFFSET 1>|
        | |source:||||
        | |  PROJECT(foo)||||
        | |   |output_exprs:||||
        | |   |  c1|0|INTEGER|1|
        | |   |  c2|1|INTEGER|2|
        | |   |source:||||
        | |   |  SINGLE||||
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
