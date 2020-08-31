use crate::runner::query;

#[test]
fn select_predicate_constant() {
    query(
        r#"SELECT 1 where true"#,
        "
        |1|
        ",
    );

    query(
        r#"EXPLAIN SELECT 1 where true"#,
        "
        |PROJECT||||
        | |output_exprs:||||
        | |  _col1|0|INTEGER|1|
        | |source:||||
        | |  SINGLE||||
        ",
    );
}

#[test]
fn select_predicate_reference() {
    query(
        r#"SELECT foo from (select 1 as foo) where foo = 1"#,
        "
        |1|
        ",
    );

    query(
        r#"EXPLAIN SELECT foo from (select 1 as foo) where foo = 1"#,
        "
        |PROJECT||||
        | |output_exprs:||||
        | |  foo|0|INTEGER|<OFFSET 0>|
        | |source:||||
        | |  PROJECT||||
        | |   |output_exprs:||||
        | |   |  foo|0|INTEGER|1|
        | |   |source:||||
        | |   |  FILTER||||
        | |   |   |predicate:||||
        | |   |   |||BOOLEAN|TRUE|
        | |   |   |source:||||
        | |   |   |  SINGLE||||
        ",
    );
}
