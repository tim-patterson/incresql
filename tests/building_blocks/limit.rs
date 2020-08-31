use crate::runner::query;

#[test]
fn select_limit_no_offset() {
    query(
        r#"SELECT foo FROM (
        SELECT 1 as foo UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        ) LIMIT 2"#,
        "
        |1|
        |2|
        ",
    );

    query(
        r#"EXPLAIN SELECT foo FROM (SELECT 1 as foo) LIMIT 2"#,
        "
        |LIMIT||||
        | |limit: 2||||
        | |offset: 0||||
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
fn select_limit_with_offset() {
    query(
        r#"SELECT foo FROM (
        SELECT 1 as foo UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        ) LIMIT 1, 2"#,
        "
        |2|
        |3|
        ",
    );

    query(
        r#"SELECT foo FROM (
        SELECT 1 as foo UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        ) LIMIT 2 OFFSET 1"#,
        "
        |2|
        |3|
        ",
    );

    query(
        r#"EXPLAIN SELECT foo FROM (SELECT 1 as foo) LIMIT 1, 2"#,
        "
        |LIMIT||||
        | |limit: 2||||
        | |offset: 1||||
        | |source:||||
        | |  PROJECT||||
        | |   |output_exprs:||||
        | |   |  foo|0|INTEGER|1|
        | |   |source:||||
        | |   |  SINGLE||||
        ",
    );
}
