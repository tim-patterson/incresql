use crate::runner::*;

#[test]
fn select_order_by() {
    with_connection(|connection| {
        // Asc
        connection.query(
            r#"SELECT foo FROM (
                    SELECT 1 as foo UNION ALL SELECT 4 UNION ALL SELECT 3 UNION ALL SELECT 2
                    ) ORDER BY foo"#,
            "
            |1|
            |2|
            |3|
            |4|
        ",
        );

        // Desc
        connection.query(
            r#"SELECT foo FROM (
                    SELECT 1 as foo UNION ALL SELECT 4 UNION ALL SELECT 3 UNION ALL SELECT 2
                    ) ORDER BY foo desc"#,
            "
            |4|
            |3|
            |2|
            |1|
        ",
        );

        // With limit
        connection.query(
            r#"SELECT foo FROM (
                    SELECT 1 as foo UNION ALL SELECT 4 UNION ALL SELECT 3 UNION ALL SELECT 2
                    ) ORDER BY foo ASC LIMIT 2"#,
            "
            |1|
            |2|
        ",
        );
    });
}
