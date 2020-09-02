use crate::runner::*;

#[test]
fn test_inner_joins() {
    with_connection(|connection| {
        connection.query(r#"CREATE TABLE t_left (l_id INT, l_text TEXT)"#, "");

        connection.query(
            r#"INSERT INTO t_left VALUES
        (1, "la"), (1, "lb"),
        (2, "lc"), (2, "ld"),
        (3, "le"), (3, "lf"),
        (null, "lg"), (null, "lh")
        "#,
            "",
        );

        connection.query(r#"CREATE TABLE t_right (r_id INT, r_text TEXT)"#, "");

        connection.query(
            r#"INSERT INTO t_right VALUES
        (1, "ra"), (1, "rb"),
        (2, "rc"), (2, "rd"),
        (4, "re"), (4, "rf"),
        (null, "rg"), (null, "rh")
        "#,
            "",
        );

        // Test old style join(equi)
        connection.query(
            r#"SELECT * FROM t_left, t_right
        WHERE l_id = t_right.r_id
        ORDER BY l_id, r_id
        "#,
            r#"
        |1|la|1|ra|
        |1|la|1|rb|
        |1|lb|1|ra|
        |1|lb|1|rb|
        |2|lc|2|rc|
        |2|lc|2|rd|
        |2|ld|2|rc|
        |2|ld|2|rd|
        "#,
        );

        // Test new style join(equi)
        connection.query(
            r#"SELECT * FROM t_left join t_right
        on l_id = t_right.r_id
        ORDER BY l_id, r_id
        "#,
            r#"
        |1|la|1|ra|
        |1|la|1|rb|
        |1|lb|1|ra|
        |1|lb|1|rb|
        |2|lc|2|rc|
        |2|lc|2|rd|
        |2|ld|2|rc|
        |2|ld|2|rd|
        "#,
        );

        // Make sure plan is sane for old style join
        connection.query(
            r#"EXPLAIN SELECT * FROM t_left, t_right
        WHERE l_id = t_right.r_id
        ORDER BY l_id, r_id
        "#,
            r#"
        |SORT||||
        | |sort_exprs:||||
        | |  ||INTEGER|<OFFSET 0> (ASC)|
        | |  ||INTEGER|<OFFSET 2> (ASC)|
        | |source:||||
        | |  PROJECT||||
        | |   |output_exprs:||||
        | |   |  l_id|0|INTEGER|<OFFSET 1>|
        | |   |  l_text|1|TEXT|<OFFSET 2>|
        | |   |  r_id|2|INTEGER|<OFFSET 4>|
        | |   |  r_text|3|TEXT|<OFFSET 5>|
        | |   |source:||||
        | |   |  JOIN||||
        | |   |   |predicate:||||
        | |   |   |||BOOLEAN|`=`(<OFFSET 0>, <OFFSET 3>)|
        | |   |   |left:||||
        | |   |   |  PROJECT||||
        | |   |   |   |output_exprs:||||
        | |   |   |   |  key_0|0|INTEGER|<OFFSET 0>|
        | |   |   |   |  l_id|1|INTEGER|<OFFSET 0>|
        | |   |   |   |  l_text|2|TEXT|<OFFSET 1>|
        | |   |   |   |source:||||
        | |   |   |   |  TABLE(t_left)||||
        | |   |   |   |   |columns:||||
        | |   |   |   |   |  l_id|0|INTEGER||
        | |   |   |   |   |  l_text|1|TEXT||
        | |   |   |right:||||
        | |   |   |  PROJECT||||
        | |   |   |   |output_exprs:||||
        | |   |   |   |  key_0|0|INTEGER|<OFFSET 0>|
        | |   |   |   |  r_id|1|INTEGER|<OFFSET 0>|
        | |   |   |   |  r_text|2|TEXT|<OFFSET 1>|
        | |   |   |   |source:||||
        | |   |   |   |  TABLE(t_right)||||
        | |   |   |   |   |columns:||||
        | |   |   |   |   |  r_id|0|INTEGER||
        | |   |   |   |   |  r_text|1|TEXT||
        "#,
        );

        // Test join thats not equi
        connection.query(
            r#"SELECT * FROM t_left join t_right
        on l_id + t_right.r_id = 3
        ORDER BY l_id, r_id
        "#,
            r#"
        |1|la|2|rc|
        |1|la|2|rd|
        |1|lb|2|rc|
        |1|lb|2|rd|
        |2|lc|1|ra|
        |2|lc|1|rb|
        |2|ld|1|ra|
        |2|ld|1|rb|
        "#,
        );
    });
}

#[test]
fn test_left_joins() {
    with_connection(|connection| {
        connection.query(r#"CREATE TABLE t_left (l_id INT, l_text TEXT)"#, "");

        connection.query(
            r#"INSERT INTO t_left VALUES
        (1, "la"), (1, "lb"),
        (2, "lc"), (2, "ld"),
        (3, "le"), (3, "lf"),
        (null, "lg"), (null, "lh")
        "#,
            "",
        );

        connection.query(r#"CREATE TABLE t_right (r_id INT, r_text TEXT)"#, "");

        connection.query(
            r#"INSERT INTO t_right VALUES
        (1, "ra"), (1, "rb"),
        (2, "rc"), (2, "rd"),
        (4, "re"), (4, "rf"),
        (null, "rg"), (null, "rh")
        "#,
            "",
        );

        // Test basic
        connection.query(
            r#"SELECT * FROM t_left left outer join t_right
        on l_id = t_right.r_id
        ORDER BY l_text, r_id
        "#,
            r#"
        |1|la|1|ra|
        |1|la|1|rb|
        |1|lb|1|ra|
        |1|lb|1|rb|
        |2|lc|2|rc|
        |2|lc|2|rd|
        |2|ld|2|rc|
        |2|ld|2|rd|
        |3|le|NULL|NULL|
        |3|lf|NULL|NULL|
        |NULL|lg|NULL|NULL|
        |NULL|lh|NULL|NULL|
        "#,
        );

        // Test non_equi
        connection.query(
            r#"SELECT * FROM t_left left outer join t_right
        on l_id + t_right.r_id = 3
        ORDER BY l_text, r_id
        "#,
            r#"
        |1|la|2|rc|
        |1|la|2|rd|
        |1|lb|2|rc|
        |1|lb|2|rd|
        |2|lc|1|ra|
        |2|lc|1|rb|
        |2|ld|1|ra|
        |2|ld|1|rb|
        |3|le|NULL|NULL|
        |3|lf|NULL|NULL|
        |NULL|lg|NULL|NULL|
        |NULL|lh|NULL|NULL|
        "#,
        );

        // Test constant (no joins succeed)
        connection.query(
            r#"SELECT * FROM t_left left outer join t_right
        on false
        ORDER BY l_text, r_id
        "#,
            r#"
        |1|la|NULL|NULL|
        |1|lb|NULL|NULL|
        |2|lc|NULL|NULL|
        |2|ld|NULL|NULL|
        |3|le|NULL|NULL|
        |3|lf|NULL|NULL|
        |NULL|lg|NULL|NULL|
        |NULL|lh|NULL|NULL|
        "#,
        );

        // test filters after joins
        connection.query(
            r#"SELECT * FROM t_left left outer join t_right
        on l_id = t_right.r_id
        WHERE l_id = 1
        ORDER BY l_text, r_id
        "#,
            r#"
        |1|la|1|ra|
        |1|la|1|rb|
        |1|lb|1|ra|
        |1|lb|1|rb|
        "#,
        );

        connection.query(
            r#"SELECT * FROM t_left left outer join t_right
        on l_id = t_right.r_id
        WHERE r_id = 1
        ORDER BY l_text, r_id
        "#,
            r#"
        |1|la|1|ra|
        |1|la|1|rb|
        |1|lb|1|ra|
        |1|lb|1|rb|
        "#,
        );

        // Test emulating a minus
        connection.query(
            r#"SELECT t_left.* FROM t_left left outer join t_right
        on l_id = t_right.r_id
        WHERE r_id IS NULL
        ORDER BY l_text
        "#,
            r#"
        |3|le|
        |3|lf|
        |NULL|lg|
        |NULL|lh|
        "#,
        );
    });
}
