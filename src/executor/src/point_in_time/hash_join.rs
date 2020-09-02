use crate::point_in_time::BoxedExecutor;
use crate::scalar_expression::EvalScalar;
use crate::utils::{right_size_new_to, transmute_muf_buf};
use crate::ExecutionError;
use ast::expr::Expression;
use ast::rel::logical::JoinType;
use data::{Datum, Session, TupleIter};
use std::collections::HashMap;
use std::slice::Iter;
use std::sync::Arc;

/// A hash join takes 2 inputs left and right with the join keys being the first key_len
/// columns being the equi join condition.  Any non-equi-join conditions can be filtered
/// by a downstream filter executor.
/// The right input will be fully consumed first to populate the hashtable.
/// The output rows will be a combination of left and right.
pub struct HashJoinExecutor {
    left: BoxedExecutor,
    right: BoxedExecutor,
    key_len: usize,
    non_equi_condition: Expression,
    session: Arc<Session>,
    left_len: usize,
    join_type: JoinType,
    hash_table: Option<HashMap<Vec<Datum<'static>>, Bucket>>,
    tuple_buf: Vec<Datum<'static>>,
    left_freq: i64,
    freq: i64,
    bucket_iter: Iter<'static, (Vec<Datum<'static>>, i64)>,
    done: bool,
}

type Bucket = Vec<(Vec<Datum<'static>>, i64)>;

impl HashJoinExecutor {
    /// Creates a new hash join executor, due to join conditions for left outer joins
    /// not acting the same as the filter operator we must pull these in and evaluate them
    /// here.
    pub fn new(
        left: BoxedExecutor,
        right: BoxedExecutor,
        key_len: usize,
        non_equi_condition: Expression,
        join_type: JoinType,
        session: Arc<Session>,
    ) -> Self {
        let tuple_buf = right_size_new_to(left.column_count() + right.column_count());
        let left_len = left.column_count();
        HashJoinExecutor {
            left,
            right,
            key_len,
            non_equi_condition,
            session,
            left_len,
            join_type,
            hash_table: None,
            tuple_buf,
            left_freq: 0,
            freq: 0,
            bucket_iter: [].iter(),
            done: false,
        }
    }
}

impl TupleIter for HashJoinExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), ExecutionError> {
        // Our join may have multiple matches on the same join key, to handle that when we get
        // a hit we must populate the left side of the tuple and then walk an iterator
        // of the right side values.

        // The offset where we must write the non-key columns out to.
        let right_offset = self.left.column_count() + self.key_len;

        // If we're part way through iterating through a bucket lets carry on.
        while let Some((right_tuple, freq)) = self.bucket_iter.next() {
            let buf = transmute_muf_buf(&mut self.tuple_buf);
            for (idx, datum) in right_tuple.iter().enumerate() {
                buf[right_offset + idx] = datum.ref_clone();
            }
            if self.non_equi_condition.eval_scalar(&self.session, buf) == Datum::from(true) {
                self.freq = *freq * self.left_freq;
                return Ok(());
            }
        }

        // Otherwise build the hashtable if needed.
        if self.hash_table.is_none() {
            let mut hash_table: HashMap<Vec<Datum<'static>>, Bucket> = HashMap::new();
            while let Some((tuple, freq)) = self.right.next()? {
                let key: Vec<_> = tuple[0..(self.key_len)]
                    .iter()
                    .map(Datum::as_static)
                    .collect();
                if key.iter().any(Datum::is_null) {
                    // If any of the join keys are null we don't want to put into
                    // the join.
                    continue;
                }
                let rest = tuple[(self.key_len)..]
                    .iter()
                    .map(Datum::as_static)
                    .collect();

                let bucket = hash_table.entry(key).or_default();
                bucket.push((rest, freq));
            }
            self.hash_table = Some(hash_table);
        }

        let hash_table = self.hash_table.as_mut().unwrap();

        // Walk down the left tuples until we find a hit.
        'outer: loop {
            if let Some((tuple, left_freq)) = self.left.next()? {
                if let Some(bucket) = hash_table.get(&tuple[0..(self.key_len)]) {
                    // We've got a hit, populate the left side of the tuple
                    let buf = transmute_muf_buf(&mut self.tuple_buf);
                    for (idx, datum) in tuple.iter().enumerate() {
                        buf[idx] = datum.ref_clone();
                        // Write out the key portion for the right side.
                        if idx < self.key_len {
                            buf[idx + right_offset - self.key_len] = datum.ref_clone();
                        }
                    }
                    self.left_freq = left_freq;
                    self.bucket_iter = unsafe { std::mem::transmute(bucket.iter()) };

                    // Process the first item in the bucket
                    while let Some((right_tuple, right_freq)) = self.bucket_iter.next() {
                        for (idx, datum) in right_tuple.iter().enumerate() {
                            buf[right_offset + idx] = datum.ref_clone();
                        }
                        self.freq = *right_freq * left_freq;

                        if self.non_equi_condition.eval_scalar(&self.session, buf)
                            == Datum::from(true)
                        {
                            break 'outer;
                        }
                    }
                }

                if self.join_type == JoinType::LeftOuter {
                    // Populate the left side of the output tuple
                    let buf = transmute_muf_buf(&mut self.tuple_buf);
                    for (idx, datum) in tuple.iter().enumerate() {
                        buf[idx] = datum.ref_clone();
                    }
                    // Null out the remainder
                    for d in &mut buf[(self.left_len)..] {
                        *d = Datum::Null;
                    }
                    self.freq = left_freq;

                    break;
                }
            } else {
                // We're done...
                self.done = true;
                break;
            }
        }

        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        if self.done {
            None
        } else {
            Some((&self.tuple_buf, self.freq))
        }
    }

    fn column_count(&self) -> usize {
        self.left.column_count() + self.right.column_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point_in_time::sort::SortExecutor;
    use crate::point_in_time::values::ValuesExecutor;
    use ast::expr::{CompiledColumnReference, Expression, SortExpression};
    use data::{DataType, Session, SortOrder};
    use std::sync::Arc;

    #[test]
    fn test_inner_join() -> Result<(), ExecutionError> {
        let left_values = vec![
            vec![Datum::from("a"), Datum::from(1)],
            vec![Datum::from("b"), Datum::from(2)],
            vec![Datum::from("b"), Datum::from(3)],
            vec![Datum::from("c"), Datum::from(4)],
            vec![Datum::Null, Datum::from(10)],
        ];
        let right_values = vec![
            vec![Datum::from("a"), Datum::from(5)],
            vec![Datum::from("b"), Datum::from(6)],
            vec![Datum::from("b"), Datum::from(7)],
            vec![Datum::from("d"), Datum::from(8)],
            vec![Datum::Null, Datum::from(10)],
        ];
        let left_source = Box::from(ValuesExecutor::new(Box::from(left_values.into_iter()), 2));
        let right_source = Box::from(ValuesExecutor::new(Box::from(right_values.into_iter()), 2));
        let session = Arc::new(Session::new(1));

        let executor = HashJoinExecutor::new(
            left_source,
            right_source,
            1,
            Expression::from(true),
            JoinType::Inner,
            session,
        );

        // Sort on the two numeric columns
        let mut sorted = SortExecutor::new(
            Arc::new(Session::new(1)),
            Box::from(executor),
            vec![
                SortExpression {
                    ordering: SortOrder::Asc,
                    expression: Expression::CompiledColumnReference(CompiledColumnReference {
                        offset: 1,
                        datatype: DataType::Text,
                    }),
                },
                SortExpression {
                    ordering: SortOrder::Asc,
                    expression: Expression::CompiledColumnReference(CompiledColumnReference {
                        offset: 3,
                        datatype: DataType::Text,
                    }),
                },
            ],
        );

        // 1-1 join
        assert_eq!(
            sorted.next()?,
            Some((
                [
                    Datum::from("a"),
                    Datum::from(1),
                    Datum::from("a"),
                    Datum::from(5)
                ]
                .as_ref(),
                1
            ))
        );
        // many-many
        assert_eq!(
            sorted.next()?,
            Some((
                [
                    Datum::from("b"),
                    Datum::from(2),
                    Datum::from("b"),
                    Datum::from(6)
                ]
                .as_ref(),
                1
            ))
        );
        assert_eq!(
            sorted.next()?,
            Some((
                [
                    Datum::from("b"),
                    Datum::from(2),
                    Datum::from("b"),
                    Datum::from(7)
                ]
                .as_ref(),
                1
            ))
        );
        assert_eq!(
            sorted.next()?,
            Some((
                [
                    Datum::from("b"),
                    Datum::from(3),
                    Datum::from("b"),
                    Datum::from(6)
                ]
                .as_ref(),
                1
            ))
        );
        assert_eq!(
            sorted.next()?,
            Some((
                [
                    Datum::from("b"),
                    Datum::from(3),
                    Datum::from("b"),
                    Datum::from(7)
                ]
                .as_ref(),
                1
            ))
        );
        // Done, we shouldn't get rows for c or d as no matches.
        assert_eq!(sorted.next()?, None);
        Ok(())
    }

    #[test]
    fn test_left_outer_join() -> Result<(), ExecutionError> {
        let left_values = vec![
            vec![Datum::from("a"), Datum::from(1)],
            vec![Datum::from("b"), Datum::from(2)],
            vec![Datum::Null, Datum::from(10)],
        ];
        let right_values = vec![
            vec![Datum::from("a"), Datum::from(4)],
            vec![Datum::Null, Datum::from(10)],
        ];
        let left_source = Box::from(ValuesExecutor::new(Box::from(left_values.into_iter()), 2));
        let right_source = Box::from(ValuesExecutor::new(Box::from(right_values.into_iter()), 2));
        let session = Arc::new(Session::new(1));

        let executor = HashJoinExecutor::new(
            left_source,
            right_source,
            1,
            Expression::from(true),
            JoinType::LeftOuter,
            session,
        );

        // Sort on the two numeric columns
        let mut sorted = SortExecutor::new(
            Arc::new(Session::new(1)),
            Box::from(executor),
            vec![
                SortExpression {
                    ordering: SortOrder::Asc,
                    expression: Expression::CompiledColumnReference(CompiledColumnReference {
                        offset: 1,
                        datatype: DataType::Text,
                    }),
                },
                SortExpression {
                    ordering: SortOrder::Asc,
                    expression: Expression::CompiledColumnReference(CompiledColumnReference {
                        offset: 3,
                        datatype: DataType::Text,
                    }),
                },
            ],
        );

        assert_eq!(
            sorted.next()?,
            Some((
                [
                    Datum::from("a"),
                    Datum::from(1),
                    Datum::from("a"),
                    Datum::from(4)
                ]
                .as_ref(),
                1
            ))
        );
        assert_eq!(
            sorted.next()?,
            Some((
                [Datum::from("b"), Datum::from(2), Datum::Null, Datum::Null].as_ref(),
                1
            ))
        );
        assert_eq!(
            sorted.next()?,
            Some((
                [Datum::Null, Datum::from(10), Datum::Null, Datum::Null].as_ref(),
                1
            ))
        );
        // Done, we shouldn't get rows for c or d as no matches.
        assert_eq!(sorted.next()?, None);
        Ok(())
    }
}
