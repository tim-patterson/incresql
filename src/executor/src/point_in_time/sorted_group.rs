use crate::aggregate_expression::{AggregateExpression, EvalAggregateRow};
use crate::point_in_time::BoxedExecutor;
use crate::utils::{right_size_new, transmute_muf_buf};
use crate::ExecutionError;
use ast::expr::Expression;
use data::{Datum, PeekableIter, Session, TupleIter};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A Group by executor that only works if the tuples fed to it
/// arrive sorted by the grouping key.
/// In order for the upstream to have sorted on the grouping key the
/// grouping keys must have already have been evaluated...
/// so we'll expect that the incoming tuples are prefixed with the
/// grouping keys.
pub struct SortedGroupExecutor {
    source: PeekableIter<dyn TupleIter<E = ExecutionError>>,
    session: Arc<Session>,
    key_len: usize,
    expressions: Vec<AggregateExpression>,
    current_state: Vec<Datum<'static>>,
    output_tuple: Vec<Datum<'static>>,
    state: State,
}

#[derive(Eq, PartialEq)]
enum State {
    Initial,
    Processing,
    Done,
}

impl SortedGroupExecutor {
    pub fn new(
        source: BoxedExecutor,
        session: Arc<Session>,
        key_len: usize,
        expressions: Vec<Expression>,
    ) -> Self {
        let expressions: Vec<_> = expressions.iter().map(AggregateExpression::from).collect();
        let current_state = expressions.initialize();
        let output_tuple = right_size_new(&expressions);
        SortedGroupExecutor {
            source: PeekableIter::from(source),
            session,
            key_len,
            expressions,
            current_state,
            output_tuple,
            state: State::Initial,
        }
    }
}

impl TupleIter for SortedGroupExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), ExecutionError> {
        // When we enter advance we'll pull off one record, apply/hash it
        // and then iter until we run off the end
        /// Hash the key for a tuple
        fn hash_tuple(tuple: &[Datum], key_len: usize) -> u64 {
            let mut hasher = DefaultHasher::new();
            tuple[0..key_len].hash(&mut hasher);
            hasher.finish()
        }

        // Special case where key size is 0
        if self.key_len == 0 && self.state == State::Initial {
            self.expressions.reset(&mut self.current_state);
            while let Some((tuple, freq)) = self.source.next()? {
                self.expressions
                    .apply(&self.session, tuple, freq, &mut self.current_state);
            }
            self.expressions.finalize(
                &self.session,
                &self.current_state,
                transmute_muf_buf(&mut self.output_tuple),
            );
            self.state = State::Processing;
        } else if self.key_len == 0 && self.state == State::Processing {
            self.state = State::Done;
        } else {
            // Standard grouping logic

            let group_hash = if let Some((tuple, freq)) = self.source.next()? {
                self.expressions.reset(&mut self.current_state);
                self.expressions
                    .apply(&self.session, tuple, freq, &mut self.current_state);
                hash_tuple(tuple, self.key_len)
            } else {
                self.state = State::Done;
                return Ok(());
            };

            loop {
                if let Some((tuple, freq)) = self.source.peek()? {
                    let hash = hash_tuple(tuple, self.key_len);
                    if hash != group_hash {
                        // We've stepped into the next tuple, finalize the row and break
                        self.expressions.finalize(
                            &self.session,
                            &self.current_state,
                            transmute_muf_buf(&mut self.output_tuple),
                        );
                        break;
                    }
                    self.expressions
                        .apply(&self.session, tuple, freq, &mut self.current_state);
                    // "advance" the inter
                    self.source.lock_in();
                } else {
                    // No next record to peek at, we need to act like we've stepped into a
                    // new key and write out our current state
                    self.expressions.finalize(
                        &self.session,
                        &self.current_state,
                        transmute_muf_buf(&mut self.output_tuple),
                    );
                    break;
                }
            }
        }
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        if self.state == State::Done {
            None
        } else {
            Some((&self.output_tuple, 1))
        }
    }

    fn column_count(&self) -> usize {
        self.expressions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point_in_time::values::ValuesExecutor;
    use ast::expr::{CompiledAggregate, CompiledColumnReference, Expression};
    use data::DataType;
    use functions::registry::Registry;
    use functions::FunctionSignature;

    #[test]
    fn test_sorted_group_executor() -> Result<(), ExecutionError> {
        let session = Arc::new(Session::new(1));
        let values = vec![
            vec![Datum::from("a"), Datum::from(1)],
            vec![Datum::from("a"), Datum::from(2)],
            vec![Datum::from("b"), Datum::from(3)],
            vec![Datum::from("b"), Datum::from(4)],
            vec![Datum::from("c"), Datum::from(5)],
        ];

        let source = Box::from(ValuesExecutor::new(Box::from(values.into_iter()), 1));

        // Lookup sum function
        let (sig, sum_function) = Registry::default()
            .resolve_function(&FunctionSignature {
                name: "sum",
                args: vec![DataType::Integer],
                ret: DataType::Null,
            })
            .unwrap();

        // Select col1, sum(col2)
        let expressions = vec![
            Expression::CompiledColumnReference(CompiledColumnReference {
                offset: 0,
                datatype: DataType::Text,
            }),
            Expression::CompiledAggregate(CompiledAggregate {
                function: sum_function.as_aggregate(),
                args: vec![Expression::CompiledColumnReference(
                    CompiledColumnReference {
                        offset: 1,
                        datatype: DataType::Integer,
                    },
                )]
                .into_boxed_slice(),
                expr_buffer: vec![].into_boxed_slice(),
                signature: Box::new(sig),
            }),
        ];

        let mut executor = SortedGroupExecutor::new(source, session, 1, expressions);

        assert_eq!(
            executor.next()?,
            Some(([Datum::from("a"), Datum::from(3)].as_ref(), 1))
        );
        assert_eq!(
            executor.next()?,
            Some(([Datum::from("b"), Datum::from(7)].as_ref(), 1))
        );
        assert_eq!(
            executor.next()?,
            Some(([Datum::from("c"), Datum::from(5)].as_ref(), 1))
        );
        assert_eq!(executor.next()?, None);

        Ok(())
    }

    #[test]
    fn test_sorted_group_executor_no_rows() -> Result<(), ExecutionError> {
        // When key size is zero we must return a row even if there's no input.
        // ie select count() from foo where false;
        let session = Arc::new(Session::new(1));
        let values = vec![];
        let source = Box::from(ValuesExecutor::new(Box::from(values.into_iter()), 1));

        // Lookup count function
        let (sig, count_function) = Registry::default()
            .resolve_function(&FunctionSignature {
                name: "count",
                args: vec![],
                ret: DataType::Null,
            })
            .unwrap();

        // Select count()
        let expressions = vec![Expression::CompiledAggregate(CompiledAggregate {
            function: count_function.as_aggregate(),
            args: vec![].into_boxed_slice(),
            expr_buffer: vec![].into_boxed_slice(),
            signature: Box::new(sig),
        })];

        let mut executor = SortedGroupExecutor::new(source, session, 0, expressions);

        assert_eq!(
            executor.next()?,
            Some(([Datum::from(0 as i64)].as_ref(), 1))
        );
        assert_eq!(executor.next()?, None);

        Ok(())
    }
}
