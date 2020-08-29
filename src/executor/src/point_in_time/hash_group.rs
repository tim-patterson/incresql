use crate::aggregate_expression::{AggregateExpression, EvalAggregateRow};
use crate::point_in_time::BoxedExecutor;
use crate::utils::{right_size_new, transmute_muf_buf};
use crate::ExecutionError;
use ast::expr::Expression;
use data::{Datum, Session, SortOrder, TupleIter};
use std::collections::hash_map::IntoIter;
use std::collections::HashMap;
use std::sync::Arc;

/// A Group by executor that can accept tuples in any order and stores the
/// partial aggregates in a hashmap.
/// This executor doesn't properly handle the case where there's no grouping keys,
/// sorted_group should be used for that instead.
pub struct HashGroupExecutor {
    source: BoxedExecutor,
    session: Arc<Session>,
    key_size: usize,
    expressions: Vec<AggregateExpression>,
    state: HashMap<Vec<u8>, Vec<Datum<'static>>>,
    state_iter: Option<IntoIter<Vec<u8>, Vec<Datum<'static>>>>,
    output_state: Vec<Datum<'static>>,
    output_tuple: Vec<Datum<'static>>,
    done: bool,
}

impl HashGroupExecutor {
    pub fn new(
        source: BoxedExecutor,
        session: Arc<Session>,
        key_size: usize,
        expressions: Vec<Expression>,
    ) -> Self {
        let expressions: Vec<_> = expressions.iter().map(AggregateExpression::from).collect();
        let output_tuple = right_size_new(&expressions);
        HashGroupExecutor {
            source,
            session,
            key_size,
            expressions,
            state: HashMap::new(),
            state_iter: None,
            output_tuple,
            output_state: vec![],
            done: false,
        }
    }
}

impl TupleIter for HashGroupExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), ExecutionError> {
        if self.state_iter.is_none() {
            let mut key_buf = vec![];
            while let Some((tuple, freq)) = self.source.next()? {
                key_buf.clear();
                for datum in &tuple[..(self.key_size)] {
                    datum.as_sortable_bytes(SortOrder::Asc, &mut key_buf);
                }

                if let Some(state) = self.state.get_mut(&key_buf) {
                    self.expressions.apply(&self.session, tuple, freq, state);
                } else {
                    let mut key = vec![];
                    std::mem::swap(&mut key, &mut key_buf);
                    let mut state = self.expressions.initialize();
                    self.expressions
                        .apply(&self.session, tuple, freq, &mut state);
                    self.state.insert(key, state);
                }
            }

            let mut state = HashMap::new();
            std::mem::swap(&mut state, &mut self.state);
            self.state_iter = Some(state.into_iter());
        }

        if let Some((_key, state)) = self.state_iter.as_mut().unwrap().next() {
            // The output tuple may borrow from the state so we need to put both the
            // state and the output_tuple on the SortedGroupExecutor struct.
            self.output_state = state;

            self.expressions.finalize(
                &self.session,
                &self.output_state,
                transmute_muf_buf(&mut self.output_tuple),
            );
        } else {
            self.done = true;
        }

        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        if self.done {
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
    use crate::point_in_time::sort::SortExecutor;
    use crate::point_in_time::values::ValuesExecutor;
    use ast::expr::{CompiledAggregate, CompiledColumnReference, Expression, SortExpression};
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

        let executor = HashGroupExecutor::new(source, Arc::clone(&session), 1, expressions);
        let mut sorted = SortExecutor::new(
            session,
            Box::from(executor),
            vec![SortExpression {
                ordering: SortOrder::Asc,
                expression: Expression::CompiledColumnReference(CompiledColumnReference {
                    offset: 0,
                    datatype: DataType::Text,
                }),
            }],
        );

        assert_eq!(
            sorted.next()?,
            Some(([Datum::from("a"), Datum::from(3)].as_ref(), 1))
        );
        assert_eq!(
            sorted.next()?,
            Some(([Datum::from("b"), Datum::from(7)].as_ref(), 1))
        );
        assert_eq!(
            sorted.next()?,
            Some(([Datum::from("c"), Datum::from(5)].as_ref(), 1))
        );
        assert_eq!(sorted.next()?, None);

        Ok(())
    }
}
