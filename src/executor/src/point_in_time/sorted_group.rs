use crate::aggregate_expression::{AggregateExpression, EvalAggregateRow};
use crate::point_in_time::BoxedExecutor;
use crate::ExecutionError;
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
    key_size: usize,
    expressions: Vec<AggregateExpression>,
    current_state: Vec<Datum<'static>>,
    current_hash: u64,
}

impl SortedGroupExecutor {
    #[allow(dead_code)]
    pub fn new(
        source: BoxedExecutor,
        session: Arc<Session>,
        key_size: usize,
        expressions: Vec<AggregateExpression>,
    ) -> Self {
        let current_state = expressions.initialize();
        SortedGroupExecutor {
            source: PeekableIter::from(source),
            session,
            key_size,
            expressions,
            current_state,
            current_hash: 0,
        }
    }
}

impl TupleIter for SortedGroupExecutor {
    type E = ExecutionError;

    fn advance(&mut self) -> Result<(), ExecutionError> {
        while let Some((tuple, freq)) = self.source.next()? {
            let mut hasher = DefaultHasher::new();
            tuple[0..self.key_size].hash(&mut hasher);
            let key_hash = hasher.finish();

            if key_hash != self.current_hash {
                // TODO copy old state out somehow?

                self.expressions.reset(&mut self.current_state);
            }
            self.expressions
                .apply(&self.session, tuple, freq, &mut self.current_state);
        }
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        unimplemented!()
    }

    fn column_count(&self) -> usize {
        self.expressions.len()
    }
}
