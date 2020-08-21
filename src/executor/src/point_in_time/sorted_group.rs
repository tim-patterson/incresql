use crate::aggregate_expression::AggregateExpression;
use crate::point_in_time::BoxedExecutor;
use crate::ExecutionError;
use data::{Datum, TupleIter};

/// A Group by executor that only works if the tuples fed to it
/// arrive sorted by the grouping key.
/// In order for the upstream to have sorted on the grouping key the
/// grouping keys must have already have been evaluated...
/// so we'll expect that the incoming tuples are prefixed with the
/// grouping keys.
pub struct SortedGroupExecutor {
    source: BoxedExecutor,
    key_size: usize,
    expressions: Vec<AggregateExpression>,
}

impl SortedGroupExecutor {
    pub fn new(
        source: BoxedExecutor,
        key_size: usize,
        expressions: Vec<AggregateExpression>,
    ) -> Self {
        SortedGroupExecutor {
            source,
            key_size,
            expressions,
        }
    }
}

impl TupleIter<ExecutionError> for SortedGroupExecutor {
    fn advance(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        unimplemented!()
    }

    fn column_count(&self) -> usize {
        self.expressions.len()
    }
}
