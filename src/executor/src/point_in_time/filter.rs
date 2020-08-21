use crate::point_in_time::BoxedExecutor;
use crate::scalar_expression::EvalScalar;
use crate::ExecutionError;
use ast::expr::Expression;
use data::{Datum, Session, TupleIter};
use std::sync::Arc;

pub struct FilterExecutor {
    source: BoxedExecutor,
    session: Arc<Session>,
    predicate: Expression,
}

impl FilterExecutor {
    pub fn new(session: Arc<Session>, source: BoxedExecutor, predicate: Expression) -> Self {
        FilterExecutor {
            source,
            session,
            predicate,
        }
    }
}

impl TupleIter<ExecutionError> for FilterExecutor {
    fn advance(&mut self) -> Result<(), ExecutionError> {
        while let Some((tuple, _freq)) = self.source.next()? {
            if self.predicate.eval_scalar(&self.session, tuple) == Datum::from(true) {
                break;
            }
        }
        Ok(())
    }

    fn get(&self) -> Option<(&[Datum], i64)> {
        self.source.get()
    }

    fn column_count(&self) -> usize {
        self.source.column_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point_in_time::values::ValuesExecutor;
    use crate::ExecutionError;
    use ast::expr::CompiledColumnReference;
    use data::DataType;

    #[test]
    fn test_filter_executor() -> Result<(), ExecutionError> {
        let session = Arc::new(Session::new(1));
        let values = vec![
            vec![Datum::from(1), Datum::from(false)],
            vec![Datum::from(2), Datum::from(true)],
            vec![Datum::from(3), Datum::from(false)],
        ];

        let source = Box::from(ValuesExecutor::new(Box::from(values.into_iter()), 2));

        let predicate = Expression::CompiledColumnReference(CompiledColumnReference {
            offset: 1,
            datatype: DataType::Boolean,
        });

        let mut executor = FilterExecutor::new(session, source, predicate);
        assert_eq!(
            executor.next()?,
            Some(([Datum::from(2), Datum::from(true)].as_ref(), 1))
        );
        assert_eq!(executor.next()?, None);

        Ok(())
    }
}
