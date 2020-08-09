use crate::expression::EvalScalarRow;
use crate::point_in_time::Executor;
use crate::utils::right_size_new;
use crate::ExecutionError;
use ast::expr::Expression;
use data::{Datum, Session};
use std::sync::Arc;

pub struct ProjectExecutor {
    source: Box<dyn Executor>,
    session: Arc<Session>,
    expressions: Vec<Expression>,

    tuple_buffer: Vec<Datum<'static>>,
}

impl ProjectExecutor {
    pub fn new(
        session: Arc<Session>,
        source: Box<dyn Executor>,
        expressions: Vec<Expression>,
    ) -> Self {
        let tuple_buffer = right_size_new(&expressions);
        ProjectExecutor {
            source,
            session,
            expressions,
            tuple_buffer,
        }
    }
}

impl Executor for ProjectExecutor {
    // When we get a tuple from the next/get method, the values are only valid until the next call.
    // The project builds a new tuple from the source tuple, those values may have references back
    // to some byte buffer etc in the source.  Its all safe as to call advance our consumer has to
    // no longer be immutably borrowing from us. And there's no way for our source to advance
    // without that coming through us.
    // We need a little unsafe to muddle with the lifetimes to get past the rust compiler

    #[allow(clippy::transmute_ptr_to_ptr)]
    fn advance(&mut self) -> Result<(), ExecutionError> {
        if let Some((tuple, _freq)) = self.source.next()? {
            self.expressions.eval_scalar(&self.session, tuple, unsafe {
                std::mem::transmute::<&mut [Datum<'_>], &mut [Datum<'_>]>(&mut self.tuple_buffer)
            });
        }
        Ok(())
    }

    #[allow(clippy::transmute_ptr_to_ptr)]
    fn get(&self) -> Option<(&[Datum], i32)> {
        self.source.get().map(|(_tuple, freq)| {
            (
                unsafe { std::mem::transmute::<&[Datum<'_>], &[Datum<'_>]>(&self.tuple_buffer) },
                freq,
            )
        })
    }

    fn column_count(&self) -> usize {
        self.expressions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::point_in_time::single::SingleExecutor;
    use crate::ExecutionError;

    #[test]
    fn test_project_executor() -> Result<(), ExecutionError> {
        let session = Arc::new(Session::new(1));
        let mut executor = ProjectExecutor::new(
            session,
            Box::from(SingleExecutor::new()),
            vec![Expression::from(1), Expression::from(2)],
        );

        assert_eq!(executor.column_count(), 2);

        assert_eq!(
            executor.next()?,
            Some(([Datum::from(1), Datum::from(2)].as_ref(), 1))
        );
        assert_eq!(executor.next()?, None);
        Ok(())
    }
}
