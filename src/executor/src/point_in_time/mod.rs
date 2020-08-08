use crate::point_in_time::project::ProjectExecutor;
use crate::point_in_time::single::SingleExecutor;
use crate::ExecutionError;
use ast::rel::point_in_time::PointInTimeOperator;
use data::{Datum, Session};
use std::sync::Arc;

mod project;
mod single;

/// Point in time executor, essentially a streaming iterator
pub trait Executor {
    /// Advance the iterator to the next position, should be called before get for a new iter
    fn advance(&mut self) -> Result<(), ExecutionError>;

    /// Get the data at the current position of the iterator, the i32 is a frequency/
    fn get(&self) -> Option<(&[Datum], i32)>;

    /// Short cut function that calls advance followed by get.
    fn next(&mut self) -> Result<Option<(&[Datum], i32)>, ExecutionError> {
        self.advance()?;
        Ok(self.get())
    }

    /// Returns the count of columns from this operator. Used to help size buffers etc
    fn column_count(&self) -> usize;
}

pub fn build_executor(session: &Arc<Session>, plan: &PointInTimeOperator) -> Box<dyn Executor> {
    match plan {
        PointInTimeOperator::Single => Box::from(SingleExecutor::new()),
        PointInTimeOperator::Project(project) => Box::from(ProjectExecutor::new(
            Arc::clone(session),
            build_executor(session, &project.source),
            project.expressions.clone(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::Expression;
    use ast::rel::point_in_time;

    #[test]
    fn test_build_executor() -> Result<(), ExecutionError> {
        let session = Arc::new(Session::new(1));
        let plan = PointInTimeOperator::Project(point_in_time::Project {
            expressions: vec![Expression::Literal(Datum::from(1))],
            source: Box::new(PointInTimeOperator::Single),
        });

        let mut executor = build_executor(&session, &plan);
        // Due to the trait objects we can't really match against the built executor, but we can
        // run it!
        assert_eq!(executor.next()?, Some(([Datum::from(1)].as_ref(), 1)));

        assert_eq!(executor.next()?, None);
        Ok(())
    }
}
