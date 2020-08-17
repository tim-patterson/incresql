use crate::point_in_time::filter::FilterExecutor;
use crate::point_in_time::limit::LimitExecutor;
use crate::point_in_time::project::ProjectExecutor;
use crate::point_in_time::single::SingleExecutor;
use crate::point_in_time::union_all::UnionAllExecutor;
use crate::point_in_time::values::ValuesExecutor;
use crate::ExecutionError;
use ast::rel::point_in_time::PointInTimeOperator;
use data::{Session, TupleIter};
use std::sync::Arc;

mod filter;
mod limit;
mod project;
mod single;
mod table_scan;
mod union_all;
mod values;

pub type BoxedExecutor = Box<dyn TupleIter<ExecutionError>>;

pub fn build_executor(
    session: &Arc<Session>,
    plan: &PointInTimeOperator,
) -> Box<dyn TupleIter<ExecutionError>> {
    match plan {
        PointInTimeOperator::Single => Box::from(SingleExecutor::new()),
        PointInTimeOperator::Project(project) => Box::from(ProjectExecutor::new(
            Arc::clone(session),
            build_executor(session, &project.source),
            project.expressions.clone(),
        )),
        PointInTimeOperator::Filter(filter) => Box::from(FilterExecutor::new(
            Arc::clone(session),
            build_executor(session, &filter.source),
            filter.predicate.clone(),
        )),
        PointInTimeOperator::Limit(limit) => Box::from(LimitExecutor::new(
            build_executor(session, &limit.source),
            limit.offset,
            limit.limit,
        )),
        PointInTimeOperator::Values(values) => Box::from(ValuesExecutor::new(
            Box::from(values.data.clone().into_iter()),
            values.column_count,
        )),
        PointInTimeOperator::UnionAll(union_all) => Box::from(UnionAllExecutor::new(
            union_all
                .sources
                .iter()
                .map(|source| build_executor(session, source))
                .collect(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::Expression;
    use ast::rel::point_in_time;
    use data::Datum;

    #[test]
    fn test_build_executor() -> Result<(), ExecutionError> {
        let session = Arc::new(Session::new(1));
        let plan = PointInTimeOperator::Project(point_in_time::Project {
            expressions: vec![Expression::from(1)],
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
