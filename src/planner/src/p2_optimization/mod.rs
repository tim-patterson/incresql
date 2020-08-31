use crate::{Planner, PlannerError};
use ast::rel::logical::LogicalOperator;
use data::Session;

mod fold_constants;
mod predicate_pushdown;

impl Planner {
    /// Optimizes the query by rewriting parts of it to be more efficient.
    pub fn optimize(
        &self,
        mut query: LogicalOperator,
        session: &Session,
    ) -> Result<LogicalOperator, PlannerError> {
        fold_constants::fold_constants(&mut query, session);
        predicate_pushdown::predicate_pushdown(&mut query, &self.function_registry);
        // After pushing down the predicates it can open up some more options for constant folding
        fold_constants::fold_constants(&mut query, session);
        Ok(query)
    }
}
