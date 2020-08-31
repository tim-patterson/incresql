use crate::{Planner, PlannerError};
use ast::rel::logical::LogicalOperator;
use data::Session;

mod filter_pushdown;
mod fold_constants;

impl Planner {
    /// Optimizes the query by rewriting parts of it to be more efficient.
    pub fn optimize(
        &self,
        mut query: LogicalOperator,
        session: &Session,
    ) -> Result<LogicalOperator, PlannerError> {
        fold_constants::fold_constants(&mut query, session);
        filter_pushdown::filter_pushdown(&mut query);
        Ok(query)
    }
}
