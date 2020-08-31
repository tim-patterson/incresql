use crate::p2_optimization::collapse_projects;
use crate::{Planner, PlannerError};
use ast::rel::logical::LogicalOperator;
use data::Session;

mod normalize_joins;

impl Planner {
    /// Rewrites the query in a way that the execution planning can make use of
    pub fn common_transforms(
        &self,
        mut query: LogicalOperator,
        session: &Session,
    ) -> Result<LogicalOperator, PlannerError> {
        normalize_joins::normalize_joins(&mut query, session, &self.function_registry);
        // Normalize joins creates a whole bunch of unneeded projects this should clean
        // them up
        collapse_projects::collapse_projects(&mut query);
        Ok(query)
    }
}
