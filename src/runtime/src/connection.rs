use crate::{QueryError, Runtime};
use ast::rel::statement::Statement;
use data::Session;
use executor::point_in_time::{build_executor, Executor};
use parser::parse;
use planner::plan_for_point_in_time;

/// Represents a connection to the database.  Note this is the logical connection, not the physical
/// tcp connection.
#[derive(Debug)]
pub struct Connection<'a> {
    pub connection_id: u32,
    pub session: Session,
    pub runtime: &'a Runtime,
}

impl Drop for Connection<'_> {
    fn drop(&mut self) {
        self.runtime.remove_connection(self.connection_id);
    }
}

impl Connection<'_> {
    pub fn execute_statement(&self, query: &str) -> Result<Box<dyn Executor>, QueryError> {
        let parse_tree = parse(query)?;
        match parse_tree {
            Statement::Query(logical_operator) => {
                let physical_operator = plan_for_point_in_time(logical_operator)?;
                Ok(build_executor(&physical_operator))
            }
        }
    }

    pub fn change_database(&self, database: &str) {
        *self.session.current_database.write().unwrap() = String::from(database);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::Datum;

    #[test]
    fn test_execute_statement() -> Result<(), QueryError> {
        let runtime = Runtime::new();
        let connection = runtime.new_connection();
        let mut executor = connection.execute_statement("select 1")?;
        assert_eq!(executor.next()?, Some(([Datum::from(1)].as_ref(), 1)));
        Ok(())
    }
}
