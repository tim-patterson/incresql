use crate::{QueryError, Runtime};
use ast::expr::Expression;
use ast::rel::logical::{LogicalOperator, Values};
use ast::rel::statement::Statement;
use data::{DataType, Session, TupleIter};
use executor::point_in_time::{build_executor, BoxedExecutor};
use parser::parse;
use planner::Field;
use std::sync::Arc;

/// Represents a connection to the database.  Note this is the logical connection, not the physical
/// tcp connection.
#[derive(Debug)]
pub struct Connection<'a> {
    pub connection_id: u32,
    pub session: Arc<Session>,
    pub runtime: &'a Runtime,
}

impl Drop for Connection<'_> {
    fn drop(&mut self) {
        self.runtime.remove_connection(self.connection_id);
    }
}

impl Connection<'_> {
    pub fn execute_statement(
        &self,
        query: &str,
    ) -> Result<(Vec<Field>, BoxedExecutor), QueryError> {
        let parse_tree = parse(query)?;

        // For almost everything we'll rewrite into some kinda logical operator
        let logical_operator = match parse_tree {
            Statement::ShowFunctions => {
                let data = self
                    .runtime
                    .planner
                    .function_registry
                    .list_functions()
                    .map(|name| vec![Expression::from(name)])
                    .collect();

                LogicalOperator::Values(Values {
                    fields: vec![(DataType::Text, String::from("function_name"))],
                    data,
                })
            }
            Statement::ShowDatabases => {
                return self.execute_statement("SELECT name as database FROM incresql.databases")
            }
            Statement::ShowTables => {
                return self.execute_statement(
                    "SELECT name as table FROM incresql.tables WHERE database_name = database()",
                );
            }
            Statement::UseDatabase(database) => {
                *self.session.current_database.write().unwrap() = database;
                return Ok((vec![], TupleIter::empty()));
            }
            Statement::Query(logical_operator) => logical_operator,
            Statement::Explain(explain) => self
                .runtime
                .planner
                .explain_logical(explain.operator, &self.session)?,
            Statement::CreateDatabase(create_database) => {
                let mut catalog = self.runtime.planner.catalog.write().unwrap();
                catalog.create_database(&create_database.name)?;
                return Ok((vec![], TupleIter::empty()));
            }
            Statement::DropDatabase(database) => {
                let mut catalog = self.runtime.planner.catalog.write().unwrap();
                catalog.drop_database(&database)?;
                return Ok((vec![], TupleIter::empty()));
            }
            Statement::CreateTable(create_table) => {
                let mut catalog = self.runtime.planner.catalog.write().unwrap();
                let database = create_table
                    .database
                    .unwrap_or_else(|| self.session.current_database.read().unwrap().to_string());

                catalog.create_table(&database, &create_table.name, &create_table.columns)?;
                return Ok((vec![], TupleIter::empty()));
            }
        };

        let plan = self
            .runtime
            .planner
            .plan_for_point_in_time(logical_operator, &self.session)?;
        let executor = build_executor(&self.session, &plan.operator);
        Ok((plan.fields, executor))
    }

    pub fn change_database(&self, database: &str) -> Result<(), QueryError> {
        *self.session.current_database.write().unwrap() = String::from(database);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::{DataType, Datum};

    #[test]
    fn test_execute_statement() -> Result<(), QueryError> {
        let runtime = Runtime::new_for_test();
        let connection = runtime.new_connection();
        let (fields, mut executor) = connection.execute_statement("select 1")?;
        assert_eq!(
            fields,
            vec![Field {
                qualifier: None,
                alias: "_col1".to_string(),
                data_type: DataType::Integer
            }]
        );
        assert_eq!(executor.next()?, Some(([Datum::from(1)].as_ref(), 1)));
        Ok(())
    }

    #[test]
    fn test_execute_statement_rewrite() -> Result<(), QueryError> {
        let runtime = Runtime::new_for_test();
        let connection = runtime.new_connection();
        let (fields, _executor) = connection.execute_statement("show functions")?;
        assert_eq!(
            fields,
            vec![Field {
                qualifier: None,
                alias: "function_name".to_string(),
                data_type: DataType::Text
            }]
        );
        Ok(())
    }

    #[test]
    fn test_change_database() -> Result<(), QueryError> {
        let runtime = Runtime::new_for_test();
        let connection = runtime.new_connection();
        connection.change_database("change_to_foo")?;
        assert_eq!(
            *connection.session.current_database.read().unwrap(),
            "change_to_foo"
        );
        Ok(())
    }
}
