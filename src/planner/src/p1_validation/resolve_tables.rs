use crate::p1_validation::{column_aliases, sub_in_special_vars};
use crate::PlannerError;
use ast::rel::logical::{LogicalOperator, ResolvedTable};
use ast::statement::Statement;
use catalog::{Catalog, TableOrView};
use data::Session;

pub(super) fn resolve_tables(
    catalog: &Catalog,
    operator: &mut LogicalOperator,
    session: &Session,
) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        resolve_tables(catalog, child, session)?;
    }

    if let LogicalOperator::TableReference(table_ref) = operator {
        // In a block to drop the lock as we need  to get write access to it further down for
        // views
        let item = {
            let current_db = session.current_database.read().unwrap();
            let database = table_ref.database.as_ref().unwrap_or(&current_db);
            let table_name = &table_ref.table;

            catalog.item(database, table_name)?
        };
        match item.item {
            TableOrView::Table(table) => {
                *operator = LogicalOperator::ResolvedTable(ResolvedTable {
                    columns: item.columns,
                    table,
                })
            }
            TableOrView::View(view) => {
                if let Statement::Query(op) =
                    parser::parse(&view.sql).expect("Parse failed for view?")
                {
                    *operator = op;
                    // Run the planner over the subbed-in sql up to the current phase
                    sub_in_special_vars::sub_in_special_vars(operator);
                    column_aliases::normalize_column_aliases(operator);
                    // Use a session with the "current" db being the same as the one the
                    let mut current_db = view.db_context;
                    {
                        std::mem::swap(
                            &mut current_db,
                            &mut session.current_database.write().unwrap(),
                        );
                    }
                    for child in operator.children_mut() {
                        resolve_tables(catalog, child, session)?;
                    }
                    // TODO on a failure this will leave the current db changed...
                    std::mem::swap(
                        &mut current_db,
                        &mut session.current_database.write().unwrap(),
                    );
                } else {
                    panic!("Bogus view")
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::logical::fields_for_operator;
    use crate::Field;
    use ast::rel::logical::TableReference;
    use data::DataType;

    #[test]
    fn test_resolve_table_qualified() -> Result<(), PlannerError> {
        let catalog = Catalog::new_for_test().unwrap();
        let session = Session::new(1);
        let mut operator = LogicalOperator::TableReference(TableReference {
            database: Some("incresql".to_string()),
            table: "databases".to_string(),
        });

        resolve_tables(&catalog, &mut operator, &session)?;
        let fields: Vec<_> = fields_for_operator(&operator).collect();

        assert_eq!(
            fields,
            vec![Field {
                qualifier: None,
                alias: "name".to_string(),
                data_type: DataType::Text
            }]
        );

        Ok(())
    }

    #[test]
    fn test_resolve_table_unqualified() -> Result<(), PlannerError> {
        let catalog = Catalog::new_for_test().unwrap();
        let session = Session::new(1);
        *session.current_database.write().unwrap() = "incresql".to_string();
        let mut operator = LogicalOperator::TableReference(TableReference {
            database: None,
            table: "databases".to_string(),
        });

        resolve_tables(&catalog, &mut operator, &session)?;
        let fields: Vec<_> = fields_for_operator(&operator).collect();

        assert_eq!(
            fields,
            vec![Field {
                qualifier: None,
                alias: "name".to_string(),
                data_type: DataType::Text
            }]
        );

        Ok(())
    }
}
