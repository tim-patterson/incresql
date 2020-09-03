use crate::PlannerError;
use ast::rel::logical::{LogicalOperator, ResolvedTable};
use catalog::Catalog;
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
        let current_db = session.current_database.read().unwrap();
        let database = table_ref.database.as_ref().unwrap_or(&current_db);
        let table_name = &table_ref.table;

        let table = catalog.item(database, table_name)?;
        *operator = LogicalOperator::ResolvedTable(ResolvedTable { table: table.table })
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
