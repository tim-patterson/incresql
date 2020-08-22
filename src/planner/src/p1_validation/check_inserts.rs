use crate::utils::logical::fields_for_operator;
use crate::PlannerError;
use ast::rel::logical::LogicalOperator;

/// Checks to make sure we're inserting rows with the right datatypes/length
pub(super) fn check_inserts(operator: &mut LogicalOperator) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        check_inserts(child)?;
    }

    if let LogicalOperator::TableInsert(table_insert) = operator {
        let table_fields: Vec<_> = fields_for_operator(&table_insert.table)
            .map(|f| f.data_type)
            .collect();
        let source_fields: Vec<_> = fields_for_operator(&table_insert.source)
            .map(|f| f.data_type)
            .collect();

        if table_fields != source_fields {
            Err(PlannerError::InsertMismatch(table_fields, source_fields))
        } else {
            Ok(())
        }
    } else {
        Ok(())
    }
}
