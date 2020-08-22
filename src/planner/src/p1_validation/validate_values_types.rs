use crate::utils::expr::type_for_expression;
use crate::PlannerError;
use ast::rel::logical::{LogicalOperator, TableInsert};
use data::DataType;

/// Walks "values" (ie insert .. values ()) and populates types in the header,
/// has to happen fairly early on in the planning
pub(super) fn validate_values_types(query: &mut LogicalOperator) -> Result<(), PlannerError> {
    for child in query.children_mut() {
        validate_values_types(child)?;
    }

    if let LogicalOperator::TableInsert(TableInsert { table, source }) = query {
        if let (LogicalOperator::Values(values), LogicalOperator::ResolvedTable(resolved_tables)) =
            (source.as_mut(), table.as_mut())
        {
            values.fields = resolved_tables
                .table
                .columns()
                .iter()
                .map(|(alias, dt)| (*dt, alias.clone()))
                .collect();

            let table_types: Vec<_> = values
                .fields
                .iter()
                .map(|(datatype, _)| *datatype)
                .collect();
            for row in &values.data {
                let row_types: Vec<_> = row.iter().map(type_for_expression).collect();
                let is_match = row_types
                    .iter()
                    .zip(table_types.iter())
                    .all(|(row, table)| row == table || *row == DataType::Null);
                if !is_match {
                    return Err(PlannerError::InsertMismatch(table_types, row_types));
                }
            }
        }
    }
    Ok(())
}
