use crate::utils::logical::fields_for_operator;
use crate::PlannerError;
use ast::rel::logical::LogicalOperator;

/// Checks to make sure the union all children are compatible with each other.
pub(super) fn check_unions(operator: &mut LogicalOperator) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        check_unions(child)?;
    }

    if let LogicalOperator::UnionAll(union_all) = operator {
        let mut rest = union_all.sources.iter_mut().enumerate();
        let (_, first) = rest.next().unwrap();
        let first_fields: Vec<_> = fields_for_operator(first).map(|f| f.data_type).collect();
        for (operator_idx, operator) in rest {
            let fields: Vec<_> = fields_for_operator(operator).map(|f| f.data_type).collect();

            if first_fields != fields {
                return Err(PlannerError::UnionAllMismatch(
                    first_fields,
                    fields,
                    operator_idx,
                ));
            }
        }
    }

    Ok(())
}
