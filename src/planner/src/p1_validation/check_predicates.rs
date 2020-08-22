use crate::utils::expr::type_for_expression;
use crate::PlannerError;
use ast::rel::logical::LogicalOperator;
use data::DataType;

/// Checks to make sure all predicate expressions are boolean expressions
pub(super) fn check_predicates(operator: &mut LogicalOperator) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        check_predicates(child)?;
    }

    if let LogicalOperator::Filter(filter) = operator {
        match type_for_expression(&filter.predicate) {
            DataType::Boolean | DataType::Null => {}
            datatype => {
                return Err(PlannerError::PredicateNotBoolean(
                    datatype,
                    filter.predicate.clone(),
                ))
            }
        }
    }
    Ok(())
}
