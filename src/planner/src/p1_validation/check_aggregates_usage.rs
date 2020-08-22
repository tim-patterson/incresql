use crate::PlannerError;
use ast::expr::Expression;
use ast::rel::logical::LogicalOperator;

/// Checks to make sure the only place aggregates appear is in a group by statement
pub(super) fn check_for_aggregates(operator: &mut LogicalOperator) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        check_for_aggregates(child)?;
    }

    match operator {
        LogicalOperator::GroupBy(group_by) => {
            for expr in &group_by.key_expressions {
                throw_on_aggregate(expr, "group by clause")?;
            }
        }
        LogicalOperator::Filter(filter) => {
            throw_on_aggregate(&filter.predicate, "where clause")?;
        }
        LogicalOperator::Sort(sort) => {
            for se in &sort.sort_expressions {
                throw_on_aggregate(&se.expression, "order by clause")?;
            }
        }
        // These have no expressions in them to be checked
        // Any in the project would have caused the project to be converted to a group by.
        LogicalOperator::Project(_)
        | LogicalOperator::Single
        | LogicalOperator::Limit(_)
        | LogicalOperator::Values(_)
        | LogicalOperator::TableAlias(_)
        | LogicalOperator::UnionAll(_)
        | LogicalOperator::TableReference(_)
        | LogicalOperator::ResolvedTable(_)
        | LogicalOperator::TableInsert(_)
        | LogicalOperator::NegateFreq(_) => {}
    }
    Ok(())
}

/// Returns true if the expression contains an aggregate anywhere in its expressions.
fn throw_on_aggregate(expr: &Expression, location: &'static str) -> Result<(), PlannerError> {
    if let Expression::CompiledAggregate(function) = expr {
        Err(PlannerError::AggregateNotAllowed(
            function.signature.name,
            location,
        ))
    } else {
        for child in expr.children() {
            throw_on_aggregate(child, location)?;
        }
        Ok(())
    }
}
