use crate::utils::expr::{combine_predicates, decompose_predicate};
use ast::expr::Expression;
use ast::rel::logical::{Filter, LogicalOperator};
use functions::registry::Registry;

/// Decomposes filters by splitting them at "ands" and then pushing each fragment down
/// as far as we can.
/// Filters come in two places.
/// 1. The where clauses (filter operators)
/// 2. Join conditions.
pub(super) fn filter_pushdown(operator: &mut LogicalOperator, function_registry: &Registry) {
    filter_pushdown_from_above(operator, Vec::new(), function_registry)
}

fn filter_pushdown_from_above(
    operator: &mut LogicalOperator,
    predicates: Vec<Expression>,
    function_registry: &Registry,
) {
    // The idea here is that we start at the top and for each operator we accept a list of predicates
    // from above that have been pushed down.
    // For the combined list of predicates (ie internal ones from filter and join operators combined
    // with those from above) we decide if we can push them down further or not. If we can we do,
    // other wise we wrap ourselves in a filter operator with those predicates.
    // As we push all the predicates out of a filter, filters should actually be removed.
    match operator {
        LogicalOperator::Filter(filter) => {
            let mut predicate = Expression::from(true);
            std::mem::swap(&mut predicate, &mut filter.predicate);
            let predicates = decompose_predicate(predicate).collect();

            // Push down filters
            filter_pushdown_from_above(filter.source.as_mut(), predicates, function_registry);

            // Remove the now useless filter.
            let mut source = LogicalOperator::default();
            std::mem::swap(&mut source, &mut filter.source);

            *operator = source
        }
        // We can always transparently push through these operators
        LogicalOperator::Sort(sort) => {
            filter_pushdown_from_above(sort.source.as_mut(), predicates, function_registry);
        }
        LogicalOperator::NegateFreq(source) => {
            filter_pushdown_from_above(source.as_mut(), predicates, function_registry);
        }

        // These are the operators we can never push through, (we technically could with
        // limit but it would have the opposite effect in actually creating more work
        // for the query engine)
        _ => {
            if !predicates.is_empty() {
                let mut source = LogicalOperator::default();
                std::mem::swap(&mut source, operator);

                *operator = LogicalOperator::Filter(Filter {
                    predicate: combine_predicates(predicates, function_registry),
                    source: Box::new(source),
                });
            }

            // Start pushing down again below
            for op in operator.children_mut() {
                filter_pushdown_from_above(op, vec![], function_registry);
            }
        }
    }
}
