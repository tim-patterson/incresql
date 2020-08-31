use crate::utils::expr::{
    combine_predicates, decompose_predicate, inline_expression, min_max_column_deps_for_expression,
    move_column_references,
};
use crate::utils::logical::fieldnames_for_operator;
use ast::expr::Expression;
use ast::rel::logical::{Filter, LogicalOperator};
use functions::registry::Registry;

/// Decomposes filters by splitting them at "ands" and then pushing each fragment down
/// as far as we can.
/// Filters come in two places.
/// 1. The where clauses (filter operators)
/// 2. Join conditions.
pub(super) fn predicate_pushdown(operator: &mut LogicalOperator, function_registry: &Registry) {
    pushdown_predicates_from_above(operator, Vec::new(), function_registry)
}

fn pushdown_predicates_from_above(
    operator: &mut LogicalOperator,
    mut predicates: Vec<Expression>,
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
            pushdown_predicates_from_above(filter.source.as_mut(), predicates, function_registry);

            // Remove the now useless filter.
            let mut source = LogicalOperator::default();
            std::mem::swap(&mut source, &mut filter.source);

            *operator = source
        }

        // We can always transparently push through these operators
        LogicalOperator::Sort(sort) => {
            pushdown_predicates_from_above(sort.source.as_mut(), predicates, function_registry);
        }
        LogicalOperator::NegateFreq(source) => {
            pushdown_predicates_from_above(source.as_mut(), predicates, function_registry);
        }

        LogicalOperator::Project(project) => {
            // For project we just inline the expressions and push them down
            if !predicates.is_empty() {
                let project_exprs: Vec<_> = project
                    .expressions
                    .iter()
                    .map(|ne| &ne.expression)
                    .collect();
                for pred in &mut predicates {
                    inline_expression(pred, &project_exprs);
                }
            }
            pushdown_predicates_from_above(&mut project.source, predicates, function_registry)
        }

        LogicalOperator::UnionAll(union) => {
            // Union we just push it through, worst case we end up with 3 filters immediately
            // below the union, while a little messier it will make no difference for perf.
            for source in &mut union.sources {
                pushdown_predicates_from_above(source, predicates.clone(), function_registry);
            }
        }

        LogicalOperator::Join(join) => {
            // We should always be able to push a filter into a join but deciding what
            // we can push down further is a little trickier.
            let mut join_predicate = Expression::from(true);
            std::mem::swap(&mut join_predicate, &mut join.on);

            // This is now all the conditions we can apply to the join.
            // We can bucket these into 4 buckets, predicates that depend on the
            // left table, the right table, both tables, no tables(constant)
            predicates.extend(decompose_predicate(join_predicate));
            let left_len = fieldnames_for_operator(&join.left).count();
            let mut left = vec![];
            let mut right = vec![];
            let mut both = vec![];

            for mut predicate in predicates {
                match min_max_column_deps_for_expression(&mut predicate) {
                    None => {
                        // Constant, push it down both sides
                        left.push(predicate.clone());
                        right.push(predicate)
                    }
                    Some((_min, max)) if max < left_len => left.push(predicate),
                    Some((min, _max)) if min >= left_len => right.push(predicate),
                    _ => both.push(predicate),
                }
            }
            // Fix up the indexes for the right side.
            for expr in right.iter_mut() {
                move_column_references(expr, -(left_len as isize));
            }
            // Put back join condition bits that we can't push down.
            join.on = combine_predicates(both, function_registry);
            // Push down each side
            pushdown_predicates_from_above(&mut join.left, left, function_registry);
            pushdown_predicates_from_above(&mut join.right, right, function_registry);
        }

        // The remaining operators we can never push through, (we technically could with
        // limit but it would have the opposite effect in actually creating more work
        // for the query engine)
        // TODO We can push filters through a group by where the predicates only
        // depend on the grouping keys.
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
                pushdown_predicates_from_above(op, vec![], function_registry);
            }
        }
    }
}
