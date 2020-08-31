use crate::utils::expr::*;
use crate::utils::logical::*;
use ast::expr::{CompiledColumnReference, Expression, NamedExpression};
use ast::rel::logical::LogicalOperator;
use data::Session;
use functions::registry::Registry;

/// Detects equi-joins, places their inputs in order in the source tables
/// and ensures any complex parts are pushed down to the left or right
pub(crate) fn normalize_joins(
    query: &mut LogicalOperator,
    session: &Session,
    function_registry: &Registry,
) {
    for operator in query.children_mut() {
        normalize_joins(operator, session, function_registry)
    }

    if let LogicalOperator::Join(join) = query {
        // For a join we're going to create 3 new projects.
        // one at the head of each of the inputs where we
        // can evaluate join expressions and one after
        // the join to remove the expressions we've inserted

        // Grap the current condition out, and the left and right
        let mut condition = Expression::from(true);
        std::mem::swap(&mut condition, &mut join.on);
        let mut left = LogicalOperator::default();
        let mut right = LogicalOperator::default();
        std::mem::swap(&mut left, &mut join.left);
        std::mem::swap(&mut right, &mut join.right);

        let left_len = fieldnames_for_operator(&left).count();

        let mut equi_conditions = vec![];
        let mut non_equi_conditions = vec![];

        // Decompose the expressions and group them into equi_conditions and not equi conditions.
        for expr in decompose_predicate(condition) {
            match expr {
                Expression::CompiledFunctionCall(mut function)
                    if function.signature.name == "=" =>
                {
                    let left_idxs = min_max_column_deps_for_expression(&mut function.args[0]);
                    let right_idxs = min_max_column_deps_for_expression(&mut function.args[1]);

                    match (left_idxs, right_idxs) {
                        (Some((_left_min, left_max)), Some((right_min, _right_max)))
                            if left_max < left_len && right_min >= left_len =>
                        {
                            // left and right are the right way around...
                            equi_conditions.push(function);
                        }
                        (Some((left_min, _left_max)), Some((_right_min, right_max)))
                            if left_min >= left_len && right_max < left_len =>
                        {
                            // left and right are the wrong way around, lets swap them
                            function.args.swap(0, 1);
                            equi_conditions.push(function);
                        }
                        _ => {
                            // We shouldn't get here if predicate pushdown did it's job...
                            non_equi_conditions.push(Expression::CompiledFunctionCall(function));
                        }
                    }
                }
                e => non_equi_conditions.push(e),
            }
        }

        let mut left_project = create_wrapping_project(left);
        let mut right_project = create_wrapping_project(right);

        let equi_len = equi_conditions.len();

        // Now we want to move the join exprs out of the condition into the underlying project
        for (idx, equi_expr) in equi_conditions.iter_mut().enumerate() {
            // Left
            let mut left_expr = Expression::CompiledColumnReference(CompiledColumnReference {
                offset: idx,
                datatype: type_for_expression(&equi_expr.args[0]),
            });
            std::mem::swap(&mut left_expr, &mut equi_expr.args[0]);
            left_project.expressions.insert(
                0,
                NamedExpression {
                    alias: Some(format!("key_{}", idx)),
                    expression: left_expr,
                },
            );

            // Right
            let mut right_expr = Expression::CompiledColumnReference(CompiledColumnReference {
                offset: idx + left_len + equi_len,
                datatype: type_for_expression(&equi_expr.args[1]),
            });
            std::mem::swap(&mut right_expr, &mut equi_expr.args[1]);

            move_column_references(&mut right_expr, -(left_len as isize));
            right_project.expressions.insert(
                0,
                NamedExpression {
                    alias: Some(format!("key_{}", idx)),
                    expression: right_expr,
                },
            );
        }

        // Swap in the upstream projects.
        join.left = Box::from(LogicalOperator::Project(left_project));
        join.right = Box::from(LogicalOperator::Project(right_project));

        // Reassemble the predicates
        let conditions: Vec<_> = equi_conditions
            .into_iter()
            .map(Expression::CompiledFunctionCall)
            .chain(non_equi_conditions.into_iter())
            .collect();

        join.on = combine_predicates(conditions, function_registry);

        // Create wrapping project
        let mut join_operator = LogicalOperator::default();
        std::mem::swap(&mut join_operator, query);

        let mut wrapping_project = create_wrapping_project(join_operator);

        // we now want to remove the first equi_join count columns (the left ones)
        for _ in 0..equi_len {
            wrapping_project.expressions.remove(0);
        }

        // we now want to remove equi_join count columns at the start of the right side
        for _ in 0..equi_len {
            wrapping_project.expressions.remove(left_len);
        }

        *query = LogicalOperator::Project(wrapping_project)
    }
}
