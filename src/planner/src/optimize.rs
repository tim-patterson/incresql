use crate::{Planner, PlannerError};
use ast::expr::Expression;
use ast::rel::logical::LogicalOperator;
use data::Session;

impl Planner {
    /// Optimizes the query by rewriting parts of it to be more efficient.
    pub(crate) fn optimize(
        &self,
        mut query: LogicalOperator,
        session: &Session,
    ) -> Result<LogicalOperator, PlannerError> {
        fold_constants(&mut query, session);
        Ok(query)
    }
}

/// Simplifies expressions involving only constants
fn fold_constants(query: &mut LogicalOperator, session: &Session) {
    for child in query.children_mut() {
        fold_constants(child, session);
    }

    for expr in query.expressions_mut() {
        fold_constants_for_expr(expr, session);
    }
}

fn fold_constants_for_expr(expr: &mut Expression, session: &Session) {
    match expr {
        Expression::CompiledFunctionCall(function_call) => {
            for arg in function_call.args.iter_mut() {
                fold_constants_for_expr(arg, session);
            }

            // Rust fmt doesn't seem to agree with clippy lol
            #[allow(clippy::blocks_in_if_conditions)]
            if function_call.args.iter().all(|expr| {
                if let Expression::Constant(..) = expr {
                    true
                } else {
                    false
                }
            }) {
                let function_input: Vec<_> = function_call
                    .args
                    .iter()
                    .map(|constant_expr| {
                        if let Expression::Constant(value, _) = constant_expr {
                            value.ref_clone()
                        } else {
                            // We just checked this a few lines above
                            panic!()
                        }
                    })
                    .collect();
                // Run the function and make sure the output is static
                let constant = function_call
                    .function
                    .execute(session, &function_call.signature, &function_input)
                    .as_static();

                *expr = Expression::Constant(constant, function_call.signature.ret);
            }
        }
        // Already a constant
        Expression::Constant(..) => {}
        // These should be gone by now.
        Expression::Cast(_) | Expression::FunctionCall(_) => panic!(
            "Hit {:?} in constant fold, this should be gone by now!",
            expr
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{CompiledFunctionCall, Expression, NamedExpression};
    use ast::rel::logical::Project;
    use data::DataType;
    use functions::registry::Registry;
    use functions::FunctionSignature;

    #[test]
    fn test_constant_fold() -> Result<(), PlannerError> {
        let planner = Planner::new(Registry::new(true));
        let session = Session::new(1);
        let add_signature = FunctionSignature {
            name: "+",
            args: vec![DataType::Integer, DataType::Integer],
            ret: DataType::Integer,
        };
        let (_, add_function) = planner
            .function_registry
            .resolve_scalar_function(&add_signature)?;

        // 1 + (2 + 3)
        let operator = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::CompiledFunctionCall(CompiledFunctionCall {
                    function: add_function,
                    args: Box::from(vec![
                        Expression::from(1),
                        Expression::CompiledFunctionCall(CompiledFunctionCall {
                            function: add_function,
                            args: Box::from(vec![Expression::from(2), Expression::from(3)]),
                            expr_buffer: Box::from(vec![]),
                            signature: Box::new(add_signature.clone()),
                        }),
                    ]),
                    expr_buffer: Box::from(vec![]),
                    signature: Box::new(add_signature.clone()),
                }),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let optimized = planner.optimize(operator, &session)?;

        let expected = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::from(6),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        assert_eq!(optimized, expected);

        Ok(())
    }
}
