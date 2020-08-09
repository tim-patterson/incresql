use crate::common::type_for_expression;
use crate::{Planner, PlannerError};
use ast::expr::{CompiledFunctionCall, Expression};
use ast::rel::logical::LogicalOperator;
use data::{DataType, Datum};
use functions::registry::{FunctionResolutionError, Registry};
use functions::FunctionSignature;

/// Validate the query, as part of the process of validating the query we will actually end up
/// doing all the catalog and function lookups and subbing them in.
impl Planner {
    pub fn validate(&self, mut query: LogicalOperator) -> Result<LogicalOperator, PlannerError> {
        compile_functions(&mut query, &self.function_registry)?;
        Ok(query)
    }
}

fn compile_functions(
    operator: &mut LogicalOperator,
    function_registry: &Registry,
) -> Result<(), FunctionResolutionError> {
    for child in operator.children_mut() {
        compile_functions(child, function_registry)?;
    }

    for expr in operator.expressions_mut() {
        compile_functions_in_expr(expr, function_registry)?;
    }
    Ok(())
}

fn compile_functions_in_expr(
    expression: &mut Expression,
    function_registry: &Registry,
) -> Result<(), FunctionResolutionError> {
    match expression {
        Expression::FunctionCall(function_call) => {
            for arg in function_call.args.iter_mut() {
                compile_functions_in_expr(arg, function_registry)?;
            }

            let arg_types = function_call.args.iter().map(type_for_expression).collect();

            let lookup_sig = FunctionSignature {
                name: &function_call.function_name,
                args: arg_types,
                ret: DataType::Null,
            };

            let (signature, function) = function_registry.resolve_scalar_function(&lookup_sig)?;

            let mut args = Vec::new();
            std::mem::swap(&mut args, &mut function_call.args);

            *expression = Expression::CompiledFunctionCall(CompiledFunctionCall {
                function,
                args,
                expr_buffer: vec![],
                signature: Box::new(signature),
            })
        }
        Expression::Cast(cast) => {
            compile_functions_in_expr(&mut cast.expr, function_registry)?;

            let expr_type = type_for_expression(&cast.expr);

            let function_name = match cast.datatype {
                DataType::Null => panic!("Attempted cast to null"),
                DataType::Boolean => "to_bool",
                DataType::Integer => "to_int",
                DataType::BigInt => "to_bigint",
                DataType::Decimal(..) => "to_decimal",
                DataType::Text => "to_text",
            };

            let lookup_sig = FunctionSignature {
                name: function_name,
                args: vec![expr_type],
                ret: cast.datatype,
            };

            let (signature, function) = function_registry.resolve_scalar_function(&lookup_sig)?;

            let mut expr = Expression::Literal(Datum::Null);

            std::mem::swap(&mut expr, &mut cast.expr);

            *expression = Expression::CompiledFunctionCall(CompiledFunctionCall {
                function,
                args: vec![expr],
                expr_buffer: vec![],
                signature: Box::new(signature),
            })
        }
        Expression::Literal(_) => {}
        Expression::CompiledFunctionCall(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Field;
    use ast::expr::{FunctionCall, NamedExpression};
    use ast::rel::logical::Project;
    use data::{Datum, Session};
    use functions::Function;

    // A dummy function to use in the test cases.
    #[derive(Debug)]
    struct DummyFunct {}

    impl Function for DummyFunct {
        fn execute<'a>(
            &self,
            _session: &Session,
            _sig: &FunctionSignature,
            _args: &'a [Datum<'a>],
        ) -> Datum<'a> {
            unimplemented!()
        }
    }

    #[test]
    fn test_compile_function() -> Result<(), PlannerError> {
        let planner = Planner::default();
        let raw_query = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::FunctionCall(FunctionCall {
                    function_name: "+".to_string(),
                    args: vec![
                        Expression::Literal(Datum::from(1)),
                        Expression::FunctionCall(FunctionCall {
                            function_name: "+".to_string(),
                            args: vec![
                                Expression::Literal(Datum::from(2)),
                                Expression::Literal(Datum::from(3)),
                            ],
                        }),
                    ],
                }),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let expected = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: Some("_col1".to_string()),
                expression: Expression::CompiledFunctionCall(CompiledFunctionCall {
                    function: &DummyFunct {},
                    args: vec![
                        Expression::Literal(Datum::from(1)),
                        Expression::CompiledFunctionCall(CompiledFunctionCall {
                            function: &DummyFunct {},
                            args: vec![
                                Expression::Literal(Datum::from(2)),
                                Expression::Literal(Datum::from(3)),
                            ],
                            expr_buffer: vec![],
                            signature: Box::new(FunctionSignature {
                                name: "+",
                                args: vec![DataType::Integer, DataType::Integer],
                                ret: DataType::Integer,
                            }),
                        }),
                    ],
                    expr_buffer: vec![],
                    signature: Box::new(FunctionSignature {
                        name: "+",
                        args: vec![DataType::Integer, DataType::Integer],
                        ret: DataType::Integer,
                    }),
                }),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let (fields, operator) = planner.plan_common(raw_query)?;

        assert_eq!(operator, expected);

        assert_eq!(
            fields,
            vec![Field {
                alias: String::from("_col1"),
                data_type: DataType::Integer
            }]
        );

        Ok(())
    }
}
