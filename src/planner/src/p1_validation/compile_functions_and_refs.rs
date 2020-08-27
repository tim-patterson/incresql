use crate::utils::expr::type_for_expression;
use crate::utils::logical::source_fields_for_operator;
use crate::{Field, FieldResolutionError, PlannerError};
use ast::expr::*;
use ast::rel::logical::LogicalOperator;
use data::{DataType, Datum};
use functions::registry::Registry;
use functions::{FunctionSignature, FunctionType};

pub(super) fn compile_functions(
    operator: &mut LogicalOperator,
    function_registry: &Registry,
) -> Result<(), PlannerError> {
    for child in operator.children_mut() {
        compile_functions(child, function_registry)?;
    }

    let source_fields: Vec<_> = source_fields_for_operator(operator).collect();
    for expr in operator.expressions_mut() {
        compile_functions_in_expr(expr, &source_fields, function_registry)?;
    }
    Ok(())
}

fn compile_functions_in_expr(
    expression: &mut Expression,
    source_fields: &[Field],
    function_registry: &Registry,
) -> Result<(), PlannerError> {
    match expression {
        Expression::FunctionCall(function_call) => {
            for arg in function_call.args.iter_mut() {
                compile_functions_in_expr(arg, source_fields, function_registry)?;
            }

            let arg_types = function_call.args.iter().map(type_for_expression).collect();

            let lookup_sig = FunctionSignature {
                name: &function_call.function_name,
                args: arg_types,
                ret: DataType::Null,
            };

            let (signature, function) = function_registry.resolve_function(&lookup_sig)?;

            let mut args = Vec::new();
            std::mem::swap(&mut args, &mut function_call.args);

            *expression = match function {
                FunctionType::Scalar(function) => {
                    Expression::CompiledFunctionCall(CompiledFunctionCall {
                        function,
                        args: Box::from(args),
                        expr_buffer: Box::from(vec![]),
                        signature: Box::new(signature),
                    })
                }
                FunctionType::Aggregate(function) => {
                    Expression::CompiledAggregate(CompiledAggregate {
                        function,
                        args: Box::from(args),
                        expr_buffer: Box::from(vec![]),
                        signature: Box::new(signature),
                    })
                }
                FunctionType::Compound(_) => unimplemented!(),
            };
        }
        Expression::Cast(cast) => {
            compile_functions_in_expr(&mut cast.expr, source_fields, function_registry)?;

            let expr_type = type_for_expression(&cast.expr);

            let function_name = match cast.datatype {
                DataType::Null => panic!("Attempted cast to null"),
                DataType::Boolean => "to_bool",
                DataType::Integer => "to_int",
                DataType::BigInt => "to_bigint",
                DataType::Decimal(..) => "to_decimal",
                DataType::Text => "to_text",
                DataType::ByteA => "to_bytes",
                DataType::Json => "to_json",
                DataType::Date => "to_date",
            };

            let lookup_sig = FunctionSignature {
                name: function_name,
                args: vec![expr_type],
                ret: cast.datatype,
            };

            let (signature, function) = function_registry.resolve_function(&lookup_sig)?;

            // Just an "empty" value to swap
            let mut expr = Expression::Constant(Datum::Null, DataType::Null);

            std::mem::swap(&mut expr, &mut cast.expr);
            if let FunctionType::Scalar(function) = function {
                *expression = Expression::CompiledFunctionCall(CompiledFunctionCall {
                    function,
                    args: Box::from(vec![expr]),
                    expr_buffer: Box::from(vec![]),
                    signature: Box::new(signature),
                })
            } else {
                panic!("Cast needs to be a scalar function")
            }
        }
        Expression::ColumnReference(column_reference) => {
            let indexed_source_fields = source_fields.iter().enumerate();
            let mut matching_fields: Vec<_> = if let Some(qualifier) = &column_reference.qualifier {
                indexed_source_fields
                    .filter(|(_idx, field)| {
                        field.qualifier.as_ref() == Some(qualifier)
                            && field.alias == column_reference.alias
                    })
                    .collect()
            } else {
                indexed_source_fields
                    .filter(|(_idx, field)| field.alias == column_reference.alias)
                    .collect()
            };

            if matching_fields.is_empty() {
                return Err(FieldResolutionError::NotFound(
                    ColumnReference::clone(column_reference),
                    source_fields.to_vec(),
                )
                .into());
            } else if matching_fields.len() > 1 {
                return Err(FieldResolutionError::Ambiguous(
                    ColumnReference::clone(column_reference),
                    matching_fields
                        .into_iter()
                        .map(|(_idx, field)| field.clone())
                        .collect(),
                )
                .into());
            } else {
                let (idx, field) = matching_fields.pop().unwrap();
                *expression = Expression::CompiledColumnReference(CompiledColumnReference {
                    offset: idx,
                    datatype: field.data_type,
                })
            }
        }

        // These are already good and for the ref/function call probably shouldn't exist yet.
        Expression::Constant(..)
        | Expression::CompiledFunctionCall(_)
        | Expression::CompiledAggregate(_)
        | Expression::CompiledColumnReference(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{FunctionCall, NamedExpression};
    use ast::rel::logical::Project;
    use data::Session;
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
        let function_registry = Registry::default();

        let mut operator = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::FunctionCall(FunctionCall {
                    function_name: "+".to_string(),
                    args: vec![
                        Expression::from(1),
                        Expression::FunctionCall(FunctionCall {
                            function_name: "+".to_string(),
                            args: vec![Expression::from(2), Expression::from(3)],
                        }),
                    ],
                }),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let expected = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::CompiledFunctionCall(CompiledFunctionCall {
                    function: &DummyFunct {},
                    args: Box::from(vec![
                        Expression::from(1),
                        Expression::CompiledFunctionCall(CompiledFunctionCall {
                            function: &DummyFunct {},
                            args: Box::from(vec![Expression::from(2), Expression::from(3)]),
                            expr_buffer: Box::from(vec![]),
                            signature: Box::new(FunctionSignature {
                                name: "+",
                                args: vec![DataType::Integer, DataType::Integer],
                                ret: DataType::Integer,
                            }),
                        }),
                    ]),
                    expr_buffer: Box::from(vec![]),
                    signature: Box::new(FunctionSignature {
                        name: "+",
                        args: vec![DataType::Integer, DataType::Integer],
                        ret: DataType::Integer,
                    }),
                }),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        compile_functions(&mut operator, &function_registry)?;

        assert_eq!(operator, expected);

        Ok(())
    }
}
