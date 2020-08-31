use ast::expr::{CompiledFunctionCall, Expression, FunctionCall};
use data::DataType;
use functions::registry::Registry;
use functions::{CompoundFunction, CompoundFunctionArg, FunctionSignature};
use std::iter::once;

/// Returns the datatype for an expression, will panic if called before query is normalized
pub(crate) fn type_for_expression(expr: &Expression) -> DataType {
    match expr {
        Expression::Constant(_constant, datatype) => *datatype,
        Expression::Cast(cast) => cast.datatype,
        Expression::CompiledFunctionCall(function_call) => function_call.signature.ret,
        Expression::CompiledAggregate(function_call) => function_call.signature.ret,
        Expression::CompiledColumnReference(column_reference) => column_reference.datatype,

        // These should be gone by now!
        Expression::FunctionCall(_) | Expression::ColumnReference(_) => {
            panic!("These should be gone by now!")
        }
    }
}

/// Returns true if the expression contains an aggregate anywhere in its expressions.
pub(crate) fn contains_aggregate(expr: &Expression) -> bool {
    if let Expression::CompiledAggregate(_) = expr {
        true
    } else {
        expr.children().any(contains_aggregate)
    }
}

/// This bumps all the column references up or down by some amount.
/// To be used when inserting addition columns into some source, then this can be
/// used to rewrite the offsets above
pub(crate) fn move_column_references(expression: &mut Expression, amount: isize) {
    if let Expression::CompiledColumnReference(column_ref) = expression {
        column_ref.offset = (column_ref.offset as isize + amount) as usize
    }
    for expr in expression.children_mut() {
        move_column_references(expr, amount);
    }
}

/// Takes a compound function and its inputs and rewrites the expression as an expression tree
/// of ordinary functions. This expression tree will then need to go through the function compilation
/// process itself.
pub(crate) fn assemble_compound_function(
    compound_function: &CompoundFunction,
    input_args: &[Expression],
) -> Expression {
    let args = compound_function
        .args
        .iter()
        .map(|compound_arg| match compound_arg {
            CompoundFunctionArg::Input(i) => input_args[*i].clone(),
            CompoundFunctionArg::Function(function) => {
                assemble_compound_function(function, input_args)
            }
        })
        .collect();

    Expression::FunctionCall(FunctionCall {
        function_name: compound_function.function_name.to_string(),
        args,
    })
}

/// Takes a boolean expression and splits it at all the "ands"
pub(crate) fn decompose_predicate(predicate: Expression) -> Box<dyn Iterator<Item = Expression>> {
    match predicate {
        Expression::CompiledFunctionCall(function) if function.signature.name == "and" => {
            Box::from(
                function
                    .args
                    .into_vec()
                    .into_iter()
                    .flat_map(decompose_predicate),
            )
        }
        Expression::FunctionCall(function) if &function.function_name == "and" => {
            Box::from(function.args.into_iter().flat_map(decompose_predicate))
        }
        p => Box::from(once(p)),
    }
}

/// Takes many predicates and combines them with the and function.
pub(crate) fn combine_predicates<E: IntoIterator<Item = Expression>>(
    predicates: E,
    function_registry: Registry,
) -> Expression {
    let (and_function_sig, and_function) = function_registry
        .resolve_function(&FunctionSignature {
            name: "and",
            args: vec![DataType::Boolean, DataType::Boolean],
            ret: DataType::Null,
        })
        .unwrap();
    let mut iter = predicates.into_iter();
    match iter.next() {
        Some(first) => iter.fold(first, |a, b| {
            Expression::CompiledFunctionCall(CompiledFunctionCall {
                function: and_function.as_scalar(),
                args: Box::from([a, b]),
                expr_buffer: Box::from(vec![]),
                signature: Box::new(and_function_sig.clone()),
            })
        }),
        None => Expression::from(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{ColumnReference, CompiledColumnReference};
    use parser::parse_expression;

    #[test]
    fn test_move_column_references() {
        let mut expr = Expression::CompiledColumnReference(CompiledColumnReference {
            offset: 5,
            datatype: DataType::Integer,
        });

        move_column_references(&mut expr, -3);

        assert_eq!(
            expr,
            Expression::CompiledColumnReference(CompiledColumnReference {
                offset: 2,
                datatype: DataType::Integer
            })
        );
    }

    #[test]
    fn test_decompose_predicate() {
        let expr = parse_expression("a and (c or d) and e").unwrap();

        let parts: Vec<_> = decompose_predicate(expr).collect();

        assert_eq!(
            parts,
            vec![
                Expression::ColumnReference(ColumnReference {
                    qualifier: None,
                    alias: "a".to_string(),
                    star: false
                }),
                Expression::FunctionCall(FunctionCall {
                    function_name: "or".to_string(),
                    args: vec![
                        Expression::ColumnReference(ColumnReference {
                            qualifier: None,
                            alias: "c".to_string(),
                            star: false
                        }),
                        Expression::ColumnReference(ColumnReference {
                            qualifier: None,
                            alias: "d".to_string(),
                            star: false
                        }),
                    ]
                }),
                Expression::ColumnReference(ColumnReference {
                    qualifier: None,
                    alias: "e".to_string(),
                    star: false
                }),
            ]
        );
    }

    #[test]
    fn test_combine_predicates() {
        let registry = Registry::default();
        let parts = vec![
            Expression::ColumnReference(ColumnReference {
                qualifier: None,
                alias: "a".to_string(),
                star: false,
            }),
            Expression::FunctionCall(FunctionCall {
                function_name: "or".to_string(),
                args: vec![
                    Expression::ColumnReference(ColumnReference {
                        qualifier: None,
                        alias: "c".to_string(),
                        star: false,
                    }),
                    Expression::ColumnReference(ColumnReference {
                        qualifier: None,
                        alias: "d".to_string(),
                        star: false,
                    }),
                ],
            }),
            Expression::ColumnReference(ColumnReference {
                qualifier: None,
                alias: "e".to_string(),
                star: false,
            }),
        ];

        assert_eq!(
            &combine_predicates(parts, registry).to_string(),
            "and(and(a, or(c, d)), e)"
        )
    }
}
