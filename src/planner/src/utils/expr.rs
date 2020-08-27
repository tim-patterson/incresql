use ast::expr::{Expression, FunctionCall};
use data::DataType;
use functions::{CompoundFunction, CompoundFunctionArg};

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

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::CompiledColumnReference;

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
}
