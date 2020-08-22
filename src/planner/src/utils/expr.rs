use ast::expr::Expression;
use data::DataType;

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
