use crate::utils::right_size_new;
use ast::expr::Expression;
use data::{Datum, Session};

pub trait EvalScalar {
    /// Evaluates an expression as a scalar context, needs to be mutable due to the buffers we keep
    /// for intermediate results
    fn eval_scalar<'a>(&'a mut self, session: &Session, row: &'a [Datum<'a>]) -> Datum<'a>;
}

impl EvalScalar for Expression {
    /// Evaluates a "row" of expressions as a scalar context
    #[allow(clippy::transmute_ptr_to_ptr)]
    fn eval_scalar<'a>(&'a mut self, session: &Session, row: &'a [Datum<'a>]) -> Datum<'a> {
        match self {
            // literal.clone() seemed to confuse IntelliJ here...
            Expression::Constant(literal, _) => Datum::ref_clone(literal),
            Expression::CompiledFunctionCall(function_call) => {
                // Due to datum's being able to reference data from source datums, we need to hold
                // onto all the intermediate datums just in case. Rust lifetimes don't really allow
                // us to do this in an easy way without ref counting and allocating so hence we put
                // the buffer in the expression datastructure itself and use a little unsafe to muck
                // with the lifetimes

                // right size
                if function_call.expr_buffer.len() != function_call.args.len() {
                    function_call.expr_buffer = Box::from(right_size_new(&function_call.args))
                }

                let buf = unsafe {
                    std::mem::transmute::<&mut Box<[Datum<'_>]>, &mut Box<[Datum<'_>]>>(
                        &mut function_call.expr_buffer,
                    )
                };
                function_call.args.eval_scalar(session, row, buf);

                function_call
                    .function
                    .execute(session, &function_call.signature, buf)
            }
            Expression::CompiledColumnReference(column_reference) => {
                row[column_reference.offset].ref_clone()
            }
            // These should be compiled away by this point
            Expression::FunctionCall(_) | Expression::Cast(_) | Expression::ColumnReference(_) => {
                panic!()
            }
        }
    }
}

pub trait EvalScalarRow {
    fn eval_scalar<'a>(
        &'a mut self,
        session: &Session,
        source: &'a [Datum<'a>],
        target: &mut [Datum<'a>],
    );
}

impl EvalScalarRow for [Expression] {
    fn eval_scalar<'a>(
        &'a mut self,
        session: &Session,
        source: &'a [Datum<'a>],
        target: &mut [Datum<'a>],
    ) {
        for (idx, expr) in self.iter_mut().enumerate() {
            target[idx] = expr.eval_scalar(session, source);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::CompiledFunctionCall;
    use data::DataType;
    use functions::registry::Registry;
    use functions::FunctionSignature;

    #[test]
    fn test_eval_scalar_literal() {
        let mut expression = Expression::from(1234);
        let session = Session::new(1);
        assert_eq!(expression.eval_scalar(&session, &[]), Datum::from(1234));
    }

    #[test]
    fn test_eval_scalar_function() {
        let mut signature = FunctionSignature {
            name: "+",
            args: vec![DataType::Integer, DataType::Integer],
            ret: DataType::Null,
        };
        let (computed_signature, function) = Registry::new(true)
            .resolve_scalar_function(&mut signature)
            .unwrap();

        let mut expression = Expression::CompiledFunctionCall(CompiledFunctionCall {
            function,
            signature: Box::from(computed_signature),
            expr_buffer: Box::from(vec![]),
            args: Box::from(vec![Expression::from(3), Expression::from(4)]),
        });

        let session = Session::new(1);
        assert_eq!(expression.eval_scalar(&session, &[]), Datum::from(7));
    }

    #[test]
    fn test_eval_scalar_row() {
        let mut expressions = vec![Expression::from(1234), Expression::from(5678)];
        let session = Session::new(1);
        let mut target = vec![Datum::Null, Datum::Null];
        expressions.eval_scalar(&session, &[], &mut target);

        assert_eq!(target, vec![Datum::from(1234), Datum::from(5678)]);
    }
}
