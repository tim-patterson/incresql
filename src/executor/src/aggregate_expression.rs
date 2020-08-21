use crate::scalar_expression::EvalScalarRow;
use crate::utils::{right_size_new, right_size_new_to};
use ast::expr::{CompiledAggregate, Expression};
use data::{DataType, Datum, Session};
use functions::{Function, FunctionSignature};

/// What is an aggregate expression?
/// When we break down an aggregate expression we find that an aggregate expression can be
/// thought of as a tree with 3 layers, I'll give an example.
/// Here's some math'y looking expressions.
/// `SELECT sqrt(sum(a * a) + sum(b * b)) as c`
/// We see at the top we've got a scalar expression(`sqrt(_ + _)`)
/// Next we have our 2 aggregate expressions (`sum(_)`, `sum(_)`)
/// And at the bottom we have our inputs to the aggregations (`a * a`, `b * b`)
///
/// So what does this mean?
/// Well it's all about storing aggregation state, any single expression like above might need
/// more than a single aggregate function worth of state stored. We could store this state in the
/// expression tree but this means that for something like a "hash group by" we'd have n copies of
/// the expression tree floating around which is going to take up waaaay more memory than the state
/// alone....
/// This shapes how the api looks (very similar to the AggregateFunction trait!).

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum AggregateExpression {
    // A top level constant
    Constant(Datum<'static>, DataType),
    // A top level scalar function of aggregates(and constants
    //      and other scalar functions of aggregates)
    ScalarFunctionCall(ScalarFunctionCall),
    // An aggregate of scalars
    CompiledAggregate(CompiledAggregate),
}

impl AggregateExpression {
    /// Initialize the aggregation state.
    pub fn initialize(&self) -> Vec<Datum<'static>> {
        let mut state = right_size_new_to(self.state_len());
        self.reset(&mut state);
        state
    }

    pub fn state_len(&self) -> usize {
        match self {
            AggregateExpression::ScalarFunctionCall(funct) => {
                funct.args.iter().map(Self::state_len).sum::<usize>()
            }
            AggregateExpression::CompiledAggregate(_) => 1,
            AggregateExpression::Constant(_, _) => 0,
        }
    }

    /// resets the aggregation state instead of allocating a new
    /// one
    pub fn reset(&self, state: &mut [Datum<'static>]) {
        match self {
            AggregateExpression::ScalarFunctionCall(funct) => {
                let mut offset = 0_usize;
                for arg in funct.args.iter() {
                    arg.reset(&mut state[offset..]);
                    offset += arg.state_len();
                }
            }
            AggregateExpression::CompiledAggregate(funct) => {
                state[0] = funct.function.initialize();
            }
            AggregateExpression::Constant(_, _) => {}
        }
    }

    /// Applies new inputs to the expression
    pub fn apply(
        &mut self,
        session: &Session,
        row: &[Datum],
        freq: i64,
        state: &mut [Datum<'static>],
    ) {
        match self {
            AggregateExpression::ScalarFunctionCall(funct) => {
                let mut offset = 0_usize;
                for sub_expr in funct.args.iter_mut() {
                    sub_expr.apply(session, row, freq, &mut state[offset..]);
                    offset += sub_expr.state_len();
                }
            }
            AggregateExpression::CompiledAggregate(function_call) => {
                // Eval sub exprs, see notes in scalar_expression.rs for notes.
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
                    .apply(&function_call.signature, &buf, freq, &mut state[0])
            }
            AggregateExpression::Constant(_, _) => {}
        }
    }

    /// Returns the output of the expression at the current time
    pub fn finalize<'a>(&'a mut self, session: &Session, state: &'a [Datum<'a>]) -> Datum<'a> {
        match self {
            AggregateExpression::Constant(datum, _) => datum.ref_clone(),
            AggregateExpression::ScalarFunctionCall(function_call) => {
                // Eval sub exprs, see notes in scalar_expression.rs for notes.
                if function_call.expr_buffer.len() != function_call.args.len() {
                    function_call.expr_buffer = Box::from(right_size_new(&function_call.args))
                }

                let buf = unsafe {
                    std::mem::transmute::<&mut Box<[Datum<'_>]>, &mut Box<[Datum<'_>]>>(
                        &mut function_call.expr_buffer,
                    )
                };
                for (idx, expr) in function_call.args.iter_mut().enumerate() {
                    buf[idx] = expr.finalize(session, state);
                }

                function_call.function.execute(
                    session,
                    &function_call.signature,
                    &function_call.expr_buffer,
                )
            }
            AggregateExpression::CompiledAggregate(function_call) => function_call
                .function
                .finalize(&function_call.signature, &state[0]),
        }
    }
}

/// A compiled scalar function call but one works over
/// aggregate sub expressions
#[derive(Debug, Clone)]
pub struct ScalarFunctionCall {
    pub function: &'static dyn Function,
    pub args: Box<[AggregateExpression]>,
    // Used to store the evaluation results of the sub expressions during execution
    pub expr_buffer: Box<[Datum<'static>]>,
    pub signature: Box<FunctionSignature<'static>>,
}

impl PartialEq for ScalarFunctionCall {
    fn eq(&self, other: &Self) -> bool {
        self.args == other.args && self.signature == other.signature
    }
}
impl Eq for ScalarFunctionCall {}

impl From<&Expression> for AggregateExpression {
    fn from(expr: &Expression) -> Self {
        match expr {
            Expression::Constant(datum, datatype) => {
                AggregateExpression::Constant(datum.as_static(), *datatype)
            }
            Expression::CompiledAggregate(function) => {
                AggregateExpression::CompiledAggregate(function.clone())
            }
            Expression::CompiledFunctionCall(function) => {
                let args: Vec<_> = function
                    .args
                    .iter()
                    .map(AggregateExpression::from)
                    .collect();

                AggregateExpression::ScalarFunctionCall(ScalarFunctionCall {
                    function: function.function,
                    args: args.into_boxed_slice(),
                    expr_buffer: Box::from(vec![]),
                    signature: function.signature.clone(),
                })
            }
            Expression::CompiledColumnReference(_) => {
                panic!("Hit unaggregated compiled column in aggregate expr")
            }

            Expression::FunctionCall(_) | Expression::ColumnReference(_) | Expression::Cast(_) => {
                panic!("Hit uncompiled expressions when converting to aggregation")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{CompiledColumnReference, CompiledFunctionCall};
    use functions::registry::Registry;

    #[test]
    fn test_eval_constant() {
        let expression = Expression::from(1234);
        let session = Session::new(1);

        let mut agg_expression = AggregateExpression::from(&expression);

        let result = agg_expression.finalize(&session, &[]);

        assert_eq!(result, Datum::from(1234));
    }

    #[test]
    fn test_eval_aggregate() {
        let signature = FunctionSignature {
            name: "sum",
            args: vec![DataType::Integer],
            ret: DataType::Null,
        };
        let (sig, function) = Registry::default().resolve_function(&signature).unwrap();
        let expression = Expression::CompiledAggregate(CompiledAggregate {
            function: function.as_aggregate(),
            args: vec![Expression::CompiledColumnReference(
                CompiledColumnReference {
                    offset: 0,
                    datatype: DataType::Integer,
                },
            )]
            .into_boxed_slice(),
            expr_buffer: vec![].into_boxed_slice(),
            signature: Box::new(sig),
        });
        let session = Session::new(1);

        let mut agg_expression = AggregateExpression::from(&expression);

        let mut state = agg_expression.initialize();
        agg_expression.apply(&session, &[Datum::from(1)], 1, &mut state);
        agg_expression.apply(&session, &[Datum::from(3)], 2, &mut state);

        let result = agg_expression.finalize(&session, &state);

        assert_eq!(result, Datum::from(7));
    }

    #[test]
    fn test_eval_scalar_function() {
        let signature = FunctionSignature {
            name: "+",
            args: vec![DataType::Integer, DataType::Integer],
            ret: DataType::Null,
        };
        let (sig, function) = Registry::default().resolve_function(&signature).unwrap();
        let expression = Expression::CompiledFunctionCall(CompiledFunctionCall {
            function: function.as_scalar(),
            args: vec![
                Expression::Constant(Datum::from(1), DataType::Integer),
                Expression::Constant(Datum::from(3), DataType::Integer),
            ]
            .into_boxed_slice(),
            expr_buffer: vec![].into_boxed_slice(),
            signature: Box::new(sig),
        });
        let session = Session::new(1);

        let mut agg_expression = AggregateExpression::from(&expression);

        let result = agg_expression.finalize(&session, &[]);

        assert_eq!(result, Datum::from(4));
    }
}
