use crate::scalar_expression::EvalScalarRow;
use crate::utils::{right_size_new, right_size_new_to};
use ast::expr::{CompiledAggregate, CompiledColumnReference, Expression};
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
    // Not really an aggregate but we must be able to support aggregate
    // expressions that directly reference the grouping keys (unless we
    // decompose the expressions). If non-aggregate expressions aren't part
    // of the grouping key then the results returned may be surprising (the
    // same as mysql).
    ColumnReference(CompiledColumnReference),
    // A top level scalar function of aggregates(and constants
    //      and other scalar functions of aggregates)
    ScalarFunctionCall(ScalarFunctionCall),
    // An aggregate of scalars
    CompiledAggregate(CompiledAggregate),
}

impl AggregateExpression {
    pub fn state_len(&self) -> usize {
        match self {
            AggregateExpression::ScalarFunctionCall(funct) => {
                funct.args.iter().map(Self::state_len).sum::<usize>()
            }
            AggregateExpression::CompiledAggregate(_) | AggregateExpression::ColumnReference(_) => {
                1
            }
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
            AggregateExpression::ColumnReference(_) => {
                state[0] = Datum::Null;
            }
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
            AggregateExpression::ColumnReference(column_ref) => {
                // Grabs a copy of the column ref unless we've already set it
                if state[0].is_null() {
                    state[0] = row[column_ref.offset].as_static()
                }
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
            AggregateExpression::ColumnReference(_) => state[0].ref_clone(),
        }
    }
}

/// A compiled scalar function call but one works over
/// aggregate sub expressions
#[derive(Debug, Clone)]
pub struct ScalarFunctionCall {
    function: &'static dyn Function,
    args: Box<[AggregateExpression]>,
    // Used to store the evaluation results of the sub expressions during execution
    expr_buffer: Box<[Datum<'static>]>,
    signature: Box<FunctionSignature<'static>>,
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
            Expression::CompiledColumnReference(column_ref) => {
                AggregateExpression::ColumnReference(column_ref.clone())
            }

            Expression::FunctionCall(_) | Expression::ColumnReference(_) | Expression::Cast(_) => {
                panic!("Hit uncompiled expressions when converting to aggregation")
            }
        }
    }
}

/// A trait to make it easier to deal with a whole row of aggregate expressions
/// all at once.
pub trait EvalAggregateRow {
    fn initialize(&self) -> Vec<Datum<'static>> {
        let mut state = right_size_new_to(self.state_len());
        self.reset(&mut state);
        state
    }

    fn state_len(&self) -> usize;
    fn reset(&self, state: &mut [Datum<'static>]);
    fn apply(&mut self, session: &Session, row: &[Datum], freq: i64, state: &mut [Datum<'static>]);
    fn finalize<'a>(
        &'a mut self,
        session: &Session,
        state: &'a [Datum<'a>],
        target: &mut [Datum<'a>],
    );
}

impl EvalAggregateRow for [AggregateExpression] {
    fn state_len(&self) -> usize {
        self.iter()
            .map(AggregateExpression::state_len)
            .sum::<usize>()
    }

    fn reset(&self, state: &mut [Datum<'static>]) {
        let mut offset = 0_usize;
        for expr in self.iter() {
            expr.reset(&mut state[offset..]);
            offset += expr.state_len();
        }
    }

    fn apply(&mut self, session: &Session, row: &[Datum], freq: i64, state: &mut [Datum<'static>]) {
        let mut offset = 0_usize;
        for expr in self.iter_mut() {
            expr.apply(&session, row, freq, &mut state[offset..]);
            offset += expr.state_len();
        }
    }

    fn finalize<'a>(
        &'a mut self,
        session: &Session,
        state: &'a [Datum<'a>],
        target: &mut [Datum<'a>],
    ) {
        for (idx, expr) in self.iter_mut().enumerate() {
            target[idx] = expr.finalize(session, state);
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
    fn test_eval_column_ref() {
        let expression = Expression::CompiledColumnReference(CompiledColumnReference {
            offset: 0,
            datatype: DataType::Integer,
        });
        let session = Session::new(1);

        let mut agg_expression = AggregateExpression::from(&expression);

        let mut state = right_size_new_to(agg_expression.state_len());
        agg_expression.reset(&mut state);
        agg_expression.apply(&session, &[Datum::from(1)], 1, &mut state);
        agg_expression.apply(&session, &[Datum::Null], 2, &mut state);

        let result = agg_expression.finalize(&session, &state);

        assert_eq!(result, Datum::from(1));
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

        let mut state = right_size_new_to(agg_expression.state_len());
        agg_expression.reset(&mut state);
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

    #[test]
    fn test_eval_row() {
        let expression1 = Expression::CompiledColumnReference(CompiledColumnReference {
            offset: 0,
            datatype: DataType::Integer,
        });
        let expression2 = Expression::from(1234);
        let session = Session::new(1);

        let mut agg_expressions = vec![
            AggregateExpression::from(&expression1),
            AggregateExpression::from(&expression2),
        ];

        let mut state = agg_expressions.initialize();
        agg_expressions.apply(&session, &[Datum::from(1)], 1, &mut state);
        agg_expressions.apply(&session, &[Datum::Null], 2, &mut state);

        let mut target = right_size_new(&agg_expressions);
        agg_expressions.finalize(&session, &state, &mut target);

        assert_eq!(target, vec![Datum::from(1), Datum::from(1234)]);
    }
}
