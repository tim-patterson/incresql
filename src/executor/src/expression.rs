use ast::expr::Expression;
use data::Datum;

pub trait EvalScalar {
    /// Evaluates an expression as a scalar context
    fn eval_scalar(&self, _row: &[Datum]) -> Datum;
}

impl EvalScalar for Expression {
    /// Evaluates a "row" of expressions as a scalar context
    fn eval_scalar(&self, _row: &[Datum]) -> Datum {
        match self {
            Expression::Literal(literal) => literal.clone(),
        }
    }
}

pub trait EvalScalarRow {
    fn eval_scalar<'a>(&'a self, source: &[Datum], target: &mut [Datum<'a>]);
}

impl EvalScalarRow for Vec<Expression> {
    fn eval_scalar<'a>(&'a self, source: &[Datum], target: &mut [Datum<'a>]) {
        for (idx, expr) in self.iter().enumerate() {
            target[idx] = expr.eval_scalar(source);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eval_scalar() {
        let expression = Expression::Literal(Datum::from(1234));
        assert_eq!(expression.eval_scalar(&[]), Datum::from(1234));
    }

    #[test]
    fn test_eval_scalar_row() {
        let expressions = vec![
            Expression::Literal(Datum::from(1234)),
            Expression::Literal(Datum::from(5678)),
        ];
        let mut target = vec![Datum::Null, Datum::Null];
        expressions.eval_scalar(&[], &mut target);

        assert_eq!(target, vec![Datum::from(1234), Datum::from(5678)]);
    }
}
