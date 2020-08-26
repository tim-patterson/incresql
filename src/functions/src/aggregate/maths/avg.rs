use crate::registry::Registry;
use crate::{AggregateFunction, FunctionDefinition, FunctionSignature, FunctionType};
use data::rust_decimal::Decimal;
use data::{DataType, Datum, DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE};
use num_traits::Zero;

#[derive(Debug)]
struct IntAvg {}

/// Average will keep sum and a count,
/// for avg(int) both these will bigints
impl AggregateFunction for IntAvg {
    fn state_size(&self) -> usize {
        2
    }

    fn initialize(&self, state: &mut [Datum<'static>]) {
        state[0] = Datum::from(0 as i64);
        state[1] = Datum::from(0 as i64);
    }

    fn apply<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        args: &[Datum<'a>],
        freq: i64,
        state: &mut [Datum<'static>],
    ) {
        if let Some(i) = args[0].as_maybe_integer() {
            *state[0].as_bigint_mut() += freq * (i as i64);
            *state[1].as_bigint_mut() += freq;
        }
    }

    fn merge<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        input_state: &[Datum<'static>],
        state: &mut [Datum<'static>],
    ) {
        *state[0].as_bigint_mut() += input_state[0].as_bigint();
        *state[1].as_bigint_mut() += input_state[1].as_bigint();
    }

    fn finalize<'a>(&self, _signature: &FunctionSignature, state: &'a [Datum<'a>]) -> Datum<'a> {
        if state[1].as_bigint() == 0 {
            Datum::Null
        } else {
            Datum::from(
                Decimal::new(state[0].as_bigint(), 0) / Decimal::new(state[1].as_bigint(), 0),
            )
        }
    }

    fn supports_retract(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct BigIntAvg {}

/// Average will keep sum and a count,
/// for avg(bigint) both these will bigints
impl AggregateFunction for BigIntAvg {
    fn state_size(&self) -> usize {
        2
    }

    fn initialize(&self, state: &mut [Datum<'static>]) {
        state[0] = Datum::from(0 as i64);
        state[1] = Datum::from(0 as i64);
    }

    fn apply<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        args: &[Datum<'a>],
        freq: i64,
        state: &mut [Datum<'static>],
    ) {
        if let Some(i) = args[0].as_maybe_bigint() {
            *state[0].as_bigint_mut() += freq * i;
            *state[1].as_bigint_mut() += freq;
        }
    }

    fn merge<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        input_state: &[Datum<'static>],
        state: &mut [Datum<'static>],
    ) {
        *state[0].as_bigint_mut() += input_state[0].as_bigint();
        *state[1].as_bigint_mut() += input_state[1].as_bigint();
    }

    fn finalize<'a>(&self, _signature: &FunctionSignature, state: &'a [Datum<'a>]) -> Datum<'a> {
        if state[1].as_bigint() == 0 {
            Datum::Null
        } else {
            Datum::from(
                Decimal::new(state[0].as_bigint(), 0) / Decimal::new(state[1].as_bigint(), 0),
            )
        }
    }

    fn supports_retract(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct DecimalAvg {}

/// Average will keep sum and a count,
/// for avg(decimal) both these will decimal, bigint
impl AggregateFunction for DecimalAvg {
    fn state_size(&self) -> usize {
        2
    }

    fn initialize(&self, state: &mut [Datum<'static>]) {
        state[0] = Datum::from(Decimal::zero());
        state[1] = Datum::from(0 as i64);
    }

    fn apply<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        args: &[Datum<'a>],
        freq: i64,
        state: &mut [Datum<'static>],
    ) {
        if let Some(i) = args[0].as_maybe_decimal() {
            *state[0].as_decimal_mut() += Decimal::new(freq, 0) * i;
            *state[1].as_bigint_mut() += freq;
        }
    }

    fn merge<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        input_state: &[Datum<'static>],
        state: &mut [Datum<'static>],
    ) {
        *state[0].as_decimal_mut() += input_state[0].as_decimal();
        *state[1].as_bigint_mut() += input_state[1].as_bigint();
    }

    fn finalize<'a>(&self, _signature: &FunctionSignature, state: &'a [Datum<'a>]) -> Datum<'a> {
        if state[1].as_bigint() == 0 {
            Datum::Null
        } else {
            Datum::from(state[0].as_decimal() / Decimal::new(state[1].as_bigint(), 0))
        }
    }

    fn supports_retract(&self) -> bool {
        true
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "avg",
        vec![DataType::Integer],
        DataType::Decimal(DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE),
        FunctionType::Aggregate(&IntAvg {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "avg",
        vec![DataType::BigInt],
        DataType::Decimal(DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE),
        FunctionType::Aggregate(&BigIntAvg {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "avg",
        vec![DataType::Decimal(0, 0)],
        DataType::Decimal(DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE),
        FunctionType::Aggregate(&DecimalAvg {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "avg",
        args: vec![],
        ret: DataType::Decimal(DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE),
    };

    #[test]
    fn test_apply_int() {
        let funct = &IntAvg {};

        let mut state = vec![Datum::Null, Datum::Null];
        funct.initialize(&mut state);

        funct.apply(&DUMMY_SIG, &[Datum::Integer(6)], 2, &mut state);
        funct.apply(&DUMMY_SIG, &[Datum::Integer(3)], 1, &mut state);

        let answer = funct.finalize(&DUMMY_SIG, &mut state);
        // 6 + 6 + 3 = 15, 15/3 = 5
        assert_eq!(answer, Datum::from(Decimal::new(5, 0)))
    }

    #[test]
    fn test_apply_int_no_rows() {
        let funct = &IntAvg {};

        let mut state = vec![Datum::Null, Datum::Null];
        funct.initialize(&mut state);

        funct.apply(&DUMMY_SIG, &[Datum::Integer(6)], 2, &mut state);
        funct.apply(&DUMMY_SIG, &[Datum::Integer(6)], -2, &mut state);

        let answer = funct.finalize(&DUMMY_SIG, &mut state);
        assert_eq!(answer, Datum::Null)
    }

    #[test]
    fn test_merge_int() {
        let funct = &IntAvg {};

        let mut state1 = vec![Datum::Null, Datum::Null];
        funct.initialize(&mut state1);
        funct.apply(&DUMMY_SIG, &[Datum::Integer(6)], 2, &mut state1);

        let mut state2 = vec![Datum::Null, Datum::Null];
        funct.initialize(&mut state2);
        funct.apply(&DUMMY_SIG, &[Datum::Integer(3)], 1, &mut state2);

        funct.merge(&DUMMY_SIG, &state2, &mut state1);

        let answer = funct.finalize(&DUMMY_SIG, &mut state1);

        assert_eq!(answer, Datum::from(Decimal::new(5, 0)))
    }
}
