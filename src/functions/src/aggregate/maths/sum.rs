use crate::registry::Registry;
use crate::{AggregateFunction, FunctionDefinition, FunctionSignature, FunctionType};
use data::rust_decimal::prelude::Zero;
use data::rust_decimal::Decimal;
use data::{DataType, Datum, DECIMAL_MAX_PRECISION};

#[derive(Debug)]
struct IntSum {}

/// Sum across ints
impl AggregateFunction for IntSum {
    fn initialize(&self) -> Datum<'static> {
        Datum::from(0)
    }

    fn apply<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        args: &[Datum<'a>],
        freq: i64,
        state: &mut Datum<'static>,
    ) {
        if let Some(i) = args[0].as_maybe_integer() {
            *state.as_integer_mut() += freq as i32 * i;
        }
    }

    fn merge<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        input_state: &Datum<'static>,
        state: &mut Datum<'static>,
    ) {
        *state.as_integer_mut() += input_state.as_integer()
    }

    fn supports_retract(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct BigintSum {}

/// Sum across ints
impl AggregateFunction for BigintSum {
    fn initialize(&self) -> Datum<'static> {
        Datum::from(0 as i64)
    }

    fn apply<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        args: &[Datum<'a>],
        freq: i64,
        state: &mut Datum<'static>,
    ) {
        if let Some(i) = args[0].as_maybe_bigint() {
            *state.as_bigint_mut() += freq * i;
        }
    }

    fn merge<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        input_state: &Datum<'static>,
        state: &mut Datum<'static>,
    ) {
        *state.as_bigint_mut() += input_state.as_bigint()
    }

    fn supports_retract(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct DecimalSum {}

impl AggregateFunction for DecimalSum {
    fn initialize(&self) -> Datum<'static> {
        Datum::Decimal(Decimal::zero())
    }

    fn apply<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        args: &[Datum<'a>],
        freq: i64,
        state: &mut Datum<'static>,
    ) {
        if let Some(d) = args[0].as_maybe_decimal() {
            *state.as_decimal_mut() += d * Decimal::new(freq, 0);
        }
    }

    fn merge<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        input_state: &Datum<'static>,
        state: &mut Datum<'static>,
    ) {
        *state.as_decimal_mut() += input_state.as_decimal()
    }

    fn supports_retract(&self) -> bool {
        true
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "sum",
        vec![DataType::Integer],
        DataType::Integer,
        FunctionType::Aggregate(&IntSum {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "sum",
        vec![DataType::BigInt],
        DataType::BigInt,
        FunctionType::Aggregate(&BigintSum {}),
    ));

    registry.register_function(FunctionDefinition::new_with_type_resolver(
        "sum",
        vec![DataType::Decimal(0, 0)],
        |args| {
            if let DataType::Decimal(_, scale) = args[0] {
                DataType::Decimal(DECIMAL_MAX_PRECISION, scale)
            } else {
                panic!()
            }
        },
        FunctionType::Aggregate(&DecimalSum {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "sum",
        args: vec![],
        ret: DataType::Null,
    };

    #[test]
    fn test_apply_int() {
        let funct = &IntSum {};

        let mut state = funct.initialize();

        funct.apply(&DUMMY_SIG, &[Datum::Integer(5)], 2, &mut state);
        funct.apply(&DUMMY_SIG, &[Datum::Integer(2)], -1, &mut state);

        let answer = funct.finalize(&DUMMY_SIG, &mut state);

        assert_eq!(answer, Datum::from(8))
    }

    #[test]
    fn test_merge_int() {
        let funct = &IntSum {};

        let mut state1 = funct.initialize();
        funct.apply(&DUMMY_SIG, &[Datum::Integer(5)], 2, &mut state1);

        let mut state2 = funct.initialize();
        funct.apply(&DUMMY_SIG, &[Datum::Integer(2)], -1, &mut state2);

        funct.merge(&DUMMY_SIG, &state2, &mut state1);

        let answer = funct.finalize(&DUMMY_SIG, &mut state1);

        assert_eq!(answer, Datum::from(8))
    }

    #[test]
    fn test_apply_bigint() {
        let funct = &BigintSum {};

        let mut state = funct.initialize();

        funct.apply(&DUMMY_SIG, &[Datum::BigInt(5)], 2, &mut state);
        funct.apply(&DUMMY_SIG, &[Datum::BigInt(2)], -1, &mut state);

        let answer = funct.finalize(&DUMMY_SIG, &mut state);

        assert_eq!(answer, Datum::from(8 as i64))
    }

    #[test]
    fn test_merge_bigint() {
        let funct = &BigintSum {};

        let mut state1 = funct.initialize();
        funct.apply(&DUMMY_SIG, &[Datum::BigInt(5)], 2, &mut state1);

        let mut state2 = funct.initialize();
        funct.apply(&DUMMY_SIG, &[Datum::BigInt(2)], -1, &mut state2);

        funct.merge(&DUMMY_SIG, &state2, &mut state1);

        let answer = funct.finalize(&DUMMY_SIG, &mut state1);

        assert_eq!(answer, Datum::from(8 as i64))
    }

    #[test]
    fn test_apply_decimal() {
        let funct = &DecimalSum {};

        let mut state = funct.initialize();

        funct.apply(
            &DUMMY_SIG,
            &[Datum::from(Decimal::new(5, 0))],
            2,
            &mut state,
        );
        funct.apply(
            &DUMMY_SIG,
            &[Datum::from(Decimal::new(15, 1))],
            -1,
            &mut state,
        );

        let answer = funct.finalize(&DUMMY_SIG, &mut state);

        assert_eq!(answer, Datum::from(Decimal::new(85, 1)))
    }

    #[test]
    fn test_merge_decimal() {
        let funct = &DecimalSum {};

        let mut state1 = funct.initialize();
        funct.apply(
            &DUMMY_SIG,
            &[Datum::from(Decimal::new(5, 0))],
            2,
            &mut state1,
        );

        let mut state2 = funct.initialize();
        funct.apply(
            &DUMMY_SIG,
            &[Datum::from(Decimal::new(15, 1))],
            -1,
            &mut state2,
        );

        funct.merge(&DUMMY_SIG, &state2, &mut state1);

        let answer = funct.finalize(&DUMMY_SIG, &mut state1);

        assert_eq!(answer, Datum::from(Decimal::new(85, 1)))
    }
}
