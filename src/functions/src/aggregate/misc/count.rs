use crate::registry::Registry;
use crate::{AggregateFunction, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum};

#[derive(Debug)]
struct Count {}

impl AggregateFunction for Count {
    fn initialize(&self, state: &mut [Datum<'static>]) {
        state[0] = Datum::from(0 as i64);
    }

    fn apply<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        args: &[Datum<'a>],
        freq: i64,
        state: &mut [Datum<'static>],
    ) {
        if args.is_empty() || !args[0].is_null() {
            *state[0].as_bigint_mut() += freq;
        }
    }

    fn merge<'a>(
        &self,
        _signature: &FunctionSignature<'a>,
        input_state: &[Datum<'static>],
        state: &mut [Datum<'static>],
    ) {
        *state[0].as_bigint_mut() += input_state[0].as_bigint()
    }

    fn supports_retract(&self) -> bool {
        true
    }
}

pub fn register_builtins(registry: &mut Registry) {
    // 0 Arg count
    registry.register_function(FunctionDefinition::new(
        "count",
        vec![],
        DataType::BigInt,
        FunctionType::Aggregate(&Count {}),
    ));
    // 1 Arg count
    registry.register_function(FunctionDefinition::new(
        "count",
        vec![DataType::Null],
        DataType::BigInt,
        FunctionType::Aggregate(&Count {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "count",
        args: vec![],
        ret: DataType::BigInt,
    };

    #[test]
    fn test_apply() {
        let funct = &Count {};

        let mut state = vec![Datum::Null];
        funct.initialize(&mut state);

        funct.apply(&DUMMY_SIG, &[], 10, &mut state);
        funct.apply(&DUMMY_SIG, &[], -2, &mut state);

        let answer = funct.finalize(&DUMMY_SIG, &mut state);

        assert_eq!(answer, Datum::from(8 as i64))
    }

    #[test]
    fn test_merge() {
        let funct = &Count {};

        let mut state1 = vec![Datum::Null];
        funct.initialize(&mut state1);
        funct.apply(&DUMMY_SIG, &[], 10, &mut state1);

        let mut state2 = vec![Datum::Null];
        funct.initialize(&mut state2);
        funct.apply(&DUMMY_SIG, &[], -2, &mut state2);

        funct.merge(&DUMMY_SIG, &state2, &mut state1);

        let answer = funct.finalize(&DUMMY_SIG, &mut state1);

        assert_eq!(answer, Datum::from(8 as i64))
    }
}
