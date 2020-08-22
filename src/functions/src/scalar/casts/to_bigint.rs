use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::rust_decimal::prelude::ToPrimitive;
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct ToBigIntFromBoolean {}

impl Function for ToBigIntFromBoolean {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(a) = args[0].as_maybe_boolean() {
            Datum::BigInt(if a { 1 } else { 0 })
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToBigIntFromInt {}

impl Function for ToBigIntFromInt {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(i) = args[0].as_maybe_integer() {
            Datum::from(i as i64)
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToBigIntFromBigInt {}

impl Function for ToBigIntFromBigInt {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        args[0].clone()
    }
}

#[derive(Debug)]
struct ToBigIntFromDecimal {}

impl Function for ToBigIntFromDecimal {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(a) = args[0].as_maybe_decimal() {
            a.to_i64().map(Datum::from).unwrap_or(Datum::Null)
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToBigIntFromText {}

impl Function for ToBigIntFromText {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(a) = args[0].as_maybe_text() {
            a.parse::<i64>()
                .ok()
                .map(Datum::from)
                .unwrap_or(Datum::Null)
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "to_bigint",
        vec![DataType::Boolean],
        DataType::BigInt,
        FunctionType::Scalar(&ToBigIntFromBoolean {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_bigint",
        vec![DataType::Integer],
        DataType::BigInt,
        FunctionType::Scalar(&ToBigIntFromInt {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_bigint",
        vec![DataType::BigInt],
        DataType::BigInt,
        FunctionType::Scalar(&ToBigIntFromBigInt {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_bigint",
        vec![DataType::Decimal(0, 0)],
        DataType::BigInt,
        FunctionType::Scalar(&ToBigIntFromDecimal {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_bigint",
        vec![DataType::Text],
        DataType::BigInt,
        FunctionType::Scalar(&ToBigIntFromText {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::rust_decimal::Decimal;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "to_bigint",
        args: vec![],
        ret: DataType::BigInt,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            ToBigIntFromBoolean {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_from_bool() {
        assert_eq!(
            ToBigIntFromBoolean {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(true)]),
            Datum::from(1_i64)
        )
    }

    #[test]
    fn test_from_int() {
        assert_eq!(
            ToBigIntFromInt {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(1)]),
            Datum::from(1_i64)
        )
    }

    #[test]
    fn test_from_bigint() {
        assert_eq!(
            ToBigIntFromBigInt {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(1_i64)]),
            Datum::from(1_i64)
        )
    }

    #[test]
    fn test_from_decimal() {
        assert_eq!(
            ToBigIntFromDecimal {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(Decimal::new(10, 1))]
            ),
            Datum::from(1_i64)
        )
    }

    #[test]
    fn test_from_text() {
        assert_eq!(
            ToBigIntFromText {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from("1")]),
            Datum::from(1_i64)
        )
    }
}
