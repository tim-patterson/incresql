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

#[derive(Debug)]
struct ToBigIntFromJson {}

impl Function for ToBigIntFromJson {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        // We need to try both the json::number and the json::text
        if let Some(i) = args[0]
            .as_maybe_json()
            .and_then(|j| j.get_number())
            .and_then(|d| d.to_i64())
        {
            Datum::from(i)
        } else if let Some(i) = args[0]
            .as_maybe_json()
            .and_then(|j| j.get_string())
            .and_then(|s| s.parse::<i64>().ok())
        {
            Datum::from(i)
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

    registry.register_function(FunctionDefinition::new(
        "to_bigint",
        vec![DataType::Json],
        DataType::BigInt,
        FunctionType::Scalar(&ToBigIntFromJson {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::json::OwnedJson;
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

    #[test]
    fn test_from_json() {
        assert_eq!(
            ToBigIntFromJson {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(OwnedJson::parse("1").unwrap())]
            ),
            Datum::from(1_i64)
        );

        assert_eq!(
            ToBigIntFromJson {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(OwnedJson::parse("\"1\"").unwrap())]
            ),
            Datum::from(1_i64)
        );

        assert_eq!(
            ToBigIntFromJson {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(OwnedJson::parse("12345.2").unwrap())]
            ),
            Datum::from(12345_i64)
        );
    }
}
