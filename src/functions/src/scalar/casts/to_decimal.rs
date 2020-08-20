use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::rust_decimal::Decimal;
use data::{DataType, Datum, Session, DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE};
use std::str::FromStr;

#[derive(Debug)]
struct ToDecimalFromBoolean {}

impl Function for ToDecimalFromBoolean {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(a) = args[0].as_boolean() {
            Datum::from(if a {
                Decimal::new(1, 0)
            } else {
                Decimal::new(0, 0)
            })
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToDecimalFromInt {}

impl Function for ToDecimalFromInt {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(a) = args[0].as_integer() {
            Datum::from(Decimal::from(a))
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToDecimalFromBigInt {}

impl Function for ToDecimalFromBigInt {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(a) = args[0].as_bigint() {
            Datum::from(Decimal::from(a))
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToDecimalFromDecimal {}

impl Function for ToDecimalFromDecimal {
    fn execute<'a>(
        &self,
        _session: &Session,
        signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(mut d) = args[0].as_decimal() {
            if let DataType::Decimal(_p, s) = signature.ret {
                // We'll rescale to match the cast, (down scaling only, no point upscaling as it just potentially loses
                // data
                if (s as u32) < d.scale() {
                    d.rescale(s as u32);
                }
                Datum::from(d)
            } else {
                panic!()
            }
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct ToDecimalFromText {}

impl Function for ToDecimalFromText {
    fn execute<'a>(
        &self,
        _session: &Session,
        signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let Some(a) = args[0].as_text() {
            if let (Ok(mut d), DataType::Decimal(_p, s)) = (Decimal::from_str(a), signature.ret) {
                // We'll rescale to match the cast, (down scaling only, no point upscaling as it just potentially loses
                // data
                if (s as u32) < d.scale() {
                    d.rescale(s as u32);
                }
                Datum::from(d)
            } else {
                Datum::Null
            }
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "to_decimal",
        vec![DataType::Boolean],
        DataType::Decimal(1, 0),
        FunctionType::Scalar(&ToDecimalFromBoolean {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_decimal",
        vec![DataType::Integer],
        DataType::Decimal(10, 0),
        FunctionType::Scalar(&ToDecimalFromInt {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_decimal",
        vec![DataType::BigInt],
        DataType::Decimal(20, 0),
        FunctionType::Scalar(&ToDecimalFromBigInt {}),
    ));

    registry.register_function(FunctionDefinition::new_with_type_resolver(
        "to_decimal",
        vec![DataType::Decimal(0, 0)],
        // Remembering this is just the default that can be overridden in casts, this value will only
        // be used if called as a function
        |args| args[0],
        FunctionType::Scalar(&ToDecimalFromDecimal {}),
    ));

    registry.register_function(FunctionDefinition::new(
        "to_decimal",
        vec![DataType::Text],
        DataType::Decimal(DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE),
        FunctionType::Scalar(&ToDecimalFromText {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::rust_decimal::Decimal;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "to_decimal",
        args: vec![],
        ret: DataType::Decimal(10, 2),
    };

    #[test]
    fn test_null() {
        assert_eq!(
            ToDecimalFromBoolean {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_from_bool() {
        assert_eq!(
            ToDecimalFromBoolean {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(true)]),
            Datum::from(Decimal::new(1, 0))
        )
    }

    #[test]
    fn test_from_int() {
        assert_eq!(
            ToDecimalFromInt {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(1)]),
            Datum::from(Decimal::new(1, 0))
        )
    }

    #[test]
    fn test_from_bigint() {
        assert_eq!(
            ToDecimalFromBigInt {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from(1_i64)]),
            Datum::from(Decimal::new(1, 0))
        )
    }

    #[test]
    fn test_from_decimal() {
        assert_eq!(
            ToDecimalFromDecimal {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(Decimal::new(123456, 4))]
            ),
            Datum::from(Decimal::new(1235, 2))
        )
    }

    #[test]
    fn test_from_text() {
        assert_eq!(
            ToDecimalFromText {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::from("1234.5678")]),
            Datum::from(Decimal::new(123457, 2))
        )
    }
}
