use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature};
use data::{DataType, Datum, Session, DECIMAL_MAX_PRECISION};
use std::cmp::{max, min};

#[derive(Debug)]
struct AddInteger {}

impl Function for AddInteger {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_integer(), args[1].as_integer()) {
            Datum::from(a + b)
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct AddBigint {}

impl Function for AddBigint {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_bigint(), args[1].as_bigint()) {
            Datum::from(a + b)
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct AddDecimal {}

impl Function for AddDecimal {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_decimal(), args[1].as_decimal()) {
            Datum::from(a + b)
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "+",
        vec![DataType::Integer, DataType::Integer],
        DataType::Integer,
        &AddInteger {},
    ));

    registry.register_function(FunctionDefinition::new(
        "+",
        vec![DataType::BigInt, DataType::BigInt],
        DataType::BigInt,
        &AddBigint {},
    ));

    registry.register_function(FunctionDefinition::new_with_type_resolver(
        "+",
        vec![DataType::Decimal(0, 0), DataType::Decimal(0, 0)],
        |args| {
            if let (DataType::Decimal(p1, s1), DataType::Decimal(p2, s2)) = (args[0], args[1]) {
                DataType::Decimal(min(max(p1, p2) + 1, DECIMAL_MAX_PRECISION), max(s1, s2))
            } else {
                panic!()
            }
        },
        &AddDecimal {},
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::rust_decimal::Decimal;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "+",
        args: vec![],
        ret: DataType::Integer,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            AddInteger {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null, Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_add_int() {
        assert_eq!(
            AddInteger {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(1), Datum::from(2)]
            ),
            Datum::from(3)
        )
    }

    #[test]
    fn test_add_bigint() {
        assert_eq!(
            AddBigint {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(1_i64), Datum::from(2_i64)]
            ),
            Datum::from(3_i64)
        )
    }

    #[test]
    fn test_add_decimal() {
        assert_eq!(
            AddDecimal {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[
                    Datum::from(Decimal::new(123, 1)),
                    Datum::from(Decimal::new(1234, 2))
                ]
            ),
            Datum::from(Decimal::new(2464, 2))
        )
    }
}
