use crate::registry::Registry;
use crate::{Function, FunctionDefinition};
use data::{DataType, Datum, Session, DECIMAL_MAX_PRECISION};
use std::cmp::{max, min};

#[derive(Debug)]
struct SubtractInteger {}

impl Function for SubtractInteger {
    fn execute<'a>(&self, _session: &Session, args: &'a [Datum<'a>]) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_integer(), args[1].as_integer()) {
            Datum::from(a - b)
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct SubtractBigint {}

impl Function for SubtractBigint {
    fn execute<'a>(&self, _session: &Session, args: &'a [Datum<'a>]) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_bigint(), args[1].as_bigint()) {
            Datum::from(a - b)
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct SubtractDecimal {}

impl Function for SubtractDecimal {
    fn execute<'a>(&self, _session: &Session, args: &'a [Datum<'a>]) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_decimal(), args[1].as_decimal()) {
            Datum::from(a - b)
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "-",
        vec![DataType::Integer, DataType::Integer],
        DataType::Integer,
        &SubtractInteger {},
    ));

    registry.register_function(FunctionDefinition::new(
        "-",
        vec![DataType::BigInt, DataType::BigInt],
        DataType::BigInt,
        &SubtractBigint {},
    ));

    registry.register_function(FunctionDefinition::new_with_type_resolver(
        "-",
        vec![DataType::Decimal(0, 0), DataType::Decimal(0, 0)],
        |args| {
            if let (DataType::Decimal(p1, s1), DataType::Decimal(p2, s2)) = (args[0], args[1]) {
                DataType::Decimal(min(max(p1, p2) + 1, DECIMAL_MAX_PRECISION), max(s1, s2))
            } else {
                panic!()
            }
        },
        &SubtractDecimal {},
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::Decimal;

    #[test]
    fn test_null() {
        assert_eq!(
            SubtractInteger {}.execute(&Session::new(1), &[Datum::Null, Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_sub_int() {
        assert_eq!(
            SubtractInteger {}.execute(&Session::new(1), &[Datum::from(10), Datum::from(2)]),
            Datum::from(8)
        )
    }

    #[test]
    fn test_sub_bigint() {
        assert_eq!(
            SubtractBigint {}.execute(&Session::new(1), &[Datum::from(10_i64), Datum::from(2_i64)]),
            Datum::from(8_i64)
        )
    }

    #[test]
    fn test_sub_decimal() {
        assert_eq!(
            SubtractDecimal {}.execute(
                &Session::new(1),
                &[
                    Datum::from(Decimal::new(2464, 2)),
                    Datum::from(Decimal::new(1234, 2))
                ]
            ),
            Datum::from(Decimal::new(1230, 2))
        )
    }
}
