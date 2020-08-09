use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature};
use data::{DataType, Datum, Session, DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE};

#[derive(Debug)]
struct DivideInteger {}

impl Function for DivideInteger {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_integer(), args[1].as_integer()) {
            Datum::from(a / b)
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct DivideBigint {}

impl Function for DivideBigint {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_bigint(), args[1].as_bigint()) {
            Datum::from(a / b)
        } else {
            Datum::Null
        }
    }
}

#[derive(Debug)]
struct DivideDecimal {}

impl Function for DivideDecimal {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if let (Some(a), Some(b)) = (args[0].as_decimal(), args[1].as_decimal()) {
            let mut d = a / b;
            if d.scale() > DECIMAL_MAX_SCALE as u32 {
                d.rescale(DECIMAL_MAX_SCALE as u32);
            }
            Datum::from(d)
        } else {
            Datum::Null
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "/",
        vec![DataType::Integer, DataType::Integer],
        DataType::Integer,
        &DivideInteger {},
    ));

    registry.register_function(FunctionDefinition::new(
        "/",
        vec![DataType::BigInt, DataType::BigInt],
        DataType::BigInt,
        &DivideBigint {},
    ));

    registry.register_function(FunctionDefinition::new(
        "/",
        vec![DataType::Decimal(0, 0), DataType::Decimal(0, 0)],
        DataType::Decimal(DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE),
        &DivideDecimal {},
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::rust_decimal::Decimal;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "/",
        args: vec![],
        ret: DataType::Integer,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            DivideInteger {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null, Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_divide_int() {
        assert_eq!(
            DivideInteger {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(5), Datum::from(2)]
            ),
            Datum::from(2)
        )
    }

    #[test]
    fn test_divide_bigint() {
        assert_eq!(
            DivideBigint {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(5_i64), Datum::from(2_i64)]
            ),
            Datum::from(2_i64)
        )
    }

    #[test]
    fn test_divide_decimal() {
        assert_eq!(
            DivideDecimal {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[
                    Datum::from(Decimal::new(10, 1)),
                    Datum::from(Decimal::new(3, 1))
                ]
            ),
            Datum::from(Decimal::new(333333333333333, 14))
        )
    }
}
