use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::DataType::Decimal;
use data::{DataType, Datum, Session, DECIMAL_MAX_PRECISION};
use std::cmp::{max, min};

/// Returns the first non-null result
#[derive(Debug)]
struct IfFn {}

impl Function for IfFn {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if args[0].as_maybe_boolean() == Some(true) {
            args[1].ref_clone()
        } else {
            args[2].ref_clone()
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    for datatype in &[
        DataType::Boolean,
        DataType::Integer,
        DataType::BigInt,
        DataType::Text,
        DataType::ByteA,
        DataType::Date,
        DataType::Timestamp,
        DataType::Json,
        Decimal(0, 0),
    ] {
        let args = vec![DataType::Boolean, *datatype, *datatype];
        if *datatype == Decimal(0, 0) {
            registry.register_function(FunctionDefinition::new_with_type_resolver(
                "if",
                args,
                // The same basic logic for decimal return type as add etc
                // Here we're basically change the p & s of decimal to instead represent
                // the whole number digits and the frac digits, The resulting decimal
                // should contain the max of each and then we turn back into p & s.
                |args| match (args[1], args[2]) {
                    (DataType::Decimal(p1, s1), DataType::Decimal(p2, s2)) => {
                        let s = max(s1, s2);
                        let p = min(max(p1 - s1, p2 - s2) + s, DECIMAL_MAX_PRECISION);
                        DataType::Decimal(p, s)
                    }
                    (DataType::Decimal(p, s), _) => DataType::Decimal(p, s),
                    (_, DataType::Decimal(p, s)) => DataType::Decimal(p, s),
                    _ => unreachable!(),
                },
                FunctionType::Scalar(&IfFn {}),
            ))
        } else {
            registry.register_function(FunctionDefinition::new(
                "if",
                args,
                *datatype,
                FunctionType::Scalar(&IfFn {}),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "if",
        args: vec![],
        ret: DataType::Boolean,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            IfFn {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::Null, Datum::from(1), Datum::from(2)]
            ),
            Datum::from(2)
        );
    }

    #[test]
    fn test_true() {
        assert_eq!(
            IfFn {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(true), Datum::from("T"), Datum::from("F")]
            ),
            Datum::from("T")
        );
    }

    #[test]
    fn test_false() {
        assert_eq!(
            IfFn {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(false), Datum::from("T"), Datum::from("F")]
            ),
            Datum::from("F")
        );
    }

    #[test]
    fn test_decimal_type() {
        let registry = Registry::default();
        let (sig, _function) = registry
            .resolve_function(&FunctionSignature {
                name: "if",
                // 10.0 and 2.4
                args: vec![DataType::Boolean, Decimal(10, 0), Decimal(6, 4)],
                ret: DataType::Null,
            })
            .unwrap();
        // Expect 10.4 == (14, 4)
        assert_eq!(sig.ret, Decimal(14, 4))
    }
}
