use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::DataType::Decimal;
use data::{DataType, Datum, Session, DECIMAL_MAX_PRECISION};
use std::cmp::{max, min};

/// Returns the first non-null result
#[derive(Debug)]
struct Coalesce {}

impl Function for Coalesce {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        args.iter()
            .find(|d| !d.is_null())
            .map_or(Datum::Null, Datum::ref_clone)
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
        for arg_count in 1..11 {
            let args = (0..arg_count).map(|_| *datatype).collect();
            if *datatype == Decimal(0, 0) {
                registry.register_function(FunctionDefinition::new_with_type_resolver(
                    "coalesce",
                    args,
                    // Here we're basically change the p & s of decimal to instead represent
                    // the whole number digits and the frac digits, The resulting decimal
                    // should contain the max of each and then we turn back into p & s.
                    |args| {
                        let (w, s) = args
                            .iter()
                            .filter(|d| **d != DataType::Null)
                            .map(|d| {
                                if let DataType::Decimal(p, s) = d {
                                    // The whole_number, and frac parts
                                    (*p - *s, *s)
                                } else {
                                    panic!()
                                }
                            })
                            .fold((0, 0), |(w1, s1), (w2, s2)| (max(w1, w2), max(s1, s2)));

                        DataType::Decimal(min(DECIMAL_MAX_PRECISION, w + s), s)
                    },
                    FunctionType::Scalar(&Coalesce {}),
                ))
            } else {
                registry.register_function(FunctionDefinition::new(
                    "coalesce",
                    args,
                    *datatype,
                    FunctionType::Scalar(&Coalesce {}),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "coalesce",
        args: vec![],
        ret: DataType::Boolean,
    };

    #[test]
    fn test_int() {
        assert_eq!(
            Coalesce {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::Null, Datum::from(1), Datum::Null]
            ),
            Datum::from(1)
        );

        assert_eq!(
            Coalesce {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(1), Datum::from(2), Datum::from(3)]
            ),
            Datum::from(1)
        );

        assert_eq!(
            Coalesce {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::Null, Datum::Null, Datum::from(2)]
            ),
            Datum::from(2)
        );
    }

    #[test]
    fn test_decimal_type() {
        let registry = Registry::default();
        let (sig, _function) = registry
            .resolve_function(&FunctionSignature {
                name: "coalesce",
                // 10.0 and 2.4
                args: vec![Decimal(10, 0), Decimal(6, 4)],
                ret: DataType::Null,
            })
            .unwrap();
        // Expect 10.4 == (14, 4)
        assert_eq!(sig.ret, Decimal(14, 4))
    }
}
