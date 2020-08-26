use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct Gte {}

impl Function for Gte {
    fn execute<'a>(
        &self,
        _session: &Session,
        _signature: &FunctionSignature,
        args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        if args[0].is_null() || args[1].is_null() {
            Datum::Null
        } else {
            Datum::from(args[0] >= args[1])
        }
    }
}

pub fn register_builtins(registry: &mut Registry) {
    for datatype in &[
        DataType::Boolean,
        DataType::Integer,
        DataType::BigInt,
        DataType::Decimal(0, 0),
        DataType::Text,
    ] {
        registry.register_function(FunctionDefinition::new(
            ">=",
            vec![*datatype, *datatype],
            DataType::Boolean,
            FunctionType::Scalar(&Gte {}),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: ">=",
        args: vec![],
        ret: DataType::Boolean,
    };

    #[test]
    fn test_null() {
        assert_eq!(
            Gte {}.execute(&Session::new(1), &DUMMY_SIG, &[Datum::Null, Datum::from(1)]),
            Datum::Null
        )
    }

    #[test]
    fn test_gte() {
        assert_eq!(
            Gte {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(1), Datum::from(1)]
            ),
            Datum::from(true)
        );

        assert_eq!(
            Gte {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(1), Datum::from(0)]
            ),
            Datum::from(true)
        );

        assert_eq!(
            Gte {}.execute(
                &Session::new(1),
                &DUMMY_SIG,
                &[Datum::from(1), Datum::from(2)]
            ),
            Datum::from(false)
        );
    }
}
