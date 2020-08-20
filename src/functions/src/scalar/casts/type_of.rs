use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature, FunctionType};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct TypeOf {}

impl Function for TypeOf {
    fn execute<'a>(
        &self,
        _session: &Session,
        signature: &FunctionSignature,
        _args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        Datum::from(signature.args[0].to_string())
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "type_of",
        vec![DataType::Null],
        DataType::Text,
        FunctionType::Scalar(&TypeOf {}),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let sig = FunctionSignature {
            name: "type_of",
            args: vec![DataType::Null],
            ret: DataType::Text,
        };

        assert_eq!(
            TypeOf {}.execute(&Session::new(1), &sig, &[Datum::Null]),
            Datum::from("NULL")
        )
    }

    #[test]
    fn test_decimal() {
        let sig = FunctionSignature {
            name: "type_of",
            args: vec![DataType::Decimal(1, 2)],
            ret: DataType::Text,
        };

        assert_eq!(
            TypeOf {}.execute(&Session::new(1), &sig, &[Datum::Null]),
            Datum::from("DECIMAL(1,2)")
        )
    }
}
