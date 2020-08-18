use crate::registry::Registry;
use crate::{Function, FunctionDefinition, FunctionSignature};
use data::{DataType, Datum, Session};

#[derive(Debug)]
struct Database {}

impl Function for Database {
    fn execute<'a>(
        &self,
        session: &Session,
        _signature: &FunctionSignature,
        _args: &'a [Datum<'a>],
    ) -> Datum<'a> {
        Datum::from(session.current_database.read().unwrap().to_string())
    }
}

pub fn register_builtins(registry: &mut Registry) {
    registry.register_function(FunctionDefinition::new(
        "database",
        vec![],
        DataType::Text,
        &Database {},
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SIG: FunctionSignature = FunctionSignature {
        name: "database",
        args: vec![],
        ret: DataType::Text,
    };

    #[test]
    fn test_database() {
        let session = Session::new(1);
        *session.current_database.write().unwrap() = "foobar".to_string();
        assert_eq!(
            Database {}.execute(&session, &DUMMY_SIG, &[]),
            Datum::from("foobar")
        )
    }
}
