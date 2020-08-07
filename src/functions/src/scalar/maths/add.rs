use crate::Function;
use data::{Datum, Session};

#[derive(Debug)]
struct AddInteger {}

impl Function for AddInteger {
    fn execute<'a>(&self, _session: &Session, args: &'a [Datum<'a>]) -> Datum<'a> {
        let maybe_a: Option<i32> = (&args[0]).into();
        let maybe_b: Option<i32> = (&args[1]).into();
        if let (Some(a), Some(b)) = (maybe_a, maybe_b) {
            Datum::from(a + b)
        } else {
            Datum::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        assert_eq!(
            AddInteger {}.execute(&Session::new(1), &[Datum::Null, Datum::Null]),
            Datum::Null
        )
    }

    #[test]
    fn test_add() {
        assert_eq!(
            AddInteger {}.execute(&Session::new(1), &[Datum::from(1), Datum::from(2)]),
            Datum::from(3)
        )
    }
}
