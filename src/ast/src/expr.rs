use data::Datum;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expression {
    Literal(Datum<'static>),
}
