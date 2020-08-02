use data::Datum;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expression {
    Literal(Datum<'static>),
}

/// Named expression, ie select foo as bar
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NamedExpression {
    pub alias: Option<String>,
    pub expression: Expression,
}
