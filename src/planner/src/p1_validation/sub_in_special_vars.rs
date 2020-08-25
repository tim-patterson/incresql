use ast::expr::*;
use ast::rel::logical::LogicalOperator;
use data::{DataType, Datum};

/// Mysql uses some @@ magic variables that they can select.
/// This is here to replace some of them with Constants
pub(super) fn sub_in_special_vars(query: &mut LogicalOperator) {
    for child in query.children_mut() {
        sub_in_special_vars(child);
    }

    for expression in query.expressions_mut() {
        if let Expression::ColumnReference(ColumnReference {
            qualifier: _,
            alias,
            star: _,
        }) = expression
        {
            if alias.starts_with("@@") {
                let constant = match alias.as_str() {
                    "@@max_allowed_packet" => {
                        Expression::Constant(Datum::from(0xffffff), DataType::Integer)
                    }
                    "@@socket" => Expression::Constant(Datum::from(""), DataType::Text),

                    _ => continue,
                };
                *expression = constant;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::expr::{Expression, NamedExpression};
    use ast::rel::logical::Project;
    use data::{DataType, Datum};

    #[test]
    fn test_sub_in_special_vars() {
        let mut operator = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: Some(String::from("1")),
                expression: Expression::ColumnReference(ColumnReference {
                    qualifier: None,
                    alias: "@@max_allowed_packet".to_string(),
                    star: false,
                }),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        sub_in_special_vars(&mut operator);

        assert_eq!(
            operator.expressions_mut().next().unwrap(),
            &mut Expression::Constant(Datum::from(0xffffff), DataType::Integer)
        );
    }
}
