use crate::common::type_for_expression;
use crate::{Planner, PlannerError};
use ast::expr::{Expression, NamedExpression};
use ast::rel::logical::{LogicalOperator, Values};
use data::{DataType, Session};
use std::fmt::{Display, Formatter};

impl Planner {
    /// Produce a plain that simply prints out the execution plan
    pub fn explain_logical(
        &self,
        query: LogicalOperator,
        session: &Session,
    ) -> Result<LogicalOperator, PlannerError> {
        let (_fields, operator) = self.plan_common(query, session)?;

        let mut lines = vec![];
        let mut padding = Padding::default();
        render_plan(&operator, &mut lines, &mut padding);

        let data = lines
            .into_iter()
            .map(|(line, expr)| {
                let expr_datum = expr
                    .map(Expression::from)
                    .unwrap_or_else(|| Expression::from(""));
                vec![Expression::from(line), expr_datum]
            })
            .collect();

        Ok(LogicalOperator::Values(Values {
            fields: vec![
                (DataType::Text, String::from("tree")),
                (DataType::Text, String::from("expression")),
            ],
            data,
        }))
    }
}

#[derive(Default)]
struct Padding {
    pads: Vec<&'static str>,
}

impl Padding {
    fn push(&mut self, pad: &'static str) {
        self.pads.push(pad);
    }

    fn pop(&mut self) {
        self.pads.pop();
    }
}

impl Display for Padding {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for pad in &self.pads {
            f.write_str(pad)?;
        }
        Ok(())
    }
}

fn render_plan(
    operator: &LogicalOperator,
    lines: &mut Vec<(String, Option<String>)>,
    padding: &mut Padding,
) {
    match operator {
        LogicalOperator::Single => {
            lines.push((format!("{}SINGLE", padding), None));
        }
        LogicalOperator::Project(project) => {
            lines.push((format!("{}PROJECT", padding), None));
            padding.push(" |");
            lines.push((format!("{}exprs:", padding), None));
            render_named_expressions(&project.expressions, lines, padding);
            lines.push((format!("{}source:", padding), None));
            padding.push("  ");
            render_plan(&project.source, lines, padding);
            padding.pop();
            padding.pop();
        }
        LogicalOperator::Values(values) => {
            lines.push((format!("{}VALUES", padding), None));
            padding.push(" |");
            lines.push((format!("{}values:", padding), None));
            for row in &values.data {
                let formatted_row = row
                    .iter()
                    .map(|datum| format!("{:#}", datum))
                    .collect::<Vec<_>>()
                    .join(", ");
                lines.push((format!("{}  {}", padding, formatted_row), None));
            }
            padding.pop();
        }
    }
}

fn render_named_expressions(
    exprs: &[NamedExpression],
    lines: &mut Vec<(String, Option<String>)>,
    padding: &mut Padding,
) {
    for named_expr in exprs {
        let expr = &named_expr.expression;
        let datatype = type_for_expression(expr);
        let alias = named_expr.alias.as_ref().unwrap();
        lines.push((
            format!("{}  {} <{}>", padding, alias, datatype),
            Some(expr.to_string()),
        ));
    }
}
