use crate::common::{fields_for_operator, type_for_expression};
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
        render_plan(&operator, &mut lines, &mut padding, None);

        let data = lines
            .into_iter()
            .map(|(line, idx, expr)| {
                let idx_datum = idx
                    .map(|idx| Expression::from(idx.to_string()))
                    .unwrap_or_else(|| Expression::from(""));
                let expr_datum = expr
                    .map(Expression::from)
                    .unwrap_or_else(|| Expression::from(""));
                vec![Expression::from(line), idx_datum, expr_datum]
            })
            .collect();

        Ok(LogicalOperator::Values(Values {
            fields: vec![
                (DataType::Text, String::from("tree")),
                (DataType::Text, String::from("idx")),
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
    lines: &mut Vec<(String, Option<usize>, Option<String>)>,
    padding: &mut Padding,
    alias: Option<&str>,
) {
    match operator {
        LogicalOperator::Single => {
            lines.push((format!("{}SINGLE", padding), None, None));
        }
        LogicalOperator::Project(project) => {
            if let Some(name) = alias {
                lines.push((format!("{}PROJECT({})", padding, name), None, None));
            } else {
                lines.push((format!("{}PROJECT", padding), None, None));
            }
            padding.push(" |");
            lines.push((format!("{}exprs:", padding), None, None));
            render_named_expressions(&project.expressions, &mut 0, lines, padding);
            lines.push((format!("{}source:", padding), None, None));
            padding.push("  ");
            render_plan(&project.source, lines, padding, None);
            padding.pop();
            padding.pop();
        }
        LogicalOperator::Filter(filter) => {
            if let Some(name) = alias {
                lines.push((format!("{}FILTER({})", padding, name), None, None));
            } else {
                lines.push((format!("{}FILTER", padding), None, None));
            }
            padding.push(" |");
            lines.push((
                format!("{}predicate:", padding),
                None,
                Some(filter.predicate.to_string()),
            ));
            lines.push((format!("{}source:", padding), None, None));
            padding.push("  ");
            render_plan(&filter.source, lines, padding, None);
            padding.pop();
            padding.pop();
        }
        LogicalOperator::Limit(limit) => {
            if let Some(name) = alias {
                lines.push((format!("{}LIMIT({})", padding, name), None, None));
            } else {
                lines.push((format!("{}LIMIT", padding), None, None));
            }
            padding.push(" |");
            lines.push((format!("{}offset: {}", padding, limit.offset), None, None));
            lines.push((format!("{}limit: {}", padding, limit.limit), None, None));
            lines.push((format!("{}source:", padding), None, None));
            padding.push("  ");
            render_plan(&limit.source, lines, padding, None);
            padding.pop();
            padding.pop();
        }
        LogicalOperator::Values(values) => {
            if let Some(name) = alias {
                lines.push((format!("{}VALUES({})", padding, name), None, None));
            } else {
                lines.push((format!("{}VALUES", padding), None, None));
            }
            padding.push(" |");
            lines.push((format!("{}values:", padding), None, None));
            for row in &values.data {
                let formatted_row = row
                    .iter()
                    .map(|datum| format!("{:#}", datum))
                    .collect::<Vec<_>>()
                    .join(", ");
                lines.push((format!("{}  {}", padding, formatted_row), None, None));
            }
            padding.pop();
        }
        LogicalOperator::UnionAll(union_all) => {
            if let Some(name) = alias {
                lines.push((format!("{}UNION_ALL({})", padding, name), None, None));
            } else {
                lines.push((format!("{}UNION_ALL", padding), None, None));
            }
            padding.push(" |");
            lines.push((format!("{}sources:", padding), None, None));
            padding.push("  ");
            for source in &union_all.sources {
                render_plan(source, lines, padding, None);
            }
            padding.pop();
            padding.pop();
        }
        LogicalOperator::ResolvedTable(table) => {
            if let Some(name) = alias {
                lines.push((format!("{}TABLE({})", padding, name), None, None));
            } else {
                lines.push((format!("{}TABLE", padding), None, None));
            }
            padding.push(" |");
            lines.push((format!("{}cols:", padding), None, None));
            for (idx, (alias, datatype)) in table.table.columns().iter().enumerate() {
                lines.push((
                    format!("{}  {} <{}>", padding, alias, datatype),
                    Some(idx),
                    None,
                ));
            }
            padding.pop();
        }
        LogicalOperator::TableInsert(table_insert) => {
            if let Some(name) = alias {
                lines.push((format!("{}INSERT({})", padding, name), None, None));
            } else {
                lines.push((format!("{}INSERT", padding), None, None));
            }
            padding.push(" |");
            lines.push((format!("{}cols:", padding), None, None));
            for (idx, field) in fields_for_operator(&table_insert.table).enumerate() {
                lines.push((
                    format!("{}  {} <{}>", padding, field.alias, field.data_type),
                    Some(idx),
                    None,
                ));
            }
            lines.push((format!("{}source:", padding), None, None));
            padding.push("  ");
            render_plan(&table_insert.source, lines, padding, None);
            padding.pop();
            padding.pop();
        }
        LogicalOperator::TableReference(_) => panic!(),
        LogicalOperator::TableAlias(table_alias) => {
            // We don't render a table alias, we simply pass down the alias to annotate the operator
            // below
            render_plan(
                &table_alias.source,
                lines,
                padding,
                Some(&table_alias.alias),
            );
        }
    }
}

/// Renders the named expressions as expected for the explain output.
/// Total index is a pointer to a counter so we can keep track of the current expr
/// offset
fn render_named_expressions(
    exprs: &[NamedExpression],
    total_idx: &mut usize,
    lines: &mut Vec<(String, Option<usize>, Option<String>)>,
    padding: &mut Padding,
) {
    for named_expr in exprs {
        let idx = *total_idx;
        *total_idx += 1;
        let expr = &named_expr.expression;
        let datatype = type_for_expression(expr);
        let alias = named_expr.alias.as_ref().unwrap();
        lines.push((
            format!("{}  {} <{}>", padding, alias, datatype),
            Some(idx),
            Some(expr.to_string()),
        ));
    }
}
