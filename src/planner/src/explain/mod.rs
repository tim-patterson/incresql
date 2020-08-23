mod logical;

use crate::utils::expr::type_for_expression;
use crate::Planner;
use ast::expr::{Expression, NamedExpression, SortExpression};
use ast::rel::logical::{LogicalOperator, Values};
use data::DataType;
use std::fmt::{Display, Formatter};

/// A trait to be implemented by the nodes to be rendered in an explain plan
pub trait ExplainNode {
    // Returns the name of the node, ie PROJECT
    fn node_name(&self) -> String;

    // Any return expressions defined by this node. If this node is simply a pass through
    // like a limit or a filter it should be left blank.
    fn expressions(&self) -> &[NamedExpression];

    // Any columns from tables, rendered in the same as expressions, but without
    // the expression itself
    fn table_columns(&self) -> &[(String, DataType)];

    fn limit_offset(&self) -> Option<(i64, i64)>;

    fn predicate(&self) -> Option<&Expression>;

    fn sort_expressions(&self) -> &[SortExpression];

    fn grouping_keys(&self) -> &[Expression];

    // The "sources" for this node, most nodes will return a single source, but things
    // like unions may return many sources.
    fn child_nodes(&self) -> Vec<(String, &Self)>;
}

impl Planner {
    /// Produce a plan that simply prints out the execution plan.
    /// The resultant plan will have 3 columns:
    /// tree - a textual representation of the operators.
    /// column_idx - For output expressions, the index at which they appear
    /// expression_type - The resultant type of any expression
    /// expression - The expression itself.
    pub fn explain<N: ExplainNode>(&self, node: &N) -> LogicalOperator {
        let mut lines = vec![];
        let mut padding = Padding::default();
        render_node(node, &mut lines, &mut padding);

        let data = lines
            .into_iter()
            .map(|line| {
                let idx_datum = line
                    .column_idx
                    .map(|idx| Expression::from(idx.to_string()))
                    .unwrap_or_else(|| Expression::from(""));
                let datatype_datum = line
                    .expression_type
                    .map(|dt| Expression::from(dt.to_string()))
                    .unwrap_or_else(|| Expression::from(""));
                let expr_datum = line
                    .expression
                    .map(Expression::from)
                    .unwrap_or_else(|| Expression::from(""));
                vec![
                    Expression::from(line.tree),
                    idx_datum,
                    datatype_datum,
                    expr_datum,
                ]
            })
            .collect();

        LogicalOperator::Values(Values {
            fields: vec![
                (DataType::Text, String::from("tree")),
                (DataType::Text, String::from("col_idx")),
                (DataType::Text, String::from("datatype")),
                (DataType::Text, String::from("expression")),
            ],
            data,
        })
    }
}

/// A single explain line
struct ExplainLine {
    tree: String,
    column_idx: Option<usize>,
    expression_type: Option<DataType>,
    expression: Option<String>,
}

impl ExplainLine {
    fn tree_only(padding: &Padding, tree: &str) -> Self {
        ExplainLine {
            tree: format!("{}{}", padding, tree),
            column_idx: None,
            expression_type: None,
            expression: None,
        }
    }

    fn full(
        padding: &Padding,
        tree: &str,
        column_idx: usize,
        expression_type: DataType,
        expression: String,
    ) -> Self {
        ExplainLine {
            tree: format!("{}{}", padding, tree),
            column_idx: Some(column_idx),
            expression_type: Some(expression_type),
            expression: Some(expression),
        }
    }

    fn expr_only(padding: &Padding, expression_type: DataType, expression: String) -> Self {
        ExplainLine {
            tree: padding.to_string(),
            column_idx: None,
            expression_type: Some(expression_type),
            expression: Some(expression),
        }
    }
}

fn render_node<N: ExplainNode>(node: &N, lines: &mut Vec<ExplainLine>, padding: &mut Padding) {
    lines.push(ExplainLine::tree_only(padding, &node.node_name()));
    padding.push(" |");
    // output cols
    if !node.expressions().is_empty() {
        lines.push(ExplainLine::tree_only(padding, "output_exprs:"));
        padding.push("  ");
        for (idx, expr) in node.expressions().iter().enumerate() {
            lines.push(ExplainLine::full(
                padding,
                expr.alias.as_ref().unwrap(),
                idx,
                type_for_expression(&expr.expression),
                expr.expression.to_string(),
            ));
        }
        padding.pop();
    }

    // table columns
    if !node.table_columns().is_empty() {
        lines.push(ExplainLine::tree_only(padding, "columns:"));
        padding.push("  ");
        for (idx, (alias, dt)) in node.table_columns().iter().enumerate() {
            lines.push(ExplainLine::full(padding, alias, idx, *dt, String::new()));
        }
        padding.pop();
    }

    // limit/offset
    if let Some((limit, offset)) = node.limit_offset() {
        lines.push(ExplainLine::tree_only(
            padding,
            &format!("limit: {}", limit),
        ));
        lines.push(ExplainLine::tree_only(
            padding,
            &format!("offset: {}", offset),
        ));
    }

    // predicate
    if let Some(expr) = node.predicate() {
        lines.push(ExplainLine::tree_only(padding, "predicate:"));
        lines.push(ExplainLine::expr_only(
            padding,
            type_for_expression(expr),
            expr.to_string(),
        ));
    }

    // group by keys
    if !node.grouping_keys().is_empty() {
        lines.push(ExplainLine::tree_only(padding, "group_keys:"));
        for expr in node.grouping_keys() {
            lines.push(ExplainLine::expr_only(
                padding,
                type_for_expression(expr),
                expr.to_string(),
            ));
        }
    }

    // sort expressions
    if !node.sort_expressions().is_empty() {
        lines.push(ExplainLine::tree_only(padding, "sort_exprs:"));
        padding.push("  ");
        for se in node.sort_expressions() {
            lines.push(ExplainLine::expr_only(
                padding,
                type_for_expression(&se.expression),
                format!("{} ({})", &se.expression, se.ordering),
            ));
        }
        padding.pop();
    }

    // Sources
    for (source_name, source) in &node.child_nodes() {
        lines.push(ExplainLine::tree_only(
            padding,
            &format!("{}:", source_name),
        ));
        padding.push("  ");
        render_node(*source, lines, padding);
        padding.pop();
    }

    padding.pop()
}

/// A helper class to deal with pushing and popping padding
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
