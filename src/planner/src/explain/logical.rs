use crate::explain::ExplainNode;
use ast::expr::{Expression, NamedExpression, SortExpression};
use ast::rel::logical::LogicalOperator;
use data::DataType;

impl ExplainNode for LogicalOperator {
    fn node_name(&self) -> String {
        match self {
            LogicalOperator::Single => "SINGLE".to_string(),
            LogicalOperator::GroupBy(_) => "GROUP".to_string(),
            LogicalOperator::Project(_) => "PROJECT".to_string(),
            LogicalOperator::Sort(_) => "SORT".to_string(),
            LogicalOperator::Values(_) => "VALUES".to_string(),
            LogicalOperator::ResolvedTable(_) | LogicalOperator::TableReference(_) => {
                "TABLE".to_string()
            }
            LogicalOperator::Filter(_) => "FILTER".to_string(),
            LogicalOperator::Limit(_) => "LIMIT".to_string(),
            LogicalOperator::TableAlias(table_alias) => {
                format!("{}({})", table_alias.source.node_name(), table_alias.alias)
            }
            LogicalOperator::UnionAll(_) => "UNION_ALL".to_string(),
            LogicalOperator::TableInsert(_) => "INSERT".to_string(),
            LogicalOperator::NegateFreq(_) => "NEGATE".to_string(),
        }
    }

    fn expressions(&self) -> &[NamedExpression] {
        match self {
            LogicalOperator::TableAlias(table_alias) => table_alias.source.expressions(),
            LogicalOperator::Project(project) => &project.expressions,
            LogicalOperator::GroupBy(group_by) => &group_by.expressions,
            _ => &[],
        }
    }

    fn table_columns(&self) -> &[(String, DataType)] {
        match self {
            LogicalOperator::TableAlias(table_alias) => table_alias.source.table_columns(),
            LogicalOperator::ResolvedTable(table) => table.table.columns(),
            _ => &[],
        }
    }

    fn limit_offset(&self) -> Option<(i64, i64)> {
        match self {
            LogicalOperator::TableAlias(table_alias) => table_alias.source.limit_offset(),
            LogicalOperator::Limit(limit) => Some((limit.limit, limit.offset)),
            _ => None,
        }
    }

    fn predicate(&self) -> Option<&Expression> {
        match self {
            LogicalOperator::TableAlias(table_alias) => table_alias.source.predicate(),
            LogicalOperator::Filter(filter) => Some(&filter.predicate),
            _ => None,
        }
    }

    fn sort_expressions(&self) -> &[SortExpression] {
        match self {
            LogicalOperator::TableAlias(table_alias) => table_alias.source.sort_expressions(),
            LogicalOperator::Sort(sort) => &sort.sort_expressions,
            _ => &[],
        }
    }

    fn grouping_keys(&self) -> &[Expression] {
        match self {
            LogicalOperator::TableAlias(table_alias) => table_alias.source.grouping_keys(),
            LogicalOperator::GroupBy(group) => &group.key_expressions,
            _ => &[],
        }
    }

    fn child_nodes(&self) -> Vec<(String, &Self)> {
        match self {
            LogicalOperator::Single => vec![],
            LogicalOperator::GroupBy(group) => vec![("source".to_string(), group.source.as_ref())],
            LogicalOperator::Project(project) => {
                vec![("source".to_string(), project.source.as_ref())]
            }
            LogicalOperator::Sort(sort) => vec![("source".to_string(), sort.source.as_ref())],
            LogicalOperator::Values(_)
            | LogicalOperator::ResolvedTable(_)
            | LogicalOperator::TableReference(_) => vec![],
            LogicalOperator::Filter(filter) => vec![("source".to_string(), filter.source.as_ref())],
            LogicalOperator::Limit(limit) => vec![("source".to_string(), limit.source.as_ref())],
            LogicalOperator::TableAlias(table_alias) => table_alias.source.child_nodes(),
            LogicalOperator::UnionAll(union) => union
                .sources
                .iter()
                .enumerate()
                .map(|(idx, child)| (format!("source_{}", idx), child))
                .collect(),
            LogicalOperator::TableInsert(insert) => {
                vec![("source".to_string(), insert.source.as_ref())]
            }
            LogicalOperator::NegateFreq(source) => vec![("source".to_string(), source.as_ref())],
        }
    }
}
