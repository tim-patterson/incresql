use crate::expr::{Expression, NamedExpression, SortExpression};
use data::DataType;
use std::iter::{empty, once};
use storage::Table;

/// Represents a query in the generic sense, generated from the parser, and validated and
/// modified by the planner.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum LogicalOperator {
    // These may appear anywhere in a logical operator at anytime
    Single, // No from clause, ie select 1 + 1
    Project(Project),
    Filter(Filter),
    Sort(Sort),
    Limit(Limit),
    Values(Values),
    TableAlias(TableAlias),
    UnionAll(UnionAll),
    TableReference(TableReference),
    ResolvedTable(ResolvedTable),
    TableInsert(TableInsert),
    NegateFreq(Box<LogicalOperator>),
}

impl Default for LogicalOperator {
    fn default() -> Self {
        LogicalOperator::Single
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Project {
    pub distinct: bool, // Comes from parser, planner will rewrite to a group by
    pub expressions: Vec<NamedExpression>,
    pub source: Box<LogicalOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Filter {
    pub predicate: Expression,
    pub source: Box<LogicalOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Limit {
    pub offset: i64,
    pub limit: i64,
    pub source: Box<LogicalOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Sort {
    pub sort_expressions: Vec<SortExpression>,
    pub source: Box<LogicalOperator>,
}

/// An operator that just feeds up a fixed set of values.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Values {
    // If not populated the planner will fill this in
    pub fields: Vec<(DataType, String)>,
    pub data: Vec<Vec<Expression>>,
}

/// An operator whose sole purpose is to capture table aliases
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TableAlias {
    pub alias: String,
    pub source: Box<LogicalOperator>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UnionAll {
    pub sources: Vec<LogicalOperator>,
}

/// A "table" reference, ie "FROM foo",
/// This table could be a table, a view or even a CTE
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TableReference {
    pub database: Option<String>,
    pub table: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ResolvedTable {
    pub table: Table,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TableInsert {
    // Table is a logical operator here which might seem a bit weird,
    // but it allows all the table resolving code to be handled by all
    // the existing code paths. Only TableReference and resolved table
    // are valid here.
    pub table: Box<LogicalOperator>,
    pub source: Box<LogicalOperator>,
}

impl LogicalOperator {
    /// Iterates over the named(output) expressions *owned* by this operator.
    /// To iterate over the output fields instead use one of the fields methods in the planner
    pub fn named_expressions(&self) -> Box<dyn Iterator<Item = &NamedExpression> + '_> {
        match self {
            LogicalOperator::Project(project) => Box::from(project.expressions.iter()),
            LogicalOperator::Single
            | LogicalOperator::Filter(_)
            | LogicalOperator::Limit(_)
            | LogicalOperator::Sort(_)
            | LogicalOperator::Values(_)
            | LogicalOperator::TableAlias(_)
            | LogicalOperator::UnionAll(_)
            | LogicalOperator::TableReference(_)
            | LogicalOperator::ResolvedTable(_)
            | LogicalOperator::TableInsert(_)
            | LogicalOperator::NegateFreq(_) => Box::from(empty()),
        }
    }

    /// Iterates over the named(output) expressions *owned* by this operator.
    /// To iterate over the output fields instead use one of the fields methods.
    pub fn named_expressions_mut(&mut self) -> Box<dyn Iterator<Item = &mut NamedExpression> + '_> {
        match self {
            LogicalOperator::Project(project) => Box::from(project.expressions.iter_mut()),
            LogicalOperator::Single
            | LogicalOperator::Filter(_)
            | LogicalOperator::Limit(_)
            | LogicalOperator::Sort(_)
            | LogicalOperator::Values(_)
            | LogicalOperator::TableAlias(_)
            | LogicalOperator::UnionAll(_)
            | LogicalOperator::TableReference(_)
            | LogicalOperator::ResolvedTable(_)
            | LogicalOperator::TableInsert(_)
            | LogicalOperator::NegateFreq(_) => Box::from(empty()),
        }
    }

    /// Iterates over all expressions contained within the operator
    pub fn expressions_mut(&mut self) -> Box<dyn Iterator<Item = &mut Expression> + '_> {
        match self {
            LogicalOperator::Project(project) => {
                Box::from(project.expressions.iter_mut().map(|ne| &mut ne.expression))
            }
            LogicalOperator::Filter(filter) => Box::from(once(&mut filter.predicate)),
            LogicalOperator::Values(values) => {
                Box::from(values.data.iter_mut().flat_map(|row| row.iter_mut()))
            }
            LogicalOperator::Sort(sort) => Box::from(
                sort.sort_expressions
                    .iter_mut()
                    .map(|se| &mut se.expression),
            ),
            LogicalOperator::Single
            | LogicalOperator::Limit(_)
            | LogicalOperator::TableAlias(_)
            | LogicalOperator::UnionAll(_)
            | LogicalOperator::TableReference(_)
            | LogicalOperator::ResolvedTable(_)
            | LogicalOperator::TableInsert(_)
            | LogicalOperator::NegateFreq(_) => Box::from(empty()),
        }
    }

    /// Iterates over the immediate child operators of this operator
    pub fn children_mut(&mut self) -> Box<dyn Iterator<Item = &mut LogicalOperator> + '_> {
        match self {
            LogicalOperator::Project(project) => Box::from(once(project.source.as_mut())),
            LogicalOperator::Filter(filter) => Box::from(once(filter.source.as_mut())),
            LogicalOperator::Limit(limit) => Box::from(once(limit.source.as_mut())),
            LogicalOperator::Sort(sort) => Box::from(once(sort.source.as_mut())),
            LogicalOperator::TableAlias(table_alias) => {
                Box::from(once(table_alias.source.as_mut()))
            }
            LogicalOperator::TableInsert(table_insert) => Box::from(
                once(table_insert.table.as_mut()).chain(once(table_insert.source.as_mut())),
            ),
            LogicalOperator::UnionAll(union_all) => Box::from(union_all.sources.iter_mut()),
            LogicalOperator::NegateFreq(source) => Box::from(once(source.as_mut())),
            LogicalOperator::Single
            | LogicalOperator::Values(_)
            | LogicalOperator::TableReference(_)
            | LogicalOperator::ResolvedTable(_) => Box::from(empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Expression;
    use data::{DataType, Datum};

    #[test]
    fn test_named_expressions_mut() {
        let mut operator = LogicalOperator::Single;
        let children: Vec<_> = operator.named_expressions_mut().collect();

        assert_eq!(children, Vec::<&mut NamedExpression>::new());

        let mut operator = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![
                NamedExpression {
                    alias: Some(String::from("1")),
                    expression: Expression::Constant(Datum::Null, DataType::Null),
                },
                NamedExpression {
                    alias: Some(String::from("2")),
                    expression: Expression::Constant(Datum::Null, DataType::Null),
                },
            ],
            source: Box::new(LogicalOperator::Single),
        });

        let aliases: Vec<_> = operator
            .named_expressions_mut()
            .map(|ne| ne.alias.as_ref().unwrap())
            .collect();

        assert_eq!(aliases, vec!["1", "2"]);
    }

    #[test]
    fn test_children_mut() {
        let mut operator = LogicalOperator::Single;
        let children: Vec<_> = operator.children_mut().collect();

        assert_eq!(children, Vec::<&mut LogicalOperator>::new());

        // Double level project!
        let mut operator = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![],
            source: Box::new(LogicalOperator::Project(Project {
                distinct: false,
                expressions: vec![],
                source: Box::new(LogicalOperator::Single),
            })),
        });

        let children: Vec<_> = operator.children_mut().collect();

        let mut expected = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![],
            source: Box::new(LogicalOperator::Single),
        });

        assert_eq!(children, vec![&mut expected]);
    }

    #[test]
    fn test_expressions_mut() {
        let mut operator = LogicalOperator::Single;
        let children: Vec<_> = operator.expressions_mut().collect();

        assert_eq!(children, Vec::<&mut Expression>::new());

        let mut operator = LogicalOperator::Project(Project {
            distinct: false,
            expressions: vec![NamedExpression {
                alias: None,
                expression: Expression::from(1),
            }],
            source: Box::new(LogicalOperator::Single),
        });

        let children: Vec<_> = operator.expressions_mut().collect();

        assert_eq!(children, vec![&mut Expression::from(1)]);
    }
}
