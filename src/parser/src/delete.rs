use crate::atoms::{kw, qualified_reference};
use crate::select::{limit_clause, where_clause};
use crate::whitespace::ws_0;
use crate::ParserResult;
use ast::rel::logical::{Filter, Limit, LogicalOperator, TableAlias, TableInsert, TableReference};
use nom::combinator::{cut, map, opt};
use nom::sequence::{pair, preceded, tuple};

/// Parses a delete statement
/// A delete is really like a subset of select (from below the from), supporting
/// only where and limit.
pub fn delete(input: &str) -> ParserResult<LogicalOperator> {
    map(
        preceded(
            kw("DELETE"),
            pair(
                cut(preceded(tuple((ws_0, kw("FROM"), ws_0)), table_reference)),
                cut(tuple((
                    opt(preceded(ws_0, where_clause)),
                    opt(preceded(ws_0, limit_clause)),
                ))),
            ),
        ),
        |(table_reference, (where_option, limit_option))| {
            // For the from portion of the delete we should wrap the table in an alias to support
            // qualified references in the where clauses
            let table_alias =
                if let LogicalOperator::TableReference(TableReference { database: _, table }) =
                    &table_reference
                {
                    table
                } else {
                    panic!()
                };

            let mut query = LogicalOperator::TableAlias(TableAlias {
                alias: table_alias.to_string(),
                source: Box::new(table_reference.clone()),
            });

            if let Some(predicate) = where_option {
                query = LogicalOperator::Filter(Filter {
                    predicate,
                    source: Box::new(query),
                });
            }

            if let Some((offset, limit)) = limit_option {
                query = LogicalOperator::Limit(Limit {
                    offset,
                    limit,
                    source: Box::new(query),
                });
            }

            LogicalOperator::TableInsert(TableInsert {
                table: Box::new(table_reference),
                source: Box::new(LogicalOperator::NegateFreq(Box::from(query))),
            })
        },
    )(input)
}

/// Parse as a table_reference
fn table_reference(input: &str) -> ParserResult<LogicalOperator> {
    map(qualified_reference, |(database, table)| {
        LogicalOperator::TableReference(TableReference { database, table })
    })(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete() {
        let table_ref = LogicalOperator::TableReference(TableReference {
            database: None,
            table: "foo".to_string(),
        });

        assert_eq!(
            delete("delete from foo limit 1").unwrap().1,
            LogicalOperator::TableInsert(TableInsert {
                table: Box::new(table_ref.clone()),
                source: Box::new(LogicalOperator::NegateFreq(Box::new(
                    LogicalOperator::Limit(Limit {
                        offset: 0,
                        limit: 1,
                        source: Box::new(LogicalOperator::TableAlias(TableAlias {
                            alias: "foo".to_string(),
                            source: Box::new(table_ref)
                        }))
                    })
                )))
            })
        );
    }
}
