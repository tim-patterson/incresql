use crate::{register_builtins, FunctionDefinition, FunctionSignature, FunctionType};
use data::DataType;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

/// A repository for functions. Used by the planner to resolve the correct functions
#[derive(Debug)]
pub struct Registry {
    functions: BTreeMap<&'static str, Vec<FunctionDefinition>>,
}

impl Default for Registry {
    fn default() -> Self {
        Registry::new(true)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum FunctionResolutionError {
    FunctionNotFound(String),
    MatchingSignatureNotFound(String, Vec<DataType>),
}

impl Display for FunctionResolutionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionResolutionError::FunctionNotFound(function_name) => {
                f.write_fmt(format_args!("Function \"{}\" not found", function_name))
            }
            FunctionResolutionError::MatchingSignatureNotFound(function_name, args) => {
                f.write_fmt(format_args!(
                    "Cannot find variant for function \"{}\" that accepts types {:?}",
                    function_name, args
                ))
            }
        }
    }
}

impl Registry {
    pub fn new(with_builtins: bool) -> Self {
        let mut registry = Registry {
            functions: BTreeMap::new(),
        };

        if with_builtins {
            register_builtins(&mut registry)
        }

        registry
    }

    pub(crate) fn register_function(&mut self, function_definition: FunctionDefinition) {
        self.functions
            .entry(function_definition.signature.name)
            .or_insert_with(Vec::new)
            .push(function_definition);
    }

    pub fn resolve_function(
        &self,
        function_signature: &FunctionSignature,
    ) -> Result<(FunctionSignature<'static>, FunctionType), FunctionResolutionError> {
        if let Some(candidates) = self.functions.get(function_signature.name) {
            // Rank and filter candidates.
            let top_candidate = candidates
                .iter()
                .filter_map(|candidate| {
                    if candidate.signature.args.len() == function_signature.args.len() {
                        candidate
                            .signature
                            .args
                            .iter()
                            .zip(function_signature.args.iter())
                            .map(Registry::datatype_rank)
                            .fold(Some(0_u32), |a,b|
                                if let (Some(a), Some(b)) =(a,b) {
                                    Some(a + b)
                                } else {
                                    None
                                }
                            ).map(|rank| (rank, candidate))
                    } else {
                        None
                    }
                }).next();

            if let Some((_rank, candidate)) = top_candidate {
                // TODO we've hit a blocker here, we'd need to either
                // 1. accept the list of input expressions so we can mutate the casts in.
                // 2. return all candidates to the caller and let them handle it.
                // 3. figure out how to represent compound functions and return them instead.
                // #3 would support us representing regex functions as a compiler function
                // wrapped in an evaluator and if the regex was a constant then the constant
                // folding would automagically handle that for us. The same applies to jsonpath
                // expressions.
                panic!();
                function_signature.args.iter().zip(&candidate.signature.args);

                // Calculate return type,
                // There's 3 paths here.
                // 1. A return type is specified in the function signature, used for cast(foo as decimal(2,3)),
                // 2. A custom_return_type_resolver from the function def is used to calculate the return type based on the input args
                // 3. A hardcoded return type from the function is used.
                let ret = if function_signature.ret != DataType::Null {
                    function_signature.ret
                } else if let Some(type_resolver) = candidate.custom_return_type_resolver {
                    type_resolver(&function_signature.args)
                } else {
                    candidate.signature.ret
                };
                let return_signature = FunctionSignature {
                    name: candidate.signature.name,
                    args: function_signature.args.clone(),
                    ret,
                };

                Ok((return_signature, candidate.function.clone()))
            } else {
                Err(FunctionResolutionError::MatchingSignatureNotFound(
                    function_signature.name.to_string(),
                    function_signature.args.clone(),
                ))
            }
        } else {
            Err(FunctionResolutionError::FunctionNotFound(
                function_signature.name.to_string(),
            ))
        }
    }

    pub fn list_functions(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.functions
            .iter()
            .map(|(function_name, _defs)| *function_name)
    }

    /// Returns a "closeness" ranking of our desire to type widen
    /// from one type to another type. None is returned where
    /// we wont type widen.
    /// 0 is the highest closeness, we use this for identity or upcasting nulls, ie
    /// int -> int.
    fn datatype_rank(from: DataType, to: DataType) -> Option<u32> {
        if from == to || from == DataType::Null || to == DataType::Null {
            return Some(0)
        }

        match (from, to) {
            // Special case for decimal, functions that accept decimal
            // accept any sized decimals.
            (DataType::Decimal(_,_), DataType::Decimal(_,_)) => Some(0),
            // Int can be cast to bigint and decimal safely
            (DataType::Integer, DataType::BigInt) => Some(1),
            (DataType::Integer, DataType::Decimal(_,_)) => Some(2),
            // Bigint can be cast to decimal safely
            (DataType::BigInt, DataType::Decimal(_,_)) => Some(1),
            _ => None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::DataType;

    #[test]
    fn test_registry_resolve() {
        let registry = Registry::new(true);

        let mut sig = FunctionSignature {
            name: "+",
            args: vec![DataType::BigInt, DataType::BigInt],
            ret: DataType::Null,
        };

        let (function_sig, _function) = registry.resolve_function(&mut sig).unwrap();

        assert_eq!(function_sig.ret, DataType::BigInt);
    }

    #[test]
    fn test_registry_unknown_function() {
        let registry = Registry::new(true);

        let mut sig = FunctionSignature {
            name: "unknown",
            args: vec![DataType::BigInt, DataType::BigInt],
            ret: DataType::Null,
        };

        let err = registry.resolve_function(&mut sig).unwrap_err();

        assert_eq!(
            err,
            FunctionResolutionError::FunctionNotFound("unknown".to_string())
        );
    }

    #[test]
    fn test_registry_resolve_null_param() {
        let registry = Registry::new(true);

        let mut sig = FunctionSignature {
            name: "+",
            args: vec![DataType::BigInt, DataType::Null],
            ret: DataType::Null,
        };

        let (function_sig, _function) = registry.resolve_function(&mut sig).unwrap();

        assert_eq!(function_sig.ret, DataType::BigInt);
    }

    #[test]
    fn test_registry_resolve_decimal() {
        let registry = Registry::new(true);

        let mut sig = FunctionSignature {
            name: "+",
            args: vec![DataType::Decimal(28, 3), DataType::Decimal(28, 7)],
            ret: DataType::Null,
        };

        let (computed_signature, _function) = registry.resolve_function(&mut sig).unwrap();

        assert_eq!(computed_signature.ret, DataType::Decimal(28, 7));
    }
}
