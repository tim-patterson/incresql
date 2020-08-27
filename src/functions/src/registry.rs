use crate::{register_builtins, CompoundFunction, FunctionDefinition, FunctionSignature};
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
    ) -> Result<CompoundFunction, FunctionResolutionError> {
        if let Some(candidates) = self.functions.get(function_signature.name) {
            // Filter candidates
            let candidate_list: Vec<_> = candidates
                .iter()
                .filter(|candidate| {
                    if candidate.signature.args.len() == function_signature.args.len() {
                        candidate
                            .signature
                            .args
                            .iter()
                            .zip(function_signature.args.iter())
                            .all(|(d1, d2)| {
                                if let (DataType::Decimal(..), DataType::Decimal(..)) = (d1, d2) {
                                    true
                                } else if *d1 == DataType::Null || *d2 == DataType::Null {
                                    // Null types are really more like wildcards
                                    true
                                } else {
                                    d1 == d2
                                }
                            })
                    } else {
                        false
                    }
                })
                .collect();

            if let Some(candidate) = candidate_list.first() {
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
                let mut resolved_function = candidate.function.clone();
                resolved_function.signature = return_signature;

                Ok(resolved_function)
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

        let function = registry.resolve_function(&mut sig).unwrap();

        assert_eq!(function.signature.ret, DataType::BigInt);
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

        let function = registry.resolve_function(&mut sig).unwrap();

        assert_eq!(function.signature.ret, DataType::BigInt);
    }

    #[test]
    fn test_registry_resolve_decimal() {
        let registry = Registry::new(true);

        let mut sig = FunctionSignature {
            name: "+",
            args: vec![DataType::Decimal(28, 3), DataType::Decimal(28, 7)],
            ret: DataType::Null,
        };

        let function = registry.resolve_function(&mut sig).unwrap();

        assert_eq!(function.signature.ret, DataType::Decimal(28, 7));
    }
}
