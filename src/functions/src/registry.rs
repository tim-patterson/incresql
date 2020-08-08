use crate::{register_builtins, Function, FunctionDefinition, FunctionSignature};
use data::DataType;
use std::collections::HashMap;

/// A repository for functions. Used by the planner to resolve the correct functions
#[derive(Debug)]
pub struct Registry {
    functions: HashMap<&'static str, Vec<FunctionDefinition>>,
}

impl Default for Registry {
    fn default() -> Self {
        Registry::new(true)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum FunctionResolutionError {
    FunctionNotFound,
    MatchingSignatureNotFound,
}

impl Registry {
    pub fn new(with_builtins: bool) -> Self {
        let mut registry = Registry {
            functions: HashMap::new(),
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

    pub fn resolve_scalar_function(
        &mut self,
        function_signature: &mut FunctionSignature,
    ) -> Result<&'static dyn Function, FunctionResolutionError> {
        if let Some(candidates) = self.functions.get(function_signature.name) {
            // Filter candidates
            let mut candidate_list: Vec<_> = candidates
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
                                } else {
                                    d1 == d2
                                }
                            })
                    } else {
                        false
                    }
                })
                .collect();

            if let Some(candidate) = candidate_list.pop() {
                // Populate return type
                function_signature.ret =
                    if let Some(type_resolver) = candidate.custom_return_type_resolver {
                        type_resolver(&function_signature.args)
                    } else {
                        candidate.signature.ret
                    };

                Ok(candidate.function)
            } else {
                Err(FunctionResolutionError::MatchingSignatureNotFound)
            }
        } else {
            Err(FunctionResolutionError::FunctionNotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::DataType;

    #[test]
    fn test_registry_resolve() {
        let mut registry = Registry::new(true);

        let mut sig = FunctionSignature {
            name: "+",
            args: vec![DataType::BigInt, DataType::BigInt],
            ret: DataType::Null,
        };

        let function = registry.resolve_scalar_function(&mut sig);

        assert_eq!(sig.ret, DataType::BigInt);

        function.unwrap();
    }

    #[test]
    fn test_registry_resolve_decimal() {
        let mut registry = Registry::new(true);

        let mut sig = FunctionSignature {
            name: "+",
            args: vec![DataType::Decimal(28, 3), DataType::Decimal(28, 7)],
            ret: DataType::Null,
        };

        let function = registry.resolve_scalar_function(&mut sig);

        assert_eq!(sig.ret, DataType::Decimal(28, 7));

        function.unwrap();
    }
}
