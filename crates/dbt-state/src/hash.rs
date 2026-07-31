use dbt_common::path::DbtPath;
use dbt_schemas::schemas::{
    DbtModel, DbtSeed, DbtTest, InternalDbtNode, InternalDbtNodeAttributes, common::Access,
    config_excluded_keys, macros::DbtMacro,
};
use md5;
use serde::Serialize;
use serde_json::to_string;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
};
use std::{io, io::BufReader};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NodeHashError {
    #[error("failed to serialize dbt State node property: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("failed to read seed file: {0}")]
    SeedIOError(#[from] io::Error),
}

#[derive(Debug)]
pub struct NodeStateHashes {
    pub node_hash: String,
    pub node_body_hash: Option<String>,
    pub node_configs_hash: Option<String>,
    pub node_persisted_descriptions_hash: Option<String>,
    pub node_macros_hash: Option<String>,
    pub node_contract_hash: Option<String>,
}

pub fn node_state_hashes<'a>(
    node: &'a dyn InternalDbtNodeAttributes,
    project_root: &DbtPath,
    macro_resolver: impl Fn(&str) -> Option<&'a DbtMacro>,
) -> Result<NodeStateHashes, NodeHashError> {
    let type_erased_node = node.as_any();

    let node_body_hash = type_erased_node.downcast_ref::<DbtSeed>().map_or_else(
        || node_body_hash(node),
        |s| seed_node_body_hash(s, project_root).map(Some),
    )?;
    let node_configs_hash = node_configs_hash(node)?;
    let node_persisted_descriptions_hash = node_persisted_docs_hash(node)?;

    let node_macros_hash = node_macros_hash(node, macro_resolver)?;
    let mut node_contract_hash = None;
    let node_fqn = Some(node.selector_string());

    let node_hash = if let Some(model_node) = type_erased_node.downcast_ref::<DbtModel>() {
        node_contract_hash = Some(model_node_contract_hash(model_node)?);
        let node_ref_representation_hash = model_node_ref_representation_hash(model_node)?;

        // ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L180
        hash_md5_list(vec![
            node_body_hash.as_ref(),
            node_configs_hash.as_ref(),
            node_persisted_descriptions_hash.as_ref(),
            node_macros_hash.as_ref(),
            node_fqn.as_ref(),
            node_contract_hash.as_ref(),
            node_ref_representation_hash.as_ref(),
        ])?
    } else if type_erased_node.downcast_ref::<DbtSeed>().is_some() {
        // seeds just return the body hash directly or an empty string if it is not populated
        // ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L203
        node_body_hash.clone().unwrap_or_else(|| "".to_owned())
    } else if let Some(data_test_node) = type_erased_node.downcast_ref::<DbtTest>()
        && data_test_node.__test_attr__.test_metadata.is_some()
    {
        // __test_attr__.test_metadata being supplied means it's a generic test as opposed to a singular test
        // to match the Python code, we only have a special case for generic tests
        // ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L193
        hash_md5_list(vec![node_configs_hash.as_ref(), node_fqn.as_ref()])?
    } else {
        // fallback node_hash impl that covers a bunch of bases on the other node types
        // ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L43
        hash_md5_list(vec![
            node_body_hash.as_ref(),
            node_configs_hash.as_ref(),
            node_persisted_descriptions_hash.as_ref(),
            node_macros_hash.as_ref(),
            node_fqn.as_ref(),
        ])?
    };

    let state_hashes = NodeStateHashes {
        node_hash,
        node_body_hash,
        node_configs_hash,
        node_persisted_descriptions_hash,
        node_macros_hash,
        node_contract_hash,
    };

    Ok(state_hashes)
}

#[derive(Debug, Serialize)]
struct PersistedDocsHashValues<'a> {
    description: Option<&'a str>,
    columns: Option<BTreeMap<&'a str, &'a str>>,
}

#[derive(Serialize)]
struct NodeRefRepresentationHashValues<'a> {
    access: Option<&'a Access>,
    deprecation_date: Option<&'a str>,
    latest_version: &'a str,
}

fn node_body_hash(node: &dyn InternalDbtNode) -> Result<Option<String>, NodeHashError> {
    //ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L58
    node.common()
        .raw_code
        .as_ref()
        .filter(|rc| !rc.is_empty())
        .map(hash_md5)
        .transpose()
}

fn node_configs_hash(node: &dyn InternalDbtNode) -> Result<Option<String>, NodeHashError> {
    //ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L65
    let keys_to_exclude = config_excluded_keys(node.resource_type());

    let filtered = node
        .base()
        .unrendered_config
        .iter()
        .filter(|(name, _)| !keys_to_exclude.contains(&name.as_str()))
        .collect::<BTreeMap<_, _>>();

    if !filtered.is_empty() {
        hash_md5(&filtered).map(Some)
    } else {
        Ok(None)
    }
}

fn node_persisted_docs_hash(node: &dyn InternalDbtNode) -> Result<Option<String>, NodeHashError> {
    // ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L76
    let base_attr = node.base();

    base_attr
        .persist_docs
        .as_ref()
        .filter(|pd| pd.columns.is_some() || pd.relation.is_some())
        .map(|d| {
            let mut hash_values = PersistedDocsHashValues {
                description: None,
                columns: None,
            };

            if let Some(persist_relation_description) = d.relation
                && persist_relation_description
            {
                let description = node.common().description.as_deref().unwrap_or("");
                hash_values.description = Some(description);
            }

            if let Some(persist_column_descriptions) = d.columns
                && persist_column_descriptions
            {
                let cols = base_attr
                    .columns
                    .iter()
                    .map(|col| {
                        let name = &*col.name;
                        let description = col.description.as_deref().unwrap_or("");
                        (name, description)
                    })
                    .collect::<BTreeMap<_, _>>();

                hash_values.columns = Some(cols);
            }

            hash_md5(&hash_values)
        })
        .transpose()
}

fn node_macros_hash<'a>(
    node: &'a dyn InternalDbtNode,
    macro_resolver: impl Fn(&str) -> Option<&'a DbtMacro>,
) -> Result<Option<String>, NodeHashError> {
    //ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L93
    if node.base().depends_on.macros.is_empty() {
        return Ok(None);
    }

    let macro_sqls = resolve_macro_tree(node, macro_resolver)
        .values()
        .map(|m| Some(&*m.macro_sql))
        .collect::<Vec<_>>();

    hash_md5_list(macro_sqls).map(Some)
}

fn resolve_macro_tree<'a>(
    node: &'a dyn InternalDbtNode,
    macro_resolver: impl Fn(&str) -> Option<&'a DbtMacro>,
) -> BTreeMap<&'a String, &'a DbtMacro> {
    // recursively resolve all the macros of the current node, plus all the
    // macros that *those* macros depend on - etc until there are no more macros
    let mut all_macros = BTreeMap::new();

    let mut queue = node
        .base()
        .depends_on
        .macros
        .iter()
        .collect::<BTreeSet<_>>();

    while let Some(next_id) = queue.pop_first() {
        if let Some(m) = macro_resolver(next_id) {
            all_macros.insert(next_id, m);

            for id in &m.depends_on.macros {
                if !all_macros.contains_key(id) {
                    queue.insert(id);
                }
            }
        }
    }

    all_macros
}

fn model_node_contract_hash(node: &DbtModel) -> Result<String, NodeHashError> {
    //ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L152
    let fallback = "enforced:false";

    let value_to_hash = node
        .__model_attr__
        .contract
        .as_ref()
        .map(|contract| {
            let checksum_string = contract.checksum.as_ref().and_then(|c| to_string(c).ok());

            if contract.enforced
                && let Some(checksum) = checksum_string
            {
                format!("enforced:true|checksum:{checksum}")
            } else {
                fallback.to_owned()
            }
        })
        .unwrap_or_else(|| fallback.to_owned());

    hash_md5(&value_to_hash)
}

fn model_node_ref_representation_hash(node: &DbtModel) -> Result<Option<String>, NodeHashError> {
    //ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L170
    node.latest_version()
        .map(|v| {
            let lv = v.to_string();
            let access = node.get_access();
            let fields = NodeRefRepresentationHashValues {
                latest_version: lv.as_str(),
                access: access.as_ref(),
                deprecation_date: node.__model_attr__.deprecation_date.as_deref(),
            };

            hash_md5(&fields)
        })
        .transpose()
}

fn seed_node_body_hash(
    node: &DbtSeed,
    project_root_path: &DbtPath,
) -> Result<String, NodeHashError> {
    // ref: https://github.com/fivetran/query-cache/blob/4ab87c72f2783c230905c5c75ec19eb76314069e/clients/dbt_state/src/dbt_state/node_hash_calculator.py#L203
    let seed_relative_path = &node.common().original_file_path;
    let seed_full_path = project_root_path.join(seed_relative_path);

    let mut reader = BufReader::new(File::open(seed_full_path)?);
    let mut ctx = md5::Context::new();
    io::copy(&mut reader, &mut ctx)?;

    Ok(format!("{:x}", ctx.compute()))
}

fn hash_md5<T>(value: &T) -> Result<String, NodeHashError>
where
    T: Serialize + ?Sized,
{
    hash_md5_list(vec![Some(value)])
}

fn hash_md5_list<T>(values: Vec<Option<&T>>) -> Result<String, NodeHashError>
where
    T: Serialize + ?Sized,
{
    // if values is empty, this will result in "" being hashed which mimics the Python implementation
    let joined = values
        .iter()
        .filter_map(|maybe_v| maybe_v.and_then(|v| Some(to_string(v))))
        .collect::<Result<Vec<_>, _>>()?
        .join("");

    let digest = md5::compute(joined);
    Ok(format!("{:x}", digest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::path::DbtPath;
    use dbt_schemas::schemas::{
        CommonAttributes, DbtSeedAttr, TestMetadata,
        common::{DbtContract, PersistDocsConfig},
        dbt_column::DbtColumn,
        macros::MacroDependsOn,
        serde::StringOrInteger,
    };
    use dbt_yaml::Value as YmlValue;
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_node(fqn: &[&str]) -> DbtTest {
        let mut t = DbtTest::default();
        t.__common_attr__.fqn = fqn.iter().map(|s| s.to_string()).collect();
        t
    }

    /// A `DbtTest` with `test_metadata` set represents a generic (schema) test,
    /// as opposed to a singular test (which hits the same fallback path as
    /// snapshots - see `test_node` above).
    fn generic_test_node(fqn: &[&str]) -> DbtTest {
        let mut t = test_node(fqn);
        t.__test_attr__.test_metadata = Some(TestMetadata {
            name: "unique".to_string(),
            kwargs: BTreeMap::default(),
            namespace: None,
        });
        t
    }

    fn model_node(fqn: &[&str]) -> DbtModel {
        let mut m = DbtModel::default();
        m.__common_attr__.fqn = fqn.iter().map(|s| s.to_string()).collect();
        m
    }

    fn resolver<'a>(
        macros: &'a BTreeMap<String, DbtMacro>,
    ) -> impl Fn(&str) -> Option<&'a DbtMacro> {
        move |id| macros.get(id)
    }

    fn dummy_project_root() -> DbtPath {
        "hash-tests".into()
    }

    mod body_hash {
        use super::*;

        #[test]
        fn node_body_hash_present_when_raw_code_set() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__common_attr__.raw_code = Some("SELECT * FROM table".to_string());
            assert!(node_body_hash(&node).unwrap().is_some());
        }

        #[test]
        fn node_body_hash_none_when_raw_code_absent() {
            let node = test_node(&["project", "models", "test_model"]);
            assert_eq!(node_body_hash(&node).unwrap(), None);
        }

        #[test]
        fn node_body_hash_none_when_raw_code_empty() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__common_attr__.raw_code = Some(String::new());
            assert_eq!(node_body_hash(&node).unwrap(), None);
        }

        #[test]
        fn node_body_hash_differs_for_different_raw_code() {
            let mut a = test_node(&["project", "models", "test_model"]);
            a.__common_attr__.raw_code = Some("SELECT * FROM table1".to_string());
            let mut b = test_node(&["project", "models", "test_model"]);
            b.__common_attr__.raw_code = Some("SELECT * FROM table2".to_string());
            assert_ne!(node_body_hash(&a).unwrap(), node_body_hash(&b).unwrap());
        }

        #[test]
        fn seed_node_body_hash_matches_md5_hex() {
            let tmpdir = TempDir::new().unwrap();
            let project_root: DbtPath = tmpdir.path().into();
            let seed_file_path = project_root.join("seed_data.csv");

            let seed_bytes = b"id,name\n1,Ada\n2,Grace\n";
            fs::write(&seed_file_path, seed_bytes).unwrap();

            let expected_hash = "e146991e1c07585745c5a65f06a517e9";

            let common_attr = CommonAttributes {
                original_file_path: seed_file_path.get_relative_path(&project_root).unwrap(),
                ..Default::default()
            };

            let seed = DbtSeed {
                __common_attr__: common_attr,
                ..Default::default()
            };

            assert_eq!(
                seed_node_body_hash(&seed, &project_root).unwrap(),
                expected_hash
            );
        }
    }

    mod configs_hash {
        use super::*;

        #[test]
        fn node_configs_hash_present_when_unrendered_config_set() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__base_attr__.unrendered_config = BTreeMap::from([
                (
                    "materialized".to_string(),
                    YmlValue::string("table".to_string()),
                ),
                ("enabled".to_string(), YmlValue::bool(true)),
            ]);
            assert!(node_configs_hash(&node).unwrap().is_some());
        }

        #[test]
        fn node_configs_hash_excludes_alias_schema_database_tags_group_for_models() {
            let mut with_excluded = model_node(&["project", "models", "test_model"]);
            with_excluded.__base_attr__.unrendered_config = BTreeMap::from([
                (
                    "materialized".to_string(),
                    YmlValue::string("table".to_string()),
                ),
                (
                    "alias".to_string(),
                    YmlValue::string("my_alias".to_string()),
                ),
                (
                    "schema".to_string(),
                    YmlValue::string("my_schema".to_string()),
                ),
                (
                    "database".to_string(),
                    YmlValue::string("my_db".to_string()),
                ),
                (
                    "tags".to_string(),
                    dbt_yaml::from_str(r#"["tag1"]"#).unwrap(),
                ),
                (
                    "group".to_string(),
                    YmlValue::string("my_group".to_string()),
                ),
                ("enabled".to_string(), YmlValue::bool(true)),
            ]);

            let mut without_excluded = model_node(&["project", "models", "test_model"]);
            without_excluded.__base_attr__.unrendered_config = BTreeMap::from([
                (
                    "materialized".to_string(),
                    YmlValue::string("table".to_string()),
                ),
                ("enabled".to_string(), YmlValue::bool(true)),
            ]);

            assert_eq!(
                node_configs_hash(&with_excluded).unwrap(),
                node_configs_hash(&without_excluded).unwrap()
            );
        }

        #[test]
        fn node_configs_hash_excludes_specific_keys_for_generic_tests_too() {
            let mut with_excluded = generic_test_node(&["project", "models", "test1"]);
            with_excluded.__base_attr__.unrendered_config = BTreeMap::from([
                (
                    "severity".to_string(),
                    YmlValue::string("error".to_string()),
                ),
                (
                    "alias".to_string(),
                    YmlValue::string("my_alias".to_string()),
                ),
            ]);

            let mut without_excluded = generic_test_node(&["project", "models", "test1"]);
            without_excluded.__base_attr__.unrendered_config = BTreeMap::from([(
                "severity".to_string(),
                YmlValue::string("error".to_string()),
            )]);

            assert_eq!(
                node_configs_hash(&with_excluded).unwrap(),
                node_configs_hash(&without_excluded).unwrap()
            );
        }

        #[test]
        fn node_configs_hash_none_when_unrendered_config_empty() {
            let node = test_node(&["project", "models", "test_model"]);
            assert_eq!(node_configs_hash(&node).unwrap(), None);
        }

        #[test]
        fn node_configs_hash_none_when_all_keys_excluded() {
            let mut node = model_node(&["project", "models", "test_model"]);
            node.__base_attr__.unrendered_config = BTreeMap::from([
                (
                    "alias".to_string(),
                    YmlValue::string("my_alias".to_string()),
                ),
                (
                    "schema".to_string(),
                    YmlValue::string("my_schema".to_string()),
                ),
                (
                    "database".to_string(),
                    YmlValue::string("my_db".to_string()),
                ),
                (
                    "tags".to_string(),
                    dbt_yaml::from_str(r#"["tag1"]"#).unwrap(),
                ),
            ]);
            assert_eq!(node_configs_hash(&node).unwrap(), None);
        }
    }

    mod persisted_docs_hash {
        use super::*;

        #[test]
        fn node_persisted_docs_hash_none_when_no_persist_docs() {
            let node = test_node(&["project", "models", "test_model"]);
            assert_eq!(node_persisted_docs_hash(&node).unwrap(), None);
        }

        #[test]
        fn node_persisted_docs_hash_none_when_persist_docs_empty() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__base_attr__.persist_docs = Some(PersistDocsConfig::default());
            assert_eq!(node_persisted_docs_hash(&node).unwrap(), None);
        }

        #[test]
        fn node_persisted_docs_hash_relation_persisted_varies_with_description() {
            let mut with_desc = test_node(&["project", "models", "test_model"]);
            with_desc.__base_attr__.persist_docs = Some(PersistDocsConfig {
                relation: Some(true),
                columns: None,
            });
            with_desc.__common_attr__.description = Some("My model description".to_string());

            let mut without_desc = test_node(&["project", "models", "test_model"]);
            without_desc.__base_attr__.persist_docs = Some(PersistDocsConfig {
                relation: Some(true),
                columns: None,
            });

            let with_hash = node_persisted_docs_hash(&with_desc).unwrap();
            let without_hash = node_persisted_docs_hash(&without_desc).unwrap();
            assert!(with_hash.is_some());
            assert!(without_hash.is_some());
            assert_ne!(with_hash, without_hash);
        }

        #[test]
        fn node_persisted_docs_hash_columns_persisted_varies_with_column_descriptions() {
            let col = |name: &str, desc: Option<&str>| {
                Arc::new(DbtColumn {
                    name: name.to_string(),
                    description: desc.map(str::to_string),
                    ..Default::default()
                })
            };

            let mut with_desc = test_node(&["project", "models", "test_model"]);
            with_desc.__base_attr__.persist_docs = Some(PersistDocsConfig {
                relation: None,
                columns: Some(true),
            });
            with_desc.__base_attr__.columns = vec![col("col1", Some("Column 1 description"))];

            let mut none_desc = test_node(&["project", "models", "test_model"]);
            none_desc.__base_attr__.persist_docs = Some(PersistDocsConfig {
                relation: None,
                columns: Some(true),
            });
            none_desc.__base_attr__.columns = vec![col("col1", None)];

            assert_ne!(
                node_persisted_docs_hash(&with_desc).unwrap(),
                node_persisted_docs_hash(&none_desc).unwrap()
            );
        }

        #[test]
        fn node_persisted_docs_hash_combines_relation_and_columns() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__base_attr__.persist_docs = Some(PersistDocsConfig {
                relation: Some(true),
                columns: Some(true),
            });
            node.__common_attr__.description = Some("Model description".to_string());
            node.__base_attr__.columns = vec![Arc::new(DbtColumn {
                name: "col1".to_string(),
                description: Some("Column 1".to_string()),
                ..Default::default()
            })];

            assert!(node_persisted_docs_hash(&node).unwrap().is_some());
        }

        #[test]
        fn node_persisted_docs_hash_some_when_columns_persisted_but_no_columns_present() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__base_attr__.persist_docs = Some(PersistDocsConfig {
                relation: None,
                columns: Some(true),
            });
            assert!(node_persisted_docs_hash(&node).unwrap().is_some());
        }
    }

    mod macros_hash {
        use super::*;

        #[test]
        fn node_macros_hash_none_when_no_macros_referenced() {
            let node = test_node(&["project", "models", "test_model"]);
            let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
            assert_eq!(
                node_macros_hash(&node, resolver(&macros_map)).unwrap(),
                None
            );
        }

        #[test]
        fn node_macros_hash_present_for_direct_macro() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__base_attr__.depends_on.macros = vec!["macro.project.test1".to_string()];

            let macros_map = BTreeMap::from([(
                "macro.project.test1".to_string(),
                DbtMacro {
                    macro_sql: "{% macro test1() %} SELECT 1 {% endmacro %}".to_string(),
                    ..Default::default()
                },
            )]);

            assert!(
                node_macros_hash(&node, resolver(&macros_map))
                    .unwrap()
                    .is_some()
            );
        }

        #[test]
        fn node_macros_hash_combines_multiple_direct_macros() {
            let macros_map = BTreeMap::from([
                (
                    "macro1".to_string(),
                    DbtMacro {
                        macro_sql: "SQL1".to_string(),
                        ..Default::default()
                    },
                ),
                (
                    "macro2".to_string(),
                    DbtMacro {
                        macro_sql: "SQL2".to_string(),
                        ..Default::default()
                    },
                ),
            ]);

            let mut with_two = test_node(&["project", "models", "test_model"]);
            with_two.__base_attr__.depends_on.macros =
                vec!["macro1".to_string(), "macro2".to_string()];
            let two_hash = node_macros_hash(&with_two, resolver(&macros_map)).unwrap();

            let mut with_one = test_node(&["project", "models", "test_model"]);
            with_one.__base_attr__.depends_on.macros = vec!["macro1".to_string()];
            let one_hash = node_macros_hash(&with_one, resolver(&macros_map)).unwrap();

            assert_ne!(two_hash, one_hash);
        }

        #[test]
        fn node_macros_hash_does_not_include_upstream_node_macros() {
            // node_macros_hash only ever looks at this node's own depends_on.macros, never
            // depends_on.nodes, so an upstream node's macros can never leak in.
            let mut node = test_node(&["project", "models", "test_model"]);
            let mut upstream_node = test_node(&["project", "models", "upstream"]);
            upstream_node
                .base_mut()
                .depends_on
                .macros
                .push("macro_from_upstream".to_owned());

            node.base_mut()
                .depends_on
                .nodes
                .push(upstream_node.unique_id());

            assert!(!node.base().depends_on.nodes.is_empty());
            assert!(node.base().depends_on.macros.is_empty());

            let macros_map = BTreeMap::from([(
                "macro_from_upstream".to_string(),
                DbtMacro {
                    macro_sql: "SQL_FROM_UPSTREAM".to_string(),
                    ..Default::default()
                },
            )]);
            assert_eq!(
                node_macros_hash(&node, resolver(&macros_map)).unwrap(),
                None
            );
        }

        #[test]
        fn node_macros_hash_includes_transitive_macro_dependencies() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__base_attr__.depends_on.macros = vec!["macro1".to_string()];

            let macros_with_dep = BTreeMap::from([
                (
                    "macro1".to_string(),
                    DbtMacro {
                        macro_sql: "SQL1".to_string(),
                        depends_on: MacroDependsOn {
                            macros: vec!["macro2".to_string()],
                        },
                        ..Default::default()
                    },
                ),
                (
                    "macro2".to_string(),
                    DbtMacro {
                        macro_sql: "SQL2".to_string(),
                        ..Default::default()
                    },
                ),
            ]);
            let with_dep_hash = node_macros_hash(&node, resolver(&macros_with_dep)).unwrap();

            let macros_without_dep = BTreeMap::from([(
                "macro1".to_string(),
                DbtMacro {
                    macro_sql: "SQL1".to_string(),
                    ..Default::default()
                },
            )]);
            let without_dep_hash = node_macros_hash(&node, resolver(&macros_without_dep)).unwrap();

            assert_ne!(with_dep_hash, without_dep_hash);
        }

        #[test]
        fn node_macros_hash_some_when_macro_reference_is_unresolvable() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__base_attr__.depends_on.macros = vec!["missing_macro".to_string()];
            let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
            assert!(
                node_macros_hash(&node, resolver(&macros_map))
                    .unwrap()
                    .is_some()
            );
        }

        #[test]
        fn node_macros_hash_is_deterministic_regardless_of_insertion_order() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__base_attr__.depends_on.macros = (0..10).map(|i| format!("macro_{i}")).collect();

            let macros_map: BTreeMap<String, DbtMacro> = (0..10)
                .map(|i| {
                    (
                        format!("macro_{i}"),
                        DbtMacro {
                            macro_sql: format!("SQL_{i}"),
                            ..Default::default()
                        },
                    )
                })
                .collect();

            let first = node_macros_hash(&node, resolver(&macros_map)).unwrap();
            for _ in 0..9 {
                let repeat = node_macros_hash(&node, resolver(&macros_map)).unwrap();
                assert_eq!(first, repeat);
            }
        }
    }

    mod contract_hash {
        use super::*;

        #[test]
        fn model_node_contract_hash_enforced_with_checksum_differs_from_not_enforced() {
            let mut enforced = model_node(&["project", "models", "test_model"]);
            enforced.__model_attr__.contract = Some(DbtContract {
                enforced: true,
                checksum: Some(YmlValue::string("abc123".to_string())),
                ..Default::default()
            });

            let mut not_enforced = model_node(&["project", "models", "test_model"]);
            not_enforced.__model_attr__.contract = Some(DbtContract {
                enforced: false,
                ..Default::default()
            });

            assert_ne!(
                model_node_contract_hash(&enforced).unwrap(),
                model_node_contract_hash(&not_enforced).unwrap()
            );
        }

        #[test]
        fn model_node_contract_hash_enforced_without_checksum_matches_not_enforced() {
            let mut enforced_no_checksum = model_node(&["project", "models", "test_model"]);
            enforced_no_checksum.__model_attr__.contract = Some(DbtContract {
                enforced: true,
                checksum: None,
                ..Default::default()
            });

            let mut not_enforced_with_checksum = model_node(&["project", "models", "test_model"]);
            not_enforced_with_checksum.__model_attr__.contract = Some(DbtContract {
                enforced: false,
                checksum: Some(YmlValue::string("abc123".to_string())),
                ..Default::default()
            });

            // Both collapse to the "enforced:false" state per query-cache's node_contract_hash.
            assert_eq!(
                model_node_contract_hash(&enforced_no_checksum).unwrap(),
                model_node_contract_hash(&not_enforced_with_checksum).unwrap()
            );
        }

        #[test]
        fn model_node_contract_hash_no_contract_matches_not_enforced() {
            let no_contract = model_node(&["project", "models", "test_model"]);

            let mut not_enforced = model_node(&["project", "models", "test_model"]);
            not_enforced.__model_attr__.contract = Some(DbtContract {
                enforced: false,
                ..Default::default()
            });

            assert_eq!(
                model_node_contract_hash(&no_contract).unwrap(),
                model_node_contract_hash(&not_enforced).unwrap()
            );
        }
    }

    mod node_ref_representation_hash {
        use super::*;

        #[test]
        fn model_ref_repr_hash_present_with_model_attributes() {
            let mut node = model_node(&["project", "models", "test_model"]);
            node.__model_attr__.latest_version = Some(StringOrInteger::Integer(2));
            node.__model_attr__.access = Access::Public;
            node.__model_attr__.deprecation_date = Some("2025-12-31".to_string());

            assert!(model_node_ref_representation_hash(&node).unwrap().is_some());
        }

        #[test]
        fn model_ref_repr_hash_differs_across_versions() {
            let mut v1 = model_node(&["project", "models", "test_model"]);
            v1.__model_attr__.latest_version = Some(StringOrInteger::Integer(1));
            v1.__model_attr__.access = Access::Public;

            let mut v2 = model_node(&["project", "models", "test_model"]);
            v2.__model_attr__.latest_version = Some(StringOrInteger::Integer(2));
            v2.__model_attr__.access = Access::Public;

            assert_ne!(
                model_node_ref_representation_hash(&v1).unwrap(),
                model_node_ref_representation_hash(&v2).unwrap()
            );
        }

        #[test]
        fn node_state_hashes_changes_when_a_component_changes() {
            let mut node = test_node(&["project", "models", "test_model"]);
            node.__common_attr__.raw_code = Some("SELECT 1".to_string());
            node.__base_attr__.unrendered_config = BTreeMap::from([(
                "materialized".to_string(),
                YmlValue::string("table".to_string()),
            )]);
            node.__base_attr__.depends_on.macros = vec!["macro1".to_string()];

            let macros_map = BTreeMap::from([(
                "macro1".to_string(),
                DbtMacro {
                    macro_sql: "MACRO_SQL".to_string(),
                    ..Default::default()
                },
            )]);

            let base_hash = node_state_hashes(&node, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash;

            node.__common_attr__.raw_code = Some("SELECT 2".to_string());
            let changed_hash =
                node_state_hashes(&node, &dummy_project_root(), resolver(&macros_map))
                    .unwrap()
                    .node_hash;

            assert_ne!(base_hash, changed_hash);
        }
    }

    #[test]
    fn test_node_state_hashes_always_returns_a_valid_digest() {
        let node = test_node(&["project", "models", "test_model"]);
        let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
        let node_hash = node_state_hashes(&node, &dummy_project_root(), resolver(&macros_map))
            .unwrap()
            .node_hash;
        assert_eq!(node_hash.len(), 32);
    }

    #[test]
    fn test_node_state_hashes_different_fqns_produce_different_hashes() {
        let node_a = test_node(&["project", "models", "model1"]);
        let node_b = test_node(&["project", "models", "model2"]);
        let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
        assert_ne!(
            node_state_hashes(&node_a, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash,
            node_state_hashes(&node_b, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash
        );
    }

    #[test]
    fn model_node_state_hashes_includes_contract_and_ref_representation() {
        let mut base = model_node(&["project", "models", "test_model"]);
        base.__common_attr__.raw_code = Some("SELECT 1".to_string());
        base.__model_attr__.latest_version = Some(StringOrInteger::Integer(2));
        base.__model_attr__.access = Access::Public;

        let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
        let base_hash = node_state_hashes(&base, &dummy_project_root(), resolver(&macros_map))
            .unwrap()
            .node_hash;

        let mut different_contract = base.clone();
        different_contract.__model_attr__.contract = Some(DbtContract {
            enforced: true,
            checksum: Some(YmlValue::string("abc123".to_string())),
            ..Default::default()
        });
        let different_contract_hash = node_state_hashes(
            &different_contract,
            &dummy_project_root(),
            resolver(&macros_map),
        )
        .unwrap()
        .node_hash;
        assert_ne!(base_hash, different_contract_hash);

        base.__model_attr__.latest_version = Some(StringOrInteger::Integer(3));
        let different_version_hash =
            node_state_hashes(&base, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash;
        assert_ne!(base_hash, different_version_hash);
    }

    #[test]
    fn model_node_state_hashes_always_returns_a_valid_digest() {
        let node = model_node(&["project", "models", "test_model"]);
        let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
        let node_hash = node_state_hashes(&node, &dummy_project_root(), resolver(&macros_map))
            .unwrap()
            .node_hash;
        assert_eq!(node_hash.len(), 32);
    }

    #[test]
    fn model_node_state_hashes_different_fqns_produce_different_hashes() {
        let node_a = model_node(&["project", "models", "model1"]);
        let node_b = model_node(&["project", "models", "model2"]);
        let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
        assert_ne!(
            node_state_hashes(&node_a, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash,
            node_state_hashes(&node_b, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash
        );
    }

    #[test]
    fn generic_test_node_state_hashes_node_hash_ignores_raw_code_and_macros() {
        let macros_map = BTreeMap::from([(
            "macro1".to_string(),
            DbtMacro {
                macro_sql: "MACRO_SQL".to_string(),
                ..Default::default()
            },
        )]);

        let mut a = generic_test_node(&["project", "models", "test_model"]);
        a.__common_attr__.raw_code = Some("SELECT 1".to_string());
        a.__base_attr__.unrendered_config = BTreeMap::from([(
            "severity".to_string(),
            YmlValue::string("error".to_string()),
        )]);
        a.__base_attr__.depends_on.macros = vec!["macro1".to_string()];

        let mut b = generic_test_node(&["project", "models", "test_model"]);
        b.__common_attr__.raw_code = Some("SELECT 2".to_string());
        b.__base_attr__.unrendered_config = BTreeMap::from([(
            "severity".to_string(),
            YmlValue::string("error".to_string()),
        )]);
        b.__base_attr__.depends_on.macros = vec![];

        assert_eq!(
            node_state_hashes(&a, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash,
            node_state_hashes(&b, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash
        );
    }

    #[test]
    fn generic_test_node_state_hashes_node_hash_always_returns_a_valid_digest() {
        let node = generic_test_node(&["project", "models", "test_model"]);
        let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
        let node_hash = node_state_hashes(&node, &dummy_project_root(), resolver(&macros_map))
            .unwrap()
            .node_hash;
        assert_eq!(node_hash.len(), 32);
    }

    #[test]
    fn generic_test_node_state_hashes_different_configs_produce_different_hashes() {
        let mut a = generic_test_node(&["project", "models", "test1"]);
        a.__base_attr__.unrendered_config = BTreeMap::from([(
            "severity".to_string(),
            YmlValue::string("error".to_string()),
        )]);

        let mut b = generic_test_node(&["project", "models", "test1"]);
        b.__base_attr__.unrendered_config =
            BTreeMap::from([("severity".to_string(), YmlValue::string("warn".to_string()))]);

        let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
        assert_ne!(
            node_state_hashes(&a, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash,
            node_state_hashes(&b, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash
        );
    }

    #[test]
    fn generic_test_node_state_hashes_different_fqns_produce_different_hashes() {
        let a = generic_test_node(&["project", "models", "test1"]);
        let b = generic_test_node(&["project", "models", "test2"]);
        let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
        assert_ne!(
            node_state_hashes(&a, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash,
            node_state_hashes(&b, &dummy_project_root(), resolver(&macros_map))
                .unwrap()
                .node_hash
        );
    }

    #[test]
    fn seed_node_state_hashes_node_hash_equals_body_hash() {
        let tmp = TempDir::new().unwrap();
        let project_root: DbtPath = tmp.path().into();
        let seed_file_path = project_root.join("test_seed.csv");
        fs::write(&seed_file_path, b"id,name\n1,Alice\n2,Bob\n").unwrap();

        let seed = DbtSeed {
            __seed_attr__: DbtSeedAttr {
                root_path: Some(project_root.to_path_buf()),
                ..Default::default()
            },
            __common_attr__: CommonAttributes {
                original_file_path: seed_file_path.get_relative_path(&project_root).unwrap(),
                fqn: vec![
                    "project".to_string(),
                    "seeds".to_string(),
                    "test_seed".to_string(),
                ],
                ..Default::default()
            },
            ..Default::default()
        };

        let macros_map: BTreeMap<String, DbtMacro> = BTreeMap::new();
        let hashes = node_state_hashes(&seed, &project_root, resolver(&macros_map)).unwrap();

        assert_eq!(hashes.node_hash, hashes.node_body_hash.unwrap());
    }
}
