use crate::schemas::serde::bool_or_string_bool;
use dbt_proc_macros::{DefaultTo, Resolvable};
use dbt_yaml::{DbtSchema, ShouldBe};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, btree_map::Iter};

// Type aliases for clarity
type YmlValue = dbt_yaml::Value;

use crate::schemas::project::configs::common::default_tags;
use crate::schemas::{
    project::{ResolvableConfig, TypedRecursiveConfig},
    serde::StringOrArrayOfStrings,
};

// NOTE: No #[skip_serializing_none] - we handle None serialization in serialize_with_mode
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct ProjectSemanticModelConfig {
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<IndexMap<String, YmlValue>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,

    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectSemanticModelConfig>>,
}

impl TypedRecursiveConfig for ProjectSemanticModelConfig {
    fn type_name() -> &'static str {
        "semantic_model"
    }

    fn iter_children(&self) -> Iter<'_, String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

// NOTE: No #[skip_serializing_none] - we handle None serialization in serialize_with_mode
#[derive(
    Resolvable, DefaultTo, Deserialize, Serialize, Debug, Clone, Default, PartialEq, Eq, DbtSchema,
)]
pub struct SemanticModelConfig {
    #[resolved(promote, method = get_enabled_with_default)]
    pub enabled: Option<bool>,
    pub group: Option<String>,
    #[serde(serialize_with = "crate::schemas::serde::serialize_option_as_empty_map")]
    pub meta: Option<IndexMap<String, YmlValue>>,
    #[default_to(skip)]
    #[serde(
        default,
        serialize_with = "crate::schemas::nodes::serialize_none_as_empty_list"
    )]
    pub tags: Option<StringOrArrayOfStrings>,
}

impl From<ProjectSemanticModelConfig> for SemanticModelConfig {
    fn from(config: ProjectSemanticModelConfig) -> Self {
        Self {
            enabled: config.enabled,
            group: config.group,
            meta: config.meta,
            tags: config.tags,
        }
    }
}

impl From<SemanticModelConfig> for ProjectSemanticModelConfig {
    fn from(config: SemanticModelConfig) -> Self {
        Self {
            enabled: config.enabled,
            group: config.group,
            meta: config.meta,
            tags: config.tags,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl ResolvableConfig<SemanticModelConfig> for SemanticModelConfig {
    type Resolved = ResolvedSemanticModelConfig;
    type PackageDefaults = ();
    type ResolveDefaults = ();

    fn get_enabled_with_default(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    fn disable(&mut self) {
        self.enabled = Some(false);
    }

    fn apply_package_defaults(&mut self, _: ()) {}

    fn finalize(self) -> ResolvedSemanticModelConfig {
        self.finalize_resolved()
    }

    fn default_to(&mut self, parent: &SemanticModelConfig) {
        default_tags(&mut self.tags, &parent.tags);
        self.default_to_fields(parent);
    }
}
