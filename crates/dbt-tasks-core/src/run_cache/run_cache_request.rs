//! Translates `dbt-tasks` runtime state into dbt State service requests.
//!
//! This module owns extraction from dbt schema/task types, adapter-specific
//! relation rendering, seed file hashing, and task-layer context assembly before
//! lowering into the stable request-builder inputs in `dbt-state`.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use chrono::{DateTime, Utc};
use dbt_adapter::relation::create_relation_from_node;
use dbt_adapter_core::AdapterType;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_schemas::schemas::common::{DbtIncrementalStrategy, DbtMaterialization, OnSchemaChange};
use dbt_schemas::schemas::project::{
    DataTestConfig, ModelConfig, SeedConfig, SnapshotConfig, WarehouseSpecificNodeConfig,
};
use dbt_schemas::schemas::{
    DbtModel, DbtSeed, DbtSnapshot, DbtTest, InternalDbtNode, InternalDbtNodeAttributes,
};
use dbt_state::proto::query_cache::{
    QueryDependency, StaleUpstreamPolicy, SubmitEnrichedSqlRequest, SubmitValuesRequest,
    TableModifiedInfo, TableProperties,
};
use dbt_state::request_builder::{
    ExecutionTypeInput, NodeIdentity, RequestBuildError, SemanticExtraConfig, SemanticExtras,
    SubmitEnrichedSqlRequestInput, SubmitValuesRequestInput, execution_type_from_input,
    seed_semantic_extras, seed_values_hash_reader, sql_semantic_extras,
};
use dbt_telemetry::NodeType;
use serde::Serialize;
use serde_json::Value;

/// Semantic-extras keys mirroring the dbt-core plugin (run_cache.py). They fold a
/// microbatch model run's resolved event-time window into the model-level cache
/// key so an unchanged window no-ops and a different window executes. The values
/// are stored as raw ISO-8601 strings (not JSON-encoded, unlike the config-derived
/// extras) so both engines produce the same semantic hash for the same window.
const MICROBATCH_EVENT_TIME_START_KEY: &str = "__microbatch_event_time_start";
const MICROBATCH_EVENT_TIME_END_KEY: &str = "__microbatch_event_time_end";

#[derive(Clone, Debug)]
/// Execution-time SQL request inputs assembled by `dbt-tasks`.
///
/// This stays separate from the `dbt-state` request-builder input so the
/// `dbt-state` crate only sees normalized values and does not depend on task/schema
/// runtime types.
pub struct SqlRunCacheRequestContext {
    pub adapter_type: AdapterType,
    pub dialect: String,
    pub sql: String,
    pub tables: Vec<TableModifiedInfo>,
    pub query_dependencies: Vec<QueryDependency>,
    pub freshness_tolerance_seconds: i64,
    pub lenient_dependencies: Vec<String>,
    pub tolerate_nondeterminism: bool,
    pub full_refresh: bool,
    pub clone_time_travel_limit: Option<i64>,
    pub clone_table_properties: Option<TableProperties>,
    pub clone_chain_depth_limit: Option<i64>,
    pub default_schema: String,
    /// How the service should aggregate per-dependency freshness checks for
    /// this request. Derived from the model's
    /// `freshness.build_after.updates_on` config (defaults to ANY).
    pub stale_upstream_policy: StaleUpstreamPolicy,
    /// Resolved `(start, end)` event-time window for a microbatch model's whole
    /// run. Folded into the model-level cache key so an unchanged window no-ops
    /// and a different window executes. `None` for non-microbatch nodes.
    pub microbatch_window: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

#[derive(Clone, Debug)]
/// Execution-time seed request inputs assembled by `dbt-tasks`.
///
/// This context may include task-layer details, such as the project root, before
/// being lowered into the stable `dbt-state` request-builder input.
pub struct SeedRunCacheRequestContext<'a> {
    pub adapter_type: AdapterType,
    pub dialect: String, // TODO: remove because redundant when adapter_type is present in context
    pub project_root: &'a Path,
    pub last_modified_epoch: Option<i64>,
    pub clone_time_travel_limit: Option<i64>,
    pub clone_table_properties: Option<TableProperties>,
}

pub fn build_model_sql_request(
    model: &DbtModel,
    context: SqlRunCacheRequestContext,
) -> FsResult<SubmitEnrichedSqlRequest> {
    let execution_type =
        execution_type_from_input(&model_execution_type_input(model, context.full_refresh))
            .map_err(request_build_error)?;
    let mut semantic_extras =
        sql_semantic_extras(&model_sql_semantic_extra_config(model).map_err(request_build_error)?)
            .map_err(request_build_error)?;

    // Fold the resolved microbatch event-time window into the model-level cache
    // key. Mirrors the dbt-core plugin: the raw ISO-8601 bounds are stored under
    // the same keys so both engines hash the same window identically.
    if let Some((start, end)) = context.microbatch_window {
        semantic_extras.insert(
            MICROBATCH_EVENT_TIME_START_KEY.to_string(),
            microbatch_window_isoformat(start),
        );
        semantic_extras.insert(
            MICROBATCH_EVENT_TIME_END_KEY.to_string(),
            microbatch_window_isoformat(end),
        );
    }

    Ok(build_sql_request_input(model, context, execution_type, semantic_extras)?.into_proto())
}

/// Whether a model executes as microbatch. Kept identical to the SA-side
/// `try_get_microbatch_model`, which is the authoritative gate governing actual
/// batch execution and window resolution: an incremental materialization whose
/// resolved `incremental_strategy` is `microbatch`. Matching it exactly ensures
/// the run-cache guard treats a node as microbatch iff it will actually run as
/// one, so the window requirement and the batch execution never disagree.
pub fn is_microbatch_model(model: &DbtModel) -> bool {
    model.base().materialized == DbtMaterialization::Incremental
        && model.__model_attr__.incremental_strategy == Some(DbtIncrementalStrategy::Microbatch)
}

/// Format a microbatch window bound to match Python's `datetime.isoformat()` for
/// the timezone-aware UTC datetimes dbt uses (e.g. `2020-01-01T00:00:00+00:00`).
/// Microbatch windows are always aligned to batch boundaries, so there is never a
/// sub-second component to reconcile between the two formatters.
fn microbatch_window_isoformat(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
}

pub fn build_snapshot_sql_request(
    snapshot: &DbtSnapshot,
    context: SqlRunCacheRequestContext,
) -> FsResult<SubmitEnrichedSqlRequest> {
    let execution_type = execution_type_from_input(&snapshot_execution_type_input(
        snapshot,
        context.full_refresh,
    ))
    .map_err(request_build_error)?;

    let semantic_extras = sql_semantic_extras(
        &snapshot_config_sql_semantic_extra_config(&snapshot.deprecated_config)
            .map_err(request_build_error)?,
    )
    .map_err(request_build_error)?;

    Ok(build_sql_request_input(snapshot, context, execution_type, semantic_extras)?.into_proto())
}

pub fn build_test_sql_request(
    test: &DbtTest,
    context: SqlRunCacheRequestContext,
) -> FsResult<SubmitEnrichedSqlRequest> {
    let execution_type =
        execution_type_from_input(&test_execution_type_input()).map_err(request_build_error)?;

    let semantic_extras = sql_semantic_extras(
        &test_config_sql_semantic_extra_config(&test.deprecated_config)
            .map_err(request_build_error)?,
    )
    .map_err(request_build_error)?;

    // Mirrors dbt-core's `_build_submit_enriched_sql_request` (run_cache.py
    // L992-996): data tests submit with `target_table=None` so the service's
    // `_no_target_table_expr` branch fires and Skip can be returned without
    // requiring the audit relation to exist in the warehouse. With a
    // populated target_table here, the service would require it to be in
    // the request's `tables` list with a `last_modified_epoch` to mark the
    // target as "existing" — which is not possible when
    // `store_failures_as=None` skips the audit relation materialization.
    let mut input = build_sql_request_input(test, context, execution_type, semantic_extras)?;
    input.target_table = None;
    Ok(input.into_proto())
}

pub fn build_seed_values_request(
    seed: &DbtSeed,
    context: SeedRunCacheRequestContext<'_>,
) -> FsResult<SubmitValuesRequest> {
    let seed_file = context
        .project_root
        .join(seed.common().original_file_path.as_path());
    let values_hash = seed_values_hash_reader(File::open(&seed_file).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to open seed file for dbt State request {}: {}",
            seed_file.display(),
            e
        )
    })?)
    .map_err(request_build_error)?;

    let semantic_extras =
        seed_semantic_extras(&seed_semantic_extra_config(seed).map_err(request_build_error)?)
            .map_err(request_build_error)?;

    Ok(SubmitValuesRequestInput {
        target_table: target_table_for_node(context.adapter_type, seed)?,
        dialect: context.dialect,
        default_catalog: seed.database(),
        values_hash,
        semantic_extras,
        last_modified_epoch: context.last_modified_epoch,
        labels: node_identity(seed).labels(),
        clone_time_travel_limit: context.clone_time_travel_limit,
        clone_table_properties: context.clone_table_properties,
    }
    .into_proto())
}

pub fn node_identity(node: &dyn InternalDbtNodeAttributes) -> NodeIdentity {
    NodeIdentity {
        name: node.common().name.clone(),
        fqn: node.common().fqn.clone(),
        unique_id: node.common().unique_id.clone(),
    }
}

pub fn target_table_for_node(
    adapter_type: AdapterType,
    node: &dyn InternalDbtNodeAttributes,
) -> FsResult<String> {
    Ok(create_relation_from_node(adapter_type, node, None)?.semantic_fqn())
}

pub fn model_execution_type_input(model: &DbtModel, full_refresh: bool) -> ExecutionTypeInput {
    let materialized = &model.base().materialized;
    ExecutionTypeInput {
        resource_type: NodeType::Model,
        is_view: materialized == &DbtMaterialization::View,
        is_custom_materialization: matches!(materialized, DbtMaterialization::Unknown(_)),
        is_incremental: materialized == &DbtMaterialization::Incremental,
        full_refresh,
        incremental_strategy: model_incremental_strategy(model),
        has_unique_key: model.deprecated_config.unique_key.is_some(),
    }
}

pub fn snapshot_execution_type_input(
    _snapshot: &DbtSnapshot,
    full_refresh: bool,
) -> ExecutionTypeInput {
    ExecutionTypeInput {
        resource_type: NodeType::Snapshot,
        is_view: false,
        is_custom_materialization: false,
        is_incremental: false,
        full_refresh,
        incremental_strategy: None,
        has_unique_key: false,
    }
}

pub fn test_execution_type_input() -> ExecutionTypeInput {
    ExecutionTypeInput {
        resource_type: NodeType::Test,
        is_view: false,
        is_custom_materialization: false,
        is_incremental: false,
        full_refresh: false,
        incremental_strategy: None,
        has_unique_key: false,
    }
}

pub fn model_sql_semantic_extra_config(
    model: &DbtModel,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    let mut extras = model_config_sql_semantic_extra_config(&model.deprecated_config)?;

    // The Python plugin emits `on_schema_change` only for `table` and
    // `incremental` materializations, falling back to the dbt-core default
    // ("ignore") when unset. Mirror that here so semantic hashes match —
    // other materializations don't carry this field over the wire even when
    // the user sets it explicitly.
    let materialized = &model.base().materialized;
    let emits_on_schema_change = matches!(
        materialized,
        DbtMaterialization::Table | DbtMaterialization::Incremental
    );
    if emits_on_schema_change {
        if !extras.contains_key("on_schema_change") {
            extras.insert(
                "on_schema_change".to_string(),
                Some(serde_json::to_value(OnSchemaChange::default())?),
            );
        }
    } else {
        extras.remove("on_schema_change");
    }

    // `constraints` lives on the resolved model attributes (merging schema.yml
    // declarations and any `{{ config(constraints=...) }}` override), not on
    // `ModelConfig` directly, so it can't be picked up in
    // `model_config_sql_semantic_extra_config`. Only include it when set,
    // mirroring the Python plugin's `if key in node_config` gating.
    if !model.__model_attr__.constraints.is_empty() {
        extras.insert(
            "constraints".to_string(),
            Some(serde_json::to_value(&model.__model_attr__.constraints)?),
        );
    }

    Ok(extras)
}

pub fn seed_semantic_extra_config(
    seed: &DbtSeed,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    seed_config_semantic_extra_config(&seed.deprecated_config)
}

pub fn model_clone_table_properties(model: &DbtModel) -> Option<TableProperties> {
    model_config_clone_table_properties(&model.deprecated_config)
}

pub fn snapshot_clone_table_properties(snapshot: &DbtSnapshot) -> Option<TableProperties> {
    snapshot_config_clone_table_properties(&snapshot.deprecated_config)
}

pub fn seed_clone_table_properties(seed: &DbtSeed) -> Option<TableProperties> {
    seed_config_clone_table_properties(&seed.deprecated_config)
}

fn build_sql_request_input(
    node: &dyn InternalDbtNodeAttributes,
    context: SqlRunCacheRequestContext,
    execution_type: dbt_state::proto::query_cache::ModelExecutionType,
    semantic_extras: SemanticExtras,
) -> FsResult<SubmitEnrichedSqlRequestInput> {
    Ok(SubmitEnrichedSqlRequestInput {
        target_table: Some(target_table_for_node(context.adapter_type, node)?),
        dialect: context.dialect,
        default_catalog: node.database(),
        default_schema: Some(context.default_schema),
        execution_type,
        sql: context.sql,
        tables: context.tables,
        query_dependencies: context.query_dependencies,
        semantic_extras,
        freshness_tolerance_seconds: context.freshness_tolerance_seconds,
        lenient_dependencies: context.lenient_dependencies,
        tolerate_nondeterminism: context.tolerate_nondeterminism,
        labels: node_identity(node).labels(),
        clone_time_travel_limit: context.clone_time_travel_limit,
        clone_table_properties: context.clone_table_properties,
        stale_upstream_policy: context.stale_upstream_policy,
        clone_chain_depth_limit: context.clone_chain_depth_limit,
    })
}

fn model_incremental_strategy(model: &DbtModel) -> Option<String> {
    model
        .__model_attr__
        .incremental_strategy
        .as_ref()
        .or(model.deprecated_config.incremental_strategy.as_ref())
        .map(ToString::to_string)
}

fn model_config_sql_semantic_extra_config(
    config: &ModelConfig,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    let mut extras = SemanticExtraConfig::new();

    insert_json(
        &mut extras,
        "on_schema_change",
        config.on_schema_change.as_ref(),
    )?;
    insert_json(
        &mut extras,
        "incremental_predicates",
        config.incremental_predicates.as_ref(),
    )?;
    insert_json(
        &mut extras,
        "merge_update_columns",
        config.merge_update_columns.as_ref(),
    )?;
    insert_json(
        &mut extras,
        "merge_exclude_columns",
        config.merge_exclude_columns.as_ref(),
    )?;
    insert_json(&mut extras, "contract", config.contract.as_ref())?;
    insert_json(&mut extras, "unique_key", config.unique_key.as_ref())?;
    insert_json(&mut extras, "grants", config.grants.as_ref())?;
    insert_json(&mut extras, "event_time", config.event_time.as_ref())?;
    insert_json(&mut extras, "sql_header", config.sql_header.as_ref())?;
    insert_json(&mut extras, "lookback", config.lookback.as_ref())?;
    insert_json(&mut extras, "table_format", config.table_format.as_ref())?;

    extras.extend(warehouse_specific_semantic_extra_config(
        &config.__warehouse_specific_config__,
    )?);

    Ok(extras)
}

/// Extracts the dialect-specific and shared warehouse config keys that mirror
/// Python's `SEMANTIC_EXTRAS_CONFIG_KEYS` (run_cache.py). Shared across models,
/// snapshots, and tests since all three embed the same
/// `WarehouseSpecificNodeConfig`.
fn warehouse_specific_semantic_extra_config(
    whc: &WarehouseSpecificNodeConfig,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    let mut extras = SemanticExtraConfig::new();

    // Shared
    insert_json(&mut extras, "cluster_by", whc.cluster_by.as_ref())?;
    insert_json(&mut extras, "partition_by", whc.partition_by.as_ref())?;

    // Databricks
    insert_json(
        &mut extras,
        "auto_liquid_cluster",
        whc.auto_liquid_cluster.as_ref(),
    )?;
    insert_json(&mut extras, "databricks_tags", whc.databricks_tags.as_ref())?;
    insert_json(&mut extras, "file_format", whc.file_format.as_ref())?;
    insert_json(&mut extras, "location_root", whc.location_root.as_ref())?;
    insert_json(
        &mut extras,
        "include_full_name_in_path",
        whc.include_full_name_in_path.as_ref(),
    )?;
    insert_json(&mut extras, "clustered_by", whc.clustered_by.as_ref())?;
    insert_json(&mut extras, "buckets", whc.buckets.as_ref())?;
    insert_json(
        &mut extras,
        "liquid_clustered_by",
        whc.liquid_clustered_by.as_ref(),
    )?;
    insert_json(&mut extras, "tblproperties", whc.tblproperties.as_ref())?;

    // Snowflake
    insert_json(&mut extras, "transient", whc.transient.as_ref())?;
    insert_json(&mut extras, "target_lag", whc.target_lag.as_ref())?;
    insert_json(&mut extras, "refresh_mode", whc.refresh_mode.as_ref())?;
    insert_json(&mut extras, "immutable_where", whc.immutable_where.as_ref())?;
    insert_json(&mut extras, "copy_grants", whc.copy_grants.as_ref())?;
    insert_json(
        &mut extras,
        "tmp_relation_type",
        whc.tmp_relation_type.as_ref(),
    )?;

    // Postgres
    insert_json(&mut extras, "unlogged", whc.unlogged.as_ref())?;
    insert_json(&mut extras, "indexes", whc.indexes.as_ref().as_ref())?;

    // BigQuery
    insert_json(
        &mut extras,
        "require_partition_filter",
        whc.require_partition_filter.as_ref(),
    )?;
    insert_json(
        &mut extras,
        "partition_expiration_days",
        whc.partition_expiration_days.as_ref(),
    )?;
    insert_json(
        &mut extras,
        "hours_to_expiration",
        whc.hours_to_expiration.as_ref(),
    )?;

    // Redshift
    insert_json(&mut extras, "dist", whc.dist.as_ref())?;
    insert_json(&mut extras, "sort", whc.sort.as_ref())?;
    insert_json(&mut extras, "sort_type", whc.sort_type.as_ref())?;

    Ok(extras)
}

fn seed_config_semantic_extra_config(
    config: &SeedConfig,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    let mut extras = SemanticExtraConfig::new();

    if let Some(column_types) = config.column_types.as_ref() {
        let plain_column_types: BTreeMap<String, String> = column_types
            .iter()
            .map(|(name, data_type)| (name.clone().into_inner(), data_type.clone()))
            .collect();
        extras.insert(
            "column_types".to_string(),
            Some(serde_json::to_value(plain_column_types)?),
        );
    }
    insert_json(&mut extras, "quote_columns", config.quote_columns.as_ref())?;
    if let Some(delimiter) = config.delimiter.as_ref() {
        extras.insert(
            "delimiter".to_string(),
            Some(Value::String(delimiter.clone().into_inner())),
        );
    }

    Ok(extras)
}

fn snapshot_config_sql_semantic_extra_config(
    config: &SnapshotConfig,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    let mut extras = SemanticExtraConfig::new();

    insert_json(&mut extras, "unique_key", config.unique_key.as_ref())?;
    insert_json(&mut extras, "grants", config.grants.as_ref())?;
    insert_json(&mut extras, "event_time", config.event_time.as_ref())?;

    extras.extend(warehouse_specific_semantic_extra_config(
        &config.__warehouse_specific_config__,
    )?);

    Ok(extras)
}

fn test_config_sql_semantic_extra_config(
    config: &DataTestConfig,
) -> Result<SemanticExtraConfig, RequestBuildError> {
    let mut extras = SemanticExtraConfig::new();

    insert_json(&mut extras, "severity", config.severity.as_ref())?;
    insert_json(&mut extras, "limit", config.limit.as_ref())?;
    insert_json(&mut extras, "where", config.where_.as_ref())?;
    insert_json(&mut extras, "fail_calc", config.fail_calc.as_ref())?;
    insert_json(&mut extras, "warn_if", config.warn_if.as_ref())?;
    insert_json(&mut extras, "error_if", config.error_if.as_ref())?;
    insert_json(
        &mut extras,
        "store_failures",
        config.store_failures.as_ref(),
    )?;
    insert_json(
        &mut extras,
        "store_failures_as",
        config.store_failures_as.as_ref(),
    )?;
    insert_json(&mut extras, "sql_header", config.sql_header.as_ref())?;

    extras.extend(warehouse_specific_semantic_extra_config(
        &config.__warehouse_specific_config__,
    )?);

    Ok(extras)
}

fn model_config_clone_table_properties(config: &ModelConfig) -> Option<TableProperties> {
    clone_table_properties_from_values(
        // hours_to_expiration is loosely typed (may be a non-numeric string such
        // as "null"); only a parseable integer drives the dev-clone expiration.
        config
            .__warehouse_specific_config__
            .hours_to_expiration
            .as_ref()
            .and_then(|v| v.to_string().parse::<u64>().ok()),
        config
            .__warehouse_specific_config__
            .partition_expiration_days,
    )
}

fn snapshot_config_clone_table_properties(config: &SnapshotConfig) -> Option<TableProperties> {
    clone_table_properties_from_values(
        // hours_to_expiration is loosely typed (may be a non-numeric string such
        // as "null"); only a parseable integer drives the dev-clone expiration.
        config
            .__warehouse_specific_config__
            .hours_to_expiration
            .as_ref()
            .and_then(|v| v.to_string().parse::<u64>().ok()),
        config
            .__warehouse_specific_config__
            .partition_expiration_days,
    )
}

fn seed_config_clone_table_properties(config: &SeedConfig) -> Option<TableProperties> {
    clone_table_properties_from_values(
        // hours_to_expiration is loosely typed (may be a non-numeric string such
        // as "null"); only a parseable integer drives the dev-clone expiration.
        config
            .__warehouse_specific_config__
            .hours_to_expiration
            .as_ref()
            .and_then(|v| v.to_string().parse::<u64>().ok()),
        config
            .__warehouse_specific_config__
            .partition_expiration_days,
    )
}

fn clone_table_properties_from_values(
    hours_to_expiration: Option<u64>,
    partition_expiration_days: Option<u64>,
) -> Option<TableProperties> {
    let hours_to_expiration = positive_i32(hours_to_expiration);
    let partition_expiration_days = positive_i32(partition_expiration_days);

    if hours_to_expiration.is_none() && partition_expiration_days.is_none() {
        return None;
    }

    Some(TableProperties {
        hours_to_expiration,
        partition_expiration_days,
    })
}

fn positive_i32(value: Option<u64>) -> Option<i32> {
    value
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn insert_json<T: Serialize>(
    extras: &mut SemanticExtraConfig,
    key: &str,
    value: Option<&T>,
) -> Result<(), RequestBuildError> {
    if let Some(value) = value {
        extras.insert(key.to_string(), Some(serde_json::to_value(value)?));
    }
    Ok(())
}

fn request_build_error(error: RequestBuildError) -> Box<dbt_common::FsError> {
    fs_err!(
        ErrorCode::Generic,
        "Failed to build dbt State request: {}",
        error
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::io_args::StaticAnalysisKind;
    use dbt_common::path::DbtPath;
    use dbt_schemas::schemas::common::{
        Access, DbtIncrementalStrategy, DbtMaterialization, DbtUniqueKey, OnSchemaChange,
        ResolvedQuoting, Severity, StoreFailuresAs,
    };
    use dbt_schemas::schemas::nodes::AdapterAttr;
    use dbt_schemas::schemas::serde::StringOrArrayOfStrings;
    use dbt_schemas::schemas::{
        CommonAttributes, DbtModelAttr, DbtSeedAttr, DbtSnapshotAttr, DbtTestAttr,
        IntrospectionKind, NodeBaseAttributes,
    };
    use dbt_state::proto::query_cache::ModelExecutionType;
    use dbt_state::request_builder::{execution_type_from_input, seed_values_hash};
    use dbt_yaml::Spanned;
    use indexmap::IndexMap;
    use std::collections::BTreeMap;

    fn make_common(unique_id: &str, name: &str) -> CommonAttributes {
        CommonAttributes {
            unique_id: unique_id.to_string(),
            name: name.to_string(),
            package_name: "jaffle_shop".to_string(),
            fqn: vec!["jaffle_shop".to_string(), name.to_string()],
            original_file_path: DbtPath::from(format!("models/{name}.sql")),
            tags: vec![],
            meta: IndexMap::new(),
            ..Default::default()
        }
    }

    fn make_base(materialized: DbtMaterialization, alias: &str) -> NodeBaseAttributes {
        NodeBaseAttributes {
            database: "analytics".to_string(),
            schema: "marts".to_string(),
            alias: alias.to_string(),
            materialized,
            quoting: ResolvedQuoting::trues(),
            static_analysis: Spanned::new(StaticAnalysisKind::On),
            enabled: true,
            ..Default::default()
        }
    }

    fn make_model(materialized: DbtMaterialization) -> DbtModel {
        DbtModel {
            __common_attr__: make_common("model.jaffle_shop.orders", "orders"),
            __base_attr__: make_base(materialized, "orders"),
            __model_attr__: DbtModelAttr {
                access: Access::default(),
                introspection: IntrospectionKind::None,
                incremental_strategy: Some(DbtIncrementalStrategy::Merge),
                ..Default::default()
            },
            __adapter_attr__: AdapterAttr::default(),
            deprecated_config: ModelConfig {
                incremental_strategy: Some(DbtIncrementalStrategy::Merge),
                incremental_predicates: Some(vec![
                    "updated_at >= current_date".to_string(),
                    "deleted_at is null".to_string(),
                ]),
                unique_key: Some(DbtUniqueKey::Single("id".to_string())),
                on_schema_change: Some(OnSchemaChange::SyncAllColumns),
                merge_update_columns: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                    "status".to_string(),
                    "updated_at".to_string(),
                ])),
                ..Default::default()
            },
            __other__: BTreeMap::new(),
        }
    }

    fn make_snapshot() -> DbtSnapshot {
        DbtSnapshot {
            __common_attr__: make_common("snapshot.jaffle_shop.orders_snapshot", "orders_snapshot"),
            __base_attr__: make_base(DbtMaterialization::Snapshot, "orders_snapshot"),
            __snapshot_attr__: DbtSnapshotAttr::default(),
            __adapter_attr__: AdapterAttr::default(),
            ..Default::default()
        }
    }

    fn make_test() -> DbtTest {
        DbtTest {
            __common_attr__: make_common(
                "test.jaffle_shop.not_null_orders_id",
                "not_null_orders_id",
            ),
            __base_attr__: make_base(DbtMaterialization::Test, "not_null_orders_id"),
            __test_attr__: DbtTestAttr::default(),
            __adapter_attr__: AdapterAttr::default(),
            manifest_original_file_path: DbtPath::from("tests/not_null_orders_id.sql"),
            deprecated_config: DataTestConfig {
                severity: Some(Severity::Error),
                limit: Some(10),
                where_: Some("id > 0".to_string()),
                fail_calc: Some("count(*)".to_string()),
                warn_if: Some(">0".to_string()),
                error_if: Some(">100".to_string()),
                store_failures: Some(true),
                store_failures_as: Some(StoreFailuresAs::Table),
                sql_header: Some("/* header */".to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn make_seed() -> DbtSeed {
        let mut column_types = BTreeMap::new();
        column_types.insert(Spanned::new("id".to_string()), "integer".to_string());

        DbtSeed {
            __common_attr__: CommonAttributes {
                original_file_path: DbtPath::from("seeds/cities.csv"),
                ..make_common("seed.jaffle_shop.cities", "cities")
            },
            __base_attr__: make_base(DbtMaterialization::Seed, "cities"),
            __seed_attr__: DbtSeedAttr::default(),
            deprecated_config: SeedConfig {
                column_types: Some(column_types),
                quote_columns: Some(true),
                delimiter: Some(Spanned::new("|".to_string())),
                ..Default::default()
            },
            __other__: BTreeMap::new(),
        }
    }

    fn sql_context(full_refresh: bool) -> SqlRunCacheRequestContext {
        SqlRunCacheRequestContext {
            adapter_type: AdapterType::Snowflake,
            dialect: "snowflake".to_string(),
            sql: "select * from raw.orders".to_string(),
            tables: vec![TableModifiedInfo {
                name: "raw.orders".to_string(),
                last_modified_epoch: Some(123),
            }],
            query_dependencies: vec![],
            freshness_tolerance_seconds: 2700,
            lenient_dependencies: vec![],
            tolerate_nondeterminism: true,
            full_refresh,
            clone_time_travel_limit: None,
            clone_table_properties: None,
            clone_chain_depth_limit: None,
            default_schema: "marts".to_string(),
            stale_upstream_policy: StaleUpstreamPolicy::Any,
            microbatch_window: None,
        }
    }

    #[test]
    fn model_request_uses_fusion_node_identity_target_and_semantic_extras() {
        let model = make_model(DbtMaterialization::Incremental);
        let request = build_model_sql_request(&model, sql_context(false)).unwrap();

        assert_eq!(
            request.target_table.as_deref(),
            Some(r#""analytics"."marts"."orders""#)
        );
        assert_eq!(request.default_catalog, "analytics");
        assert_eq!(request.execution_type, ModelExecutionType::Merge as i32);
        assert_eq!(
            request.labels.get("dbt_node_unique_id").unwrap(),
            "model.jaffle_shop.orders"
        );
        assert_eq!(
            request.labels.get("dbt_node_fqn").unwrap(),
            "jaffle_shop.orders"
        );
        assert_eq!(
            request.semantic_extras.get("on_schema_change").unwrap(),
            "\"sync_all_columns\""
        );
        assert_eq!(
            request
                .semantic_extras
                .get("incremental_predicates")
                .unwrap(),
            "[\"updated_at >= current_date\",\"deleted_at is null\"]"
        );
        assert_eq!(
            request.semantic_extras.get("merge_update_columns").unwrap(),
            "[\"status\",\"updated_at\"]"
        );
        assert_eq!(request.semantic_extras.get("unique_key").unwrap(), "\"id\"");
    }

    #[test]
    fn microbatch_window_folds_raw_iso_bounds_into_semantic_extras() {
        use chrono::TimeZone;

        // A microbatch model, so the test stays valid if the window fold is ever
        // gated on `is_microbatch_model`.
        let mut model = make_model(DbtMaterialization::Incremental);
        model.__model_attr__.incremental_strategy = Some(DbtIncrementalStrategy::Microbatch);
        model.deprecated_config.incremental_strategy = Some(DbtIncrementalStrategy::Microbatch);
        let start = Utc.with_ymd_and_hms(2020, 1, 2, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 4, 0, 0, 0).unwrap();

        let mut context = sql_context(false);
        context.microbatch_window = Some((start, end));

        let request = build_model_sql_request(&model, context).unwrap();

        // Raw ISO-8601 strings (not JSON-encoded, so no surrounding quotes),
        // matching the dbt-core plugin so both engines hash the window the same.
        assert_eq!(
            request
                .semantic_extras
                .get("__microbatch_event_time_start")
                .unwrap(),
            "2020-01-02T00:00:00+00:00"
        );
        assert_eq!(
            request
                .semantic_extras
                .get("__microbatch_event_time_end")
                .unwrap(),
            "2020-01-04T00:00:00+00:00"
        );
    }

    #[test]
    fn non_microbatch_request_omits_microbatch_window_extras() {
        let model = make_model(DbtMaterialization::Incremental);
        let request = build_model_sql_request(&model, sql_context(false)).unwrap();

        assert!(
            !request
                .semantic_extras
                .contains_key("__microbatch_event_time_start")
        );
        assert!(
            !request
                .semantic_extras
                .contains_key("__microbatch_event_time_end")
        );
    }

    #[test]
    fn table_and_incremental_default_on_schema_change_to_ignore_when_unset() {
        for materialization in [DbtMaterialization::Table, DbtMaterialization::Incremental] {
            let mut model = make_model(materialization.clone());
            model.deprecated_config.on_schema_change = None;
            model.deprecated_config.incremental_predicates = None;
            model.deprecated_config.merge_update_columns = None;

            let request = build_model_sql_request(&model, sql_context(false)).unwrap();

            assert_eq!(
                request.semantic_extras.get("on_schema_change").unwrap(),
                "\"ignore\"",
                "{materialization:?}"
            );
            assert!(
                !request
                    .semantic_extras
                    .contains_key("incremental_predicates"),
                "{materialization:?}"
            );
            assert!(
                !request.semantic_extras.contains_key("merge_update_columns"),
                "{materialization:?}"
            );
        }
    }

    #[test]
    fn view_model_omits_on_schema_change_even_when_set() {
        let mut model = make_model(DbtMaterialization::View);
        model.deprecated_config.on_schema_change = Some(OnSchemaChange::SyncAllColumns);

        let request = build_model_sql_request(&model, sql_context(false)).unwrap();

        assert!(!request.semantic_extras.contains_key("on_schema_change"));
    }

    #[test]
    fn model_execution_type_maps_merge_without_unique_key_to_append() {
        let mut model = make_model(DbtMaterialization::Incremental);
        model.deprecated_config.unique_key = None;

        let execution_type =
            execution_type_from_input(&model_execution_type_input(&model, false)).unwrap();

        assert_eq!(execution_type, ModelExecutionType::Append);
    }

    #[test]
    fn model_full_refresh_suppresses_incremental_skip_type() {
        let model = make_model(DbtMaterialization::Incremental);
        let request = build_model_sql_request(&model, sql_context(true)).unwrap();

        assert_eq!(request.execution_type, ModelExecutionType::Full as i32);
    }

    #[test]
    fn model_full_refresh_keeps_view_execution_type() {
        let model = make_model(DbtMaterialization::View);
        let request = build_model_sql_request(&model, sql_context(true)).unwrap();

        assert_eq!(request.execution_type, ModelExecutionType::View as i32);
    }

    #[test]
    fn custom_materialization_maps_to_custom_execution_type() {
        let model = make_model(DbtMaterialization::Unknown(
            "my_materialization".to_string(),
        ));

        let execution_type =
            execution_type_from_input(&model_execution_type_input(&model, false)).unwrap();

        assert_eq!(execution_type, ModelExecutionType::DbtCustom);
    }

    #[test]
    fn custom_incremental_strategy_maps_to_custom_execution_type() {
        // A standard `incremental` model with a user-defined
        // `get_incremental_<name>_sql` strategy is submitted as DBT_CUSTOM so it
        // still benefits from dbt State instead of failing open.
        let mut model = make_model(DbtMaterialization::Incremental);
        let strategy = DbtIncrementalStrategy::Custom("insert_only".to_string());
        model.__model_attr__.incremental_strategy = Some(strategy.clone());
        model.deprecated_config.incremental_strategy = Some(strategy);

        let request = build_model_sql_request(&model, sql_context(false)).unwrap();

        assert_eq!(request.execution_type, ModelExecutionType::DbtCustom as i32);
    }

    #[test]
    fn snapshot_request_uses_snapshot_execution_type_during_full_refresh() {
        let snapshot = make_snapshot();

        let request = build_snapshot_sql_request(&snapshot, sql_context(false)).unwrap();
        assert_eq!(request.execution_type, ModelExecutionType::Snapshot as i32);
        assert_eq!(
            request.target_table.as_deref(),
            Some(r#""analytics"."marts"."orders_snapshot""#)
        );

        let request = build_snapshot_sql_request(&snapshot, sql_context(true)).unwrap();
        assert_eq!(request.execution_type, ModelExecutionType::Snapshot as i32);
    }

    #[test]
    fn data_test_request_keeps_default_schema_without_target_table() {
        let mut test = make_test();
        test.__base_attr__.schema = "ANALYTICS".to_string();
        let mut context = sql_context(false);
        context.default_schema = test.schema();

        let request = build_test_sql_request(&test, context).unwrap();

        assert_eq!(request.target_table, None);
        assert_eq!(request.default_catalog, "analytics");
        assert_eq!(request.default_schema.as_deref(), Some("ANALYTICS"));
        assert_eq!(
            request.execution_type,
            ModelExecutionType::DbtDataTest as i32
        );
    }

    #[test]
    fn seed_request_uses_md5_file_hash_and_seed_semantic_extras() {
        let tempdir = tempfile::tempdir().unwrap();
        let seeds_dir = tempdir.path().join("seeds");
        std::fs::create_dir(&seeds_dir).unwrap();
        let seed_bytes = b"id|city\n1|Chicago\n";
        std::fs::write(seeds_dir.join("cities.csv"), seed_bytes).unwrap();

        let seed = make_seed();
        let request = build_seed_values_request(
            &seed,
            SeedRunCacheRequestContext {
                adapter_type: AdapterType::Snowflake,
                dialect: "snowflake".to_string(),
                project_root: tempdir.path(),
                last_modified_epoch: Some(456),
                clone_time_travel_limit: Some(3600),
                clone_table_properties: None,
            },
        )
        .unwrap();

        assert_eq!(request.target_table, r#""analytics"."marts"."cities""#);
        assert_eq!(request.default_catalog, "analytics");
        assert_eq!(request.values_hash, seed_values_hash(seed_bytes));
        assert_eq!(request.last_modified_epoch, Some(456));
        assert_eq!(request.clone_time_travel_limit, Some(3600));
        assert_eq!(
            request.semantic_extras.get("column_types").unwrap(),
            "{\"id\":\"integer\"}"
        );
        assert_eq!(
            request.semantic_extras.get("quote_columns").unwrap(),
            "true"
        );
        assert_eq!(request.semantic_extras.get("delimiter").unwrap(), "\"|\"");
        assert_eq!(request.labels.get("dbt_node_name").unwrap(), "cities");
    }

    #[test]
    fn model_semantic_extras_include_expanded_config_keys() {
        use dbt_common::serde_utils::Omissible;
        use dbt_schemas::schemas::common::{ClusterConfig, ConstraintType, DbtContract};
        use dbt_schemas::schemas::properties::ModelConstraint;
        use dbt_schemas::schemas::serde::{GrantConfig, OmissibleGrantConfig};

        let mut model = make_model(DbtMaterialization::Table);
        model.__model_attr__.constraints = vec![ModelConstraint {
            type_: ConstraintType::NotNull,
            expression: Some("id".to_string()),
            ..Default::default()
        }];
        model.deprecated_config.contract = Some(DbtContract {
            enforced: true,
            ..Default::default()
        });
        model.deprecated_config.event_time = Some("created_at".to_string());
        model.deprecated_config.sql_header = Some("/* header */".to_string());
        model.deprecated_config.lookback = Some(3);
        model.deprecated_config.table_format = Some("iceberg".to_string());
        model.deprecated_config.grants =
            OmissibleGrantConfig(Omissible::Present(GrantConfig(IndexMap::from([(
                "select".to_string(),
                StringOrArrayOfStrings::ArrayOfStrings(vec!["public".to_string()]),
            )]))));
        model
            .deprecated_config
            .__warehouse_specific_config__
            .cluster_by = Some(ClusterConfig::List(vec!["id".to_string()]));
        model
            .deprecated_config
            .__warehouse_specific_config__
            .auto_liquid_cluster = Some(true);

        let request = build_model_sql_request(&model, sql_context(false)).unwrap();

        assert_eq!(
            request.semantic_extras.get("contract").unwrap(),
            "{\"alias_types\":true,\"enforced\":true}"
        );
        let constraints = request.semantic_extras.get("constraints").unwrap();
        assert!(constraints.contains("\"type\":\"not_null\""));
        assert!(constraints.contains("\"expression\":\"id\""));
        assert_eq!(
            request.semantic_extras.get("event_time").unwrap(),
            "\"created_at\""
        );
        assert_eq!(
            request.semantic_extras.get("sql_header").unwrap(),
            "\"/* header */\""
        );
        assert_eq!(request.semantic_extras.get("lookback").unwrap(), "3");
        assert_eq!(
            request.semantic_extras.get("table_format").unwrap(),
            "\"iceberg\""
        );
        assert_eq!(
            request.semantic_extras.get("grants").unwrap(),
            "{\"select\":[\"public\"]}"
        );
        assert_eq!(
            request.semantic_extras.get("cluster_by").unwrap(),
            "[\"id\"]"
        );
        assert_eq!(
            request.semantic_extras.get("auto_liquid_cluster").unwrap(),
            "true"
        );
    }

    #[test]
    fn snapshot_semantic_extras_include_config_and_warehouse_keys() {
        let mut snapshot = make_snapshot();
        snapshot.deprecated_config.unique_key =
            Some(StringOrArrayOfStrings::String("id".to_string()));
        snapshot.deprecated_config.event_time = Some("created_at".to_string());
        snapshot
            .deprecated_config
            .__warehouse_specific_config__
            .transient = Some(true);

        let request = build_snapshot_sql_request(&snapshot, sql_context(false)).unwrap();

        assert_eq!(request.semantic_extras.get("unique_key").unwrap(), "\"id\"");
        assert_eq!(
            request.semantic_extras.get("event_time").unwrap(),
            "\"created_at\""
        );
        assert_eq!(request.semantic_extras.get("transient").unwrap(), "true");
    }

    #[test]
    fn test_semantic_extras_include_data_test_config_keys() {
        let test = make_test();

        let request = build_test_sql_request(&test, sql_context(false)).unwrap();

        assert_eq!(
            request.semantic_extras.get("severity").unwrap(),
            "\"ERROR\""
        );
        assert_eq!(request.semantic_extras.get("limit").unwrap(), "10");
        assert_eq!(request.semantic_extras.get("where").unwrap(), "\"id > 0\"");
        assert_eq!(
            request.semantic_extras.get("fail_calc").unwrap(),
            "\"count(*)\""
        );
        assert_eq!(request.semantic_extras.get("warn_if").unwrap(), "\">0\"");
        assert_eq!(request.semantic_extras.get("error_if").unwrap(), "\">100\"");
        assert_eq!(
            request.semantic_extras.get("store_failures").unwrap(),
            "true"
        );
        assert_eq!(
            request.semantic_extras.get("store_failures_as").unwrap(),
            "\"table\""
        );
        assert_eq!(
            request.semantic_extras.get("sql_header").unwrap(),
            "\"/* header */\""
        );
        // Data tests always submit with target_table=None (see build_test_sql_request).
        assert!(request.target_table.is_none());
    }
}
