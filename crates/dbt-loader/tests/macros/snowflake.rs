use std::collections::BTreeMap;

use dbt_adapter_core::AdapterType;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, default_mock_config};

#[test]
fn python_table_tmp_relation_type_is_allowed() {
    let harness = MacroTestHarness::for_adapter(AdapterType::Snowflake)
        .load_all_macros()
        .build()
        .expect("harness should build");

    let config = default_mock_config();
    config.on("get", |args| {
        let key = args.first().and_then(|v| v.as_str());
        let default = args.get(1).cloned().unwrap_or(Value::UNDEFINED);
        match key {
            Some("tmp_relation_type") => Ok(Value::from("table")),
            _ => Ok(default),
        }
    });

    let ctx = BTreeMap::from([("config".to_string(), Value::from_dyn_object(config))]);

    let rendered = harness
        .render(
            "{{ dbt_snowflake_get_tmp_relation_type('default', none, 'python') }}",
            ctx,
        )
        .expect("table is valid for Python tmp_relation_type");

    assert_eq!(rendered.trim(), "table");
}
