#![cfg(feature = "builtins")]
use std::collections::HashMap;

use minijinja::{context, render, Environment, ErrorKind, State, UndefinedBehavior};

use similar_asserts::assert_eq;

#[test]
fn test_lenient_undefined() {
    let mut env = Environment::new();
    env.add_filter("test", |state: &State, value: String| -> String {
        assert_eq!(state.undefined_behavior(), UndefinedBehavior::Lenient);
        assert_eq!(value, "");
        value
    });

    assert_eq!(env.undefined_behavior(), UndefinedBehavior::Lenient);
    assert_eq!(render!(in env, "<{{ true.missing_attribute }}>"), "<>");
    assert_eq!(
        env.render_str("{{ undefined.missing_attribute }}", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::UndefinedError
    );
    assert_eq!(
        render!(in env, "<{% for x in undefined %}...{% endfor %}>"),
        "<>"
    );
    assert_eq!(render!(in env, "{{ 'foo' is in(undefined) }}"), "False");
    assert_eq!(render!(in env, "<{{ undefined }}>"), "<>");
    assert_eq!(render!(in env, "{{ undefined is undefined }}"), "True");
    assert_eq!(
        render!(in env, "{{ x.foo is undefined }}", x => HashMap::<String, String>::new()),
        "True"
    );
    assert_eq!(render!(in env, "{{ undefined|list }}"), "[]");
    // `dictsort` matches dbt Core, which errors here. `items` is a deliberate
    // divergence: Core returns a generator, but a silent empty list hides typos.
    for source in ["{{ undefined|dictsort }}", "{{ undefined|items }}"] {
        let err = env.render_str(source, (), &[]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidOperation, "{source}");
        assert!(
            err.to_string()
                .contains("cannot convert value into pair list"),
            "{source}: {err}"
        );
    }
    assert_eq!(render!(in env, "<{{ undefined|test }}>"), "<>");
    assert_eq!(render!(in env, "{{ 42 in undefined }}"), "False");
}

#[test]
fn test_allow_all_undefined() {
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::AllowAll);

    assert_eq!(render!(in env, "{{ undefined|dictsort }}"), "[]");
    assert_eq!(render!(in env, "{{ undefined|items }}"), "[]");
    assert_eq!(render!(in env, "{{ undefined.a.b.c|dictsort }}"), "[]");
    assert_eq!(render!(in env, "{{ undefined.a.b.c|items }}"), "[]");
    assert_eq!(
        render!(in env, "{{ x.nodes|dictsort }}", x => HashMap::<String, String>::new()),
        "[]"
    );
    assert_eq!(
        render!(in env, "{{ x.nodes|items }}", x => HashMap::<String, String>::new()),
        "[]"
    );
}

/// A defined but non-map operand errors in every phase. `dictsort` matches dbt
/// Core here; `items` is stricter than Core by design.
#[test]
fn test_non_map_pair_list_always_errors() {
    for behavior in [
        UndefinedBehavior::AllowAll,
        UndefinedBehavior::Lenient,
        UndefinedBehavior::Chainable,
        UndefinedBehavior::Strict,
    ] {
        let mut env = Environment::new();
        env.set_undefined_behavior(behavior);

        for operand in [r#""hello""#, "42", "none"] {
            for filter in ["dictsort", "items"] {
                let source = format!("{{{{ {operand}|{filter} }}}}");
                let err = env.render_str(&source, (), &[]).unwrap_err();
                assert_eq!(
                    err.kind(),
                    ErrorKind::InvalidOperation,
                    "{behavior:?} / {source}"
                );
                assert!(
                    err.to_string()
                        .contains("cannot convert value into pair list"),
                    "{behavior:?} / {source}: {err}"
                );
            }
        }
    }
}

#[test]
fn test_strict_undefined() {
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::Strict);

    assert_eq!(
        env.render_str("{{ true.missing_attribute }}", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::UndefinedError
    );
    assert_eq!(
        env.render_str("{{ undefined.missing_attribute }}", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::UndefinedError
    );
    assert_eq!(
        env.render_str("<{% for x in undefined %}...{% endfor %}>", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::UndefinedError
    );
    assert_eq!(
        env.render_str("{{ 'foo' is in(undefined) }}", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::UndefinedError
    );
    assert_eq!(
        env.render_str("<{{ undefined }}>", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::UndefinedError
    );
    assert_eq!(render!(in env, "{{ undefined is undefined }}"), "True");
    assert_eq!(
        render!(in env, "{{ x.foo is undefined }}", x => HashMap::<String, String>::new()),
        "True"
    );
    assert_eq!(
        env.render_str(
            "{% if x.foo %}...{% endif %}",
            context! { x => HashMap::<String, String>::new() },
            &[]
        )
        .unwrap_err()
        .kind(),
        ErrorKind::UndefinedError
    );
    assert_eq!(
        env.render_str("{{ undefined|list }}", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidOperation
    );
    assert_eq!(
        env.render_str("{{ undefined|dictsort }}", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidOperation
    );
    assert_eq!(
        env.render_str("{{ undefined|items }}", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidOperation
    );
    assert_eq!(
        env.render_str("{{ 42 in undefined }}", (), &[])
            .unwrap_err()
            .kind(),
        ErrorKind::UndefinedError
    );
}

#[test]
fn test_chainable_undefined() {
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::Chainable);
    env.add_filter("test", |state: &State, value: String| -> String {
        assert_eq!(state.undefined_behavior(), UndefinedBehavior::Chainable);
        assert_eq!(value, "");
        value
    });

    assert_eq!(render!(in env, "<{{ true.missing_attribute }}>"), "<>");
    assert_eq!(render!(in env, "<{{ undefined.missing_attribute }}>"), "<>");
    assert_eq!(
        render!(in env, "<{% for x in undefined %}...{% endfor %}>"),
        "<>"
    );
    assert_eq!(
        render!(in env, "{{ x.foo is undefined }}", x => HashMap::<String, String>::new()),
        "True"
    );
    assert_eq!(render!(in env, "{{ 'foo' is in(undefined) }}"), "False");
    assert_eq!(render!(in env, "<{{ undefined }}>"), "<>");
    assert_eq!(render!(in env, "{{ undefined is undefined }}"), "True");
    assert_eq!(render!(in env, "{{ undefined|list }}"), "[]");
    assert_eq!(render!(in env, "{{ undefined|dictsort }}"), "[]");
    assert_eq!(render!(in env, "{{ undefined|items }}"), "[]");
    assert_eq!(render!(in env, "<{{ undefined|test }}>"), "<>");
    assert_eq!(render!(in env, "{{ 42 in undefined }}"), "False");
}
