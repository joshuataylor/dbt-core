//! Parse-time listener that warns when a `{% snapshot %}`/`{% docs %}` block
//! name is followed by a non-empty suffix dbt-core silently discards.

use std::path::PathBuf;

use dbt_common::tracing::dbt_emit::emit_warn_log_from_fs_error;
use dbt_common::{CodeLocationWithFile, ErrorCode, fs_err};
use minijinja::listener::{BlockNameKind, TokenizerEventListener};
use minijinja::machinery::{Span, Token};

/// Emits a deprecation warning for malformed `{% snapshot %}`/`{% docs %}`
/// block names via the parser's `on_malformed_block_name` event.
#[derive(Debug)]
pub struct MalformedBlockNameListener {
    path: PathBuf,
}

impl MalformedBlockNameListener {
    /// Creates a new listener for the given source file path and IO args.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn block_label(kind: BlockNameKind) -> &'static str {
        match kind {
            BlockNameKind::Snapshot => "Snapshot",
            BlockNameKind::Docs => "Docs",
        }
    }
}

impl TokenizerEventListener for MalformedBlockNameListener {
    fn on_source_token(&self, _token: &Token<'_>, _span: &Span) {}

    fn on_malformed_block_name(&self, kind: BlockNameKind, name: &str, name_span: &Span) {
        let location = CodeLocationWithFile::new(
            name_span.start_line,
            name_span.start_col,
            name_span.start_offset,
            self.path.clone(),
        );
        let warning = fs_err!(
            code => ErrorCode::MalformedBlockName,
            loc => location,
            "{} block name `{}` is followed by extra characters that dbt \
             silently ignores. Use a name that is a single valid identifier. \
             This will become an error in a future release.",
            Self::block_label(kind),
            name,
        );
        emit_warn_log_from_fs_error(*warning);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;

    use std::path::PathBuf;

    use dbt_common::ErrorCode;
    use dbt_common::tracing::fs_error_log::FsErrorLog;
    use dbt_tracing::{
        SeverityNumber,
        init::create_tracing_subcriber_with_layer,
        layer::ConsumerLayer,
        test_support::mocks::{TestLayer, test_data_layer},
    };
    use minijinja::compiler::lexer::WhitespaceConfig;
    use minijinja::compiler::parser::Parser;
    use minijinja::listener::TokenizerEventListener;
    use minijinja::syntax::SyntaxConfig;

    fn warnings_for(src: &str, statement_types: &[&str]) -> Vec<ErrorCode> {
        let (test_layer, _, _, log_records) = TestLayer::new();
        let subscriber = create_tracing_subcriber_with_layer(
            tracing::level_filters::LevelFilter::TRACE,
            test_data_layer(
                1,
                None,
                false,
                std::iter::empty(),
                std::iter::once(Box::new(test_layer) as ConsumerLayer),
            ),
            &[],
        )
        .expect("test tracing subscriber should be valid");

        tracing::subscriber::with_default(subscriber, || {
            let listener: Rc<dyn TokenizerEventListener> =
                Rc::new(MalformedBlockNameListener::new(PathBuf::from("test.sql")));
            let mut parser = Parser::new_with_tokenizer_listeners(
                src,
                "test.sql",
                false,
                SyntaxConfig::builder().build().unwrap(),
                WhitespaceConfig::default(),
                &[listener],
            );
            parser.parse_top_level_statements(statement_types).unwrap();
        });

        log_records
            .lock()
            .unwrap()
            .iter()
            .map(|record| {
                assert_eq!(record.severity_number, SeverityNumber::Warn);
                let warning = record
                    .attributes
                    .downcast_ref::<FsErrorLog>()
                    .expect("warning should retain its FsError");
                warning.get_fs_error().code
            })
            .collect()
    }

    #[test]
    fn malformed_snapshot_reports_warning() {
        let warnings = warnings_for(
            "{% snapshot stg_crm.sql %}select 1{% endsnapshot %}",
            &["snapshot"],
        );
        assert_eq!(warnings, vec![ErrorCode::MalformedBlockName]);
    }

    #[test]
    fn clean_docs_reports_no_warning() {
        let warnings = warnings_for("{% docs my_doc %}content{% enddocs %}", &["docs"]);
        assert!(warnings.is_empty(), "got {warnings:?}");
    }
}
