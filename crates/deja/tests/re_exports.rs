#[test]
fn facade_schema_version_matches_record_crate() {
    assert_eq!(
        deja::CURRENT_EVENT_SCHEMA_VERSION,
        deja_runtime::CURRENT_EVENT_SCHEMA_VERSION,
        "the public deja facade must expose the same event schema version that deja-runtime writes"
    );
}
