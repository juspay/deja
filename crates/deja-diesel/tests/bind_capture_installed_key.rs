//! The process-wide path: `capture_query` reads the key installed at boot.
//! Its own binary, because the key is installed once per process.

use deja_diesel::capture_query;
use deja_runtime::capture_key::{install_capture_key, CaptureKey};
use diesel::{ExpressionMethods, QueryDsl};

diesel::table! {
    attempt (id) {
        id -> Text,
    }
}

#[test]
fn capture_query_uses_the_key_installed_at_boot() {
    install_capture_key(Some(b"boot-key")).expect("first install");
    let captured = capture_query(&attempt::table.filter(attempt::id.eq("a_1")));
    let image = captured.binds.expect("an installed key produces an image");
    assert_eq!(
        image["key_id"].as_str(),
        CaptureKey::new(b"boot-key").as_ref().map(CaptureKey::id)
    );
    assert!(!captured.sql.contains("-- binds"), "{}", captured.sql);
    assert!(
        install_capture_key(Some(b"other")).is_err(),
        "the key is installed once"
    );
}
