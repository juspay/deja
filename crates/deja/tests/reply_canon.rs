//! A boundary whose CONTRACT says its reply is unordered — while the Rust type
//! says `Vec` — gets that fact stamped on the event by the codec that captured
//! it, from evidence the codec alone holds: the command it sent, the statement
//! it ran. Nobody declares a path. And it is stamped, not sorted: the reply is
//! recorded in the order it arrived.
#![allow(unused_braces)]

use serde_json::json;

fn recording_enabled() -> deja_context::ContextGuard {
    deja_context::enter(
        deja_context::ContextSnapshot::new("req-reply-canon").with_recording_decision(true),
    )
}

/// A scan: Redis returns a page of keys in hash-table order.
#[deja::redis(
    correlation = Some("req-reply-canon".to_string()),
    codec = SerdeCodec,
    args = { json!({ "command": "SCAN", "pattern": pattern }) },
)]
fn scan(pattern: &str) -> Vec<String> {
    let _ = pattern;
    // Deliberately NOT sorted, so the test can see the order is preserved.
    vec!["k3".to_owned(), "k1".to_owned(), "k2".to_owned()]
}

/// A get: one value, and its order is nobody's business.
#[deja::redis(
    correlation = Some("req-reply-canon".to_string()),
    codec = SerdeCodec,
    args = { json!({ "command": "GET", "key": key }) },
)]
fn get(key: &str) -> Vec<String> {
    let _ = key;
    vec!["only".to_owned()]
}

/// A range: Redis returns a LIST slice, and its order is the value.
#[deja::redis(
    correlation = Some("req-reply-canon".to_string()),
    codec = SerdeCodec,
    args = { json!({ "command": "LRANGE", "key": key }) },
)]
fn lrange(key: &str) -> Vec<String> {
    let _ = key;
    vec!["second".to_owned(), "first".to_owned()]
}

/// A site that records no command at all: nothing to derive from.
#[deja::redis(
    correlation = Some("req-reply-canon".to_string()),
    codec = SerdeCodec,
    args = { json!({ "key": key }) },
)]
fn commandless(key: &str) -> Vec<String> {
    let _ = key;
    vec!["b".to_owned(), "a".to_owned()]
}

#[test]
fn the_redis_kit_marks_an_unordered_reply_by_its_command_and_nothing_else() {
    let _rec = recording_enabled();
    let artifacts = tempfile::tempdir().expect("tempdir");
    deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::Recording(
        std::sync::Arc::new(
            deja_runtime::RecordingHook::new(artifacts.path()).expect("recording hook"),
        ),
    )))
    .expect("install recording hook");

    assert_eq!(scan("k*"), ["k3", "k1", "k2"]);
    assert_eq!(get("k"), ["only"]);
    assert_eq!(lrange("l"), ["second", "first"]);
    assert_eq!(commandless("k"), ["b", "a"]);

    deja_runtime::flush_global_hook().expect("flush events");
    let events = deja_runtime::read_events(artifacts.path()).expect("events");
    assert_eq!(events.len(), 4);

    let canon_of = |index: usize| -> Option<String> {
        events[index]
            .declaration
            .as_ref()
            .and_then(|declaration| declaration.reply_canon.as_ref())
            .map(|canon| canon.id.clone())
    };

    // SCAN: the reply canon names the reply's rows as a bag…
    assert_eq!(
        canon_of(0).as_deref(),
        Some(deja::codec::UNORDERED_VALUE_ROWS_CANON),
        "a SCAN reply is marked unordered"
    );
    // …and the rows themselves are exactly as they arrived. MARKED, not sorted.
    assert_eq!(
        events[0].result,
        json!(["k3", "k1", "k2"]),
        "the recorded reply keeps its arrival order"
    );

    // Everything else is untouched: a GET, a LIST read whose order IS the value,
    // and a site that never said which command it sent.
    assert_eq!(canon_of(1), None, "a GET reply is not marked");
    assert_eq!(
        canon_of(2),
        None,
        "an LRANGE reply is ordered and stays unmarked"
    );
    assert_eq!(canon_of(3), None, "no recorded command, no derivation");

    // The kit's static declaration survives beside the stamped clause: the redis
    // effect is still declared on the marked event.
    assert_eq!(
        events[0]
            .declaration
            .as_ref()
            .and_then(|declaration| declaration.effect),
        Some(deja::EffectKind::Redis),
        "the site's static declaration is composed with, not replaced by, the stamp"
    );
}

#[test]
fn a_statement_orders_its_rows_only_at_the_top_level() {
    use deja::db::sql_has_top_level_order_by as ordered;

    assert!(!ordered(r#"SELECT "a", "b" FROM "t" WHERE "x" = $1"#));
    assert!(ordered(r#"SELECT "a" FROM "t" ORDER BY "a""#));
    assert!(
        ordered(r#"select "a" from "t" order by "a" desc"#),
        "case-insensitive"
    );
    assert!(
        !ordered(r#"SELECT "a" FROM "t" WHERE "b" IN (SELECT "b" FROM "u" ORDER BY "c")"#),
        "an ORDER BY inside a subquery orders the subquery, not these rows"
    );
    assert!(
        !ordered(r#"SELECT "a" FROM "t" WHERE "note" = 'please ORDER BY name'"#),
        "the words inside a string literal are not a clause"
    );
    assert!(
        !ordered(r#"SELECT "a" FROM "t" WHERE "id" = $1 -- binds: ["ORDER BY x"]"#),
        "diesel's bind list is not the statement"
    );
    assert!(
        ordered(r#"UPDATE "t" SET "a" = $1 WHERE "b" = $2 RETURNING "a" ORDER BY "a""#),
        "a RETURNING clause can be ordered too"
    );
    assert!(
        !ordered(r#"UPDATE "t" SET "a" = $1 WHERE "b" = $2 RETURNING "a""#),
        "…and usually is not"
    );
    // Conservative on anything it cannot follow: answer "ordered", which leaves
    // the recorded result exactly as it was before this existed.
    assert!(
        ordered(r#"SELECT "a" FROM "t" WHERE "b" IN (SELECT "b""#),
        "unbalanced parens"
    );
    assert!(
        ordered(r#"SELECT "a" FROM "t" WHERE "note" = 'unterminated"#),
        "unterminated quote"
    );
}

#[cfg(feature = "error-stack")]
#[test]
fn the_db_codec_marks_a_row_set_that_no_statement_ordered() {
    use deja::db::{recorded_output, StateAxis};

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Row {
        id: u32,
    }
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Db;
    impl std::fmt::Display for Db {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("db")
        }
    }
    impl error_stack::Context for Db {}

    let rows: Result<Vec<Row>, error_stack::Report<Db>> = Ok(vec![Row { id: 2 }, Row { id: 1 }]);
    let one: Result<Row, error_stack::Report<Db>> = Ok(Row { id: 7 });
    let failed: Result<Vec<Row>, error_stack::Report<Db>> = Err(error_stack::report!(Db));

    let unordered = recorded_output(
        StateAxis::Read,
        "t",
        r#"SELECT "id" FROM "t" WHERE "x" = $1"#,
        &rows,
    );
    assert_eq!(
        unordered
            .reply_canon
            .as_ref()
            .map(|canon| canon.id.as_str()),
        Some(deja::codec::UNORDERED_VALUE_ROWS_CANON),
        "rows with no ORDER BY are a bag by the statement's own contract"
    );
    // MARKED, not sorted: the rows keep the order the database sent them in.
    assert_eq!(unordered.result["value"], json!([{ "id": 2 }, { "id": 1 }]));

    let ordered = recorded_output(
        StateAxis::Read,
        "t",
        r#"SELECT "id" FROM "t" ORDER BY "id""#,
        &rows,
    );
    assert!(
        ordered.reply_canon.is_none(),
        "an ORDER BY makes the order the value"
    );

    let single = recorded_output(StateAxis::Read, "t", r#"SELECT "id" FROM "t""#, &one);
    assert!(
        single.reply_canon.is_none(),
        "one row is not a collection of rows"
    );

    let err = recorded_output(StateAxis::Read, "t", r#"SELECT "id" FROM "t""#, &failed);
    assert!(
        err.reply_canon.is_none(),
        "an error arm has no rows to mark"
    );
}
