//! `ResultCodec<T, E>` + `db::recorded_output` — the fold's typed-error
//! contract: "recording threw ⇒ replay throws the SAME typed error", plus the
//! explicit-producer state-key derivation (result rows + SQL binds).
#![cfg(feature = "error-stack")]

use deja::codec::{ReplayCodec, ResultCodec};
use deja::db::{binds_read_keys, recorded_output, StateAxis};

/// Stand-in for `diesel_models::errors::DatabaseError` — fieldless + serde.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum DbError {
    NotFound,
    UniqueViolation,
    Others,
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl error_stack::Context for DbError {}

type DbResult<T> = Result<T, error_stack::Report<DbError>>;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct UserRow {
    user_id: String,
    merchant_id: String,
}

fn sample_row() -> UserRow {
    UserRow {
        user_id: "user_1".to_owned(),
        merchant_id: "merch_1".to_owned(),
    }
}

#[test]
fn ok_arm_round_trips() {
    let ok: DbResult<UserRow> = Ok(sample_row());
    let (envelope, is_error) = ResultCodec::<UserRow, DbError>::capture(&ok);
    assert!(!is_error);
    let back = ResultCodec::<UserRow, DbError>::reconstruct(envelope)
        .expect("ok envelope reconstructs");
    assert_eq!(back.expect("ok arm"), sample_row());
}

#[test]
fn err_arm_round_trips_the_same_typed_context() {
    let err: DbResult<UserRow> = Err(error_stack::report!(DbError::UniqueViolation));
    let (envelope, is_error) = ResultCodec::<UserRow, DbError>::capture(&err);
    assert!(is_error);
    assert_eq!(
        envelope.get("kind").and_then(serde_json::Value::as_str),
        Some("UniqueViolation"),
        "fieldless enum kind must serialize as the bare variant string \
         (byte-compatible with the legacy hand-rolled mapping)"
    );
    let back = ResultCodec::<UserRow, DbError>::reconstruct(envelope)
        .expect("err envelope reconstructs");
    match back {
        Err(report) => assert_eq!(*report.current_context(), DbError::UniqueViolation),
        Ok(_) => panic!("expected the Err arm"),
    }
}

#[test]
fn envelope_is_parseable_as_the_legacy_database_result() {
    // Compat contract: downstream envelope consumers (seed planner, row-key
    // visitor) parse DejaDatabaseResult; the generic codec must emit that shape.
    let ok: DbResult<UserRow> = Ok(sample_row());
    let (envelope, _) = ResultCodec::<UserRow, DbError>::capture(&ok);
    let parsed: deja::value::DejaDatabaseResult =
        serde_json::from_value(envelope).expect("parses as DejaDatabaseResult");
    match parsed.payload {
        deja::value::DejaDatabaseResultPayload::Ok { value, .. } => {
            assert_eq!(
                value.get("user_id").and_then(serde_json::Value::as_str),
                Some("user_1")
            );
        }
        other => panic!("expected Ok payload, got {other:?}"),
    }

    let err: DbResult<UserRow> = Err(error_stack::report!(DbError::NotFound));
    let (envelope, _) = ResultCodec::<UserRow, DbError>::capture(&err);
    let parsed: deja::value::DejaDatabaseResult =
        serde_json::from_value(envelope).expect("err parses as DejaDatabaseResult");
    match parsed.payload {
        deja::value::DejaDatabaseResultPayload::Err { kind, .. } => {
            assert_eq!(kind, "NotFound");
        }
        other => panic!("expected Err payload, got {other:?}"),
    }
}

#[test]
fn unknown_kind_is_a_reconstruction_failure_not_a_fabrication() {
    // A recorded kind the candidate's error type no longer names (e.g. the
    // legacy collapsed "Other") must fail reconstruction so the seam
    // fail-stops instead of inventing an error.
    let envelope = serde_json::json!({
        "version": 1,
        "result": "Err",
        "kind": "Other",
        "message": "legacy collapsed kind",
    });
    assert!(ResultCodec::<UserRow, DbError>::reconstruct(envelope).is_none());
}

#[test]
fn binds_parser_derives_a_row_exact_read_key() {
    let sql = r#"SELECT "users"."user_id" FROM "users" WHERE "users"."user_id" = $1 -- binds: ["user_42"]"#;
    let keys = binds_read_keys("users", sql);
    assert_eq!(keys.len(), 1, "one pk equality bind → one key: {keys:?}");
    // The wire form must be the SAME row-exact key a result row would produce.
    let expected = deja::db::row_state_key("users", &serde_json::json!({ "user_id": "user_42" }))
        .expect("known pk column yields a row key")
        .to_wire();
    assert_eq!(keys[0], expected);
}

#[test]
fn binds_parser_never_guesses() {
    // Unknown table → no pragmatic PK → no keys.
    assert!(binds_read_keys("unknown_table", r#"SELECT 1 WHERE "id" = $1 -- binds: ["x"]"#).is_empty());
    // Unparseable binds (rich debug shapes) → no keys, no panic.
    assert!(binds_read_keys(
        "users",
        r#"SELECT 1 WHERE "user_id" = $1 -- binds: [SomeEnum::Variant]"#
    )
    .is_empty());
    // No binds marker at all → no keys.
    assert!(binds_read_keys("users", r#"SELECT 1"#).is_empty());
}

#[test]
fn notfound_read_records_the_binds_read_key() {
    // THE gap this exists to close: a NotFound read has no result row, but its
    // identity must still land in the read set for seed planning.
    let err: DbResult<UserRow> = Err(error_stack::report!(DbError::NotFound));
    let sql = r#"SELECT * FROM "users" WHERE "users"."user_id" = $1 -- binds: ["ghost_user"]"#;
    let output = recorded_output(StateAxis::Read, "users", sql, &err);
    assert!(output.is_error);
    assert_eq!(output.read_set.len(), 1, "binds key expected: {:?}", output.read_set);
    assert!(output.write_set.is_empty());
}

#[test]
fn ok_read_records_row_keys_and_image() {
    let ok: DbResult<UserRow> = Ok(sample_row());
    let sql = r#"SELECT * FROM "users" WHERE "users"."user_id" = $1 -- binds: ["user_1"]"#;
    let output = recorded_output(StateAxis::Read, "users", sql, &ok);
    assert!(!output.is_error);
    assert!(
        !output.read_set.is_empty(),
        "result-row key expected: {:?}",
        output.read_set
    );
    assert!(output.write_set.is_empty());
    assert!(output.result_image.is_some(), "row image expected");
}

#[test]
fn touch_axis_records_both_sides() {
    let ok: DbResult<UserRow> = Ok(sample_row());
    let sql = r#"UPDATE "users" SET "merchant_id" = $1 WHERE "users"."user_id" = $2 -- binds: ["m2", "user_1"]"#;
    let output = recorded_output(StateAxis::Touch, "users", sql, &ok);
    assert!(!output.read_set.is_empty());
    assert!(!output.write_set.is_empty());
}
