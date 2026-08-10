//! The physical/semantic two-representation split on db row images
//! (docs/design/wire-faithful-seeding.md): `row_image_payload_with_wire`
//! attaches wrapper-captured binary wire values to the serde row images, and
//! `recorded_output_with_wire` receives the capture IN-BAND from the boundary
//! that took it off the query's own return path. Every rejection path must
//! fall back to the plain semantic payload — a physical image is dropped on
//! doubt, never misattached.

use deja::db::{row_image_payload, row_image_payload_with_wire, DbRowImage, WireColumn, WireRow};

fn wire_column(name: &str, oid: u32, bytes: &[u8]) -> WireColumn {
    WireColumn {
        name: name.to_string(),
        type_oid: Some(oid),
        bytes: Some(bytes.to_vec()),
    }
}

fn null_wire_column(name: &str) -> WireColumn {
    WireColumn {
        name: name.to_string(),
        type_oid: None,
        bytes: None,
    }
}

fn parse_row(payload: serde_json::Value) -> DbRowImage {
    serde_json::from_value(payload).expect("payload parses as a typed row image")
}

#[test]
fn single_row_attaches_wire_hex_oid_and_format() {
    // The issue #35 shape: an externally tagged enum in the SEMANTIC value of
    // a varchar column, while the PHYSICAL wire value is the raw text the
    // database actually sent. No content comparison happens between the two —
    // their disagreement is the reason the physical image exists.
    let value = serde_json::json!({
        "attempt_id": "att_1",
        "connector_transaction_id": {"TxnId": "txn_raw"},
        "capture_on": null,
    });
    let wire = [WireRow {
        columns: vec![
            wire_column("attempt_id", 1043, b"att_1"),
            wire_column("connector_transaction_id", 1043, b"txn_raw"),
            null_wire_column("capture_on"),
        ],
    }];
    let payload =
        row_image_payload_with_wire("payment_attempt", &value, &wire).expect("payload built");
    let row = parse_row(payload);
    assert_eq!(row.wire_format.as_deref(), Some("binary"));

    let by_name = |name: &str| {
        row.columns
            .iter()
            .find(|column| column.name == name)
            .expect("column present")
    };
    let poison = by_name("connector_transaction_id");
    assert_eq!(poison.wire.as_deref(), Some("74786e5f726177")); // "txn_raw"
    assert_eq!(poison.type_oid, Some(1043));
    assert_eq!(
        poison.value,
        serde_json::json!({"TxnId": "txn_raw"}),
        "the semantic value stays exactly as recorded today"
    );
    let null_column = by_name("capture_on");
    assert_eq!(null_column.wire, None, "SQL NULL carries no wire bytes");
    assert!(null_column.value.is_null());
}

#[test]
fn multi_row_results_pair_by_index() {
    let value = serde_json::json!([
        {"id": "a"},
        {"id": "b"},
    ]);
    let wire = [
        WireRow {
            columns: vec![wire_column("id", 25, b"a")],
        },
        WireRow {
            columns: vec![wire_column("id", 25, b"b")],
        },
    ];
    let payload = row_image_payload_with_wire("t", &value, &wire).expect("payload built");
    let rows = payload.as_array().expect("array payload").clone();
    let first = parse_row(rows[0].clone());
    let second = parse_row(rows[1].clone());
    assert_eq!(first.columns[0].wire.as_deref(), Some("61"));
    assert_eq!(second.columns[0].wire.as_deref(), Some("62"));
}

#[test]
fn wider_capture_pairs_the_consumed_prefix() {
    // get_result() consumes only the first row of a wider result set: the
    // serde value carries one row while the capture may carry more. Prefix
    // pairing is correct (row 0 is row 0); the extras are ignored.
    let value = serde_json::json!({"id": "a"});
    let wire = [
        WireRow {
            columns: vec![wire_column("id", 25, b"a")],
        },
        WireRow {
            columns: vec![wire_column("id", 25, b"b")],
        },
    ];
    let payload = row_image_payload_with_wire("t", &value, &wire).expect("payload built");
    let row = parse_row(payload);
    assert_eq!(row.columns[0].wire.as_deref(), Some("61"));
}

#[test]
fn fewer_captured_rows_than_serde_rows_drops_the_attachment() {
    let value = serde_json::json!([{"id": "a"}, {"id": "b"}]);
    let wire = [WireRow {
        columns: vec![wire_column("id", 25, b"a")],
    }];
    let payload = row_image_payload_with_wire("t", &value, &wire).expect("payload built");
    assert_eq!(
        payload,
        row_image_payload("t", &value).expect("plain payload"),
        "an unjoinable capture must yield the byte-identical semantic payload"
    );
}

#[test]
fn missing_column_name_drops_the_attachment() {
    let value = serde_json::json!({"id": "a", "status": "ok"});
    let wire = [WireRow {
        columns: vec![wire_column("id", 25, b"a")],
    }];
    let payload = row_image_payload_with_wire("t", &value, &wire).expect("payload built");
    assert_eq!(payload, row_image_payload("t", &value).expect("plain"));
}

#[test]
fn duplicate_column_name_drops_the_attachment() {
    let value = serde_json::json!({"id": "a"});
    let wire = [WireRow {
        columns: vec![wire_column("id", 25, b"a"), wire_column("id", 25, b"b")],
    }];
    let payload = row_image_payload_with_wire("t", &value, &wire).expect("payload built");
    assert_eq!(payload, row_image_payload("t", &value).expect("plain"));
}

#[test]
fn null_misalignment_drops_the_attachment() {
    // Semantic null but wire bytes present (or vice versa) means the pairing
    // is wrong — drop the whole physical image.
    let value = serde_json::json!({"id": null});
    let wire = [WireRow {
        columns: vec![wire_column("id", 25, b"a")],
    }];
    let payload = row_image_payload_with_wire("t", &value, &wire).expect("payload built");
    assert_eq!(payload, row_image_payload("t", &value).expect("plain"));

    let value = serde_json::json!({"id": "a"});
    let wire = [WireRow {
        columns: vec![null_wire_column("id")],
    }];
    let payload = row_image_payload_with_wire("t", &value, &wire).expect("payload built");
    assert_eq!(payload, row_image_payload("t", &value).expect("plain"));
}

#[test]
fn one_bad_row_drops_the_attachment_for_the_whole_result() {
    // All-or-nothing: a capture that fails validation on any row is suspect
    // as a whole (it may be a swapped result set).
    let value = serde_json::json!([{"id": "a"}, {"id": null}]);
    let wire = [
        WireRow {
            columns: vec![wire_column("id", 25, b"a")],
        },
        WireRow {
            columns: vec![wire_column("id", 25, b"b")], // null misalignment
        },
    ];
    let payload = row_image_payload_with_wire("t", &value, &wire).expect("payload built");
    assert_eq!(payload, row_image_payload("t", &value).expect("plain"));
}

#[test]
fn plain_payload_carries_no_wire_keys() {
    // Old-tape control: the semantic payload is byte-identical to what the
    // recorder emitted before wire capture existed.
    let value = serde_json::json!({"id": "a"});
    let payload = row_image_payload("t", &value).expect("plain payload");
    let rendered = serde_json::to_string(&payload).expect("serializes");
    assert!(!rendered.contains("wire"));
}

#[cfg(feature = "error-stack")]
mod recorded_output_in_band {
    use super::*;
    use deja::db::{recorded_output, recorded_output_with_wire, StateAxis};

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    enum DbError {
        NotFound,
    }
    impl std::fmt::Display for DbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }
    impl error_stack::Context for DbError {}

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct AttemptRow {
        attempt_id: String,
        connector_transaction_id: Option<serde_json::Value>,
    }

    #[test]
    fn the_passed_wire_rows_become_the_physical_image() {
        // The capture arrives as an argument — the same pair the captured
        // query helpers return — so there is nothing ambient to be evicted or
        // out of scope. The `sql` plays no part in the pairing.
        let boundary = r#"SELECT * FROM "wi_attempt" WHERE "wi_attempt"."attempt_id" = $1 -- binds: ["att_1"]"#;
        let wire = vec![WireRow {
            columns: vec![
                wire_column("attempt_id", 1043, b"att_1"),
                wire_column("connector_transaction_id", 1043, b"txn_raw"),
            ],
        }];
        let ok: Result<AttemptRow, error_stack::Report<DbError>> = Ok(AttemptRow {
            attempt_id: "att_1".into(),
            connector_transaction_id: Some(serde_json::json!({"TxnId": "txn_raw"})),
        });
        let output =
            recorded_output_with_wire(StateAxis::Read, "wi_attempt", boundary, &ok, Some(&wire));
        let image = output.result_image.expect("row image expected");
        let row: DbRowImage = serde_json::from_value(image).expect("typed image");
        assert_eq!(row.wire_format.as_deref(), Some("binary"));
        let poison = row
            .columns
            .iter()
            .find(|column| column.name == "connector_transaction_id")
            .expect("column");
        assert_eq!(poison.wire.as_deref(), Some("74786e5f726177"));
        assert_eq!(poison.type_oid, Some(1043));
    }

    #[test]
    fn an_err_result_attaches_no_image_whatever_rode_along() {
        // The pair shape delivers a wire Option even for Err results (the take
        // ran in the same closure); the producer must ignore it — there is no
        // Ok value to image.
        let sql = r#"SELECT * FROM "wi_gone" WHERE "wi_gone"."id" = $1 -- binds: ["g1"]"#;
        let stale = vec![WireRow {
            columns: vec![wire_column("id", 1043, b"g1")],
        }];
        let err: Result<AttemptRow, error_stack::Report<DbError>> =
            Err(error_stack::report!(DbError::NotFound));
        let output = recorded_output_with_wire(StateAxis::Read, "wi_gone", sql, &err, Some(&stale));
        assert!(output.is_error);
        assert!(output.result_image.is_none());
    }

    #[test]
    fn no_wire_means_the_plain_image_unchanged() {
        let sql = r#"SELECT * FROM "wi_plain" WHERE "wi_plain"."id" = $1 -- binds: ["p1"]"#;
        let ok: Result<AttemptRow, error_stack::Report<DbError>> = Ok(AttemptRow {
            attempt_id: "p1".into(),
            connector_transaction_id: None,
        });
        let with_none = recorded_output_with_wire(StateAxis::Read, "wi_plain", sql, &ok, None);
        let image = with_none.result_image.clone().expect("row image expected");
        let rendered = serde_json::to_string(&image).expect("serializes");
        assert!(
            !rendered.contains("wire"),
            "old-tape behavior: no wire keys without a capture"
        );

        // And the plain producer is exactly the `wire: None` case — writes and
        // uncaptured paths keep their byte-identical payload.
        let plain = recorded_output(StateAxis::Read, "wi_plain", sql, &ok);
        assert_eq!(plain.result_image, with_none.result_image);
        assert_eq!(plain.result, with_none.result);
    }
}
