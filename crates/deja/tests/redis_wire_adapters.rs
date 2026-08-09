//! `value::RedisWireValue` client adapters (#45) and serde-shape pins.
//!
//! Two guarantees, tested separately:
//!
//! 1. **Shape pins** (always compiled): the JSON encoding of every variant is
//!    byte-identical to what the vendor twin enums (`DejaRedisValue` in
//!    hyperswitch's `redis_interface`, one per client module) emit today, and
//!    both twins' historical encodings decode. This is what lets the twins be
//!    deleted without re-recording a single tape.
//! 2. **Round trips** (per client feature): `client → RedisWireValue →
//!    client` is identity for every variant the client owns, and the
//!    cross-dialect / unsupported variants map exactly as documented — loudly,
//!    never silently.

use deja::value::RedisWireValue as Wire;

/// Canonical encodings — the `redis_rs` twin's dialect, which is also what
/// `RedisWireValue` has always emitted. Expected strings are extracted from
/// the twin's serde shape (plain externally-tagged derive), so a recorded
/// tape and a post-#45 tape are byte-identical.
#[test]
fn serde_shape_pins_match_the_redis_rs_twin() {
    let to = |v: &Wire| serde_json::to_string(v).expect("wire value serializes");

    assert_eq!(to(&Wire::Null), r#""Null""#);
    assert_eq!(to(&Wire::Int(42)), r#"{"Int":42}"#);
    assert_eq!(
        to(&Wire::BulkString(vec![104, 105])),
        r#"{"BulkString":[104,105]}"#
    );
    assert_eq!(
        to(&Wire::Array(vec![
            Wire::Int(1),
            Wire::SimpleString("a".to_owned())
        ])),
        r#"{"Array":[{"Int":1},{"SimpleString":"a"}]}"#
    );
    assert_eq!(
        to(&Wire::SimpleString("pending".to_owned())),
        r#"{"SimpleString":"pending"}"#
    );
    assert_eq!(to(&Wire::Okay), r#""Okay""#);
    assert_eq!(
        to(&Wire::Map(vec![(
            Wire::SimpleString("f".to_owned()),
            Wire::Int(7)
        )])),
        r#"{"Map":[[{"SimpleString":"f"},{"Int":7}]]}"#
    );
    assert_eq!(
        to(&Wire::Attribute {
            data: Box::new(Wire::Int(1)),
            attributes: vec![(Wire::SimpleString("ttl".to_owned()), Wire::Int(3))],
        }),
        r#"{"Attribute":{"data":{"Int":1},"attributes":[[{"SimpleString":"ttl"},{"Int":3}]]}}"#
    );
    assert_eq!(
        to(&Wire::Set(vec![Wire::SimpleString("m".to_owned())])),
        r#"{"Set":[{"SimpleString":"m"}]}"#
    );
    assert_eq!(to(&Wire::Double(0.5)), r#"{"Double":0.5}"#);
    assert_eq!(to(&Wire::Boolean(true)), r#"{"Boolean":true}"#);
    assert_eq!(
        to(&Wire::VerbatimString {
            format: "txt".to_owned(),
            text: "hi".to_owned(),
        }),
        r#"{"VerbatimString":{"format":"txt","text":"hi"}}"#
    );
    assert_eq!(
        to(&Wire::Push {
            kind: "message".to_owned(),
            data: vec![Wire::Int(1)],
        }),
        r#"{"Push":{"kind":"message","data":[{"Int":1}]}}"#
    );
    assert_eq!(
        to(&Wire::UnsupportedSuccessfulValue {
            variant: "BigNumber".to_owned(),
        }),
        r#"{"UnsupportedSuccessfulValue":{"variant":"BigNumber"}}"#
    );
    // `Queued` is emitted by the FRED twin (the redis_rs client has no such
    // variant); its canonical encoding pins to that twin's byte shape.
    assert_eq!(to(&Wire::Queued), r#""Queued""#);
}

/// The fred twin's dialect (`Integer`/`String`/`Bytes` for the same concepts)
/// decodes through `#[serde(alias)]` — old fred-recorded tapes keep working.
#[test]
fn serde_shape_pins_decode_the_fred_twin_dialect() {
    let from = |s: &str| serde_json::from_str::<Wire>(s).expect("fred-dialect tape decodes");

    assert_eq!(from(r#""Null""#), Wire::Null);
    assert_eq!(from(r#"{"Integer":42}"#), Wire::Int(42));
    assert_eq!(from(r#"{"Double":0.5}"#), Wire::Double(0.5));
    assert_eq!(
        from(r#"{"String":"merchant_xyz"}"#),
        Wire::SimpleString("merchant_xyz".to_owned())
    );
    assert_eq!(
        from(r#"{"Bytes":[104,105]}"#),
        Wire::BulkString(vec![104, 105])
    );
    assert_eq!(from(r#""Queued""#), Wire::Queued);
    assert_eq!(from(r#"{"Boolean":false}"#), Wire::Boolean(false));
    assert_eq!(
        from(r#"{"Array":[{"Integer":1},{"String":"a"}]}"#),
        Wire::Array(vec![Wire::Int(1), Wire::SimpleString("a".to_owned())])
    );
    // The fred twin converts map keys through `RedisValue::Bytes`, so a
    // recorded fred map carries `Bytes` keys.
    assert_eq!(
        from(r#"{"Map":[[{"Bytes":[102]},{"Integer":7}]]}"#),
        Wire::Map(vec![(Wire::BulkString(vec![102]), Wire::Int(7))])
    );
}

/// Every canonical encoding also DECODES (the tape is read by the same type
/// that wrote it), including the structural variants.
#[test]
fn serde_shape_pins_round_trip_through_json() {
    let values = vec![
        Wire::Null,
        Wire::Int(-3),
        Wire::BulkString(vec![0, 255]),
        Wire::SimpleString("s".to_owned()),
        Wire::Double(2.25),
        Wire::Boolean(false),
        Wire::Array(vec![Wire::Null, Wire::Int(1)]),
        Wire::Map(vec![(Wire::SimpleString("k".to_owned()), Wire::Int(1))]),
        Wire::Set(vec![Wire::SimpleString("m".to_owned())]),
        Wire::Okay,
        Wire::Queued,
        Wire::Attribute {
            data: Box::new(Wire::Boolean(true)),
            attributes: vec![(Wire::SimpleString("a".to_owned()), Wire::Int(9))],
        },
        Wire::VerbatimString {
            format: "mkd".to_owned(),
            text: "# t".to_owned(),
        },
        Wire::Push {
            kind: "invalidate".to_owned(),
            data: vec![Wire::BulkString(vec![107])],
        },
        Wire::UnsupportedSuccessfulValue {
            variant: "ServerError(ERR: boom)".to_owned(),
        },
    ];
    for value in values {
        let json = serde_json::to_string(&value).expect("serializes");
        let back: Wire = serde_json::from_str(&json).expect("its own encoding decodes");
        assert_eq!(back, value, "round trip through {json}");
    }
}

#[cfg(feature = "fred")]
mod fred_roundtrip {
    use fred::types::RedisValue as Fred;

    use super::Wire;

    /// `fred → wire → fred` is identity for every variant fred owns.
    #[test]
    fn every_fred_variant_round_trips() {
        let map = fred::types::RedisMap::try_from(vec![("f", 7_i64)]).expect("literal map");
        let values = vec![
            Fred::Null,
            Fred::Boolean(true),
            Fred::Integer(7),
            Fred::Double(0.5),
            Fred::String("st".into()),
            Fred::Bytes(vec![1, 2].into()),
            Fred::Queued,
            Fred::Array(vec![Fred::Integer(1), Fred::String("a".into())]),
            Fred::Map(map),
        ];
        for value in values {
            let wire = Wire::from(value.clone());
            let back = Fred::try_from(wire.clone())
                .unwrap_or_else(|e| panic!("{wire:?} must convert back to fred: {e:?}"));
            assert_eq!(back, value, "round trip through {wire:?}");
        }
    }

    /// The wire mapping matches the fred twin's semantic choices exactly:
    /// `String` → `to_string`, `Bytes` → `to_vec`, map keys through fred's own
    /// key→value conversion (`Bytes`).
    #[test]
    fn fred_capture_mapping_matches_the_twin() {
        assert_eq!(
            Wire::from(Fred::String("st".into())),
            Wire::SimpleString("st".to_owned())
        );
        assert_eq!(
            Wire::from(Fred::Bytes(vec![1, 2].into())),
            Wire::BulkString(vec![1, 2])
        );
        assert_eq!(Wire::from(Fred::Integer(7)), Wire::Int(7));
        assert_eq!(Wire::from(Fred::Queued), Wire::Queued);
        let map = fred::types::RedisMap::try_from(vec![("f", 7_i64)]).expect("literal map");
        assert_eq!(
            Wire::from(Fred::Map(map)),
            // fred's `From<RedisKey> for RedisValue` yields `Bytes`, so the
            // recorded key is a `BulkString` — same as the twin recorded.
            Wire::Map(vec![(Wire::BulkString(b"f".to_vec()), Wire::Int(7))])
        );
    }

    /// Cross-dialect variants map to the value fred itself decodes from the
    /// same RESP frame; unrepresentable ones error loudly.
    #[test]
    fn redis_rs_dialect_variants_map_or_refuse_loudly() {
        // `+OK` decodes as `String("OK")` in fred.
        assert_eq!(
            Fred::try_from(Wire::Okay).expect("Okay maps"),
            Fred::String("OK".into())
        );
        // fred returns RESP set replies as `Array`.
        assert_eq!(
            Fred::try_from(Wire::Set(vec![Wire::Int(1)])).expect("Set maps"),
            Fred::Array(vec![Fred::Integer(1)])
        );
        // No fred representation: loud error, never a guess.
        for wire in [
            Wire::Attribute {
                data: Box::new(Wire::Int(1)),
                attributes: vec![],
            },
            Wire::VerbatimString {
                format: "txt".to_owned(),
                text: "hi".to_owned(),
            },
            Wire::Push {
                kind: "message".to_owned(),
                data: vec![],
            },
            Wire::UnsupportedSuccessfulValue {
                variant: "BigNumber".to_owned(),
            },
        ] {
            let error = Fred::try_from(wire.clone())
                .expect_err("a variant fred cannot represent must refuse");
            assert_eq!(
                *error.kind(),
                fred::error::RedisErrorKind::InvalidArgument,
                "{wire:?}"
            );
        }
    }

    /// A nested unrepresentable member fails the whole structure — replay
    /// must never hand back a silently truncated collection.
    #[test]
    fn nested_unrepresentable_member_fails_the_container() {
        let wire = Wire::Array(vec![
            Wire::Int(1),
            Wire::UnsupportedSuccessfulValue {
                variant: "BigNumber".to_owned(),
            },
        ]);
        assert!(Fred::try_from(wire).is_err());
    }
}

#[cfg(feature = "redis-rs")]
mod redis_rs_roundtrip {
    use redis::Value as Rs;

    use super::Wire;

    /// `redis-rs → wire → redis-rs` is identity for every representable
    /// variant redis-rs owns.
    #[test]
    fn every_representable_redis_rs_variant_round_trips() {
        let values = vec![
            Rs::Nil,
            Rs::Int(7),
            Rs::BulkString(vec![1, 2]),
            Rs::Array(vec![Rs::Int(1), Rs::SimpleString("a".to_owned())]),
            Rs::SimpleString("pending".to_owned()),
            Rs::Okay,
            Rs::Map(vec![(Rs::SimpleString("f".to_owned()), Rs::Int(7))]),
            Rs::Attribute {
                data: Box::new(Rs::Int(1)),
                attributes: vec![(Rs::SimpleString("ttl".to_owned()), Rs::Int(3))],
            },
            Rs::Set(vec![Rs::SimpleString("m".to_owned())]),
            Rs::Double(0.5),
            Rs::Boolean(true),
            Rs::VerbatimString {
                format: redis::VerbatimFormat::Text,
                text: "hi".to_owned(),
            },
            Rs::VerbatimString {
                format: redis::VerbatimFormat::Markdown,
                text: "# t".to_owned(),
            },
            Rs::VerbatimString {
                format: redis::VerbatimFormat::Unknown("xyz".to_owned()),
                text: "?".to_owned(),
            },
            Rs::Push {
                kind: redis::PushKind::Message,
                data: vec![Rs::BulkString(vec![107])],
            },
            Rs::Push {
                kind: redis::PushKind::Other("custom".to_owned()),
                data: vec![],
            },
        ];
        for value in values {
            let wire = Wire::from(value.clone());
            let back = Rs::try_from(wire.clone())
                .unwrap_or_else(|e| panic!("{wire:?} must convert back to redis-rs: {e:?}"));
            assert_eq!(back, value, "round trip through {wire:?}");
        }
    }

    /// The wire mapping matches the redis_rs twin's semantic choices exactly.
    #[test]
    fn redis_rs_capture_mapping_matches_the_twin() {
        assert_eq!(Wire::from(Rs::Nil), Wire::Null);
        assert_eq!(Wire::from(Rs::Okay), Wire::Okay);
        assert_eq!(
            Wire::from(Rs::VerbatimString {
                format: redis::VerbatimFormat::Markdown,
                text: "t".to_owned(),
            }),
            // `VerbatimFormat` records via Display — "mkd", as the twin did.
            Wire::VerbatimString {
                format: "mkd".to_owned(),
                text: "t".to_owned(),
            }
        );
        assert_eq!(
            Wire::from(Rs::Push {
                kind: redis::PushKind::Disconnection,
                data: vec![],
            }),
            Wire::Push {
                kind: "disconnection".to_owned(),
                data: vec![],
            }
        );
        // BigNumber has no faithful wire mapping: recorded NAMED, exactly as
        // the twin recorded it.
        assert_eq!(
            Wire::from(Rs::BigNumber(Default::default())),
            Wire::UnsupportedSuccessfulValue {
                variant: "BigNumber".to_owned(),
            }
        );
    }

    /// An unsupported recording refuses to convert back — same error kind and
    /// wording class as the twin's replay path.
    #[test]
    fn unsupported_recording_refuses_replay_back() {
        let error = Rs::try_from(Wire::UnsupportedSuccessfulValue {
            variant: "BigNumber".to_owned(),
        })
        .expect_err("an unsupported recording must not silently replay");
        assert_eq!(error.kind(), redis::ErrorKind::UnexpectedReturnType);
    }

    /// The fred-dialect `Queued` maps to what redis-rs decodes from the same
    /// `+QUEUED` frame: a plain status string (only `+OK` gets `Okay`).
    #[test]
    fn fred_dialect_queued_maps_to_simple_string() {
        assert_eq!(
            Rs::try_from(Wire::Queued).expect("Queued maps"),
            Rs::SimpleString("QUEUED".to_owned())
        );
    }
}
