use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fmt};

use deja_core::{
    group_events_into_cases,
    normalize_http_exchange_event,
    read_artifact_metadata,
    validate_correlation_health,
    AddressFamily,
    ArtifactBundle,
    ArtifactDescriptor,
    ArtifactError,
    ArtifactFidelitySummary,
    ArtifactLayoutDescriptor,
    ArtifactMetadataDocument,
    BoundaryEvent,
    CaptureFidelity,
    CorrelationStatus,
    DivergenceMarker,
    DnsBoundaryEvent,
    EnvironmentBoundaryEvent,
    EnvironmentOperation,
    EventCounts,
    EventDirection,
    EventRecord,
    HttpBodyRecord,
    HttpExchangeEvent,
    HttpHeader,
    HttpRequestRecord,
    HttpResponseRecord,
    InspectionSummaryDocument,
    PreloadBootstrap,
    PreloadMode,
    ProducerMetadata,
    ProtocolHint,
    RandomBoundaryEvent,
    RandomSource,
    RecordMetadata,
    ReplayClassification,
    SessionMetadata,
    // v2 types
    SocketBoundaryEvent,
    SocketOperation,
    SupportMatrixMetadata,
    SupportedBoundary,
    TargetMetadata,
    TimeBoundaryEvent,
    TimeSource,
    ARTIFACT_SCHEMA_VERSION_V1,
    EVENTS_FILE_NAME,
    INSPECTION_SUMMARY_FILE_NAME,
};

struct TestCase {
    name: &'static str,
    run: fn(),
}

#[derive(Debug, Default)]
struct TestRunnerArgs {
    list_only: bool,
    exact: bool,
    filter: Option<String>,
}

impl TestRunnerArgs {
    fn parse() -> Self {
        let mut args = Self::default();

        for argument in env::args().skip(1) {
            match argument.as_str() {
                "--list" => args.list_only = true,
                "--exact" => args.exact = true,
                "--nocapture" | "--quiet" => {}
                value if value.starts_with("--") => {}
                value if args.filter.is_none() => args.filter = Some(value.to_owned()),
                _ => {}
            }
        }

        args
    }

    fn matches(&self, name: &str) -> bool {
        match &self.filter {
            Some(filter) if self.exact => name == filter,
            Some(filter) => name.contains(filter),
            None => true,
        }
    }
}

fn main() {
    let args = TestRunnerArgs::parse();
    let tests = [
        TestCase {
            name: "artifact_round_trip",
            run: artifact_round_trip,
        },
        TestCase {
            name: "http_normalization_fixture_shape",
            run: http_normalization_fixture_shape,
        },
        TestCase {
            name: "corrupt_artifact_rejected",
            run: corrupt_artifact_rejected,
        },
        TestCase {
            name: "schema_mismatch_rejected",
            run: schema_mismatch_rejected,
        },
        TestCase {
            name: "socket_dns_event_round_trip",
            run: socket_dns_event_round_trip,
        },
        TestCase {
            name: "group_events_into_request_cases",
            run: group_events_into_request_cases,
        },
        TestCase {
            name: "request_id_based_grouping_interleaved",
            run: request_id_based_grouping_interleaved,
        },
        TestCase {
            name: "correlation_health_no_correlation",
            run: correlation_health_no_correlation,
        },
        TestCase {
            name: "correlation_health_healthy",
            run: correlation_health_healthy,
        },
        TestCase {
            name: "correlation_health_contamination_detected",
            run: correlation_health_contamination_detected,
        },
        TestCase {
            name: "correlation_health_orphans_detected",
            run: correlation_health_orphans_detected,
        },
        TestCase {
            name: "correlation_health_degraded_partial_coverage",
            run: correlation_health_degraded_partial_coverage,
        },
    ];

    let selected = tests
        .iter()
        .filter(|test| args.matches(test.name))
        .collect::<Vec<_>>();

    if args.list_only {
        for test in &selected {
            println!("{}: test", test.name);
        }

        return;
    }

    let mut failures = Vec::new();

    for test in selected {
        let result = panic::catch_unwind(AssertUnwindSafe(|| (test.run)()));
        if let Err(payload) = result {
            failures.push(TestFailure {
                name: test.name,
                message: PanicMessage(payload),
            });
        }
    }

    if failures.is_empty() {
        return;
    }

    for failure in failures {
        eprintln!("test '{}' failed: {}", failure.name, failure.message);
    }

    process::exit(1);
}

struct TestFailure {
    name: &'static str,
    message: PanicMessage,
}

struct PanicMessage(Box<dyn std::any::Any + Send>);

impl fmt::Display for PanicMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(message) = self.0.downcast_ref::<&'static str>() {
            formatter.write_str(message)
        } else if let Some(message) = self.0.downcast_ref::<String>() {
            formatter.write_str(message)
        } else {
            formatter.write_str("non-string panic payload")
        }
    }
}

fn artifact_round_trip() {
    let root = unique_temp_dir("artifact_round_trip");
    let artifact = sample_artifact_bundle();

    artifact
        .write_to_directory(&root)
        .expect("artifact should be written");

    let inspection = read_artifact_metadata(&root).expect("inspection metadata should load");
    assert_eq!(inspection.metadata, artifact.metadata);
    assert_eq!(inspection.metadata.preload, artifact.metadata.preload);
    assert_eq!(inspection.inspection_summary, artifact.inspection_summary);
    assert_eq!(inspection.layout.events_path, root.join(EVENTS_FILE_NAME));
    assert_eq!(
        inspection.layout.inspection_summary_path,
        root.join(INSPECTION_SUMMARY_FILE_NAME)
    );
    assert_eq!(
        inspection.metadata.support_matrix.supported_boundaries,
        vec![
            SupportedBoundary::Time,
            SupportedBoundary::Random,
            SupportedBoundary::Environment,
            SupportedBoundary::Http,
        ]
    );

    let reloaded = ArtifactBundle::read_from_directory(&root).expect("artifact should reload");
    assert_eq!(reloaded, artifact);

    assert!(matches!(
        reloaded.events[0].event,
        BoundaryEvent::Time(TimeBoundaryEvent {
            source: TimeSource::SystemTime,
            seconds: 1_711_708_800,
            nanos: 123_456_789,
        })
    ));
    assert!(matches!(
        reloaded.events[1].event,
        BoundaryEvent::Random(RandomBoundaryEvent {
            source: RandomSource::Getrandom,
            ref bytes,
        }) if bytes == &[7, 42, 99, 128]
    ));
    assert!(matches!(
        reloaded.events[2].event,
        BoundaryEvent::Environment(EnvironmentBoundaryEvent {
            operation: EnvironmentOperation::Get,
            ref key,
            value: Some(ref value),
        }) if key == "API_BASE_URL" && value == "http://127.0.0.1:8080"
    ));
    assert!(matches!(
        (&reloaded.events[3].metadata, &reloaded.events[3].event),
        (
            RecordMetadata {
                capture_fidelity: CaptureFidelity::Semantic,
                replay_classification: ReplayClassification::SemanticallyEquivalent,
                divergence_markers,
                ..
            },
            BoundaryEvent::Http(HttpExchangeEvent {
                request:
                    HttpRequestRecord {
                        method,
                        scheme,
                        authority,
                        path_and_query,
                        ..
                    },
                response:
                    HttpResponseRecord {
                        status,
                        reason_phrase,
                        body:
                            HttpBodyRecord {
                                content_type: Some(content_type),
                                bytes,
                            },
                        ..
                    },
            })
        ) if divergence_markers == &[DivergenceMarker::ExternalStateOmitted]
            && method == "GET"
            && scheme == "http"
            && authority == "example.test:8080"
            && path_and_query == "/todos?limit=1"
            && *status == 200
            && reason_phrase == "OK"
            && content_type == "application/json"
            && bytes == br#"[{"id":1}]"#
    ));

    remove_temp_dir(&root);
}

fn corrupt_artifact_rejected() {
    let root = unique_temp_dir("corrupt_artifact_rejected");
    let artifact = sample_artifact_bundle();

    artifact
        .write_to_directory(&root)
        .expect("artifact should be written");

    fs::write(root.join(EVENTS_FILE_NAME), "{not-json}\n")
        .expect("corrupt event file should be written");

    let error = ArtifactBundle::read_from_directory(&root).expect_err("artifact should fail");
    match error {
        ArtifactError::CorruptArtifact { path, message } => {
            assert_eq!(path, root.join(EVENTS_FILE_NAME));
            assert!(message.contains("invalid event record on line 1"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    remove_temp_dir(&root);
}

fn http_normalization_fixture_shape() {
    let record = EventRecord {
        metadata: RecordMetadata {
            event_id: "evt-http-fixture-1".to_owned(),
            sequence: 1,
            capture_fidelity: CaptureFidelity::Semantic,
            replay_classification: ReplayClassification::SemanticallyEquivalent,
            divergence_markers: vec![DivergenceMarker::ExternalStateOmitted],
        },
        event: BoundaryEvent::Http(HttpExchangeEvent {
            request: HttpRequestRecord {
                method: "post".to_owned(),
                scheme: "HTTP".to_owned(),
                authority: "127.0.0.1:8080".to_owned(),
                path_and_query: "fixture".to_owned(),
                headers: vec![
                    HttpHeader {
                        name: "Host".to_owned(),
                        value: "127.0.0.1:8080".to_owned(),
                    },
                    HttpHeader {
                        name: "Content-Type".to_owned(),
                        value: "text/plain".to_owned(),
                    },
                    HttpHeader {
                        name: "X-Extra".to_owned(),
                        value: "  spaced  ".to_owned(),
                    },
                ],
                body: HttpBodyRecord {
                    content_type: Some("text/plain".to_owned()),
                    bytes: b"key=value\n".to_vec(),
                },
            },
            response: HttpResponseRecord {
                status: 200,
                reason_phrase: "OK".to_owned(),
                headers: vec![HttpHeader {
                    name: "Content-Type".to_owned(),
                    value: "text/plain".to_owned(),
                }],
                body: HttpBodyRecord {
                    content_type: Some("text/plain".to_owned()),
                    bytes: b"ok\n".to_vec(),
                },
            },
        }),
        request_id: None,
    };

    let semantic = match &record.event {
        BoundaryEvent::Http(exchange) => normalize_http_exchange_event(exchange, &record.metadata),
        other => panic!("expected http record, found {other:?}"),
    };

    assert_eq!(semantic.protocol.to_string(), "HTTP/1.1");
    assert_eq!(semantic.request.method, "POST");
    assert_eq!(semantic.request.url, "http://127.0.0.1:8080/fixture");
    assert_eq!(
        semantic.request.body.content_type.as_deref(),
        Some("text/plain")
    );
    assert_eq!(semantic.request.body.byte_len, b"key=value\n".len());
    assert!(semantic
        .request
        .headers
        .iter()
        .any(|header| header.name == "x-extra" && header.value == "spaced"));

    assert_eq!(semantic.response.status, 200);
    assert_eq!(semantic.response.reason_phrase, "OK");
    assert!(!semantic.response.body_truncated);
    assert_eq!(
        semantic.response.body.content_type.as_deref(),
        Some("text/plain")
    );
    assert_eq!(semantic.response.body.byte_len, b"ok\n".len());

    assert_eq!(
        semantic.fidelity.capture_fidelity,
        CaptureFidelity::Semantic
    );
    assert_eq!(
        semantic.fidelity.replay_classification,
        ReplayClassification::SemanticallyEquivalent
    );
    assert_eq!(
        semantic.fidelity.divergence_markers,
        vec![DivergenceMarker::ExternalStateOmitted]
    );
}

fn schema_mismatch_rejected() {
    let root = unique_temp_dir("schema_mismatch_rejected");
    let artifact = sample_artifact_bundle();

    artifact
        .write_to_directory(&root)
        .expect("artifact should be written");

    let mismatched_summary = serde_json::json!({
        "schema_version": "deja.artifact/v0",
        "total_records": 4,
        "counts": {
            "time": 1,
            "random": 1,
            "environment": 1,
            "http": 1
        },
        "fidelity": {
            "exact_records": 3,
            "semantic_records": 1,
            "divergence_markers": ["external_state_omitted"]
        }
    });

    fs::write(
        root.join(INSPECTION_SUMMARY_FILE_NAME),
        serde_json::to_vec_pretty(&mismatched_summary).expect("json serialization should work"),
    )
    .expect("mismatched summary should be written");

    let error = read_artifact_metadata(&root).expect_err("schema mismatch should fail");
    match error {
        ArtifactError::SchemaVersionMismatch {
            path,
            expected,
            found,
        } => {
            assert_eq!(path, root.join(INSPECTION_SUMMARY_FILE_NAME));
            assert_eq!(expected, ARTIFACT_SCHEMA_VERSION_V1);
            assert_eq!(found, "deja.artifact/v0");
        }
        other => panic!("unexpected error: {other:?}"),
    }

    remove_temp_dir(&root);
}

fn sample_artifact_bundle() -> ArtifactBundle {
    ArtifactBundle {
        metadata: ArtifactMetadataDocument {
            schema_version: ARTIFACT_SCHEMA_VERSION_V1,
            artifact: ArtifactDescriptor {
                artifact_id: "artifact-001".to_owned(),
                created_at: "2026-03-29T12:00:00Z".to_owned(),
                producer: ProducerMetadata {
                    name: "deja-cli".to_owned(),
                    version: "0.1.0".to_owned(),
                },
                layout: ArtifactLayoutDescriptor::default(),
            },
            session: SessionMetadata {
                session_id: "session-001".to_owned(),
                recorded_at: "2026-03-29T12:00:00Z".to_owned(),
                command: vec![
                    "/usr/bin/http_fixture_client".to_owned(),
                    "--once".to_owned(),
                ],
                working_directory: "/tmp/workload".to_owned(),
                target: TargetMetadata {
                    os: "linux".to_owned(),
                    arch: "x86_64".to_owned(),
                    libc: "glibc".to_owned(),
                },
            },
            support_matrix: SupportMatrixMetadata {
                launched_child_only: true,
                supported_boundaries: vec![
                    SupportedBoundary::Time,
                    SupportedBoundary::Random,
                    SupportedBoundary::Environment,
                    SupportedBoundary::Http,
                ],
                unsupported_notes: vec![
                    "No live attach support in v1".to_owned(),
                    "No TLS HTTP capture in v1".to_owned(),
                ],
            },
            preload: Some(PreloadBootstrap::new(
                PreloadMode::Record,
                "/tmp/deja-artifact",
                "/tmp/libdeja_preload.so",
            )),
        },
        events: vec![
            EventRecord {
                metadata: RecordMetadata {
                    event_id: "evt-time-1".to_owned(),
                    sequence: 1,
                    capture_fidelity: CaptureFidelity::Exact,
                    replay_classification: ReplayClassification::DeterministicEquivalent,
                    divergence_markers: vec![],
                },
                event: BoundaryEvent::Time(TimeBoundaryEvent {
                    source: TimeSource::SystemTime,
                    seconds: 1_711_708_800,
                    nanos: 123_456_789,
                }),
                request_id: None,
            },
            EventRecord {
                metadata: RecordMetadata {
                    event_id: "evt-random-1".to_owned(),
                    sequence: 2,
                    capture_fidelity: CaptureFidelity::Exact,
                    replay_classification: ReplayClassification::DeterministicEquivalent,
                    divergence_markers: vec![],
                },
                event: BoundaryEvent::Random(RandomBoundaryEvent {
                    source: RandomSource::Getrandom,
                    bytes: vec![7, 42, 99, 128],
                }),
                request_id: None,
            },
            EventRecord {
                metadata: RecordMetadata {
                    event_id: "evt-env-1".to_owned(),
                    sequence: 3,
                    capture_fidelity: CaptureFidelity::Exact,
                    replay_classification: ReplayClassification::DeterministicEquivalent,
                    divergence_markers: vec![],
                },
                event: BoundaryEvent::Environment(EnvironmentBoundaryEvent {
                    operation: EnvironmentOperation::Get,
                    key: "API_BASE_URL".to_owned(),
                    value: Some("http://127.0.0.1:8080".to_owned()),
                }),
                request_id: None,
            },
            EventRecord {
                metadata: RecordMetadata {
                    event_id: "evt-http-1".to_owned(),
                    sequence: 4,
                    capture_fidelity: CaptureFidelity::Semantic,
                    replay_classification: ReplayClassification::SemanticallyEquivalent,
                    divergence_markers: vec![DivergenceMarker::ExternalStateOmitted],
                },
                event: BoundaryEvent::Http(HttpExchangeEvent {
                    request: HttpRequestRecord {
                        method: "GET".to_owned(),
                        scheme: "http".to_owned(),
                        authority: "example.test:8080".to_owned(),
                        path_and_query: "/todos?limit=1".to_owned(),
                        headers: vec![HttpHeader {
                            name: "accept".to_owned(),
                            value: "application/json".to_owned(),
                        }],
                        body: HttpBodyRecord {
                            content_type: None,
                            bytes: vec![],
                        },
                    },
                    response: HttpResponseRecord {
                        status: 200,
                        reason_phrase: "OK".to_owned(),
                        headers: vec![HttpHeader {
                            name: "content-type".to_owned(),
                            value: "application/json".to_owned(),
                        }],
                        body: HttpBodyRecord {
                            content_type: Some("application/json".to_owned()),
                            bytes: br#"[{"id":1}]"#.to_vec(),
                        },
                    },
                }),
                request_id: None,
            },
        ],
        inspection_summary: InspectionSummaryDocument {
            schema_version: ARTIFACT_SCHEMA_VERSION_V1,
            total_records: 4,
            counts: EventCounts {
                time: 1,
                random: 1,
                environment: 1,
                http: 1,
                socket: 0,
                dns: 0,
            },
            fidelity: ArtifactFidelitySummary {
                exact_records: 3,
                semantic_records: 1,
                divergence_markers: vec![DivergenceMarker::ExternalStateOmitted],
            },
        },
    }
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_nanos();
    path.push(format!(
        "deja-core-{label}-{}-{timestamp}",
        std::process::id()
    ));
    fs::create_dir_all(&path).expect("temp dir should be created");
    path
}

fn remove_temp_dir(path: &Path) {
    if path.exists() {
        fs::remove_dir_all(path).expect("temp dir should be removed");
    }
}

fn socket_dns_event_round_trip() {
    let root = unique_temp_dir("socket_dns_event_round_trip");

    let socket_event = EventRecord {
        metadata: RecordMetadata {
            event_id: "evt-socket-1".to_owned(),
            sequence: 1,
            capture_fidelity: CaptureFidelity::Semantic,
            replay_classification: ReplayClassification::SemanticallyEquivalent,
            divergence_markers: vec![DivergenceMarker::ExternalStateOmitted],
        },
        event: BoundaryEvent::Socket(SocketBoundaryEvent {
            operation: SocketOperation::Connect,
            direction: EventDirection::Outbound,
            peer_address: "127.0.0.1:5432".to_owned(),
            local_address: Some("127.0.0.1:54321".to_owned()),
            protocol_hint: ProtocolHint::Unknown,
            data: vec![],
            fd: 3,
            connection_id: 0,
            stream_offset: 0,
        }),
        request_id: None,
    };

    let dns_event = EventRecord {
        metadata: RecordMetadata {
            event_id: "evt-dns-2".to_owned(),
            sequence: 2,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Dns(DnsBoundaryEvent {
            hostname: "api.example.com".to_owned(),
            resolved_addresses: vec!["93.184.216.34".to_owned()],
            address_family: AddressFamily::Ipv4,
        }),
        request_id: None,
    };

    let events = vec![socket_event.clone(), dns_event.clone()];

    let artifact = ArtifactBundle {
        metadata: ArtifactMetadataDocument {
            schema_version: ARTIFACT_SCHEMA_VERSION_V1,
            artifact: ArtifactDescriptor {
                artifact_id: "artifact-socket-dns-001".to_owned(),
                created_at: "2026-04-10T12:00:00Z".to_owned(),
                producer: ProducerMetadata {
                    name: "deja-cli".to_owned(),
                    version: "0.1.0".to_owned(),
                },
                layout: ArtifactLayoutDescriptor::default(),
            },
            session: SessionMetadata {
                session_id: "session-socket-dns-001".to_owned(),
                recorded_at: "2026-04-10T12:00:00Z".to_owned(),
                command: vec!["/usr/bin/test_app".to_owned()],
                working_directory: "/tmp".to_owned(),
                target: TargetMetadata {
                    os: "linux".to_owned(),
                    arch: "x86_64".to_owned(),
                    libc: "glibc".to_owned(),
                },
            },
            support_matrix: SupportMatrixMetadata {
                launched_child_only: true,
                supported_boundaries: vec![
                    SupportedBoundary::Time,
                    SupportedBoundary::Random,
                    SupportedBoundary::Environment,
                    SupportedBoundary::Http,
                    SupportedBoundary::Socket,
                    SupportedBoundary::Dns,
                ],
                unsupported_notes: vec![],
            },
            preload: None,
        },
        events: events.clone(),
        inspection_summary: deja_core::inspection_summary_from_events(&events),
    };

    // Verify counts include socket and dns
    assert_eq!(artifact.inspection_summary.counts.socket, 1);
    assert_eq!(artifact.inspection_summary.counts.dns, 1);
    assert_eq!(artifact.inspection_summary.total_records, 2);

    // Write and reload
    artifact
        .write_to_directory(&root)
        .expect("artifact should be written");
    let reloaded = ArtifactBundle::read_from_directory(&root).expect("artifact should reload");
    assert_eq!(reloaded.events.len(), 2);

    // Verify socket event round-trips
    assert!(matches!(
        &reloaded.events[0].event,
        BoundaryEvent::Socket(SocketBoundaryEvent {
            operation: SocketOperation::Connect,
            ref peer_address,
            protocol_hint: ProtocolHint::Unknown,
            fd: 3,
            ..
        }) if peer_address == "127.0.0.1:5432"
    ));

    // Verify DNS event round-trips
    assert!(matches!(
        &reloaded.events[1].event,
        BoundaryEvent::Dns(DnsBoundaryEvent {
            ref hostname,
            ref resolved_addresses,
            address_family: AddressFamily::Ipv4,
        }) if hostname == "api.example.com"
            && resolved_addresses == &["93.184.216.34".to_owned()]
    ));

    remove_temp_dir(&root);
}

fn group_events_into_request_cases() {
    // Build a stream: env, time, HTTP (the typical fixture pattern)
    let env_event = EventRecord {
        metadata: RecordMetadata {
            event_id: "evt-environment-get-1".to_owned(),
            sequence: 1,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Environment(EnvironmentBoundaryEvent {
            operation: EnvironmentOperation::Get,
            key: "HTTP_FIXTURE_GREETING".to_owned(),
            value: Some("hello".to_owned()),
        }),
        request_id: None,
    };

    let time_event = EventRecord {
        metadata: RecordMetadata {
            event_id: "evt-time-system_time-2".to_owned(),
            sequence: 2,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Time(TimeBoundaryEvent {
            source: TimeSource::SystemTime,
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        request_id: None,
    };

    let http_event = EventRecord {
        metadata: RecordMetadata {
            event_id: "evt-http-3".to_owned(),
            sequence: 3,
            capture_fidelity: CaptureFidelity::Semantic,
            replay_classification: ReplayClassification::SemanticallyEquivalent,
            divergence_markers: vec![DivergenceMarker::ExternalStateOmitted],
        },
        event: BoundaryEvent::Http(HttpExchangeEvent {
            request: HttpRequestRecord {
                method: "POST".to_owned(),
                scheme: "http".to_owned(),
                authority: "127.0.0.1:8080".to_owned(),
                path_and_query: "/fixture".to_owned(),
                headers: vec![HttpHeader {
                    name: "Host".to_owned(),
                    value: "127.0.0.1:8080".to_owned(),
                }],
                body: HttpBodyRecord {
                    content_type: Some("text/plain".to_owned()),
                    bytes: b"hello".to_vec(),
                },
            },
            response: HttpResponseRecord {
                status: 200,
                reason_phrase: "OK".to_owned(),
                headers: vec![],
                body: HttpBodyRecord {
                    content_type: Some("text/plain".to_owned()),
                    bytes: b"ok".to_vec(),
                },
            },
        }),
        request_id: None,
    };

    let events = vec![env_event, time_event, http_event];
    let cases = group_events_into_cases(&events);

    // Single HTTP event => single case
    assert_eq!(cases.len(), 1);
    assert_eq!(cases[0].case_id, "case-1");
    assert_eq!(cases[0].inbound.method, "POST");
    assert_eq!(cases[0].inbound.path, "/fixture");
    assert_eq!(cases[0].response.status, 200);
    // The env and time events are recorded_inputs
    assert_eq!(cases[0].recorded_inputs.len(), 2);
}

fn request_id_based_grouping_interleaved() {
    // Simulate 3 interleaved requests:
    //   req-a: Time + Random + Http exchange
    //   req-b: Time + Http exchange
    //   req-c: Environment + Time + Http exchange
    //   background: 1 Time event with no request_id

    let bg_time = EventRecord {
        metadata: RecordMetadata {
            event_id: "bg-time".to_owned(),
            sequence: 0,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Time(TimeBoundaryEvent {
            source: TimeSource::SystemTime,
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        request_id: None,
    };

    let a_time = EventRecord {
        metadata: RecordMetadata {
            event_id: "a-time".to_owned(),
            sequence: 1,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Time(TimeBoundaryEvent {
            source: TimeSource::SystemTime,
            seconds: 1_700_000_001,
            nanos: 0,
        }),
        request_id: Some("req-a".to_owned()),
    };

    let b_time = EventRecord {
        metadata: RecordMetadata {
            event_id: "b-time".to_owned(),
            sequence: 2,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Time(TimeBoundaryEvent {
            source: TimeSource::SystemTime,
            seconds: 1_700_000_002,
            nanos: 0,
        }),
        request_id: Some("req-b".to_owned()),
    };

    let a_random = EventRecord {
        metadata: RecordMetadata {
            event_id: "a-rand".to_owned(),
            sequence: 3,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Random(RandomBoundaryEvent {
            source: RandomSource::DevUrandom,
            bytes: vec![0xAB, 0xCD],
        }),
        request_id: Some("req-a".to_owned()),
    };

    let c_env = EventRecord {
        metadata: RecordMetadata {
            event_id: "c-env".to_owned(),
            sequence: 4,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Environment(EnvironmentBoundaryEvent {
            operation: EnvironmentOperation::Get,
            key: "HOME".to_owned(),
            value: Some("/root".to_owned()),
        }),
        request_id: Some("req-c".to_owned()),
    };

    let c_time = EventRecord {
        metadata: RecordMetadata {
            event_id: "c-time".to_owned(),
            sequence: 5,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Time(TimeBoundaryEvent {
            source: TimeSource::SystemTime,
            seconds: 1_700_000_003,
            nanos: 0,
        }),
        request_id: Some("req-c".to_owned()),
    };

    fn make_http_exchange(method: &str, path: &str, status: u16) -> HttpExchangeEvent {
        HttpExchangeEvent {
            request: HttpRequestRecord {
                method: method.to_owned(),
                scheme: "http".to_owned(),
                authority: "localhost".to_owned(),
                path_and_query: path.to_owned(),
                headers: vec![],
                body: HttpBodyRecord {
                    content_type: None,
                    bytes: vec![],
                },
            },
            response: HttpResponseRecord {
                status,
                reason_phrase: "OK".to_owned(),
                headers: vec![],
                body: HttpBodyRecord {
                    content_type: None,
                    bytes: vec![],
                },
            },
        }
    }

    let a_http = EventRecord {
        metadata: RecordMetadata {
            event_id: "a-http".to_owned(),
            sequence: 6,
            capture_fidelity: CaptureFidelity::Semantic,
            replay_classification: ReplayClassification::SemanticallyEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Http(make_http_exchange("POST", "/a", 201)),
        request_id: Some("req-a".to_owned()),
    };

    let b_http = EventRecord {
        metadata: RecordMetadata {
            event_id: "b-http".to_owned(),
            sequence: 7,
            capture_fidelity: CaptureFidelity::Semantic,
            replay_classification: ReplayClassification::SemanticallyEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Http(make_http_exchange("GET", "/b", 200)),
        request_id: Some("req-b".to_owned()),
    };

    let c_http = EventRecord {
        metadata: RecordMetadata {
            event_id: "c-http".to_owned(),
            sequence: 8,
            capture_fidelity: CaptureFidelity::Semantic,
            replay_classification: ReplayClassification::SemanticallyEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Http(make_http_exchange("POST", "/c", 200)),
        request_id: Some("req-c".to_owned()),
    };

    // Interleave them as they'd appear in a real event stream
    let events = vec![
        bg_time, a_time, b_time, a_random, c_env, c_time, a_http, b_http, c_http,
    ];

    let cases = group_events_into_cases(&events);

    // Should produce 4 cases: __background__, req-a, req-b, req-c
    assert_eq!(cases.len(), 4, "Expected 4 cases, got {}", cases.len());

    // __background__ case should be first (BTreeMap sorts keys)
    assert_eq!(cases[0].case_id, "__background__");
    assert_eq!(cases[0].inbound.method, "BACKGROUND");
    assert_eq!(cases[0].recorded_inputs.len(), 1); // bg_time
    assert_eq!(cases[0].recorded_outputs.len(), 0);
    assert_eq!(cases[0].response.status, 0);

    // req-a: 2 recorded_inputs (time + random), 0 recorded_outputs, HTTP exchange
    let case_a = cases
        .iter()
        .find(|c| c.case_id == "req-a")
        .expect("req-a case missing");
    assert_eq!(case_a.inbound.method, "POST");
    assert_eq!(case_a.inbound.path, "/a");
    assert_eq!(case_a.recorded_inputs.len(), 2); // time + random
    assert_eq!(case_a.recorded_outputs.len(), 0);
    assert_eq!(case_a.response.status, 201);

    // req-b: 1 recorded_input (time), 0 recorded_outputs
    let case_b = cases
        .iter()
        .find(|c| c.case_id == "req-b")
        .expect("req-b case missing");
    assert_eq!(case_b.inbound.method, "GET");
    assert_eq!(case_b.inbound.path, "/b");
    assert_eq!(case_b.recorded_inputs.len(), 1);
    assert_eq!(case_b.recorded_outputs.len(), 0);
    assert_eq!(case_b.response.status, 200);

    // req-c: 2 recorded_inputs (env + time), 0 recorded_outputs
    let case_c = cases
        .iter()
        .find(|c| c.case_id == "req-c")
        .expect("req-c case missing");
    assert_eq!(case_c.inbound.method, "POST");
    assert_eq!(case_c.inbound.path, "/c");
    assert_eq!(case_c.recorded_inputs.len(), 2); // env + time
    assert_eq!(case_c.recorded_outputs.len(), 0);
    assert_eq!(case_c.response.status, 200);
}

fn correlation_health_no_correlation() {
    // Events with no request_id at all → NoCorrelation status
    let events = vec![
        make_time_event(1, None),
        make_random_event(2, None),
        make_env_event(3, None),
    ];

    let health = validate_correlation_health(&events);

    assert_eq!(health.status, CorrelationStatus::NoCorrelation);
    assert_eq!(health.total_events, 3);
    assert_eq!(health.correlated_events, 0);
    assert_eq!(health.uncorrelated_events, 3);
    assert_eq!(health.coverage_per_mille, 0);
    assert_eq!(health.contaminated_connections, 0);
    assert_eq!(health.orphaned_events, 0);
}

fn correlation_health_healthy() {
    // All events on a connection share the same request_id → Healthy
    let events = vec![
        make_socket_event(1, 100, SocketOperation::Connect, Some("req-a".to_owned())),
        make_socket_event(2, 100, SocketOperation::Send, Some("req-a".to_owned())),
        make_socket_event(3, 100, SocketOperation::Receive, Some("req-a".to_owned())),
        make_time_event(4, Some("req-a".to_owned())),
    ];

    let health = validate_correlation_health(&events);

    assert_eq!(health.status, CorrelationStatus::Healthy);
    assert_eq!(health.total_events, 4);
    assert_eq!(health.correlated_events, 4);
    assert_eq!(health.uncorrelated_events, 0);
    assert_eq!(health.coverage_per_mille, 1000); // 100%
    assert_eq!(health.clean_connections, 1);
    assert_eq!(health.contaminated_connections, 0);
    assert!(health.contamination_details.is_empty());
    assert_eq!(health.orphaned_events, 0);
}

fn correlation_health_contamination_detected() {
    // Same connection_id (42) carries two different request_ids → Contaminated
    let events = vec![
        make_socket_event(1, 42, SocketOperation::Connect, Some("req-a".to_owned())),
        make_socket_event(2, 42, SocketOperation::Send, Some("req-a".to_owned())),
        make_socket_event(3, 42, SocketOperation::Receive, Some("req-b".to_owned())), // WRONG!
        make_socket_event(4, 42, SocketOperation::Close, Some("req-b".to_owned())),
    ];

    let health = validate_correlation_health(&events);

    assert_eq!(health.status, CorrelationStatus::Contaminated);
    assert_eq!(health.contaminated_connections, 1);
    assert_eq!(health.clean_connections, 0);
    assert_eq!(health.contamination_details.len(), 1);
    assert_eq!(health.contamination_details[0].connection_id, 42);
    assert_eq!(health.contamination_details[0].request_ids.len(), 2);
    assert!(health.contamination_details[0]
        .request_ids
        .contains(&"req-a".to_owned()));
    assert!(health.contamination_details[0]
        .request_ids
        .contains(&"req-b".to_owned()));
}

fn correlation_health_orphans_detected() {
    // An event with no request_id that falls inside req-a's sequence range
    // is an orphan — it should have been tagged but wasn't.
    let events = vec![
        make_time_event(1, Some("req-a".to_owned())), // seq 1, req-a starts
        make_random_event(2, None),                   // seq 2, ORPHAN (inside req-a range)
        make_socket_event(3, 10, SocketOperation::Send, Some("req-a".to_owned())), // seq 3, req-a ends
    ];

    let health = validate_correlation_health(&events);

    assert_eq!(health.status, CorrelationStatus::Degraded);
    assert_eq!(health.orphaned_events, 1);
    assert_eq!(health.orphan_details.len(), 1);
    assert_eq!(health.orphan_details[0].sequence, 2);
    assert_eq!(health.orphan_details[0].event_type, "random");
}

fn correlation_health_degraded_partial_coverage() {
    // Some events correlated, some not, no contamination, no orphans
    let events = vec![
        make_time_event(1, None),                     // before any request
        make_time_event(2, Some("req-a".to_owned())), // req-a starts
        make_socket_event(3, 5, SocketOperation::Send, Some("req-a".to_owned())),
        make_time_event(4, Some("req-a".to_owned())), // req-a ends
        make_time_event(5, None),                     // after req-a
    ];

    let health = validate_correlation_health(&events);

    assert_eq!(health.status, CorrelationStatus::Degraded);
    assert_eq!(health.total_events, 5);
    assert_eq!(health.correlated_events, 3);
    assert_eq!(health.uncorrelated_events, 2);
    assert_eq!(health.coverage_per_mille, 600); // 3/5 = 600 per mille
    assert_eq!(health.contaminated_connections, 0);
    // seq 1 is before req-a (seq 2-4), so NOT an orphan
    // seq 5 is after req-a, so NOT an orphan
    assert_eq!(health.orphaned_events, 0);
}

// ---------------------------------------------------------------------------
// Helpers for building test events
// ---------------------------------------------------------------------------

fn make_time_event(sequence: u64, request_id: Option<String>) -> EventRecord {
    EventRecord {
        metadata: RecordMetadata {
            event_id: format!("evt-time-{sequence}"),
            sequence,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Time(TimeBoundaryEvent {
            source: TimeSource::SystemTime,
            seconds: 1_700_000_000 + sequence as i64,
            nanos: 0,
        }),
        request_id,
    }
}

fn make_random_event(sequence: u64, request_id: Option<String>) -> EventRecord {
    EventRecord {
        metadata: RecordMetadata {
            event_id: format!("evt-random-{sequence}"),
            sequence,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Random(RandomBoundaryEvent {
            source: RandomSource::Getrandom,
            bytes: vec![0xAB, 0xCD],
        }),
        request_id,
    }
}

fn make_env_event(sequence: u64, request_id: Option<String>) -> EventRecord {
    EventRecord {
        metadata: RecordMetadata {
            event_id: format!("evt-env-{sequence}"),
            sequence,
            capture_fidelity: CaptureFidelity::Exact,
            replay_classification: ReplayClassification::DeterministicEquivalent,
            divergence_markers: vec![],
        },
        event: BoundaryEvent::Environment(EnvironmentBoundaryEvent {
            operation: EnvironmentOperation::Get,
            key: "HOME".to_owned(),
            value: Some("/root".to_owned()),
        }),
        request_id,
    }
}

fn make_socket_event(
    sequence: u64,
    connection_id: u64,
    operation: SocketOperation,
    request_id: Option<String>,
) -> EventRecord {
    EventRecord {
        metadata: RecordMetadata {
            event_id: format!("evt-socket-{sequence}"),
            sequence,
            capture_fidelity: CaptureFidelity::Semantic,
            replay_classification: ReplayClassification::SemanticallyEquivalent,
            divergence_markers: vec![DivergenceMarker::ExternalStateOmitted],
        },
        event: BoundaryEvent::Socket(SocketBoundaryEvent {
            operation,
            direction: EventDirection::Outbound,
            peer_address: "127.0.0.1:5432".to_owned(),
            local_address: None,
            protocol_hint: ProtocolHint::Unknown,
            data: vec![],
            fd: 3,
            connection_id,
            stream_offset: 0,
        }),
        request_id,
    }
}
