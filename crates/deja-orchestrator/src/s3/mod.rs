//! Recording ingest — relocated to `deja-replay-core` so the in-sandbox
//! replay agent shares the exact same S3 pull path. This module is a
//! re-export shim keeping existing `crate::s3::...` call sites stable.

pub use deja_replay_core::ingest::{count_session_objects, pull_recording, IngestReport};
pub use deja_replay_core::S3Config;
