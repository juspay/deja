//! Shared foundation for the Deja sandbox replay platform.
//!
//! Linked by BOTH the dashboard (`deja-orchestrator`) and the in-sandbox
//! replay agent, so S3 key layout and configuration shapes cannot drift
//! between the two sides.

pub mod config;
pub mod ingest;
pub mod layout;
pub mod lookup;

pub use deja_compactor::S3Config;
