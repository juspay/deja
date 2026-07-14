//! Lookup-table renderer — relocated to `deja-replay-core` and shared with the
//! replay agent. This module is a re-export shim keeping existing
//! `crate::lookup::...` call sites stable.

pub use deja_replay_core::lookup::{render_lookup_table, table_for_correlation};
