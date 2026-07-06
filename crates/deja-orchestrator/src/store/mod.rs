//! Persistence layer. Currently filesystem-JSON via the helpers in
//! `crate::HarnessRoot`. SQLite/Postgres swap-in is a follow-up.

// Intentionally empty — `HarnessRoot` + `read_json` / `write_json` in
// `lib.rs` are the v1 surface. This module exists so future SQLite/Postgres
// backends slot in here without disturbing the API handlers.
