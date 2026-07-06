use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::path::{Path, PathBuf};

use deja_runtime::{compute_metrics, read_events, ReplayIndex};
use serde_json::json;

fn main() {
    if let Err(error) = real_main() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

fn real_main() -> Result<(), String> {
    let mut artifact_dir = None;
    let mut write_path = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--artifact" => artifact_dir = args.next().map(PathBuf::from),
            "--write" => write_path = args.next().map(PathBuf::from),
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    let artifact_dir = artifact_dir.ok_or_else(|| {
        "missing --artifact <DIR>; expected directory containing semantic-events.jsonl".to_string()
    })?;

    let output = semantic_metrics_json(&artifact_dir)?;
    let pretty = serde_json::to_string_pretty(&output)
        .map_err(|error| format!("failed to render metrics: {error}"))?;

    if let Some(path) = write_path {
        let compact = serde_json::to_string(&output)
            .map_err(|error| format!("failed to render compact metrics: {error}"))?;
        std::fs::write(&path, compact)
            .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    }

    println!("{pretty}");
    Ok(())
}

fn print_help() {
    eprintln!("Usage: deja-semantic-metrics --artifact <DIR> [--write <PATH>]");
}

fn semantic_metrics_json(artifact_dir: &Path) -> Result<serde_json::Value, String> {
    let events = read_events(artifact_dir)
        .map_err(|error| format!("failed to read {}: {error}", artifact_dir.display()))?;
    let metrics = compute_metrics(&events);
    let index = ReplayIndex::new(events.clone());

    let mut methods_by_boundary: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    let mut correlations = BTreeSet::new();
    let mut fingerprints = BTreeMap::new();
    let mut duration_total = 0_u64;
    let mut duration_max = 0_u64;

    for event in &events {
        let method = format!("{}::{}", event.trait_name, event.method_name);
        *methods_by_boundary
            .entry(event.boundary.clone())
            .or_default()
            .entry(method)
            .or_default() += 1;
        if let Some(id) = &event.correlation_id {
            correlations.insert(id.clone());
        }
        duration_total = duration_total.saturating_add(event.duration_us);
        duration_max = duration_max.max(event.duration_us);
    }

    for correlation_id in &correlations {
        fingerprints.insert(
            correlation_id.clone(),
            format!(
                "{:016x}",
                index.call_graph_fingerprint(Some(correlation_id.as_str()))
            ),
        );
    }

    let avg_duration_us = if metrics.total_events > 0 {
        duration_total as f64 / metrics.total_events as f64
    } else {
        0.0
    };
    let correlation_coverage_pct = if metrics.total_events > 0 {
        metrics.correlated_events as f64 * 100.0 / metrics.total_events as f64
    } else {
        100.0
    };

    Ok(json!({
        "schema_version": "deja.semantic.metrics/v1",
        "event_count": metrics.total_events,
        "dropped_events": 0,
        "correlated_events": metrics.correlated_events,
        "uncorrelated_events": metrics.uncorrelated_events,
        "correlation_coverage_pct": round1(correlation_coverage_pct),
        "unique_correlation_ids": metrics.unique_correlation_ids,
        "unique_traits": metrics.unique_traits,
        "unique_methods": metrics.unique_methods,
        "unique_call_sites": metrics.unique_call_sites,
        "error_events": metrics.error_events,
        "boundaries": metrics.boundaries,
        "methods_by_boundary": methods_by_boundary,
        "duration_us": {
            "avg": round1(avg_duration_us),
            "max": duration_max,
            "total": duration_total
        },
        "call_graph_fingerprints": fingerprints,
        "artifact": {
            "dir": artifact_dir,
            "semantic_events": artifact_dir.join("semantic-events.jsonl")
        }
    }))
}

fn round1(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}
