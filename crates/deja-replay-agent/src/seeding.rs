use std::collections::BTreeMap;
use std::process::{Command, Output};

use deja_kernel::BoundaryEvent;
use deja_replay_core::config::AgentConfig;
use serde::{Deserialize, Serialize};

/// Seed/readback sidecar written during the agent prepare phase. It mirrors the
/// legacy local lifecycle certificate so dashboard artifacts and debugging stay
/// the same across docker-compose and Kubernetes sandboxes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct SeedCertificate {
    schema_version: u16,
    #[serde(rename = "type")]
    kind: String,
    recording_id: String,
    run_id: String,
    seed_db_enabled: bool,
    summary: SeedCertificateSummary,
    entries: Vec<SeedCertificateEntry>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct SeedCertificateSummary {
    planned: usize,
    materialized: usize,
    skipped: usize,
    failed: usize,
    unsupported: usize,
    readback_matched: usize,
    readback_missing: usize,
    readback_mismatched: usize,
    readback_errors: usize,
    readback_not_run: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SeedCertificateEntry {
    correlation_id: Option<String>,
    boundary: String,
    logical_key: String,
    physical_key: Option<String>,
    db_schema: Option<String>,
    origin: deja::SeedOrigin,
    materialization: SeedMaterializationStatus,
    readback: SeedReadback,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SeedMaterializationStatus {
    Materialized,
    Skipped,
    Failed,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SeedReadback {
    status: SeedReadbackStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observed: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SeedReadbackStatus {
    Matched,
    Missing,
    Mismatched,
    Error,
    NotRun,
    Unsupported,
}

impl SeedCertificate {
    const SCHEMA_VERSION: u16 = 1;
    const KIND: &'static str = "seed_certificate";

    fn new(recording_id: &str, run_id: &str, seed_db_enabled: bool) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            kind: Self::KIND.to_owned(),
            recording_id: recording_id.to_owned(),
            run_id: run_id.to_owned(),
            seed_db_enabled,
            summary: SeedCertificateSummary::default(),
            entries: Vec::new(),
        }
    }

    fn push(&mut self, entry: SeedCertificateEntry) {
        self.summary.planned += 1;
        match entry.materialization {
            SeedMaterializationStatus::Materialized => self.summary.materialized += 1,
            SeedMaterializationStatus::Skipped => self.summary.skipped += 1,
            SeedMaterializationStatus::Failed => self.summary.failed += 1,
            SeedMaterializationStatus::Unsupported => self.summary.unsupported += 1,
        }
        match entry.readback.status {
            SeedReadbackStatus::Matched => self.summary.readback_matched += 1,
            SeedReadbackStatus::Missing => self.summary.readback_missing += 1,
            SeedReadbackStatus::Mismatched => self.summary.readback_mismatched += 1,
            SeedReadbackStatus::Error => self.summary.readback_errors += 1,
            SeedReadbackStatus::NotRun | SeedReadbackStatus::Unsupported => {
                self.summary.readback_not_run += 1;
            }
        }
        self.entries.push(entry);
    }

    pub(crate) fn summary_detail(&self) -> String {
        format!(
            "{} planned, {} materialized, {} skipped, {} failed, {} unsupported; readback matched {}, missing {}, mismatched {}, errors {}",
            self.summary.planned,
            self.summary.materialized,
            self.summary.skipped,
            self.summary.failed,
            self.summary.unsupported,
            self.summary.readback_matched,
            self.summary.readback_missing,
            self.summary.readback_mismatched,
            self.summary.readback_errors
        )
    }
}

pub(crate) struct SeedMaterializer {
    certificate: SeedCertificate,
    ambient: deja::AmbientTemplate,
    db_catalog: Option<DbCatalog>,
}

impl SeedMaterializer {
    pub(crate) fn new(cfg: &AgentConfig) -> Self {
        let seed_db_enabled = std::env::var("DEJA_SEED_DB")
            .ok()
            .map(|value| value.trim() != "0")
            .unwrap_or(true);
        Self {
            certificate: SeedCertificate::new(
                &cfg.run.recording_id,
                &cfg.run.run_id,
                seed_db_enabled,
            ),
            ambient: load_ambient_template(),
            db_catalog: None,
        }
    }

    pub(crate) fn certificate(&self) -> &SeedCertificate {
        &self.certificate
    }

    pub(crate) fn materialize_correlation(
        &mut self,
        cfg: &AgentConfig,
        correlation_id: &str,
        events: &[BoundaryEvent],
    ) {
        let corr = Some(correlation_id.to_owned());
        let plan = deja::build_seed_plan(events, Some(correlation_id)).with_ambient(&self.ambient);
        if plan.is_empty() {
            return;
        }

        let db_schema = Some(deja::db_schema_for(correlation_id));
        if self.certificate.seed_db_enabled {
            if let Some(schema) = &db_schema {
                create_db_schema(&cfg.stores.pg_url, correlation_id, schema);
            }
        }

        let mut entries = plan.iter().collect::<Vec<_>>();
        entries.sort_by_key(|entry| seed_materialization_priority(entry));

        for entry in entries {
            match entry.boundary.as_str() {
                "redis" => {
                    if is_runtime_redis_lock_key(&entry.key) {
                        let message = "runtime API lock Redis keys are intentionally not seeded";
                        eprintln!(
                            "deja-replay-agent: seed_redis correlation={correlation_id} key {} skipped: {message}",
                            entry.key
                        );
                        self.certificate.push(SeedCertificateEntry::new(
                            &corr,
                            entry,
                            None,
                            None,
                            SeedMaterializationStatus::Skipped,
                            SeedReadback::not_run(message),
                        ));
                        continue;
                    }
                    let value = render_redis_seed_value(&entry.value);
                    let key = format!("{correlation_id}:{}", entry.key);
                    let (materialization, readback) =
                        seed_redis(&cfg.stores.redis_url, &key, &value);
                    self.certificate.push(SeedCertificateEntry::new(
                        &corr,
                        entry,
                        Some(key),
                        None,
                        materialization,
                        readback,
                    ));
                }
                "db" if self.certificate.seed_db_enabled => {
                    if self.db_catalog.is_none() {
                        self.db_catalog = Some(load_db_catalog(&cfg.stores.pg_url));
                    }
                    let Some(db_catalog) = self.db_catalog.as_ref() else {
                        self.certificate.push(SeedCertificateEntry::new(
                            &corr,
                            entry,
                            None,
                            db_schema.clone(),
                            SeedMaterializationStatus::Failed,
                            SeedReadback::error(
                                "db catalog was not loaded while db seeding was enabled",
                            ),
                        ));
                        continue;
                    };
                    let (materialization, readback) = seed_db(
                        &cfg.stores.pg_url,
                        correlation_id,
                        db_schema.as_deref(),
                        db_catalog,
                        &entry.key,
                        entry.image.as_ref(),
                        &entry.value,
                    );
                    self.certificate.push(SeedCertificateEntry::new(
                        &corr,
                        entry,
                        None,
                        db_schema.clone(),
                        materialization,
                        readback,
                    ));
                }
                "db" => self.certificate.push(SeedCertificateEntry::new(
                    &corr,
                    entry,
                    None,
                    db_schema.clone(),
                    SeedMaterializationStatus::Skipped,
                    SeedReadback::not_run("db seeding disabled by DEJA_SEED_DB=0"),
                )),
                _ => self.certificate.push(SeedCertificateEntry::new(
                    &corr,
                    entry,
                    None,
                    None,
                    SeedMaterializationStatus::Unsupported,
                    SeedReadback::unsupported(
                        "seed materialization only supports redis and db boundaries",
                    ),
                )),
            }
        }

        eprintln!(
            "deja-replay-agent: seeded correlation {correlation_id} — {}",
            self.certificate.summary_detail()
        );
    }
}

impl SeedCertificateEntry {
    fn new(
        correlation_id: &Option<String>,
        entry: &deja::SeedEntry,
        physical_key: Option<String>,
        db_schema: Option<String>,
        materialization: SeedMaterializationStatus,
        readback: SeedReadback,
    ) -> Self {
        Self {
            correlation_id: correlation_id.clone(),
            boundary: entry.boundary.clone(),
            logical_key: entry.key.clone(),
            physical_key,
            db_schema,
            origin: entry.origin,
            materialization,
            readback,
        }
    }
}

impl SeedReadback {
    fn matched(expected: serde_json::Value, observed: serde_json::Value) -> Self {
        Self {
            status: SeedReadbackStatus::Matched,
            expected: Some(expected),
            observed: Some(observed),
            message: None,
        }
    }

    fn missing(expected: serde_json::Value, message: impl Into<String>) -> Self {
        Self {
            status: SeedReadbackStatus::Missing,
            expected: Some(expected),
            observed: None,
            message: Some(message.into()),
        }
    }

    fn mismatched(
        expected: serde_json::Value,
        observed: serde_json::Value,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status: SeedReadbackStatus::Mismatched,
            expected: Some(expected),
            observed: Some(observed),
            message: Some(message.into()),
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            status: SeedReadbackStatus::Error,
            expected: None,
            observed: None,
            message: Some(message.into()),
        }
    }

    fn not_run(message: impl Into<String>) -> Self {
        Self {
            status: SeedReadbackStatus::NotRun,
            expected: None,
            observed: None,
            message: Some(message.into()),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            status: SeedReadbackStatus::Unsupported,
            expected: None,
            observed: None,
            message: Some(message.into()),
        }
    }
}

fn seed_materialization_priority(entry: &deja::SeedEntry) -> u8 {
    if entry.boundary != "db" {
        return 0;
    }
    match deja::StateKey::parse(&entry.key) {
        Ok(deja::StateKey::DbRow { .. }) => 0,
        Ok(deja::StateKey::DbQuery { .. }) => 1,
        _ => 2,
    }
}

fn is_runtime_redis_lock_key(key: &str) -> bool {
    key.split(':')
        .any(|segment| segment.starts_with("API_LOCK_"))
}

fn load_ambient_template() -> deja::AmbientTemplate {
    if let Ok(path) = std::env::var("DEJA_AMBIENT_TEMPLATE") {
        if !path.trim().is_empty() {
            match std::fs::read_to_string(&path) {
                Ok(text) => {
                    let template = deja::AmbientTemplate::from_tsv(&text);
                    eprintln!(
                        "deja-replay-agent: loaded ambient template from {path} ({} entries)",
                        template.entries().len()
                    );
                    return template;
                }
                Err(error) => {
                    eprintln!(
                        "deja-replay-agent: could not read DEJA_AMBIENT_TEMPLATE={path}: {error}; falling back to demo defaults"
                    );
                }
            }
        }
    }
    deja::AmbientTemplate::demo_defaults()
}

fn seed_redis(
    redis_url: &str,
    key: &str,
    value: &str,
) -> (SeedMaterializationStatus, SeedReadback) {
    let image = RedisSeedImage::string(key, value);
    match seed_redis_image(redis_url, &image) {
        Ok(()) => (
            SeedMaterializationStatus::Materialized,
            readback_redis(redis_url, key, value),
        ),
        Err(message) => (
            SeedMaterializationStatus::Failed,
            SeedReadback::error(message),
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RedisSeedValueType {
    String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RedisSeedImage {
    physical_key: String,
    physical_key_bytes: Vec<u8>,
    value_type: RedisSeedValueType,
    raw_value: String,
    raw_value_bytes: Vec<u8>,
    ttl_seconds: Option<i64>,
}

impl RedisSeedImage {
    fn string(key: &str, value: &str) -> Self {
        Self {
            physical_key: key.to_owned(),
            physical_key_bytes: key.as_bytes().to_vec(),
            value_type: RedisSeedValueType::String,
            raw_value: value.to_owned(),
            raw_value_bytes: value.as_bytes().to_vec(),
            ttl_seconds: None,
        }
    }
}

fn seed_redis_image(redis_url: &str, image: &RedisSeedImage) -> Result<(), String> {
    eprintln!(
        "deja-replay-agent: seed_redis key {} byte(s), value {} byte(s), {:?}, ttl {:?}",
        image.physical_key_bytes.len(),
        image.raw_value_bytes.len(),
        image.value_type,
        image.ttl_seconds
    );
    match Command::new("redis-cli")
        .args([
            "-u",
            redis_url,
            "SET",
            image.physical_key.as_str(),
            image.raw_value.as_str(),
        ])
        .status()
    {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            let message = format!("seed_redis exited {status}");
            eprintln!("deja-replay-agent: {message}; continuing (best-effort)");
            Err(message)
        }
        Err(error) => {
            let message = format!("could not run seed_redis: {error}");
            eprintln!("deja-replay-agent: {message}; continuing (best-effort)");
            Err(message)
        }
    }
}

fn readback_redis(redis_url: &str, key: &str, expected: &str) -> SeedReadback {
    let exists = match redis_cli_output(redis_url, &["EXISTS", key]) {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        }
        Ok(output) => {
            return SeedReadback::error(format!(
                "redis EXISTS readback exited {}; stderr='{}'",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Err(message) => return SeedReadback::error(message),
    };
    if exists != "1" {
        return SeedReadback::missing(
            serde_json::json!(expected),
            format!("redis EXISTS returned {exists:?} after SET"),
        );
    }

    let output = match redis_cli_output(redis_url, &["--raw", "GET", key]) {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            return SeedReadback::error(format!(
                "redis GET readback exited {}; stderr='{}'",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Err(message) => return SeedReadback::error(message),
    };
    let observed_bytes = strip_redis_cli_terminator(&output.stdout);
    let expected_bytes = expected.as_bytes();
    if observed_bytes == expected_bytes {
        SeedReadback::matched(
            serde_json::json!(expected),
            serde_json::json!(String::from_utf8_lossy(observed_bytes).to_string()),
        )
    } else {
        SeedReadback::mismatched(
            serde_json::json!({
                "utf8": expected,
                "len": expected_bytes.len(),
            }),
            serde_json::json!({
                "utf8": String::from_utf8_lossy(observed_bytes).to_string(),
                "len": observed_bytes.len(),
            }),
            "redis GET returned a different value after SET",
        )
    }
}

fn redis_cli_output(redis_url: &str, redis_args: &[&str]) -> Result<Output, String> {
    let mut command = Command::new("redis-cli");
    command.args(["-u", redis_url]);
    command.args(redis_args);
    command
        .output()
        .map_err(|error| format!("could not run redis-cli readback: {error}"))
}

fn strip_redis_cli_terminator(bytes: &[u8]) -> &[u8] {
    match bytes.split_last() {
        Some((last, rest)) if *last == b'\n' => rest,
        _ => bytes,
    }
}

#[allow(clippy::too_many_arguments)]
fn seed_db(
    pg_url: &str,
    correlation_id: &str,
    schema: Option<&str>,
    catalog: &DbCatalog,
    key: &str,
    image: Option<&serde_json::Value>,
    envelope: &serde_json::Value,
) -> (SeedMaterializationStatus, SeedReadback) {
    let target = match db_seed_target_from_key(key) {
        Some(target) => target,
        None => {
            return (
                SeedMaterializationStatus::Unsupported,
                SeedReadback::unsupported("unsupported or opaque db state key"),
            );
        }
    };
    let rows = match image {
        Some(image) => match db_row_images_from_typed_payload(&target.table, image, catalog) {
            Ok(Some(rows)) => rows,
            Ok(None) => db_seed_value(envelope)
                .map(|value| target.filter_rows(db_row_images(&target.table, &value, catalog)))
                .unwrap_or_default(),
            Err(message) => {
                eprintln!("deja-replay-agent: {message}; skipping typed db seed entry");
                return (
                    SeedMaterializationStatus::Failed,
                    SeedReadback::error(message),
                );
            }
        },
        None => db_seed_value(envelope)
            .map(|value| target.filter_rows(db_row_images(&target.table, &value, catalog)))
            .unwrap_or_default(),
    };
    if rows.is_empty() {
        let message = format!(
            "seed_db {} key {} carried no seedable row payload; skipping",
            target.kind, key
        );
        eprintln!("deja-replay-agent: {message}");
        return (
            SeedMaterializationStatus::Skipped,
            SeedReadback::not_run(message),
        );
    }

    let mut sql = String::new();
    for row in &rows {
        let Some(statement) = build_insert_sql(schema, row) else {
            let message = format!(
                "seed_db {} {} could not render an insert for a seedable row",
                target.kind, target.table
            );
            eprintln!("deja-replay-agent: {message}; skipping this seed entry");
            return (
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
            );
        };
        sql.push_str(&statement);
        sql.push('\n');
    }
    if sql.is_empty() {
        return (
            SeedMaterializationStatus::Skipped,
            SeedReadback::not_run("seed_db rendered no insert SQL"),
        );
    }

    let row_count = sql.lines().count();
    eprintln!(
        "deja-replay-agent: seed_db correlation={correlation_id} {} {} ({row_count} row(s))",
        target.kind, target.table
    );
    log_seed_sql(correlation_id, "insert", &target.table, &sql);
    if seed_contains_null_column(&rows, "totp_secret") {
        eprintln!(
            "deja-replay-agent: seed_db correlation={correlation_id} {} {} NULL columns: totp_secret=NULL",
            target.kind, target.table
        );
    }

    match psql_output(pg_url, &["-v", "ON_ERROR_STOP=1", "-c", &sql]) {
        Ok(output) if output.status.success() => (
            SeedMaterializationStatus::Materialized,
            readback_db(pg_url, schema, &target, &rows),
        ),
        Ok(output) => {
            let message = format!(
                "seed_db {} exited {}; stderr='{}' stdout='{}'",
                target.table,
                output.status,
                String::from_utf8_lossy(&output.stderr).trim(),
                String::from_utf8_lossy(&output.stdout).trim()
            );
            eprintln!("deja-replay-agent: {message}; continuing (best-effort)");
            (
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
            )
        }
        Err(message) => {
            eprintln!("deja-replay-agent: {message}; continuing (best-effort)");
            (
                SeedMaterializationStatus::Failed,
                SeedReadback::error(message),
            )
        }
    }
}

fn readback_db(
    pg_url: &str,
    schema: Option<&str>,
    target: &DbSeedTarget,
    rows: &[DbRowImage],
) -> SeedReadback {
    let mut full_sql = String::new();
    for row in rows {
        let Some(statement) = build_count_sql(schema, row, None) else {
            return SeedReadback::error("cannot render db readback full-row predicate");
        };
        full_sql.push_str(&statement);
        full_sql.push('\n');
    }

    let full_counts = match run_db_readback_counts(pg_url, &full_sql, rows.len()) {
        Ok(counts) => counts,
        Err(message) => return SeedReadback::error(message),
    };
    let expected = serde_json::json!({
        "rows": rows.len(),
        "table": target.table,
        "kind": target.kind,
    });
    let mut observed = serde_json::json!({
        "full_row_matches": full_counts.clone(),
    });
    if full_counts.iter().all(|count| *count > 0) {
        return SeedReadback::matched(expected, observed);
    }

    if let Some(filter) = &target.row_filter {
        let mut key_sql = String::new();
        for row in rows {
            let Some(statement) = build_count_sql(schema, row, Some(filter)) else {
                return SeedReadback::error("cannot render db readback key predicate");
            };
            key_sql.push_str(&statement);
            key_sql.push('\n');
        }
        let key_counts = match run_db_readback_counts(pg_url, &key_sql, rows.len()) {
            Ok(counts) => counts,
            Err(message) => return SeedReadback::error(message),
        };
        if let Some(map) = observed.as_object_mut() {
            map.insert(
                "key_matches".to_owned(),
                serde_json::json!(key_counts.clone()),
            );
        }
        if key_counts.iter().any(|count| *count > 0) {
            return SeedReadback::mismatched(
                expected,
                observed,
                "db row exists by key after seed, but at least one column differs from the seed image",
            );
        }
    }

    SeedReadback::missing(
        expected,
        "db seed readback found no row matching the materialized seed image",
    )
}

fn run_db_readback_counts(
    pg_url: &str,
    sql: &str,
    expected_lines: usize,
) -> Result<Vec<u64>, String> {
    let output = psql_output(pg_url, &["-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", sql])?;
    if !output.status.success() {
        return Err(format!(
            "db seed readback exited {}; stderr='{}'",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let counts = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.trim().parse::<u64>().map_err(|error| {
                format!("db seed readback count '{line}' was not numeric: {error}")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if counts.len() != expected_lines {
        return Err(format!(
            "db seed readback returned {} count line(s), expected {expected_lines}",
            counts.len()
        ));
    }
    Ok(counts)
}

fn build_count_sql(
    schema: Option<&str>,
    row: &DbRowImage,
    filter: Option<&DbRowFilter>,
) -> Option<String> {
    let qualified_table = qualified_table(schema, &row.table);
    let predicates = match filter {
        Some(filter) => vec![db_filter_predicate(row, filter)?],
        None => {
            let mut predicates = Vec::with_capacity(row.columns.len());
            for column in &row.columns {
                predicates.push(format!(
                    "{} IS NOT DISTINCT FROM {}",
                    quote_ident(&column.metadata.name),
                    sql_literal_for_column(column)?
                ));
            }
            predicates
        }
    };
    Some(format!(
        "SELECT COUNT(*) FROM {qualified_table} WHERE {};",
        predicates.join(" AND ")
    ))
}

fn db_filter_predicate(row: &DbRowImage, filter: &DbRowFilter) -> Option<String> {
    if let Some(column) = row
        .columns
        .iter()
        .find(|column| column.metadata.name == filter.pk_column)
    {
        return Some(format!(
            "{} IS NOT DISTINCT FROM {}",
            quote_ident(&column.metadata.name),
            sql_literal_for_column(column)?
        ));
    }
    let column = DbColumnImage {
        metadata: DbColumnMetadata::unknown(&filter.pk_column),
        value: serde_json::Value::String(filter.pk_value.clone()),
    };
    Some(format!(
        "{} IS NOT DISTINCT FROM {}",
        quote_ident(&filter.pk_column),
        sql_literal_for_column(&column)?
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbSeedTarget {
    table: String,
    kind: &'static str,
    row_filter: Option<DbRowFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbRowFilter {
    pk_column: String,
    pk_value: String,
}

impl DbSeedTarget {
    fn filter_rows(&self, rows: Vec<DbRowImage>) -> Vec<DbRowImage> {
        let Some(filter) = &self.row_filter else {
            return rows;
        };
        rows.into_iter()
            .filter(|row| db_row_matches_filter(row, filter))
            .collect()
    }
}

fn db_seed_target_from_key(key: &str) -> Option<DbSeedTarget> {
    let state_key = match deja::StateKey::parse(key) {
        Ok(state_key) => state_key,
        Err(error) => {
            eprintln!(
                "deja-replay-agent: seed_db: opaque/unknown db state key '{key}': {error}; skipping"
            );
            return None;
        }
    };
    let Some(table) = state_key.db_table().map(str::to_owned) else {
        eprintln!(
            "deja-replay-agent: seed_db: typed state key '{}' has no db table; skipping",
            state_key.to_wire()
        );
        return None;
    };
    match &state_key {
        deja::StateKey::DbRow {
            pk_column,
            pk_value,
            ..
        } => Some(DbSeedTarget {
            table,
            kind: "row",
            row_filter: Some(DbRowFilter {
                pk_column: pk_column.clone(),
                pk_value: pk_value.clone(),
            }),
        }),
        deja::StateKey::DbQuery { .. } => Some(DbSeedTarget {
            table,
            kind: "query-fallback",
            row_filter: None,
        }),
        _ => {
            eprintln!(
                "deja-replay-agent: seed_db: typed state key '{}' is not a db row/query key; skipping",
                state_key.to_wire()
            );
            None
        }
    }
}

fn db_row_matches_filter(row: &DbRowImage, filter: &DbRowFilter) -> bool {
    row.columns.iter().any(|column| {
        column.metadata.name == filter.pk_column
            && db_seed_wire_value(&column.value) == filter.pk_value
    })
}

fn db_seed_wire_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned()),
    }
}

fn db_seed_value(envelope: &serde_json::Value) -> Option<serde_json::Value> {
    use deja::value::{DejaDatabaseResult, DejaDatabaseResultPayload};

    match serde_json::from_value::<DejaDatabaseResult>(envelope.clone()) {
        Ok(DejaDatabaseResult {
            payload: DejaDatabaseResultPayload::Ok { value, .. },
            ..
        }) => Some(value),
        Ok(DejaDatabaseResult {
            payload: DejaDatabaseResultPayload::Err { .. },
            ..
        }) => None,
        Err(_) => Some(envelope.clone()),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DbCatalog {
    columns_by_table: BTreeMap<String, BTreeMap<String, DbColumnMetadata>>,
}

impl DbCatalog {
    fn insert(&mut self, table: String, column: DbColumnMetadata) {
        self.columns_by_table
            .entry(table)
            .or_default()
            .insert(column.name.clone(), column);
    }

    fn metadata_for(&self, table: &str, column: &str) -> DbColumnMetadata {
        self.columns_by_table
            .get(table)
            .and_then(|cols| cols.get(column))
            .cloned()
            .unwrap_or_else(|| DbColumnMetadata::unknown(column))
    }

    fn column_count(&self) -> usize {
        self.columns_by_table.values().map(BTreeMap::len).sum()
    }
}

fn load_db_catalog(pg_url: &str) -> DbCatalog {
    let sql =
        "SELECT cls.relname, attr.attname, typ.oid::int4, typ.typname, (NOT attr.attnotnull) \
               FROM pg_catalog.pg_attribute attr \
               JOIN pg_catalog.pg_class cls ON cls.oid = attr.attrelid \
               JOIN pg_catalog.pg_namespace ns ON ns.oid = cls.relnamespace \
               JOIN pg_catalog.pg_type typ ON typ.oid = attr.atttypid \
               WHERE ns.nspname = 'public' \
                 AND attr.attnum > 0 \
                 AND NOT attr.attisdropped \
                 AND cls.relkind IN ('r', 'p') \
               ORDER BY cls.relname, attr.attnum";
    match psql_output(
        pg_url,
        &["-A", "-t", "-F", "\t", "-v", "ON_ERROR_STOP=0", "-c", sql],
    ) {
        Ok(output) if output.status.success() => {
            let mut catalog = DbCatalog::default();
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
                let parts: Vec<&str> = line.split('\t').collect();
                if parts.len() != 5 {
                    eprintln!("deja-replay-agent: skipping malformed db catalog row '{line}'");
                    continue;
                }
                catalog.insert(
                    parts[0].to_owned(),
                    DbColumnMetadata {
                        name: parts[1].to_owned(),
                        type_oid: parts[2].parse().ok(),
                        type_name: nonempty(parts[3]),
                        nullable: parse_pg_bool(parts[4]),
                    },
                );
            }
            eprintln!(
                "deja-replay-agent: loaded db catalog metadata for {} table(s), {} column(s)",
                catalog.columns_by_table.len(),
                catalog.column_count()
            );
            catalog
        }
        Ok(output) => {
            eprintln!(
                "deja-replay-agent: db catalog load exited {}; using unknown column metadata fallback: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
            DbCatalog::default()
        }
        Err(message) => {
            eprintln!(
                "deja-replay-agent: could not load db catalog metadata: {message}; using unknown column metadata fallback"
            );
            DbCatalog::default()
        }
    }
}

fn nonempty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn parse_pg_bool(value: &str) -> Option<bool> {
    match value {
        "t" | "true" | "TRUE" => Some(true),
        "f" | "false" | "FALSE" => Some(false),
        _ => None,
    }
}

fn create_db_schema(pg_url: &str, correlation_id: &str, schema: &str) {
    let schema_ident = quote_ident(schema);
    let sql = format!(
        "CREATE SCHEMA IF NOT EXISTS {schema_ident}; \
         DO $deja$ DECLARE r record; BEGIN \
           FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' LOOP \
             EXECUTE format('CREATE TABLE IF NOT EXISTS {schema_ident}.%I \
               (LIKE public.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)', \
               r.tablename, r.tablename); \
           END LOOP; \
         END $deja$;"
    );
    eprintln!(
        "deja-replay-agent: create_db_schema correlation={correlation_id} {schema} (clone of public)"
    );
    log_seed_sql(correlation_id, "create_schema", schema, &sql);
    match psql_status(pg_url, &["-v", "ON_ERROR_STOP=0", "-c", &sql]) {
        Ok(()) => {}
        Err(message) => {
            eprintln!(
                "deja-replay-agent: create_db_schema {schema} failed: {message}; continuing (best-effort)"
            );
        }
    }
}

fn log_seed_sql(correlation_id: &str, phase: &str, target: &str, sql: &str) {
    if !debug_log_enabled("DEJA_AGENT_LOG_SEED_SQL", true) {
        return;
    }
    eprintln!(
        "deja-replay-agent: seed_sql correlation={correlation_id} phase={phase} target={target}\n{sql}"
    );
}

fn debug_log_enabled(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            !matches!(value.as_str(), "0" | "false" | "off" | "no")
        })
        .unwrap_or(default)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbColumnMetadata {
    name: String,
    type_oid: Option<u32>,
    type_name: Option<String>,
    nullable: Option<bool>,
}

impl DbColumnMetadata {
    fn unknown(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            type_oid: None,
            type_name: None,
            nullable: None,
        }
    }

    fn is_bytea(&self) -> bool {
        self.type_oid == Some(17) || self.type_name.as_deref() == Some("bytea")
    }

    fn array_element_type(&self) -> Option<&str> {
        match self.type_name.as_deref()? {
            "_json" => Some("json"),
            "_jsonb" => Some("jsonb"),
            "_text" => Some("text"),
            _ => None,
        }
    }

    fn merge_typed(&self, typed: &deja::db::DbColumnImage) -> Self {
        Self {
            name: typed.name.clone(),
            type_oid: typed.type_oid.or(self.type_oid),
            type_name: typed.type_name.clone().or_else(|| self.type_name.clone()),
            nullable: typed.nullable.or(self.nullable),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DbColumnImage {
    metadata: DbColumnMetadata,
    value: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
struct DbRowImage {
    table: String,
    columns: Vec<DbColumnImage>,
}

impl DbRowImage {
    fn from_json_object(
        table: &str,
        row: &serde_json::Map<String, serde_json::Value>,
        catalog: &DbCatalog,
    ) -> Option<Self> {
        if row.is_empty() {
            return None;
        }
        let columns = row
            .iter()
            .map(|(name, value)| DbColumnImage {
                metadata: catalog.metadata_for(table, name),
                value: value.clone(),
            })
            .collect();
        Some(Self {
            table: table.to_owned(),
            columns,
        })
    }
}

fn seed_contains_null_column(rows: &[DbRowImage], column_name: &str) -> bool {
    rows.iter().any(|row| {
        row.columns
            .iter()
            .any(|column| column.metadata.name == column_name && column.value.is_null())
    })
}

fn db_row_images(table: &str, value: &serde_json::Value, catalog: &DbCatalog) -> Vec<DbRowImage> {
    match value {
        serde_json::Value::Object(map) => DbRowImage::from_json_object(table, map, catalog)
            .into_iter()
            .collect(),
        serde_json::Value::Array(items) => items
            .iter()
            .filter_map(|value| {
                value
                    .as_object()
                    .and_then(|map| DbRowImage::from_json_object(table, map, catalog))
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn db_row_images_from_typed_payload(
    expected_table: &str,
    image: &serde_json::Value,
    catalog: &DbCatalog,
) -> Result<Option<Vec<DbRowImage>>, String> {
    if !looks_like_typed_db_payload(image) {
        return Ok(None);
    }

    let typed_rows = match image {
        serde_json::Value::Array(values) => {
            let mut rows = Vec::with_capacity(values.len());
            for (idx, value) in values.iter().enumerate() {
                rows.push(typed_db_row_image(expected_table, value, catalog).map_err(
                    |message| {
                        format!(
                            "typed db row image[{idx}] for {expected_table} could not be used: {message}"
                        )
                    },
                )?);
            }
            rows
        }
        _ => vec![
            typed_db_row_image(expected_table, image, catalog).map_err(|message| {
                format!("typed db row image for {expected_table} could not be used: {message}")
            })?,
        ],
    };

    Ok(Some(typed_rows))
}

fn looks_like_typed_db_payload(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Array(values) => values.iter().any(looks_like_typed_db_payload),
        serde_json::Value::Object(map) => {
            map.get("deja_image")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| kind == deja::db::DbRowImage::KIND)
                || (map.contains_key("deja_image")
                    && map.contains_key("version")
                    && map.contains_key("table")
                    && map.contains_key("columns"))
        }
        _ => false,
    }
}

fn typed_db_row_image(
    expected_table: &str,
    value: &serde_json::Value,
    catalog: &DbCatalog,
) -> Result<DbRowImage, String> {
    let payload: deja::db::DbRowImage = serde_json::from_value(value.clone())
        .map_err(|error| format!("invalid db row image shape: {error}"))?;
    if payload.deja_image != deja::db::DbRowImage::KIND {
        return Err(format!("unsupported image kind {}", payload.deja_image));
    }
    if payload.version != deja::db::DbRowImage::VERSION {
        return Err(format!(
            "unsupported db row image version {}",
            payload.version
        ));
    }
    if payload.table != expected_table {
        return Err(format!(
            "image table {} did not match expected table {expected_table}",
            payload.table
        ));
    }
    if payload.columns.is_empty() {
        return Err("image carried no columns".to_owned());
    }
    let columns = payload
        .columns
        .iter()
        .map(|column| DbColumnImage {
            metadata: catalog
                .metadata_for(&payload.table, &column.name)
                .merge_typed(column),
            value: column.value.clone(),
        })
        .collect();
    Ok(DbRowImage {
        table: payload.table,
        columns,
    })
}

fn build_insert_sql(schema: Option<&str>, row: &DbRowImage) -> Option<String> {
    if row.columns.is_empty() {
        return None;
    }
    let col_list = row
        .columns
        .iter()
        .map(|column| quote_ident(&column.metadata.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut values = Vec::with_capacity(row.columns.len());
    for column in &row.columns {
        values.push(sql_literal_for_column(column)?);
    }
    let value_list = values.join(", ");
    let qualified_table = qualified_table(schema, &row.table);
    Some(format!(
        "INSERT INTO {qualified_table} ({col_list}) VALUES ({value_list}) ON CONFLICT DO NOTHING;"
    ))
}

fn qualified_table(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(table),
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn sql_literal_for_column(column: &DbColumnImage) -> Option<String> {
    if column.value.is_null() {
        return Some("NULL".to_owned());
    }
    if column.metadata.is_bytea() {
        let Some(bytes) = bytea_bytes_from_typed_value(&column.value) else {
            eprintln!(
                "deja-replay-agent: cannot render bytea seed value for column {}; skipping row",
                column.metadata.name
            );
            return None;
        };
        return Some(bytea_hex_literal(&bytes));
    }
    if let Some(element_type) = column.metadata.array_element_type() {
        return sql_array_literal_for_column(column, element_type);
    }
    Some(sql_literal(&column.value))
}

fn sql_array_literal_for_column(column: &DbColumnImage, element_type: &str) -> Option<String> {
    let Some(values) = array_values_from_seed_value(&column.value) else {
        eprintln!(
            "deja-replay-agent: cannot render {}[] seed value for column {}; skipping row",
            element_type, column.metadata.name
        );
        return None;
    };
    let element_literals = values
        .iter()
        .map(|value| sql_array_element_literal(value, element_type))
        .collect::<Option<Vec<_>>>()?;
    Some(format!(
        "ARRAY[{}]::{}[]",
        element_literals.join(", "),
        element_type
    ))
}

fn array_values_from_seed_value(value: &serde_json::Value) -> Option<Vec<serde_json::Value>> {
    match value {
        serde_json::Value::Array(values) => Some(values.clone()),
        serde_json::Value::String(value) => serde_json::from_str::<serde_json::Value>(value)
            .ok()
            .and_then(|parsed| match parsed {
                serde_json::Value::Array(values) => Some(values),
                _ => None,
            }),
        _ => None,
    }
}

fn sql_array_element_literal(value: &serde_json::Value, element_type: &str) -> Option<String> {
    match element_type {
        "json" | "jsonb" => {
            let json = match value {
                serde_json::Value::String(value) => {
                    serde_json::from_str::<serde_json::Value>(value)
                        .map(|parsed| parsed.to_string())
                        .unwrap_or_else(|_| serde_json::Value::String(value.clone()).to_string())
                }
                value => value.to_string(),
            };
            Some(format!("'{}'::{}", json.replace('\'', "''"), element_type))
        }
        "text" => {
            let text = match value {
                serde_json::Value::String(value) => value.clone(),
                value => value.to_string(),
            };
            Some(format!("'{}'::text", text.replace('\'', "''")))
        }
        _ => None,
    }
}

fn sql_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_owned(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => format!("'{}'", value.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

fn bytea_hex_literal(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("'\\x{hex}'::bytea")
}

fn bytea_bytes_from_typed_value(value: &serde_json::Value) -> Option<Vec<u8>> {
    match value {
        serde_json::Value::Object(map) => bytea_from_inner_array(map),
        serde_json::Value::Array(values) => bytea_from_array(values),
        serde_json::Value::String(value) => {
            if let Some(hex) = value.strip_prefix("\\x") {
                decode_hex(hex)
            } else if value.len() % 2 == 0 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                decode_hex(value)
            } else {
                Some(value.as_bytes().to_vec())
            }
        }
        _ => None,
    }
}

fn bytea_from_inner_array(map: &serde_json::Map<String, serde_json::Value>) -> Option<Vec<u8>> {
    if map.len() != 1 {
        return None;
    }
    bytea_from_array(map.get("inner")?.as_array()?)
}

fn bytea_from_array(values: &[serde_json::Value]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let n = value.as_u64()?;
        if n > 255 {
            return None;
        }
        out.push(n as u8);
    }
    Some(out)
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(chunk).ok()?;
        bytes.push(u8::from_str_radix(pair, 16).ok()?);
    }
    Some(bytes)
}

fn render_redis_seed_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn psql_status(pg_url: &str, args: &[&str]) -> Result<(), String> {
    let output = psql_output(pg_url, args)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "psql exited {}; stderr='{}' stdout='{}'",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
            String::from_utf8_lossy(&output.stdout).trim()
        ))
    }
}

fn psql_output(pg_url: &str, args: &[&str]) -> Result<Output, String> {
    let mut command = Command::new("psql");
    command.arg(pg_url);
    command.args(args);
    command
        .output()
        .map_err(|error| format!("could not run psql: {error}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn typed_db_image_without_producer_metadata_uses_catalog_metadata() {
        let typed_image = deja::db::DbRowImage::new(
            "business_profile",
            vec![
                deja::db::DbColumnImage {
                    name: "profile_id".to_owned(),
                    type_oid: None,
                    type_name: None,
                    nullable: None,
                    value: serde_json::json!("pro_123"),
                },
                deja::db::DbColumnImage {
                    name: "key".to_owned(),
                    type_oid: None,
                    type_name: None,
                    nullable: None,
                    value: serde_json::json!({"inner": [1, 2, 3]}),
                },
            ],
        )
        .to_value();
        let mut catalog = DbCatalog::default();
        catalog.insert(
            "business_profile".to_owned(),
            DbColumnMetadata {
                name: "profile_id".to_owned(),
                type_oid: Some(25),
                type_name: Some("text".to_owned()),
                nullable: Some(false),
            },
        );
        catalog.insert(
            "business_profile".to_owned(),
            DbColumnMetadata {
                name: "key".to_owned(),
                type_oid: Some(17),
                type_name: Some("bytea".to_owned()),
                nullable: Some(false),
            },
        );

        let rows = db_row_images_from_typed_payload("business_profile", &typed_image, &catalog)
            .expect("typed image parse")
            .expect("typed image should seed with catalog-backed metadata");
        let sql = build_insert_sql(Some("deja_test"), &rows[0]).expect("insert sql");

        assert!(sql.contains("INSERT INTO \"deja_test\".\"business_profile\""));
        assert!(sql.contains("'pro_123'"));
        assert!(
            sql.contains("'\\x010203'::bytea"),
            "catalog bytea metadata should render the typed value as bytea: {sql}"
        );
    }

    #[test]
    fn typed_db_image_table_mismatch_does_not_use_legacy_fallback() {
        let typed_image = deja::db::DbRowImage::new(
            "other_table",
            vec![deja::db::DbColumnImage {
                name: "profile_id".to_owned(),
                type_oid: None,
                type_name: None,
                nullable: None,
                value: serde_json::json!("pro_123"),
            }],
        )
        .to_value();

        let error = db_row_images_from_typed_payload(
            "business_profile",
            &typed_image,
            &DbCatalog::default(),
        )
        .expect_err("typed image mismatch should be a hard typed-path error");

        assert!(
            error.contains("did not match expected table business_profile"),
            "{error}"
        );
    }

    #[test]
    fn catalog_metadata_renders_typed_json_array_column_without_producer_types() {
        let typed_image = deja::db::DbRowImage::new(
            "merchant_connector_account",
            vec![deja::db::DbColumnImage {
                name: "payment_methods_enabled".to_owned(),
                type_oid: None,
                type_name: None,
                nullable: None,
                value: serde_json::Value::String(
                    r#"[{"payment_method":"card","payment_method_types":[{"payment_method_type":"credit"}]}]"#
                        .to_owned(),
                ),
            }],
        )
        .to_value();
        let mut catalog = DbCatalog::default();
        catalog.insert(
            "merchant_connector_account".to_owned(),
            DbColumnMetadata {
                name: "payment_methods_enabled".to_owned(),
                type_oid: None,
                type_name: Some("_json".to_owned()),
                nullable: Some(true),
            },
        );

        let rows =
            db_row_images_from_typed_payload("merchant_connector_account", &typed_image, &catalog)
                .expect("typed image parse")
                .expect("typed image rows");
        let sql = build_insert_sql(Some("deja_test"), &rows[0]).expect("insert sql");

        assert!(sql.contains("ARRAY["));
        assert!(sql.contains("\"payment_method\":\"card\""));
        assert!(sql.contains("::json[]"));
        assert!(
            !sql.contains(r#"'[{"payment_method""#),
            "catalog-backed typed json[] column must not render one quoted JSON array string: {sql}"
        );
    }

    #[test]
    fn json_array_column_renders_postgres_array_expression() {
        let row = DbRowImage {
            table: "merchant_connector_account".to_owned(),
            columns: vec![DbColumnImage {
                metadata: DbColumnMetadata {
                    name: "payment_methods_enabled".to_owned(),
                    type_oid: None,
                    type_name: Some("_json".to_owned()),
                    nullable: Some(true),
                },
                value: serde_json::Value::String(
                    r#"[{"payment_method":"card","payment_method_types":[{"payment_method_type":"credit"}]}]"#
                        .to_owned(),
                ),
            }],
        };

        let sql = build_insert_sql(Some("deja_test"), &row).expect("insert sql");

        assert!(sql.contains("ARRAY["));
        assert!(sql.contains("'"));
        assert!(sql.contains("\"payment_method\":\"card\""));
        assert!(sql.contains("::json[]"));
        assert!(
            !sql.contains(r#"'[{"payment_method""#),
            "json[] columns must not render one quoted JSON array string: {sql}"
        );
    }

    #[test]
    fn redis_api_lock_keys_are_runtime_state() {
        assert!(is_runtime_redis_lock_key(
            "public:API_LOCK_merchant_1783670219_payments_pay_123"
        ));
        assert!(is_runtime_redis_lock_key(
            "API_LOCK_merchant_1783670219_payments_customer_123"
        ));
        assert!(!is_runtime_redis_lock_key("settlement_rate_premium"));
        assert!(!is_runtime_redis_lock_key(
            "public:merchant_cache:merchant_1783670219"
        ));
    }
}
