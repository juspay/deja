//! The recorder's bind-derived read keys feed every recording, so moving their
//! column-to-bind mapping into the runtime must not change one key. The
//! implementation before the move is kept here verbatim as the oracle.

#![allow(clippy::unwrap_used)] // tests panic on failure by design

use deja::db::binds_read_keys;

#[allow(clippy::all)]
fn binds_read_keys_before_252(table: &str, sql: &str) -> Vec<String> {
    let Some(identity) = deja_runtime::replay::table_identity_columns(table) else {
        return Vec::new();
    };
    let Some(binds_at) = sql.rfind(" -- binds: ") else {
        return Vec::new();
    };
    let (query, binds_raw) = sql.split_at(binds_at);
    let binds_raw = binds_raw.trim_start_matches(" -- binds: ").trim();
    // Diesel debug-prints binds as a bracketed list; strings/numbers/bools
    // are JSON-compatible. Anything richer fails the parse and yields no
    // keys rather than a fabricated one.
    let Ok(binds) = serde_json::from_str::<Vec<serde_json::Value>>(binds_raw) else {
        return Vec::new();
    };

    /// Every value this statement binds by equality to `column`, in the
    /// order the predicates appear.
    fn bound_values<'a>(
        query: &str,
        binds: &'a [serde_json::Value],
        column: &str,
    ) -> Vec<&'a serde_json::Value> {
        let needle = format!("\"{column}\" = $");
        let mut values = Vec::new();
        let mut cursor = 0;
        while let Some(found) = query[cursor..].find(&needle) {
            let digits_start = cursor + found + needle.len();
            let digits: String = query[digits_start..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            cursor = digits_start;
            if let Some(value) = digits
                .parse::<usize>()
                .ok()
                .and_then(|position| position.checked_sub(1))
                .and_then(|index| binds.get(index))
            {
                values.push(value);
            }
        }
        values
    }

    // One row per equality predicate on the FIRST key column, each
    // completed by the other key columns' bound values. A statement that
    // binds one key column several times (an `IN`-style rewrite) but the
    // rest only once still addresses one row per leading value, so the
    // remaining columns reuse their single binding.
    let mut per_column: Vec<(String, Vec<&serde_json::Value>)> = Vec::new();
    for column in identity {
        let values = bound_values(query, &binds, &column);
        if values.is_empty() {
            // A key column this statement does not constrain: the
            // predicate cannot name a single row, so produce nothing.
            return Vec::new();
        }
        per_column.push((column, values));
    }
    let Some((_, leading)) = per_column.first() else {
        return Vec::new();
    };

    let mut keys = Vec::new();
    for index in 0..leading.len() {
        let row: serde_json::Map<String, serde_json::Value> = per_column
            .iter()
            .map(|(column, values)| {
                let value = values.get(index).or_else(|| values.first());
                (
                    column.clone(),
                    value.map_or(serde_json::Value::Null, |value| (*value).clone()),
                )
            })
            .collect();
        if let Some(key) =
            deja_runtime::replay::db_row_state_key(table, &serde_json::Value::Object(row))
        {
            let wire = key.to_wire();
            if !keys.contains(&wire) {
                keys.push(wire);
            }
        }
    }
    keys
}

fn register_identity() {
    deja::register_table_identity([
        ("configs".to_owned(), vec!["key".to_owned()]),
        ("business_profile".to_owned(), vec!["profile_id".to_owned()]),
        (
            "merchant_account".to_owned(),
            vec!["merchant_id".to_owned()],
        ),
        (
            "merchant_key_store".to_owned(),
            vec!["merchant_id".to_owned()],
        ),
        (
            "customers".to_owned(),
            vec!["customer_id".to_owned(), "merchant_id".to_owned()],
        ),
        (
            "incremental_authorization".to_owned(),
            vec!["authorization_id".to_owned(), "merchant_id".to_owned()],
        ),
        // The tables the recorded corpus touches most.
        (
            "payment_attempt".to_owned(),
            vec!["attempt_id".to_owned(), "merchant_id".to_owned()],
        ),
        (
            "payment_intent".to_owned(),
            vec!["payment_id".to_owned(), "merchant_id".to_owned()],
        ),
        ("address".to_owned(), vec!["address_id".to_owned()]),
        (
            "payment_methods".to_owned(),
            vec!["payment_method_id".to_owned()],
        ),
        ("process_tracker".to_owned(), vec!["id".to_owned()]),
        (
            "refund".to_owned(),
            vec!["merchant_id".to_owned(), "refund_id".to_owned()],
        ),
    ]);
}

/// Every statement shape the mapping distinguishes: unregistered tables,
/// single and composite keys, a key column left unbound (the path whose early
/// return was removed), repeated leading columns, out-of-range and `$0`
/// positions, and bind lists the strict parser accepts or refuses.
fn grid() -> Vec<(String, String)> {
    let tables: [(&str, &[&str]); 6] = [
        ("configs", &["key"]),
        ("business_profile", &["profile_id", "merchant_id"]),
        (
            "incremental_authorization",
            &["authorization_id", "merchant_id"],
        ),
        ("customers", &["customer_id", "merchant_id"]),
        ("unregistered", &["id"]),
        // A key column whose name ends another column's (`"id"` in `"merchant_id"`).
        ("process_tracker", &["id"]),
    ];
    let positions = ["1", "2", "3", "0", "9"];
    let binds = [
        r#"["a", "b", "c"]"#,
        r#"["a", 7, null]"#,
        r#"[null, "b", "a"]"#,
        r#"[true, 1.5, "a"]"#,
        r#"["a", ["b"], {"c": 1}]"#,
        r#"[Id("a"), "b", "c"]"#,
        "[]",
        r#"["a"]"#,
    ];
    let mut cases = Vec::new();
    for (table, key_columns) in tables {
        let mut columns: Vec<&str> = key_columns.to_vec();
        columns.push("other");
        if !columns.contains(&"merchant_id") {
            columns.push("merchant_id");
        }
        let mut predicates = Vec::new();
        for column in &columns {
            for position in positions {
                predicates.push(format!(r#"("{table}"."{column}" = ${position})"#));
            }
        }
        let mut wheres = vec![String::new()];
        // The leading key column bound several times, the rest once.
        if let [leading, rest @ ..] = key_columns {
            for column in rest {
                wheres.push(format!(
                    r#"(("{table}"."{leading}" = $1) OR ("{table}"."{leading}" = $2)) AND ("{table}"."{column}" = $3)"#
                ));
            }
        }
        for first in &predicates {
            wheres.push(first.clone());
            for second in &predicates {
                wheres.push(format!("{first} AND {second}"));
                wheres.push(format!("{first} OR {second}"));
            }
        }
        for clause in &wheres {
            let query = if clause.is_empty() {
                format!(r#"SELECT * FROM "{table}""#)
            } else {
                format!(r#"DELETE FROM "{table}" WHERE ({clause})"#)
            };
            cases.push((table.to_owned(), query.clone()));
            for list in binds {
                cases.push((table.to_owned(), format!("{query} -- binds: {list}")));
            }
        }
    }
    cases
}

#[test]
fn bind_read_keys_are_unchanged_by_the_move() {
    register_identity();
    let mut cases = grid();
    // Optionally, real recorded statements: one `table<TAB>sql` per line.
    if let Ok(path) = std::env::var("DEJA_BINDS_CORPUS") {
        for line in std::fs::read_to_string(path).unwrap().lines() {
            if let Some((table, sql)) = line.split_once('\t') {
                cases.push((table.to_owned(), sql.to_owned()));
            }
        }
    }
    let (mut keyed, mut unbound_key_column) = (0, 0);
    for (table, sql) in &cases {
        let before = binds_read_keys_before_252(table, sql);
        assert_eq!(binds_read_keys(table, sql), before, "{table}: {sql}");
        keyed += usize::from(!before.is_empty());
        unbound_key_column += usize::from(
            table == "business_profile"
                && sql.contains("-- binds")
                && sql.contains(r#""profile_id" = $1"#)
                && !sql.contains(r#""merchant_id" = $"#),
        );
    }
    eprintln!(
        "{} cases, {keyed} with keys, {unbound_key_column} with a key column unbound",
        cases.len()
    );
    assert!(
        keyed > 1_000,
        "the comparison must reach statements that produce keys: {keyed}"
    );
    assert!(
        unbound_key_column > 100,
        "and the unbound-key-column path: {unbound_key_column}"
    );
}
