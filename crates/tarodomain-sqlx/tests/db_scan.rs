//! Source scan: only the statement layer runs queries.
//!
//! An adapter that ran a query on the pool would escape the unit of work
//! (plan Risks: a query that forgets the transaction).

use std::fs;
use std::path::Path;

/// The files that may talk to sqlx directly.
const ALLOWED: &[&str] = &["db.rs", "unit_of_work.rs", "lib.rs"];

/// The provider takes the pools in its constructors and hands them to `Db`,
/// so it may name the pool types, but nothing else in the list.
const MAY_NAME_POOLS: &[&str] = &["provider.rs"];

const FORBIDDEN: &[&str] = &[
    "sqlx::query",
    ".fetch_all(",
    ".fetch_one(&",
    ".fetch_optional(",
    ".execute(pool",
    ".execute(&mut",
    "SqlitePool",
    "PgPool",
];

#[test]
fn only_the_statement_layer_runs_sql() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    for entry in fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap().to_owned();
        if !name.ends_with(".rs") || ALLOWED.contains(&name.as_str()) {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap();
        // A file's unit tests build their own pools; they come last.
        let text = text
            .split("\n#[cfg(all(test")
            .next()
            .and_then(|code| code.split("\n#[cfg(test)]").next())
            .unwrap_or_default()
            .to_owned();
        for needle in FORBIDDEN {
            let is_pool = needle.ends_with("Pool");
            if is_pool && MAY_NAME_POOLS.contains(&name.as_str()) {
                continue;
            }
            if text.lines().any(|line| {
                let code = line.trim_start();
                !code.starts_with("//") && code.contains(needle)
            }) {
                offenders.push(format!("{name}: {needle}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "SQL outside the statement layer: {offenders:?}"
    );
}

#[test]
fn the_scan_sees_the_statement_layer() {
    // Guards against the scan passing because the directory is empty.
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    assert!(src.join("db.rs").exists());
}
