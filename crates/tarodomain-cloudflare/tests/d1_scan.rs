//! Source scan: only the statement layer calls a database backend.
//!
//! An adapter that ran a query by itself would escape the unit of work
//! (plan Risks: a query that forgets the transaction).

use std::fs;
use std::path::Path;

/// The files that may talk to a backend or a raw D1 handle.
const ALLOWED: &[&str] = &["d1.rs", "sqlite_backend.rs"];

const FORBIDDEN: &[&str] = &[
    "backend.all(",
    "backend.run(",
    "backend.batch(",
    ".prepare(",
    "D1Database",
    "rusqlite",
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
        // Skip the platform glue's own docs and the wrapper it hands out.
        for needle in FORBIDDEN {
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
