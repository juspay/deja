//! The span cursors have one door.
//!
//! `CURRENT_PATH` and `CURRENT_LINEAGE` were two bare thread-locals holding the
//! span-path address and task lineage a boundary stamps itself with. Their
//! `on_exit` reset them from the span TREE PARENT rather than from the value the
//! thread held before the enter, so a span entered without its parent entered — a
//! spawned task polled on a worker — left the previous request's path and bucket
//! standing for whatever fired on that thread next. Four readers took them with no
//! check that a span was entered.
//!
//! Accessors alone were not the answer: the readers went THROUGH accessors, and the
//! accessors were the ones handing out a value that had no owner. So the cell is now
//! a stack of frames tagged by the span id that pushed each one, with exactly three
//! functions permitted to touch it — and that permission is asserted here, at the
//! source level, in the shape `deja-orchestrator`'s
//! `the_raw_tape_path_has_exactly_one_home` already uses for the recording tape.
//!
//! What this guard does and does not cover is worth stating plainly, because the
//! original defect lived INSIDE `on_exit`, which is a legitimate writer:
//!
//!   * this grep is the guard against the next unchecked READER — a new call site
//!     that takes the cursor without asking whose it is now fails the build,
//!     including in a test, which is where the next one will look most harmless;
//!   * the guard against the next `on_exit` is the runtime invariant in
//!     `correlation_layer`'s own test module — that a balanced enter/exit leaves
//!     this thread's cursors exactly as it found them — which needs crate-private
//!     access and so cannot live out here.
//!
//! Both are needed. Neither replaces the other.

use std::path::{Path, PathBuf};

/// The thread-local the cursors live in.
const CELL: &str = "ENTERED_SPANS";

/// The file that owns it.
const HOME: &str = "correlation_layer.rs";

/// The only functions permitted to touch it: two writers and one reader.
const DOORS: [&str; 3] = ["push_span_cursor", "pop_span_cursor", "with_current_cursor"];

#[test]
fn the_span_cursors_are_touched_only_through_their_doors() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");

    let mut foreign: Vec<String> = Vec::new();
    let mut undoored: Vec<String> = Vec::new();
    let mut doored: Vec<&str> = Vec::new();

    for path in rust_sources(&src) {
        let body = std::fs::read_to_string(&path).expect("read source");
        if !body.contains(CELL) {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) != Some(HOME) {
            foreign.push(format!(
                "{} ({} hit(s))",
                path.display(),
                body.matches(CELL).count()
            ));
            continue;
        }

        // Inside the home file, attribute every use to the function it sits in.
        let mut enclosing: Option<&str> = None;
        for (index, line) in body.lines().enumerate() {
            if let Some(name) = declared_function(line) {
                enclosing = Some(name);
            }
            if !line.contains(CELL) || is_prose(line) || line.contains("static ") {
                continue;
            }
            match enclosing.and_then(|name| DOORS.iter().copied().find(|door| *door == name)) {
                Some(door) => doored.push(door),
                None => undoored.push(format!(
                    "{}:{} (in `{}`)",
                    path.display(),
                    index + 1,
                    enclosing.unwrap_or("<no enclosing fn>")
                )),
            }
        }
    }

    assert!(
        foreign.is_empty(),
        "the span cursors escaped {HOME} — read the active span path and lineage \
         through `current_span_path` / `current_span_lineage`, which hand out \
         nothing when no span is entered. Offenders: {foreign:?}"
    );
    assert!(
        undoored.is_empty(),
        "a use of `{CELL}` outside {DOORS:?} — take the cursor through \
         `with_current_cursor`, so a reader cannot get a path or a bucket without \
         a span being entered to own it. Offenders: {undoored:?}"
    );
    for door in DOORS {
        assert!(
            doored.contains(&door),
            "`{door}` no longer touches `{CELL}` — the cursors were reshaped and \
             this guard is now vacuous; re-derive {DOORS:?} from the new shape"
        );
    }
}

/// Every `.rs` file under `dir`, recursively.
fn rust_sources(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read_dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                found.push(path);
            }
        }
    }
    found
}

/// The name a `fn` line declares, ignoring visibility.
fn declared_function(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if is_prose(trimmed) {
        return None;
    }
    let rest = ["pub(crate) ", "pub ", "async ", "unsafe "]
        .iter()
        .fold(trimmed, |rest, prefix| {
            rest.strip_prefix(prefix).unwrap_or(rest)
        });
    rest.strip_prefix("fn ")?
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .next()
        .filter(|name| !name.is_empty())
}

/// Comments and doc comments name these items constantly; only code counts.
fn is_prose(line: &str) -> bool {
    line.trim_start().starts_with("//")
}
