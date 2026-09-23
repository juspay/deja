default:
    just verify

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

test:
    cargo test --workspace

# The dashboard's result ladder decides what a run is reported as, including
# which runs may render green. Two of its rungs are unreachable from production
# data, so a live sample cannot stand in for this.
#
# Installs on first run because `web/node_modules` is gitignored; a fresh
# checkout would otherwise fail here with a missing-vitest error that says
# nothing about what to do next.
test-web:
    [ -d web/node_modules ] || npm --prefix web ci
    npm --prefix web test

verify:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    just test-web
