# deja-tui

Terminal explorer for Déjà/Hyperswitch semantic and execution-graph artifacts.

```sh
cargo run -p deja-tui -- /tmp/deja-hyperswitch-semantic-graph
cargo run -p deja-tui -- --summary /tmp/deja-hyperswitch-semantic-graph
```

The path may be an artifact root with `semantic/semantic-events.jsonl` and
`graph/execution-graph.jsonl`, or a directory directly containing either JSONL
file. The TUI tolerates missing files and malformed JSONL lines; skipped counts
are shown in the overview and summary output.

The default view is request-first: pick a request/correlation ID on the left,
move into the nested tree, then select a semantic event or graph span. The right
pane shows the full JSON record, including semantic `request`, `args`,
`response`, and `result` fields.

Keys: `q` quit, `/` search, `b` boundary filter, `r` request/correlation filter,
`e` toggle error-only rows, `tab` switch views, `left/right` switch between the
request list and nested tree, `up/down` move selection, `enter` enter the nested
tree for the selected request.
