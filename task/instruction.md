`/app/data` holds four `.pcx` session archives from a decommissioned pipe-chain processing
cluster. The container format is documented in `/app/spec/pcx-v1.md`, which is normative.

Each archive is an independent session recording: a sequence of binary-encoded events that
describe how a set of threads read and write variables, spawn and join child threads, and
exchange data through shared pipes.

Replay every session's events in file order and determine the final state of each session.

Write `/app/out/state.jsonl`: one JSON object per line for every variable held by every
alive thread across all sessions, each with exactly the keys `session` (string, the archive
basename), `thread_id` (integer) and `var` (string) and `value` (integer). Order the lines
by `session` ascending, then `thread_id` ascending, then `var` ascending.

Write `/app/out/pipes.jsonl`: one JSON object per line for every pipe in every session,
each with exactly the keys `session` (string), `pipe_id` (integer), `remaining` (array of
integers — values still in the pipe buffer after all events, in buffer order),
`total_written` (integer) and `total_read` (integer). Order by `session` ascending, then
`pipe_id` ascending.

Write `/app/out/summary.json`: a JSON array with one object per session ordered by session
name ascending, each with exactly the keys:
- `name` — the archive basename
- `total_events` — integer, number of events in the session
- `n_threads` — integer, number of distinct thread IDs that appear in the session
- `n_vars` — integer, number of distinct variable names referenced
- `n_pipes` — integer, number of distinct pipe IDs referenced
- `alive_threads` — array of integers, IDs of threads not terminated by a JOIN, ascending
- `checksum` — integer, bitwise XOR of all final variable values of alive threads
