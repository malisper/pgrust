`statement_timeout` now covers the `COPY FROM STDIN` apply phase that begins after `CopyDone`.

It does not yet cover the upload phase while the client is still sending `CopyData` frames.

Current limitation:
- The wire protocol layer buffers incoming `CopyData` bytes in memory until `CopyDone`.
- Row parsing and heap/index writes only begin after buffering is complete.

Implication:
- A client can spend arbitrary time uploading `CopyData` without triggering `statement_timeout`.
- Once `CopyDone` arrives and row application starts, the normal interrupt checks and timeout handling apply.

Future work:
- Refactor `COPY FROM STDIN` to stream rows through parsing and execution as data arrives instead of buffering the entire upload first.
- Arm the statement interrupt state across that streaming ingest path so upload-time semantics match PostgreSQL more closely.
