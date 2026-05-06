# Wire Protocol — Deferred Features

This note records what is intentionally missing from the current PostgreSQL
wire protocol implementation in `src/server.rs`.

The current code supports:

- SSL negotiation (declines with `N`)
- startup message parsing (protocol version 3.0, user/database params)
- trust authentication (AuthenticationOk with no password exchange)
- ParameterStatus messages (server_version, client_encoding, etc.)
- BackendKeyData
- ReadyForQuery
- simple query protocol (`Q` message)
- RowDescription, DataRow, CommandComplete, EmptyQueryResponse, ErrorResponse
- Terminate (`X`) handling
- thread-per-connection model

That is enough for psql to connect and run simple queries. It is not a
complete implementation of the PostgreSQL frontend/backend protocol.

## Extended query protocol

The extended query protocol (Parse, Bind, Describe, Execute, Sync, Close,
Flush) is not implemented. This is used by most client libraries (libpq with
parameterized queries, JDBC, node-postgres, etc.) for:

- prepared statements
- parameterized queries (`$1`, `$2`)
- portals with row-limited fetches
- binary result format

The server currently rejects extended query messages with an ErrorResponse.

**To add:** Implement a prepared statement cache per connection. Parse stores
a parsed plan; Bind substitutes parameters and creates a portal; Execute
fetches rows from the portal; Sync marks a transaction boundary.

## SSL/TLS support

The server declines SSL with `N`. psql falls back to plaintext, but many
clients and all production deployments require encrypted connections.

**To add:** Accept the SSLRequest, respond with `S`, and perform a TLS
handshake using `rustls` or `native-tls` before proceeding with the startup
message. Requires adding a TLS dependency and certificate configuration.

## Authentication methods

Only trust authentication is implemented (immediate AuthenticationOk). PostgreSQL
supports:

- cleartext password (AuthenticationCleartextPassword, type 3)
- MD5 password (AuthenticationMD5Password, type 5)
- SCRAM-SHA-256 (AuthenticationSASL, type 10)
- GSS/SSPI (types 7, 8, 9)
- certificate authentication

**To add:** At minimum, SCRAM-SHA-256 for compatibility with modern PostgreSQL
client defaults. Requires a user/password store and the SCRAM message exchange
(SASLInitialResponse, AuthenticationSASLContinue, SASLResponse,
AuthenticationSASLFinal).

## Cancel request

The CancelRequest message (magic number 80877102) allows a client to cancel
a running query on another connection. The server does not handle this.

**To add:** Parse the CancelRequest, match the PID and secret key to a
running connection, and set a cancellation flag that the executor checks
between tuple fetches.

## COPY protocol

The COPY IN/OUT/BOTH sub-protocol for bulk data loading and extraction is not
implemented. `COPY FROM STDIN` and `COPY TO STDOUT` use dedicated message
types (CopyInResponse `G`, CopyOutResponse `H`, CopyData `d`, CopyDone `c`,
CopyFail `f`).

**To add:** A COPY parser and a streaming interface that reads/writes rows
in the COPY wire format.

## Multi-statement queries

The simple query protocol allows semicolon-separated statements in a single
`Q` message. The current implementation treats the entire string as one
statement. PostgreSQL processes each statement in sequence, sending results
for each before ReadyForQuery.

**To add:** Split the query string on semicolons (respecting string literals),
execute each statement, and send results for each before the final
ReadyForQuery.

## Correct type OIDs in RowDescription

The current implementation sends type OID 25 (TEXT) for all columns regardless
of the actual column type. PostgreSQL sends the correct OID (23 for INT4, 16
for BOOL, 25 for TEXT, etc.), which clients use for type coercion.

**To add:** Carry the scalar type from the executor's column metadata into
the RowDescription message and map `ScalarType::Int32` to OID 23,
`ScalarType::Bool` to OID 16, etc.

## Binary format

The simple query protocol always uses text format (format code 0). The extended
query protocol can request binary format (format code 1) for result columns.
Binary format is not supported.

**To add:** Implement binary encoding for each value type (INT4 as 4 big-endian
bytes, BOOL as 1 byte, TEXT as raw bytes) and set the format code in
RowDescription when requested.

## ErrorResponse detail fields

The current ErrorResponse includes only severity (S/V), SQLSTATE code (C), and
message (M). PostgreSQL also supports:

- Detail (D) — additional error detail
- Hint (H) — suggested remediation
- Position (P) — character position in the query string
- Internal Position (p) — position in an internal query
- Internal Query (q) — the internal query text
- Where (W) — call stack context
- Schema/Table/Column/Datatype/Constraint names (s/t/c/d/n)
- File/Line/Routine (F/L/R) — source location

**To add:** Thread error context through the executor and parser so that
position, table name, and column name can be included in error responses.

## NoticeResponse

Warning and informational messages (NoticeResponse, `N`) are not sent. These
are used for deprecation warnings, implicit type coercions, and similar
non-fatal messages.

**To add:** A notice mechanism in the executor that collects warnings during
statement execution and sends them before CommandComplete.

## SET / SHOW / RESET

PostgreSQL's `SET`, `SHOW`, and `RESET` commands for runtime parameters
(search_path, work_mem, statement_timeout, etc.) are not implemented. psql
sends some of these during startup (e.g., `SET client_encoding TO 'UTF8'`).

**To add:** A per-session parameter store, parser support for SET/SHOW/RESET
syntax, and GUC-like parameter definitions.

## pg_catalog system tables

psql queries `pg_catalog` tables for tab completion, `\d` commands, and
other introspection. The server intercepts many common psql introspection
queries via shims in `src/backend/tcop/postgres.rs`, but direct SQL access
to `pg_catalog` tables still fails with "unknown table" errors. Key missing
tables include `pg_class`, `pg_attribute`, `pg_type`, `pg_namespace`,
`pg_index`, `pg_database`, `pg_roles`.

**To add:** Real virtual tables backed by catalog metadata, eliminating the
need for query-specific shims.

## LISTEN / NOTIFY

Asynchronous notification (LISTEN, NOTIFY, UNLISTEN) and the corresponding
NotificationResponse message are not implemented.

**To add:** A shared notification channel registry and an
asynchronous-message path in the connection handler.

## Streaming replication protocol

The replication protocol (walsender, walreceiver, logical replication messages)
is not implemented.

**To add:** Replication slot management, WAL streaming, and the replication
sub-protocol message types.

## Connection limits

There is no limit on the number of concurrent connections. PostgreSQL has
`max_connections`, per-user limits, and per-database limits.

**To add:** An `AtomicU32` connection counter checked at accept time, with
an immediate ErrorResponse (`53300 too_many_connections`) when the limit is
exceeded.

## Graceful shutdown

The server runs until the process is killed. There is no signal handler for
SIGTERM/SIGINT to drain active connections before exiting.

**To add:** A shutdown flag checked by the accept loop and a mechanism to
notify active connections to finish their current query and disconnect.

## Unix domain sockets

The server only listens on TCP. PostgreSQL also listens on a Unix domain
socket (typically `/var/run/postgresql/.s.PGSQL.5432`) for local connections.

**To add:** Bind a `UnixListener` alongside the `TcpListener`.

## Protocol version negotiation

The server only accepts protocol version 3.0 (196608). PostgreSQL 17+
supports protocol 3.2 with variable-length BackendKeyData. Earlier versions
(protocol 2.0) are rejected.

**To add:** Parse the minor version from the startup message. For 3.2+, use
the variable-length BackendKeyData format. Send NegotiateProtocolVersion if
the client requests unsupported protocol parameters.
