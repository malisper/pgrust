//! Idiomatic Rust port of the **libpq frontend (client) library**
//! (`src/interfaces/libpq/fe-*.c`), scoped to the simple-query + replication
//! COPY path the in-process consumers (`replication/libpqwalreceiver`, ecpg)
//! actually drive.
//!
//! # What this crate is
//!
//! A faithful, minimal-but-real frontend libpq v3 client transport. It maps the
//! `fe-*.c` translation unit onto modules:
//!
//!  * [`codec`] — `fe-misc.c` + `fe-protocol3.c` byte layer: the `pqGetInt` /
//!    `pqGets` / `pqGetnchar` readers, the `pqPutMsgStart` / `pqPutMsgEnd`
//!    writers, network byte order, the v3 framing.
//!  * [`protocol3`] — `fe-protocol3.c` startup-packet assembler
//!    (`build_startup_packet3`).
//!  * [`client`] — the `PQconnectPoll` (`fe-connect.c`) connection state-machine
//!    tail, the `PQexec` simple-query path (`fe-exec.c`), and the `pqParseInput3`
//!    message dispatch (`fe-protocol3.c`), driven synchronously over a
//!    [`transport::Transport`] byte stream.
//!  * [`result`] — the owned `PGresult` model (`fe-exec.c`); `PQclear` is `Drop`.
//!  * [`transport`] — the blocking byte-stream abstraction + the real-OS
//!    [`transport::SocketTransport`] leaf (the plaintext `--without-ssl
//!    --without-gssapi` `recv`/`send` floor).
//!  * [`registry`] — the handle-registry adapter that installs the
//!    [`interfaces_libpq_fe_seams`] opaque-handle (`PgConnId` / `PgResultId`)
//!    contract over the owned objects.
//!
//! # What is faithfully deferred (loud, never silent)
//!
//!  * **fe-auth.c / fe-auth-scram.c** — MD5 / SCRAM / SASL / GSS response loops.
//!    The trust and cleartext-password paths are real; any other
//!    `AuthenticationRequest` is a loud [`transport::TransportError::AuthFailed`]
//!    (not a faked-auth stub).
//!  * **fe-secure*.c** — TLS / GSS negotiation: this is the plaintext build; the
//!    SSL/GSS startup-request packets are not sent.
//!  * **conninfo string parsing** (`PQconninfoParse` / `PQconninfo`, the
//!    1462-line `fe-connect.c` option-table + URI parser) and the
//!    **server-encoding escapers** (`PQescapeLiteral` / `PQescapeIdentifier`):
//!    these bottom out in this crate's `registry::unported_*` loud seams.
//!  * **extended-query protocol** (`PQexecParams` / Parse-Bind-Execute), the
//!    **async `PQconnectPoll` cursor**, **fe-print.c**, and **fe-lobj.c**: out of
//!    scope for the simple-query consumers; not part of this crate's surface.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

pub mod client;
pub mod codec;
pub mod conninfo_parse;
pub mod protocol3;
pub mod registry;
pub mod result;
pub mod transport;

pub use client::PgClientConn;
pub use protocol3::{StartupParams, PG_PROTOCOL_3_0};
pub use result::{
    ExecStatusType, PGresult, PgResAttDesc, PgResAttValue, PgTransactionStatusType,
};
pub use transport::{SocketTransport, Transport, TransportError};

/// Install every grounded seam this crate owns
/// ([`interfaces_libpq_fe_seams`]'s `libpqsrv_*` transport + `pq_*` accessor
/// surface). Called once at startup from `seams-init`.
///
/// The conninfo-parse and escape legs are intentionally left at their loud
/// `registry::unported_*` default (REAL-OR-LOUD discipline): the consumer fails
/// with a clear "not yet ported" panic rather than a silent fake.
pub fn init_seams() {
    registry::install();
}

#[cfg(test)]
mod tests;
