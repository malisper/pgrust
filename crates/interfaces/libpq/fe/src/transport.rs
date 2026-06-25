//! The byte-stream transport abstraction the v3 client driver runs over.
//!
//! `fe-secure.c` (`pqsecure_read`/`pqsecure_write`) and `fe-misc.c`
//! (`pqReadData`/`pqFlush`) bottom out in `recv(2)`/`send(2)` on the connection
//! socket (optionally through the TLS/GSS layer). The driver does not hard-wire
//! a socket: it is generic over a blocking byte stream so the same state
//! machines can run over a real `TcpStream`/`UnixStream` ([`SocketTransport`],
//! the registry provider's default) or an in-process pipe (the loopback tests
//! drive a mock backend over a `socketpair`). This keeps the protocol logic
//! independent of the OS socket, which lives behind the [`SocketTransport`]
//! leaf.
//!
//! The trait is intentionally a *blocking* read/write, matching the synchronous
//! `libpqsrv_exec` / `PQexec` call shape the seam consumers use (those block
//! until the result is available). The async `PQconnectPoll` cursor model is not
//! reproduced — a synchronous driver is the faithful behaviour for the blocking
//! entry points, and is all walreceiver + ecpg require.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;

use std::io::{Read, Write};

/// Errors the client transport / driver can raise. These mirror the libpq
/// failure surfaces the consumers branch on: a connection-level failure
/// (`PQstatus == CONNECTION_BAD`), a protocol violation (the C "insufficient
/// data" / "unexpected message" paths), an OOM (the `try_reserve` failure
/// path), and a backend ErrorResponse carrying SQLSTATE + message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportError {
    /// I/O on the underlying byte stream failed or the peer closed it
    /// unexpectedly (`pqReadData` returning -1, EOF mid-message). Carries a
    /// human-readable detail for `PQerrorMessage`.
    Io(String),
    /// The backend sent a malformed or unexpected message (the C
    /// `handleSyncLoss` / "insufficient data" / "unexpected message" paths).
    ProtocolViolation,
    /// A data-derived allocation failed (`try_reserve`), the OOM path.
    OutOfMemory,
    /// Authentication failed or an unsupported auth method was requested (this
    /// minimal client implements the trust / cleartext-password paths only).
    AuthFailed(String),
}

impl TransportError {
    /// Render the error as the human-readable text libpq's `PQerrorMessage`
    /// would surface.
    pub fn message(&self) -> String {
        match self {
            TransportError::Io(s) => s.clone(),
            TransportError::ProtocolViolation => {
                "lost synchronization with server: got unexpected protocol data".to_string()
            }
            TransportError::OutOfMemory => "out of memory".to_string(),
            TransportError::AuthFailed(s) => s.clone(),
        }
    }
}

/// A blocking, ordered, reliable byte stream to the backend. `read_exact`
/// fills the whole buffer or fails; `write_all` writes the whole buffer or
/// fails; `flush` pushes buffered writes out (the analog of `pqFlush`).
pub trait Transport {
    /// Read exactly `buf.len()` bytes, blocking until they arrive. An EOF
    /// before the buffer is filled is an `Io` error (the C mid-message EOF /
    /// "server closed the connection unexpectedly" path).
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError>;

    /// Write the whole of `buf`, blocking as needed.
    fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError>;

    /// Flush buffered writes to the peer (`pqFlush`).
    fn flush(&mut self) -> Result<(), TransportError>;

    /// The underlying socket file descriptor, if any (`PQsocket`). `-1` when the
    /// transport is not socket-backed (the in-process test pipe).
    fn raw_fd(&self) -> i32 {
        -1
    }

    /// Whether at least one byte can be read without blocking — the analog of
    /// `pqReadReady` / a 0-timeout `poll(POLLIN)` on the socket. Used by the
    /// async `PQgetCopyData`/`PQconsumeInput` path so the COPY-out reader can
    /// return "no data yet" instead of blocking, letting the walreceiver flush
    /// what it has and wait on the latch+socket. The default (no fd) reports
    /// `true` so the blocking-read path is preserved for the in-process pipe.
    fn read_ready(&self) -> bool {
        true
    }
}

/// A boxed transport is itself a transport (lets the registry store a
/// connection over any concrete byte stream — TCP, Unix-domain, or a test pipe —
/// behind one `PgClientConn<Box<dyn Transport>>` type).
impl Transport for Box<dyn Transport> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        (**self).read_exact(buf)
    }
    fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        (**self).write_all(buf)
    }
    fn flush(&mut self) -> Result<(), TransportError> {
        (**self).flush()
    }
    fn raw_fd(&self) -> i32 {
        (**self).raw_fd()
    }
    fn read_ready(&self) -> bool {
        (**self).read_ready()
    }
}

/// The real-OS socket transport: the leaf where `fe-secure.c`'s plaintext
/// `pqsecure_raw_read`/`pqsecure_raw_write` (this is the `--without-ssl
/// --without-gssapi` build) bottom out in `recv(2)`/`send(2)`. Any `Read +
/// Write` std stream (a `TcpStream`, a `UnixStream`) can back it.
pub struct SocketTransport<S: Read + Write> {
    stream: S,
    fd: i32,
}

impl<S: Read + Write> SocketTransport<S> {
    /// Wrap a connected blocking stream. `fd` is the stream's file descriptor
    /// for `PQsocket` (pass `-1` if unknown / not applicable).
    pub fn new(stream: S, fd: i32) -> Self {
        SocketTransport { stream, fd }
    }
}

impl<S: Read + Write> Transport for SocketTransport<S> {
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        // std::io::Read::read_exact already returns UnexpectedEof on a short
        // read, the analog of the C mid-message EOF.
        Read::read_exact(&mut self.stream, buf).map_err(|e| {
            TransportError::Io(format!(
                "server closed the connection unexpectedly: {e}"
            ))
        })
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        Write::write_all(&mut self.stream, buf)
            .map_err(|e| TransportError::Io(format!("could not send data to server: {e}")))
    }

    fn flush(&mut self) -> Result<(), TransportError> {
        Write::flush(&mut self.stream)
            .map_err(|e| TransportError::Io(format!("could not flush data to server: {e}")))
    }

    fn raw_fd(&self) -> i32 {
        self.fd
    }

    fn read_ready(&self) -> bool {
        if self.fd < 0 {
            // No real fd to poll; fall back to the blocking-read path.
            return true;
        }
        // poll(POLLIN, timeout=0): the non-blocking readability probe libpq's
        // pqReadReady performs before an async PQgetCopyData.
        let mut pfd = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: pfd is a valid single pollfd; timeout 0 returns immediately.
        let rc = unsafe { libc::poll(&mut pfd, 1, 0) };
        // rc > 0 with POLLIN/POLLHUP/POLLERR set means a read won't block (data
        // ready, or EOF/error which read_exact will surface). rc == 0 means no
        // data yet; rc < 0 (EINTR etc.) — treat as "try a read" (matches C
        // retrying), i.e. ready.
        if rc < 0 {
            return true;
        }
        rc > 0 && (pfd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR)) != 0
    }
}
