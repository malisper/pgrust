//! The frontend v3 startup-packet assembler (`fe-protocol3.c`
//! `build_startup_packet` fused with its caller `pqBuildStartupPacket3`).
//!
//! `fe-protocol3.c` is dominated by routines that compute over the live `PGconn`
//! / `PGresult` data model while pulling bytes through the `fe-misc.c` buffer
//! layer — that message-dispatch half (`pqParseInput3` / `getRowDescriptions` /
//! `getAnotherTuple` / `pqGetErrorNotice3`) lives in [`crate::client`], driven
//! synchronously over the [`crate::transport::Transport`] byte stream. The one
//! part that is a pure byte-codec with no live-connection dependency is the
//! startup-packet builder, ported here 1:1.

/// `PG_PROTOCOL(m, n)` from `pqcomm.h`: `((m) << 16) | (n)`.
pub const fn pg_protocol(m: u32, n: u32) -> u32 {
    (m << 16) | n
}

/// `PG_PROTOCOL(3, 0)` — the protocol version this client speaks.
pub const PG_PROTOCOL_3_0: u32 = pg_protocol(3, 0);

/// One entry of libpq's `EnvironmentOptions[]` table (`PQEnvironmentOption`):
/// the name of an environment variable and the name of the corresponding SET
/// variable that env value should be turned into in the startup packet.
#[derive(Clone, Copy, Debug)]
pub struct PqEnvironmentOption {
    /// Name of an environment variable (`envName`).
    pub env_name: &'static str,
    /// Name of the corresponding SET variable (`pgName`).
    pub pg_name: &'static str,
}

/// libpq's `EnvironmentOptions[]` array, copied verbatim from `fe-connect.c`.
/// `build_startup_packet` walks it, and for each variable that is set in the
/// process environment (and not left at `"default"`) appends a `pgName=value`
/// startup option.
pub const ENVIRONMENT_OPTIONS: &[PqEnvironmentOption] = &[
    PqEnvironmentOption { env_name: "PGDATESTYLE", pg_name: "datestyle" },
    PqEnvironmentOption { env_name: "PGTZ", pg_name: "timezone" },
    PqEnvironmentOption { env_name: "PGGEQO", pg_name: "geqo" },
];

/// The subset of `PGconn` fields that `build_startup_packet` reads: the
/// negotiated protocol version plus the startup string options, already
/// resolved by `fe-connect.c` connection setup.
///
/// In C these are raw `char *` members of `PGconn` that may be NULL or point to
/// an empty string; the C macro only emits an option when the pointer is
/// non-NULL *and* the first character is non-NUL. We model "NULL or empty" as
/// `None` (a present-but-empty `Some("")` is also treated as absent, exactly
/// like the C `optval[0]` test).
#[derive(Clone, Debug, Default)]
pub struct StartupParams<'a> {
    /// `conn->pversion` — the protocol version word that leads the packet.
    pub pversion: u32,
    /// `conn->pguser`.
    pub pguser: Option<&'a str>,
    /// `conn->dbName`.
    pub db_name: Option<&'a str>,
    /// `conn->replication`.
    pub replication: Option<&'a str>,
    /// `conn->pgoptions`.
    pub pgoptions: Option<&'a str>,
    /// `conn->send_appname` — whether the application_name option is sent at all.
    pub send_appname: bool,
    /// `conn->appname` (preferred when `send_appname`).
    pub appname: Option<&'a str>,
    /// `conn->fbappname` (fallback application name when `appname` is absent).
    pub fbappname: Option<&'a str>,
    /// `conn->client_encoding_initial`.
    pub client_encoding_initial: Option<&'a str>,
}

/// Frontend version of the backend's `add_size()`, API-compatible with the
/// `pg_add_*_overflow()` helpers. Stores the result into `*dst` on success;
/// returns `true` instead if the addition overflows. 1:1 with the C static
/// `add_size_overflow`.
fn add_size_overflow(s1: usize, s2: usize, dst: &mut usize) -> bool {
    // C: result = s1 + s2; if (result < s1 || result < s2) return true;
    // Use wrapping_add so a genuine overflow wraps (and is then caught by the
    // `< s1 || < s2` test) rather than panicking in debug builds, mirroring C's
    // unsigned `size_t` wraparound semantics exactly.
    let result = s1.wrapping_add(s2);
    if result < s1 || result < s2 {
        return true;
    }
    *dst = result;
    false
}

/// Append one NUL-terminated C string's bytes to `packet`. Used by the
/// `ADD_STARTUP_OPTION` expansion below. OOM-safe (`try_reserve`).
fn append_cstr(packet: &mut Vec<u8>, s: &str) -> Result<(), StartupPacketError> {
    packet
        .try_reserve(s.len() + 1)
        .map_err(|_| StartupPacketError::OutOfMemory)?;
    packet.extend_from_slice(s.as_bytes());
    packet.push(0u8);
    Ok(())
}

/// Reasons `build_startup_packet` / [`build_startup_packet3`] can fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupPacketError {
    /// `size_t` overflow while measuring the packet length (C:
    /// `build_startup_packet` returns 0). Cannot happen for any realistically-
    /// sized connection.
    SizeOverflow,
    /// The measured length exceeded `INT_MAX` (C: `pqBuildStartupPacket3`
    /// returns NULL when `len > INT_MAX`).
    TooLong,
    /// Allocation failed while assembling the packet (C: `malloc` returned
    /// NULL).
    OutOfMemory,
}

/// `INT_MAX` for the 32-bit `int` the C code compares the length against.
const INT_MAX: usize = i32::MAX as usize;

/// Look up an environment variable, the analog of libpq's `getenv` call in
/// `build_startup_packet`. A non-UTF8 value is treated as absent (libpq reads a
/// `char *`; the byte-exact value reaches the wire, but the only consumers are
/// the three GUC names above whose values are ASCII).
fn getenv(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// `pg_strcasecmp(a, "default")` reduced to the one comparison the builder
/// needs: ASCII case-insensitive equality with the literal `"default"`.
fn is_default(val: &str) -> bool {
    val.eq_ignore_ascii_case("default")
}

/// Build a startup packet given a filled-in connection view.
///
/// 1:1 port of `build_startup_packet` *fused* with its caller
/// `pqBuildStartupPacket3`. The C code runs `build_startup_packet` twice (once
/// with `packet == NULL` to size, once to fill); since the `Vec` grows
/// OOM-safely we assemble directly into a single buffer and the "sizing" pass is
/// unnecessary. The `INT_MAX` ceiling and the `size_t` overflow checks
/// (`add_size_overflow`) are preserved so the observable success/failure
/// conditions and the emitted bytes are identical to libpq.
///
/// The emitted layout (big-endian protocol version word, then NUL-terminated
/// `name`/`value` option pairs, then a trailing NUL terminator) is byte-for-byte
/// what the backend's `ProcessStartupPacket` expects.
pub fn build_startup_packet3(
    conn: &StartupParams<'_>,
    options: &[PqEnvironmentOption],
) -> Result<Vec<u8>, StartupPacketError> {
    // We track `packet_len` with `add_size_overflow` exactly as C does, so the
    // overflow/TooLong failure modes match, even though the Vec also knows its
    // own length.
    let mut packet = Vec::<u8>::new();
    let mut packet_len: usize = 0;

    // Protocol version comes first: pg_hton32(conn->pversion), 4 bytes BE.
    packet
        .try_reserve(core::mem::size_of::<u32>())
        .map_err(|_| StartupPacketError::OutOfMemory)?;
    packet.extend_from_slice(&conn.pversion.to_be_bytes());
    packet_len += core::mem::size_of::<u32>();

    // ADD_STARTUP_OPTION(optname, optval): append optname\0 then optval\0,
    // accumulating packet_len via add_size_overflow and bailing on overflow.
    macro_rules! add_startup_option {
        ($optname:expr, $optval:expr) => {{
            let optname: &str = $optname;
            let optval: &str = $optval;
            append_cstr(&mut packet, optname)?;
            if add_size_overflow(packet_len, optname.len() + 1, &mut packet_len) {
                return Err(StartupPacketError::SizeOverflow);
            }
            append_cstr(&mut packet, optval)?;
            if add_size_overflow(packet_len, optval.len() + 1, &mut packet_len) {
                return Err(StartupPacketError::SizeOverflow);
            }
        }};
    }

    // Add user name, database name, options. The C `optval[0]` test (pointer
    // non-NULL AND first char non-NUL) is `Some(non-empty)` here.
    if let Some(v) = nonempty(conn.pguser) {
        add_startup_option!("user", v);
    }
    if let Some(v) = nonempty(conn.db_name) {
        add_startup_option!("database", v);
    }
    if let Some(v) = nonempty(conn.replication) {
        add_startup_option!("replication", v);
    }
    if let Some(v) = nonempty(conn.pgoptions) {
        add_startup_option!("options", v);
    }
    if conn.send_appname {
        // Use appname if present, otherwise use fallback.
        let val = conn.appname.or(conn.fbappname);
        if let Some(v) = nonempty(val) {
            add_startup_option!("application_name", v);
        }
    }

    if let Some(v) = nonempty(conn.client_encoding_initial) {
        add_startup_option!("client_encoding", v);
    }

    // Add any environment-driven GUC settings needed.
    for eo in options {
        if let Some(val) = getenv(eo.env_name) {
            if !is_default(&val) {
                add_startup_option!(eo.pg_name, val.as_str());
            }
        }
    }

    // Add trailing terminator.
    packet
        .try_reserve(1)
        .map_err(|_| StartupPacketError::OutOfMemory)?;
    packet.push(0u8);
    if add_size_overflow(packet_len, 1, &mut packet_len) {
        return Err(StartupPacketError::SizeOverflow);
    }

    // pqBuildStartupPacket3: reject a zero-length / over-INT_MAX result.
    if packet_len == 0 || packet_len > INT_MAX {
        return Err(StartupPacketError::TooLong);
    }

    // The byte count we tracked must match the buffer we actually built (the
    // equivalent of C's `Assert(*packetlen == len)`).
    debug_assert_eq!(packet_len, packet.len());

    Ok(packet)
}

/// C's `optval && optval[0]`: a present, non-empty string. NULL pointers and
/// pointers to an empty string both suppress the option.
fn nonempty(v: Option<&str>) -> Option<&str> {
    match v {
        Some(s) if !s.is_empty() => Some(s),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_protocol_word() {
        assert_eq!(PG_PROTOCOL_3_0, 0x0003_0000);
    }

    #[test]
    fn startup_packet_user_db_layout() {
        let params = StartupParams {
            pversion: PG_PROTOCOL_3_0,
            pguser: Some("postgres"),
            db_name: Some("mydb"),
            ..Default::default()
        };
        let p = build_startup_packet3(&params, &[]).unwrap();
        // version word (BE) + "user\0postgres\0database\0mydb\0" + terminating \0
        let mut expect = Vec::new();
        expect.extend_from_slice(&PG_PROTOCOL_3_0.to_be_bytes());
        expect.extend_from_slice(b"user\0postgres\0database\0mydb\0\0");
        assert_eq!(p, expect);
    }

    #[test]
    fn empty_options_suppressed() {
        let params = StartupParams {
            pversion: PG_PROTOCOL_3_0,
            pguser: Some(""), // empty -> suppressed, like C optval[0]==0
            ..Default::default()
        };
        let p = build_startup_packet3(&params, &[]).unwrap();
        // Just the version word + the single trailing NUL.
        let mut expect = Vec::new();
        expect.extend_from_slice(&PG_PROTOCOL_3_0.to_be_bytes());
        expect.push(0);
        assert_eq!(p, expect);
    }

    #[test]
    fn replication_option() {
        let params = StartupParams {
            pversion: PG_PROTOCOL_3_0,
            pguser: Some("repl"),
            replication: Some("true"),
            ..Default::default()
        };
        let p = build_startup_packet3(&params, &[]).unwrap();
        let mut expect = Vec::new();
        expect.extend_from_slice(&PG_PROTOCOL_3_0.to_be_bytes());
        expect.extend_from_slice(b"user\0repl\0replication\0true\0\0");
        assert_eq!(p, expect);
    }
}
