//! Differential fuzz driver: pgrust FE/BE message-length FRAMING (shipped
//! Rust) vs vendored PostgreSQL 18.3 C (csrc/pg_wireframe_oracle.c, Stamp 18.3
//! @ 62d6c7d).
//!
//! Closes the GAP that EDGE2 recorded in `edge.rs::wire_drivers` and
//! findings-edge2.md: the frontend startup-packet / `pq_getmessage` OUTER
//! length word had no verbatim-C oracle in `csrc/`, so the ST3 memory-safety
//! class (`i32::from_be_bytes(len) - 4` at INT32_MIN) could be sprayed by the
//! wire bank but not DIFFERENTIALLY checked. This vendors the narrow
//! length-arithmetic slice and wires it into the wire campaign.
//!
//! Two framing paths, selected by the leading byte:
//!   * STARTUP (backend_startup.c:532-542): `len = ntoh32(w); len -= 4;`
//!     THEN `if (len < 4 || len > 10000) reject`. Subtract-BEFORE-check — an
//!     INT_MIN length word wraps on `-4`.
//!   * GETMESSAGE (pqcomm.c:1221-1231): `len = ntoh32(w);` THEN
//!     `if (len < 4 || len > maxlen) reject; len -= 4;`. Check-BEFORE-subtract.
//!
//! The shipped-Rust arithmetic is mirrored here from the real source
//! (crates/backend/tcop/backend_startup/src/lib.rs:433-434 and
//! crates/backend/libpq/pqcomm/src/lib.rs:447-457) rather than calling the
//! real functions, which are bound to the pq recv buffer, MyProcPort, Mcx and
//! the SSL/GSS/cancel state machine (EDGE2's "too entangled"). The extracted
//! plane is the pure length arithmetic; the citation is exact so an auditor
//! can verify it byte-for-byte against the two source lines.
//!
//! RELEASE-EFFECTIVE MODEL (debug-assert-masking law): the shipped startup
//! source writes a plain `let len = i32::from_be_bytes(len_bytes) - 4;`. In a
//! release/fuzzing build (overflow checks off) that subtraction WRAPS
//! two's-complement — exactly what a compiled C backend does — so the model
//! below uses `wrapping_sub(4)`, which is the semantics that actually ships
//! and the semantics the campaign runs under. The plain `- 4` panicking under
//! debug overflow-checks is a documented fragility (findings-vendor-wire.md),
//! not the compared plane.
//!
//! Compared plane: (accept?, body_length) must be identical between the
//! shipped Rust model and the vendored C oracle. A divergence is a HIGH
//! finding (a malformed length word that pgrust accepts / mis-sizes where C
//! cleanly rejects, or vice versa).

use core::ffi::c_int;

extern "C" {
    fn pg_wireframe_startup_len(len_bytes: *const u8, out_body: *mut i32) -> c_int;
    fn pg_wireframe_getmessage_len(len_bytes: *const u8, maxlen: i32, out_body: *mut i32)
        -> c_int;
}

/// One framing verdict: `None` == reject, `Some(body_len)` == accept with the
/// body length (packet minus the 4-byte length word).
type Verdict = Option<i32>;

/// C oracle: startup-packet length framing.
fn c_startup(len_bytes: &[u8; 4]) -> Verdict {
    let mut body = 0i32;
    // SAFETY: `len_bytes` is a 4-byte array; the C reads exactly 4 bytes and
    // writes `body` only on accept. Pure function, no shared state.
    let rc = unsafe { pg_wireframe_startup_len(len_bytes.as_ptr(), &mut body) };
    if rc == 0 {
        Some(body)
    } else {
        None
    }
}

/// C oracle: pq_getmessage outer length framing.
fn c_getmessage(len_bytes: &[u8; 4], maxlen: i32) -> Verdict {
    let mut body = 0i32;
    // SAFETY: as `c_startup`.
    let rc = unsafe { pg_wireframe_getmessage_len(len_bytes.as_ptr(), maxlen, &mut body) };
    if rc == 0 {
        Some(body)
    } else {
        None
    }
}

/// Shipped-Rust model: startup-packet length framing.
///
/// Mirrors `process_startup_packet` (backend_startup/src/lib.rs:433-434):
/// ```text
///     let len = i32::from_be_bytes(len_bytes) - 4;   // wraps in release
///     if len < SIZEOF_PROTOCOL_VERSION || len > MAX_STARTUP_PACKET_LENGTH { reject }
/// ```
/// SIZEOF_PROTOCOL_VERSION = 4, MAX_STARTUP_PACKET_LENGTH = 10000.
fn rust_startup(len_bytes: &[u8; 4]) -> Verdict {
    const SIZEOF_PROTOCOL_VERSION: i32 = 4;
    const MAX_STARTUP_PACKET_LENGTH: i32 = 10000;
    // `wrapping_sub(4)` = the shipped `- 4` under release overflow semantics.
    let len = i32::from_be_bytes(*len_bytes).wrapping_sub(4);
    if len < SIZEOF_PROTOCOL_VERSION || len > MAX_STARTUP_PACKET_LENGTH {
        None
    } else {
        Some(len)
    }
}

/// Shipped-Rust model: pq_getmessage outer length framing.
///
/// Mirrors `pq_getmessage` (pqcomm/src/lib.rs:447-457):
/// ```text
///     let len = i32::from_be_bytes(lenbuf);
///     if len < 4 || len > maxlen { reject }
///     let len = (len - 4) as usize;   // len >= 4 here, cannot underflow
/// ```
fn rust_getmessage(len_bytes: &[u8; 4], maxlen: i32) -> Verdict {
    let len = i32::from_be_bytes(*len_bytes);
    if len < 4 || len > maxlen {
        None
    } else {
        // len >= 4 on this path, so `- 4` cannot underflow (matches source,
        // which does `(len - 4) as usize`).
        Some(len - 4)
    }
}

/// A representative spread of `maxlen` caps for the getmessage path. `maxlen`
/// in the backend is a caller-supplied cap (e.g. PQ_LARGE_MESSAGE_LIMIT-ish);
/// fuzzing it alongside the length word exercises the `len > maxlen` boundary
/// (including the pathological negative/zero caps).
const MAXLEN_CAPS: &[i32] = &[i32::MIN, -1, 0, 3, 4, 8, 10000, 1 << 20, i32::MAX];

/// The differential driver. Sprayed by the wire campaign as
/// `[selector][frame...]`, where `frame` begins with the big-endian length
/// word (exactly how `edge::wire_frames()` lays out its leading length field,
/// so the ST3 INT32_MIN word lands in the length position).
pub fn wire_length_diff(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    // One-thread-at-a-time through the C oracles (harness discipline; the
    // wireframe oracle is pure/stateless, but the campaign runs every driver
    // under this serial and the lint gate requires the guard on any frame that
    // reaches a C extern).
    let _oracle = crate::oracle_serial();
    let sel = data[0];
    let payload = &data[1..];

    // Assemble the 4-byte length word, zero-padding a short tail (a truncated
    // length word is itself a case the framing must survive).
    let mut w = [0u8; 4];
    for (i, b) in payload.iter().take(4).enumerate() {
        w[i] = *b;
    }

    // Even selector -> startup path; odd -> getmessage path.
    if sel & 1 == 0 {
        let c = c_startup(&w);
        let r = rust_startup(&w);
        assert_eq!(
            r, c,
            "STARTUP length-framing divergence: len_word={:02x?} rust={:?} c={:?}",
            w, r, c
        );
    } else {
        // Pick a maxlen cap from the selector so a single frame is checked
        // against several caps across the spray.
        let maxlen = MAXLEN_CAPS[(sel as usize >> 1) % MAXLEN_CAPS.len()];
        let c = c_getmessage(&w, maxlen);
        let r = rust_getmessage(&w, maxlen);
        assert_eq!(
            r, c,
            "GETMESSAGE length-framing divergence: len_word={:02x?} maxlen={} rust={:?} c={:?}",
            w, maxlen, r, c
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Enumerate every interesting length word × path × cap and assert the
    /// shipped Rust model and the vendored C oracle agree. This is the
    /// exhaustive parity gate (regression guard for the confirmed-guarded
    /// pgrust framing); always green.
    #[test]
    fn wire_length_parity_bank() {
        let _serial = crate::c_oracle_serial();
        // Big-endian length words spanning the ST3 / boundary shapes.
        let words: Vec<i32> = vec![
            i32::MIN,
            i32::MIN + 1,
            -2,
            -1,
            0,
            1,
            2,
            3,
            4,
            5,
            7,
            8,
            9,
            9999,
            10000,
            10001,
            10004,
            10005,
            1 << 20,
            i32::MAX - 1,
            i32::MAX,
        ];

        let mut checked = 0usize;
        for &v in &words {
            let w = v.to_be_bytes();
            // startup
            assert_eq!(
                rust_startup(&w),
                c_startup(&w),
                "startup divergence at len_word={v}"
            );
            checked += 1;
            // getmessage across every cap
            for &cap in MAXLEN_CAPS {
                assert_eq!(
                    rust_getmessage(&w, cap),
                    c_getmessage(&w, cap),
                    "getmessage divergence at len_word={v} maxlen={cap}"
                );
                checked += 1;
            }
        }
        assert!(checked > 100, "parity bank ran too few checks: {checked}");
    }

    /// Pin the known-good verdicts so the oracle wiring itself is proven live
    /// (an all-agreeing pair could both be broken the same way). These are the
    /// hand-computed REL_18_3 answers.
    #[test]
    fn wire_length_known_verdicts() {
        let _serial = crate::c_oracle_serial();
        // STARTUP: value 8 -> body 4 (>= sizeof(ProtocolVersion)), accept.
        assert_eq!(rust_startup(&8i32.to_be_bytes()), Some(4));
        assert_eq!(c_startup(&8i32.to_be_bytes()), Some(4));
        // STARTUP: value 4 -> body 0 < 4 -> reject.
        assert_eq!(c_startup(&4i32.to_be_bytes()), None);
        // STARTUP: INT_MIN wraps on -4 to a huge positive -> reject (NOT a
        // panic, NOT an accept) — the whole point of the ST3 surface.
        assert_eq!(c_startup(&i32::MIN.to_be_bytes()), None);
        assert_eq!(rust_startup(&i32::MIN.to_be_bytes()), None);
        // STARTUP: max legal body 10000 -> value 10004 accept; 10005 reject.
        assert_eq!(c_startup(&10004i32.to_be_bytes()), Some(10000));
        assert_eq!(c_startup(&10005i32.to_be_bytes()), None);

        // GETMESSAGE: value 4 -> body 0 accept (len>=4 ok, unlike startup).
        assert_eq!(rust_getmessage(&4i32.to_be_bytes(), 1 << 20), Some(0));
        assert_eq!(c_getmessage(&4i32.to_be_bytes(), 1 << 20), Some(0));
        // GETMESSAGE: INT_MIN -> len<4 -> reject (subtract never reached).
        assert_eq!(c_getmessage(&i32::MIN.to_be_bytes(), i32::MAX), None);
        // GETMESSAGE: value 8 > maxlen 4 -> reject.
        assert_eq!(c_getmessage(&8i32.to_be_bytes(), 4), None);
    }

    /// DETECTION-POWER CONTROL (must-fail control). A 0-divergence sweep is
    /// only evidence if the differential CAN report a divergence. Plant a
    /// buggy startup model that DROPS the upper-bound check (a plausible
    /// regression: forgetting `len > MAX_STARTUP_PACKET_LENGTH`) and assert it
    /// diverges from the vendored C on a length word C rejects. Without this,
    /// "parity bank green" is indistinguishable from an oracle that always
    /// agrees.
    #[test]
    fn wire_length_diff_detects_planted_bug() {
        let _serial = crate::c_oracle_serial();
        // Planted regression: only the lower bound is checked.
        fn rust_startup_buggy(len_bytes: &[u8; 4]) -> Verdict {
            let len = i32::from_be_bytes(*len_bytes).wrapping_sub(4);
            if len < 4 {
                None
            } else {
                Some(len) // BUG: missing `|| len > 10000`
            }
        }

        // A length word that is well above MAX_STARTUP_PACKET_LENGTH: C rejects
        // it, the buggy model accepts it -> the differential MUST catch it.
        let w = 50_000i32.to_be_bytes();
        let c = c_startup(&w);
        let buggy = rust_startup_buggy(&w);
        assert_eq!(c, None, "control precondition: C must reject len_word=50000");
        assert_ne!(
            buggy, c,
            "detection-power control FAILED: buggy model did not diverge from C"
        );
        // And the correct model must AGREE with C on the same word (so the
        // divergence is attributable to the bug, not the harness).
        assert_eq!(
            rust_startup(&w),
            c,
            "correct model unexpectedly diverges from C"
        );
    }
}
