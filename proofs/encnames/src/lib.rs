//! Kani C≡Rust equivalence: encoding-name lookups —
//! PG_char_to_encoding (oid 1264) / PG_encoding_to_char (oid 1597).
//!
//! Rust side (shipped code, path-dep — never copied):
//!  - cores mbutils::pg_char_to_encoding / mbutils::pg_encoding_to_char
//!    (crates/backend/utils/mb/mbutils/src/lib.rs:185/214) over the
//!    PG_ENCNAME (81-row) and PG_ENC2NAME (42-row) static tables;
//!  - shipped NameData framing types_tuple::NameData::namestrcpy
//!    (in-theorem for oid 1597 — the C wrapper returns a Name);
//!  - shipped fmgr wrapper adt_misc::builtins::fc_pg_char_to_encoding
//!    (wrapper-plane witness harness ONLY — see below).
//!
//! C side: proofs/encnames/c/pg_encnames.c (REL_18_STABLE
//! common/encnames.c + mb/pg_wchar.h; provenance + shims documented
//! there — notably isalnum -> C-locale ASCII model).
//!
//! CLAIMS
//!  - oid 1597 (eq_encoding_to_char_frame): FULL-i32 encoding domain;
//!    the whole 64-byte NameData frame is byte-compared (C result string
//!    projected through namestrcpy-semantics plumbing, Rust through the
//!    SHIPPED NameData::namestrcpy).  The fmgr wrapper's namein
//!    round-trip on the C side truncates at 63 bytes exactly like
//!    namestrcpy — no name in the table is near that (max 14).
//!  - oid 1264 core (eq_char_to_encoding_core_*): clean_encoding_name +
//!    binary-search equivalence over symbolic name bytes, len<=16,
//!    fenced to (a) NUL-free ASCII and (b) NUL-free valid UTF-8 (the
//!    Rust core takes &str; the NUL fence models the C-string cut that
//!    the Name arg guarantees in both implementations — the shipped
//!    wrapper cuts at the first NUL exactly like C's strlen view).
//!    spot_char_to_encoding_len63/len64 pin the NAMEDATALEN boundary
//!    (len 63 walks the clean+search path, len 64 short-circuits to -1
//!    via strlen>=NAMEDATALEN vs name.len()>=64).
//!
//! SCREENED DIVERGENCE PLANE (extraction-gap triage 2026-07-28), oid 1264
//! wrapper: adt_misc fc_pg_char_to_encoding does
//! `core::str::from_utf8(&raw[..nul]).unwrap_or("")` — non-UTF8 name
//! bytes are rejected WHOLESALE (-1), where C's clean_encoding_name
//! strips them byte-wise (e.g. "utf\xFF8": C -> "utf8" -> 6, Rust -> -1).
//! witness_wrapper_non_utf8_plane is the EXPECTED-FAIL divergence
//! witness for that plane.  RUNNER LANE: do NOT record this as
//! divergence(bug) or divergence(ratified) from the counterexample
//! alone — ground-truth on a real glibc PostgreSQL first
//! (SELECT convert_from(...)/pg_char_to_encoding via a Name containing
//! a high byte), because glibc isalnum is locale-sensitive where the
//! vendored C models the C locale; adjudicate wrapper fix vs ratified
//! difference after that, per the triage plan.
//!
//! Negative control: control_encoding_to_char_frame_shifted compares
//! the C frame for `enc` against the Rust frame for `enc + 1` — MUST
//! FAIL (run with the DEFAULT solver, as must the witness harness;
//! kissat never terminates on failing harnesses).
//!
//! Unwind derivations (in-comment at each harness): clean_encoding_name
//! iterates len+1 C-side (incl. NUL test) — caps 16/63/64; the binary
//! search runs ceil(log2(81)) = 7 probes, each strcmp <= 15 bytes (max
//! table key "shiftjis2004" + NUL); the 64B frame loops run 64 (+1 exit)
//! iterations.

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use std::os::raw::{c_char, c_int};
    use types_fmgr::LocalFcinfo;
    use types_tuple::NameData;

    extern "C" {
        /// verbatim common/encnames.c entry (NUL-terminated name in)
        fn pg_char_to_encoding(name: *const c_char) -> c_int;
        /// harness plumbing: pg_encoding_to_char projected into a 64-byte
        /// zero-padded namestrcpy-semantics frame (see C file; int return
        /// is the void/Unit goto-cc shim, value ignored)
        fn pg_encoding_to_char_frame(encoding: c_int, out: *mut u8) -> c_int;
    }

    // ---- oid 1597: full-i32 encoding -> 64-byte NameData frame ----

    /// unwind 66: two 64-iteration frame loops C-side (+1 exit each);
    /// Rust namestrcpy is fill + copy_from_slice (memcpy model, no loop).
    #[kani::proof]
    #[kani::unwind(66)]
    fn eq_encoding_to_char_frame() {
        let enc: i32 = kani::any(); // full i32 domain
        let mut cframe = [0u8; 64];
        unsafe { pg_encoding_to_char_frame(enc, cframe.as_mut_ptr()) };
        let name = mbutils::pg_encoding_to_char(enc);
        let mut n = NameData::default();
        n.namestrcpy(name); // SHIPPED framing in-theorem
        // both arms reachable: valid encoding (non-empty name) and the
        // out-of-range -> "" arm
        kani::cover!(!name.is_empty());
        kani::cover!(name.is_empty());
        assert!(n.data == cframe);
    }

    // ---- oid 1264 core: clean + binary search ----

    /// Symbolic name bytes, len<=16, NUL-free ASCII fence (subset of the
    /// valid-UTF-8 fence below; kept as the cheap always-green tier).
    /// UNRESOLVED (RVR lane 2026-07-28): the strcmp unwinding assertion
    /// FAILS at unwind 18/20 and still probes iteration 40 at unwind 40 —
    /// CBMC cannot bound the builtin strcmp against `position->name`
    /// reached via C pointer-arithmetic binary search (symbolic pointer
    /// set; deref-failure cascade at pg_encnames.c:511). All FAILED runs
    /// are unwind/model artifacts per the tight-unwind law (native
    /// differential 416k inputs = 0 diffs). Row stands as
    /// tested(differential) until the search seam is reshaped (e.g.
    /// index-based shim projection instead of pointer arithmetic).
    #[kani::proof]
    #[kani::unwind(40)]
    fn eq_char_to_encoding_core_ascii() {
        const CAP: usize = 16;
        let buf: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let mut i = 0;
        while i < len {
            // NUL-free: models the C-string cut the Name arg guarantees
            // identically on both sides; ASCII: valid UTF-8 by construction
            kani::assume(buf[i] != 0 && buf[i] < 0x80);
            i += 1;
        }
        let mut cbuf = [0u8; CAP + 1]; // NUL-terminated C view
        let mut j = 0;
        while j < len {
            cbuf[j] = buf[j];
            j += 1;
        }
        let c = unsafe { pg_char_to_encoding(cbuf.as_ptr().cast()) };
        let s = core::str::from_utf8(&buf[..len]).unwrap(); // proven ASCII
        let r = mbutils::pg_char_to_encoding(s);
        // hit and miss arms both reachable
        kani::cover!(r != -1);
        kani::cover!(r == -1);
        assert!(r == c);
    }

    /// Same theorem fenced to NUL-free valid UTF-8 (the full core domain —
    /// non-ASCII valid-UTF-8 bytes are >= 0x80 and get cleaned out by both
    /// sides' C-locale alnum filter).  Costlier than the ASCII tier (the
    /// std UTF-8 validator is in the formula); if it walls, the ASCII tier
    /// + the cleaned-byte argument above carry the row.
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_char_to_encoding_core_utf8() {
        const CAP: usize = 16;
        let buf: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let mut i = 0;
        while i < len {
            kani::assume(buf[i] != 0);
            i += 1;
        }
        let s = match core::str::from_utf8(&buf[..len]) {
            Ok(s) => s,
            Err(_) => {
                kani::assume(false); // fence: valid UTF-8 only
                return;
            }
        };
        let mut cbuf = [0u8; CAP + 1];
        let mut j = 0;
        while j < len {
            cbuf[j] = buf[j];
            j += 1;
        }
        let c = unsafe { pg_char_to_encoding(cbuf.as_ptr().cast()) };
        let r = mbutils::pg_char_to_encoding(s);
        kani::cover!(r != -1);
        // non-ASCII valid UTF-8 reachable — the cleaned-out-byte plane
        kani::cover!(s.bytes().any(|b| b >= 0x80));
        assert!(r == c);
    }

    // NAMEDATALEN boundary spots: len 63 (last length that walks the
    // clean+search path) and len 64 (both sides short-circuit to -1:
    // C strlen(name) >= NAMEDATALEN, Rust name.len() >= 64).
    // unwind 66: clean loop len+1 <= 65.
    macro_rules! len_spot {
        ($($h:ident: $len:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(66)]
            fn $h() {
                const LEN: usize = $len;
                let buf: [u8; LEN] = kani::any();
                let mut i = 0;
                while i < LEN {
                    kani::assume(buf[i] != 0 && buf[i] < 0x80);
                    i += 1;
                }
                let mut cbuf = [0u8; LEN + 1];
                let mut j = 0;
                while j < LEN {
                    cbuf[j] = buf[j];
                    j += 1;
                }
                let c = unsafe { pg_char_to_encoding(cbuf.as_ptr().cast()) };
                let s = core::str::from_utf8(&buf).unwrap();
                let r = mbutils::pg_char_to_encoding(s);
                assert!(r == c);
            }
        )*};
    }

    len_spot! {
        spot_char_to_encoding_len63: 63;
        spot_char_to_encoding_len64: 64;
    }

    // ---- oid 1264 wrapper plane: regression gate (was the divergence-#10 EXPECTED-FAIL witness; fix bd9442e253 flipped it green) ----
    //
    // Calls the SHIPPED fmgr wrapper (adt_misc fc_pg_char_to_encoding)
    // with a symbolic Name frame whose bytes are NOT fenced to UTF-8.
    // The wrapper's from_utf8().unwrap_or("") rejects non-UTF8 wholesale
    // where C cleans byte-wise -> VERIFICATION FAILED with a
    // counterexample like "utf\xFF8" (C 6 / PG_UTF8, Rust -1).
    //
    // RUNNER: DEFAULT solver + -Z concrete-playback; ground-truth the
    // counterexample on real glibc PG BEFORE recording (see module doc).
    // A PASS here would itself be a finding (witness gone = plane closed
    // or vacuous — investigate, don't celebrate).
    // RVR lane reshape 2026-07-28: the original N=8 fully-symbolic prefix
    // walled on MEMORY in CNF construction (7.1 GiB, "CBMC failed" after
    // symex completed 107s; no decodable counterexample produced). The
    // witness plane is narrowed to "utf" + one symbolic byte 0x80..=0xFF +
    // "8": every byte in that fence makes the name invalid UTF-8 (0x80-0xBF
    // are bare continuations; 0xC0-0xFF are lead bytes followed by '8'), so
    // the wrapper's from_utf8().unwrap_or("") arm fires across the WHOLE
    // fenced plane while C-locale isalnum strips the byte -> "utf8" -> 6.
    // Same claim, decodable counterexample, cheap CNF.
    #[kani::proof]
    #[kani::unwind(18)]
    fn witness_wrapper_non_utf8_plane() {
        let b: u8 = kani::any();
        kani::assume(b >= 0x80); // whole non-ASCII plane
        let mut frame = [0u8; 64];
        frame[0] = b'u';
        frame[1] = b't';
        frame[2] = b'f';
        frame[3] = b;
        frame[4] = b'8';
        // Embedded NULs are FINE: the wrapper cuts at the first NUL
        // (position of 0) exactly like C's strlen view of NameStr.
        let c = unsafe { pg_char_to_encoding(frame.as_ptr().cast()) };
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(frame.as_ptr() as usize));
        let r = match adt_misc::builtins::fc_pg_char_to_encoding(None, &mut f) {
            Ok(d) => d.as_i32(),
            Err(_) => panic!("pg_char_to_encoding wrapper errored"),
        };
        assert!(r == c); // EXPECTED FAIL — the divergence witness
    }

    // ---- negative control: rig is non-vacuous ----
    // C frame for enc vs Rust frame for enc+1 — MUST FAIL (any valid
    // encoding pair gives different names).  DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(66)]
    fn control_encoding_to_char_frame_shifted() {
        let enc: i32 = kani::any();
        kani::assume(enc >= 0 && enc < 41); // both enc and enc+1 valid
        let mut cframe = [0u8; 64];
        unsafe { pg_encoding_to_char_frame(enc, cframe.as_mut_ptr()) };
        let name = mbutils::pg_encoding_to_char(enc + 1);
        let mut n = NameData::default();
        n.namestrcpy(name);
        assert!(n.data == cframe);
    }
}
