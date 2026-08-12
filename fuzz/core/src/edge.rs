//! edge — shared ADVERSARIAL EDGE-VALUE bank + cross-cutting injection harness
//! for the differential fuzz drivers (`decoder_fuzz::*_diff`).
//!
//! Motivation (the bug class this exists to catch, grounded in real findings):
//!   * ST3 startup packet: `i32::from_be_bytes(len) - 4` PANICKED at INT32_MIN
//!     — wrapping arithmetic on an attacker-controlled length field.
//!     LESSON: feed INT_MIN/INT_MAX to EVERY length/count/size/offset/typmod
//!     field.
//!   * Q8-F1: COPY invalid-encoding after ON_ERROR ignore →
//!     `range start index N out of range for slice of length 0`.
//!     LESSON: feed EMPTY inputs ('', empty array/bytea/range, zero rows) to
//!     positions that slice/iterate.
//!   * numx / Kani integer-wraparound / SQB-F2 (missing CREATE-time
//!     validation → internal assert on call).
//!     LESSON: values that overflow on negate/abs/add, and values fed where
//!     validation is expected but maybe missing.
//!
//! The differential bar (identical to the rest of the harness): pgrust must
//! ACCEPT-or-REJECT each edge value IDENTICALLY to the vendored REL_18_3 C,
//! with the SAME error class on rejection and the SAME value on acceptance.
//! A pgrust PANIC / internal error / assertion where C gives a clean
//! user-facing error (22003 / 22023 / 2201x / 42xxx) is the HIGH-severity
//! target bug (the startup-packet / SQB-F2 class): here it surfaces as a
//! `catch_unwind`-captured [`Finding`] out of [`run_campaign`], or — when a
//! driver is run directly — as a normal libFuzzer-style panic.
//!
//! This module is the single reusable corpus every generator/driver can pull
//! from, replacing the scattered ad-hoc boundary literals with one bank keyed
//! by category.

#![allow(clippy::type_complexity)]

// ===========================================================================
// 1. THE BANK — categorized adversarial literals.
// ===========================================================================

/// int2/int16 text forms: MIN, MIN+1, MAX, MAX-1, 0, -1, 1, and the
/// OVERFLOW-STRING forms (MIN-1 / MAX+1 as text that MUST be rejected 22003).
pub const INT16_TEXT: &[&[u8]] = &[
    b"0", b"-1", b"1", b"-32768", b"-32767", b"32767", b"32766",
    // overflow-string forms (reject 22003):
    b"32768", b"-32769", b"65535", b"65536", b"99999",
];

/// int4/int32 text forms incl. overflow-string forms.
pub const INT32_TEXT: &[&[u8]] = &[
    b"0", b"-1", b"1", b"-2147483648", b"-2147483647", b"2147483647", b"2147483646",
    // overflow-string forms (reject 22003):
    b"2147483648", b"-2147483649", b"4294967295", b"4294967296", b"9999999999",
];

/// int8/int64 text forms incl. overflow-string forms.
pub const INT64_TEXT: &[&[u8]] = &[
    b"0", b"-1", b"1",
    b"-9223372036854775808", b"-9223372036854775807",
    b"9223372036854775807", b"9223372036854775806",
    // overflow-string forms (reject 22003):
    b"9223372036854775808", b"-9223372036854775809",
    // u64 boundary + beyond:
    b"18446744073709551615", b"18446744073709551616",
    b"99999999999999999999999999999999",
];

/// Values that WRAP on negate/abs (`-x` or `abs(x)` overflows) — the numx /
/// Kani wraparound class. Text forms of each width's MIN.
pub const NEGATE_OVERFLOW_TEXT: &[&[u8]] =
    &[b"-32768", b"-2147483648", b"-9223372036854775808"];

/// Empty / degenerate inputs for positions that slice, iterate, or parse.
pub const EMPTY_DEGENERATE: &[&[u8]] = &[
    b"",            // empty string — the Q8-F1 empty-slice class
    b" ",           // single space
    b"  ",          // whitespace-only
    b"\t",          // tab-only
    b"\n",          // newline-only
    b"{}",          // empty array literal
    b"{ }",         // empty array w/ space
    b"\\x",         // empty bytea (hex)
    b"empty",       // empty range keyword
    b"-",           // lone sign
    b"+",           // lone sign
    b".",           // lone point
    b"e",           // lone exponent marker
    b"0x",          // truncated hex prefix
    b"0b",          // truncated binary prefix
    b"0o",          // truncated octal prefix
    b"()",          // empty parens
    b"[]",          // empty brackets
    b"[,]",         // empty range bounds
    b",",           // lone delimiter
    b"NaN",         // special float
    b"Infinity",
    b"-Infinity",
    b"null",
    b"NULL",
];

/// Extra numeric-syntax degenerates: things that look numeric but sit on a
/// parse boundary (leading/trailing junk, doubled signs, underscores).
pub const NUMERIC_DEGENERATE: &[&[u8]] = &[
    b" 42 ", b"+ 42", b"- 42", b"--1", b"++1", b"1_000", b"_1", b"1_", b"1.2.3",
    b"1e", b"1e+", b"1e-", b"0x7fff", b"0o777", b"0b1010", b".5", b"5.", b"1e999",
    b"1e-999", b"0.00000000000000000001",
    b"9999999999999999999999999999999999999999", // 40-digit all-nines overflow
];

/// int16 boundary VALUES (for LE-encoded / value-domain arms).
pub const I16_EDGES: &[i16] = &[i16::MIN, i16::MIN + 1, -1, 0, 1, i16::MAX - 1, i16::MAX];
/// int32 boundary VALUES.
pub const I32_EDGES: &[i32] = &[i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX];
/// int64 boundary VALUES.
pub const I64_EDGES: &[i64] = &[i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];

/// TYPMOD / precision / scale / count / offset edge VALUES — the fields the
/// ST3 startup-packet bug lived in. Fed into numeric(P,S), varchar(N),
/// time(P), substring(s,START,LEN), LIMIT/OFFSET, array subscripts, etc.
pub const TYPMOD_EDGES: &[i32] = &[
    i32::MIN, i32::MIN + 1, -2, -1, 0, 1, 2, 255, 256, 1000,
    0x00FF_FFFF, // huge but plausible
    i32::MAX - 1, i32::MAX,
];

/// Zero-length / degenerate identifiers and labels (enum labels, column names,
/// COPY option strings). A COPY delimiter of `''` is a classic.
pub const DEGENERATE_IDENT: &[&[u8]] = &[
    b"", b" ", b"\"\"", b".", b"..", b"a.b", b"\t", b"1", b"select",
];

// ===========================================================================
// 1b. THE WIRE BANK — big-endian length/count/frame-header edge words.
// ===========================================================================
//
// The recv/binary-input paths (`*recv`) and the COPY-binary field framing read
// their COUNT / LENGTH / DIMENSION / ELEMENT-LENGTH prefixes as BIG-ENDIAN
// integers via `pq_getmsgint`/`pq_getmsgint64` — the OPPOSITE byte order from
// the little-endian value-domain arms above. This is the exact field family
// that panicked in the ST3 startup packet (`i32::from_be_bytes(len) - 4` at
// INT32_MIN) and the Q8-F1 COPY empty-slice. Feed INT_MIN/-1/0/huge as the
// length word, and empty/truncated payloads behind it.

/// COPY-binary per-field length sentinels. `-1` is the legal NULL marker; every
/// OTHER negative (and INT_MIN) is a framing error C rejects — a pgrust panic
/// there is the Q8-F1 class. `i32::MAX` claims a field far larger than the
/// message: the length must be validated against remaining bytes, not trusted.
pub const COPY_FIELD_LEN: &[i32] = &[-1, -2, -3, i32::MIN, i32::MIN + 1, 0, 1, 2, i32::MAX];

/// Truncated / empty raw wire messages — a recv reading ANY fixed-width prefix
/// off these underflows the buffer (the empty-slice / short-message class).
pub const WIRE_TRUNCATED: &[&[u8]] = &[
    b"",                     // empty message — recv reads count/len off nothing
    b"\x00",                 // 1 byte  (short int16 and int32)
    b"\xff",                 // 1 byte
    b"\x00\x00",             // 2 bytes (a full int16, short int32)
    b"\xff\xff",             // int16 = -1, short int32
    b"\x00\x00\x00",         // 3 bytes (truncated int32)
    b"\xff\xff\xff",         // 3 bytes
    b"\x80\x00\x00",         // 3 bytes, top bit set
];

/// Short byte tails appended behind a leading length/count word so the field
/// claims a size that does or does not match the bytes that actually follow.
const WIRE_TAILS: &[&[u8]] = &[
    b"",
    b"\x00",
    b"AB",
    b"payloadbytes",
    b"\xff\xff\xff\xff",
];

/// Header-slot edge values crossed into 2- and 3-word frame headers
/// (ndim/flags/oid, count/element-length, version/count).
const WIRE_HDR: &[i32] = &[i32::MIN, -1, 0, 1, i32::MAX, 0x00FF_FFFF];

/// The full wire-frame corpus: every big-endian length/count/frame-header shape
/// an edge value can occupy on a binary-receive or COPY-binary path. Kept a
/// runtime fn (not a const) because it materialises concatenated frames.
pub fn wire_frames() -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();

    // (a) truncated / empty raw messages.
    for t in WIRE_TRUNCATED {
        out.push(t.to_vec());
    }

    // (b) a single leading count/length word (int32 then int16, BIG-ENDIAN),
    //     alone and followed by a small tail — the "first field is a length"
    //     shape shared by array_recv ndim, record_recv column count,
    //     hstore_recv pair count, multirange_recv range count, etc.
    for &n in I32_EDGES {
        for tail in WIRE_TAILS {
            let mut v = n.to_be_bytes().to_vec();
            v.extend_from_slice(tail);
            out.push(v);
        }
    }
    for &n in I16_EDGES {
        for tail in WIRE_TAILS {
            let mut v = n.to_be_bytes().to_vec();
            v.extend_from_slice(tail);
            out.push(v);
        }
    }

    // (c) 2- and 3-word big-endian headers: (ndim, flags), (ndim, flags, oid),
    //     plus a header followed by an edge-valued per-element length word.
    for &a in WIRE_HDR {
        for &b in WIRE_HDR {
            let mut two = a.to_be_bytes().to_vec();
            two.extend_from_slice(&b.to_be_bytes());
            out.push(two.clone());
            for &c in &[i32::MIN, -1, 0, 1] {
                let mut three = two.clone();
                three.extend_from_slice(&c.to_be_bytes());
                out.push(three);
            }
            // header + per-element length = edge, then one payload byte.
            let mut elem = two.clone();
            elem.extend_from_slice(&a.to_be_bytes());
            elem.push(0x41);
            out.push(elem);
        }
    }

    // (d) COPY-binary field framing: [field-count i16 BE][field-len i32 BE][b].
    for &len in COPY_FIELD_LEN {
        let mut v = 1i16.to_be_bytes().to_vec();
        v.extend_from_slice(&len.to_be_bytes());
        v.push(0x41);
        out.push(v);
    }

    // (e) version-byte + count shape (jsonb_recv version, pg_snapshot_recv
    //     count, tsvector/tsquery headers): [version u8][count i32 BE].
    for &ver in &[0u8, 1, 2, 0x7F, 0xFF] {
        for &cnt in &[i32::MIN, -1, 0, 1, i32::MAX, 0x00FF_FFFF] {
            let mut v = vec![ver];
            v.extend_from_slice(&cnt.to_be_bytes());
            out.push(v);
        }
    }

    // (f) int64 big-endian length words (numeric weight/dscale int64 paths,
    //     interval/timestamp 8-byte fields).
    for &n in I64_EDGES {
        out.push(n.to_be_bytes().to_vec());
        let mut v = n.to_be_bytes().to_vec();
        v.push(0x41);
        out.push(v);
    }

    out
}

// ===========================================================================
// 2. RUNTIME-GENERATED BANKS (huge / length-extreme / nested).
// ===========================================================================

/// The numeric-degenerate bank with its runtime "all-nines" slot materialised.
pub fn numeric_degenerate() -> Vec<Vec<u8>> {
    NUMERIC_DEGENERATE.iter().map(|s| s.to_vec()).collect()
}

/// Huge / length-extreme inputs: very long strings, huge repeat counts, deep
/// nesting. Kept OUT of the const bank because they allocate.
pub fn huge_texts() -> Vec<Vec<u8>> {
    vec![
        vec![b'9'; 100_000],                 // 100k-digit numeric
        vec![b'a'; 65_536],                  // 64k text
        vec![b'0'; 1 << 20],                 // 1MiB of zeros (leading-zero parse)
        {
            let mut v = Vec::with_capacity(4096);
            v.extend_from_slice(b"1e");
            v.extend(std::iter::repeat(b'9').take(4096)); // huge exponent
            v
        },
        {
            // deeply nested array literal: {{{{...}}}}
            let mut v = vec![b'{'; 4096];
            v.extend(std::iter::repeat(b'}').take(4096));
            v
        },
        {
            // deeply nested parens
            let mut v = vec![b'('; 4096];
            v.extend(std::iter::repeat(b')').take(4096));
            v
        },
        {
            // huge array with many empty elements: {,,,,...}
            let mut v = vec![b'{'];
            v.extend(std::iter::repeat(b',').take(8192));
            v.push(b'}');
            v
        },
    ]
}

/// The full union of every const text edge (integers of all widths, empties,
/// degenerates, negate-overflow, identifiers). The canonical corpus a
/// generator pulls to spray one text/parse slot.
pub fn all_edge_texts() -> Vec<&'static [u8]> {
    let mut v = Vec::new();
    v.extend_from_slice(INT16_TEXT);
    v.extend_from_slice(INT32_TEXT);
    v.extend_from_slice(INT64_TEXT);
    v.extend_from_slice(NEGATE_OVERFLOW_TEXT);
    v.extend_from_slice(EMPTY_DEGENERATE);
    v.extend_from_slice(NUMERIC_DEGENERATE);
    v.extend_from_slice(DEGENERATE_IDENT);
    v.sort_unstable();
    v.dedup();
    v
}

// ===========================================================================
// 3. INJECTION HELPERS — encode edge values into driver seeds.
// ===========================================================================

/// Build a driver seed `[selector][payload]`. The differential drivers take
/// `&[u8]` shaped as a leading selector byte followed by an arm-specific
/// payload; most text-input arms take the remainder verbatim as the literal.
pub fn edge_seed(selector: u8, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + payload.len());
    v.push(selector);
    v.extend_from_slice(payload);
    v
}

/// SYSTEMATIC injection: emit one seed for EVERY selector in `0..selectors`
/// crossed with EVERY payload — so an edge literal lands not only in the arm
/// that "normally" accepts it but in every sibling slot (the cross-cutting
/// requirement). `selectors` should comfortably exceed each driver's internal
/// `sel % N` so all arms are hit.
pub fn spray_seeds<'a, I>(selectors: u8, payloads: I) -> Vec<Vec<u8>>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let payloads: Vec<&[u8]> = payloads.into_iter().collect();
    let mut out = Vec::with_capacity(selectors as usize * payloads.len());
    for sel in 0..selectors {
        for p in &payloads {
            out.push(edge_seed(sel, p));
        }
    }
    out
}

/// Every text edge + huge + LE-encoded typmod/int seeds, as owned byte
/// vectors, sprayed across `selectors` arms. This is the master corpus the
/// campaign feeds into each driver.
pub fn campaign_seeds(selectors: u8) -> Vec<Vec<u8>> {
    let mut payloads: Vec<Vec<u8>> = Vec::new();
    for t in all_edge_texts() {
        payloads.push(t.to_vec());
    }
    payloads.extend(huge_texts());
    // LE-encoded typmod / integer boundary payloads (for value-domain arms
    // that read fixed-width little-endian operands rather than text).
    for &n in TYPMOD_EDGES {
        payloads.push(n.to_le_bytes().to_vec());
    }
    for &n in I64_EDGES {
        payloads.push(n.to_le_bytes().to_vec());
    }
    // Length-prefixed forms: a u8/u16 length byte then an edge literal — hits
    // arms that read an inner length field (numeric_to_char nlen, etc.).
    for t in all_edge_texts() {
        if t.len() <= 255 {
            let mut v = vec![t.len() as u8];
            v.extend_from_slice(t);
            payloads.push(v);
        }
    }

    let refs: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
    spray_seeds(selectors, refs)
}

// ===========================================================================
// 4. THE CAMPAIGN — drive real comparators, collect divergences.
// ===========================================================================

/// A captured divergence: a driver panicked (assertion / internal error /
/// wraparound panic) on an edge seed. This is the HIGH-severity finding class.
#[derive(Debug, Clone)]
pub struct Finding {
    pub driver: &'static str,
    pub selector: u8,
    pub payload: Vec<u8>,
    pub message: String,
}

/// The cross-cutting driver table: every crate-root `*_diff` entry point that
/// exercises a numeric / length / parse / typmod / empty-slice surface.
pub fn edge_drivers() -> Vec<(&'static str, fn(&[u8]))> {
    vec![
        ("int_diff", crate::int_diff as fn(&[u8])),
        ("numutils_diff", crate::numutils_diff),
        ("numeric_io_diff", crate::numeric_io_diff),
        ("numeric_ops_diff", crate::numeric_ops_diff),
        ("vltext_diff", crate::vltext_diff),
        ("vlbytea_diff", crate::vlbytea_diff),
        // VENDOR lane: the newly-vendored hand-rolled bit/varbit text parser.
        ("varbit_io_diff", crate::varbit_io_diff),
        ("vlmisc_diff", crate::vlmisc_diff),
        ("arrayfuncs_diff", crate::arrayfuncs_diff),
        ("array_userfuncs_diff", crate::array_userfuncs_diff),
        ("fmt_num_diff", crate::fmt_num_diff),
        ("quote_diff", crate::quote_diff),
        ("json_diff", crate::json_diff),
        ("jsonbio_diff", crate::jsonbio_diff),
        ("rangetypes_diff", crate::rangetypes_diff),
        ("multirangetypes_diff", crate::multirangetypes_diff),
        ("cash_diff", crate::cash_diff),
        ("uuid_diff", crate::uuid_diff),
        ("mac_diff", crate::mac_diff),
        ("name_diff", crate::name_diff),
        ("netfam_diff", crate::netfam_diff),
    ]
}

/// Run the edge campaign: spray [`campaign_seeds`] through every
/// [`edge_drivers`] entry, catching any panic as a [`Finding`]. Returns
/// `(cases_run, findings)`.
///
/// Panics are captured (not propagated) so a single divergence does not abort
/// the sweep — but note the drivers serialize through a process-global C
/// oracle Mutex, so a genuine panic can poison it and make subsequent cases
/// report spuriously; [`run_campaign`] stops a driver's sweep on its first
/// finding to keep the ledger honest.
pub fn run_campaign(selectors: u8) -> (usize, Vec<Finding>) {
    let seeds = campaign_seeds(selectors);
    let mut cases = 0usize;
    let mut findings = Vec::new();

    // Silence the default panic hook during the sweep (we capture messages via
    // catch_unwind's payload instead).
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    for (name, drive) in edge_drivers() {
        for seed in &seeds {
            cases += 1;
            let seed_cl = seed.clone();
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drive(&seed_cl);
            }));
            if let Err(payload) = res {
                let msg = panic_message(&payload);
                findings.push(Finding {
                    driver: name,
                    selector: seed.first().copied().unwrap_or(0),
                    payload: seed.get(1..).unwrap_or(&[]).to_vec(),
                    message: msg,
                });
                // Stop this driver on first finding — the C oracle Mutex may be
                // poisoned; continuing would fabricate cascade "findings".
                break;
            }
        }
    }

    std::panic::set_hook(prev);
    (cases, findings)
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

// ===========================================================================
// 4b. THE WIRE CAMPAIGN — drive binary-receive / framing paths.
// ===========================================================================

/// Wire-frame seeds sprayed across `selectors` arms. Each driver takes a
/// leading selector byte; spraying every selector guarantees the frame lands
/// in the recv/binary arm regardless of that driver's `sel % N`.
pub fn wire_campaign_seeds(selectors: u8) -> Vec<Vec<u8>> {
    let frames = wire_frames();
    let refs: Vec<&[u8]> = frames.iter().map(|v| v.as_slice()).collect();
    spray_seeds(selectors, refs)
}

/// The wire/binary-receive driver table: every `*_diff` entry point that
/// decodes a binary-receive frame (a `*recv` reading a big-endian
/// count/length/dimension prefix) or a COPY-binary field header. These are the
/// ST3/Q8 risk surface — the length-field paths, as opposed to the (already
/// hardened) ADT text-input surface swept by [`edge_drivers`].
///
/// GAP (not vendored in this harness, so uncoverable here): the frontend
/// startup-packet path (`ProcessStartupPacket` / `pq_getmessage` outer
/// length) and the COPY text/binary row loop (`CopyReadBinaryData`,
/// `NextCopyFromRawFields`) have no verbatim-C oracle in `csrc/` — recorded in
/// findings-edge2.md as a future-vendoring lane, NOT silently skipped.
pub fn wire_drivers() -> Vec<(&'static str, fn(&[u8]))> {
    vec![
        ("snapio_diff", crate::snapio_diff as fn(&[u8])),
        ("rangetypes_diff", crate::rangetypes_diff),
        ("multirangetypes_diff", crate::multirangetypes_diff),
        // rowtypes_diff (record_recv column count) is NOT here: it pins
        // process-global seams that rangetypes/multirangetypes claim first,
        // so in a shared process it panics "seam installed twice:
        // detoast_seams::detoast_attr" (its install() guard checks the
        // syscache/fmgr seams but not the detoast one). Swept in isolation by
        // the `wire_campaign_rowtypes` test instead — see findings-edge2.md
        // EDGE2-L1. Leaving it here would abort its sweep on case 1 and bank
        // coverage it never performed.
        ("uuid_diff", crate::uuid_diff),
        ("network_diff", crate::network_diff),
        ("netfam_diff", crate::netfam_diff),
        ("cash_diff", crate::cash_diff),
        ("hstorefam_diff", crate::hstorefam_diff),
        ("ltree_diff", crate::ltree_diff),
        ("jsonbio_diff", crate::jsonbio_diff),
        ("tsquery_core_diff", crate::tsquery_core_diff),
        ("tsvector_core_diff", crate::tsvector_core_diff),
        ("timestamp_diff", crate::timestamp_diff),
        ("lsn_diff", crate::pg_lsn_diff),
        ("arrayfuncs_diff", crate::arrayfuncs_diff),
    ]
}

/// Per-driver execution accounting for one wire sweep. Exists because a
/// driver that silently no-ops (seam owned by a sibling module, unmet
/// precondition) contributes cases to the total while witnessing NOTHING —
/// a vacuous pass. `elapsed_us` near zero relative to its case count is the
/// tell; [`run_wire_campaign`] reports it so the ledger cannot bank
/// coverage a driver never actually performed.
#[derive(Debug, Clone)]
pub struct DriverStat {
    pub driver: &'static str,
    pub cases: usize,
    pub elapsed_us: u128,
    pub stopped_early: bool,
}

impl DriverStat {
    /// Mean microseconds per case — the vacuity signal.
    pub fn us_per_case(&self) -> f64 {
        if self.cases == 0 {
            0.0
        } else {
            self.elapsed_us as f64 / self.cases as f64
        }
    }
}

/// Run the wire campaign over an explicit driver table. Sprays
/// [`wire_campaign_seeds`] through each entry, catching any panic as a
/// [`Finding`]. Same stop-on-first-finding-per-driver discipline as
/// [`run_campaign`] (the C oracle Mutex can be poisoned by a genuine panic).
///
/// Returns `(cases, findings, per-driver stats)`.
pub fn run_wire_campaign_over(
    selectors: u8,
    drivers: Vec<(&'static str, fn(&[u8]))>,
) -> (usize, Vec<Finding>, Vec<DriverStat>) {
    let seeds = wire_campaign_seeds(selectors);
    let mut cases = 0usize;
    let mut findings = Vec::new();
    let mut stats = Vec::new();

    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    for (name, drive) in drivers {
        let t0 = std::time::Instant::now();
        let mut dcases = 0usize;
        let mut stopped_early = false;
        for seed in &seeds {
            cases += 1;
            dcases += 1;
            let seed_cl = seed.clone();
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drive(&seed_cl);
            }));
            if let Err(payload) = res {
                let msg = panic_message(&payload);
                findings.push(Finding {
                    driver: name,
                    selector: seed.first().copied().unwrap_or(0),
                    payload: seed.get(1..).unwrap_or(&[]).to_vec(),
                    message: msg,
                });
                stopped_early = true;
                break;
            }
        }
        stats.push(DriverStat {
            driver: name,
            cases: dcases,
            elapsed_us: t0.elapsed().as_micros(),
            stopped_early,
        });
    }

    std::panic::set_hook(prev);
    (cases, findings, stats)
}

/// Run the wire campaign over the full [`wire_drivers`] table.
pub fn run_wire_campaign(selectors: u8) -> (usize, Vec<Finding>, Vec<DriverStat>) {
    run_wire_campaign_over(selectors, wire_drivers())
}

// ===========================================================================
// 5. TESTS.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Bank sanity — always green. Deliverable: `cargo test -p` gate.
    #[test]
    fn edge_bank_sanity() {
        // Every width bank carries MIN, MAX, 0, -1, 1, and both overflow-string
        // forms.
        assert!(INT16_TEXT.contains(&b"-32768".as_slice()));
        assert!(INT16_TEXT.contains(&b"32768".as_slice())); // MAX+1 overflow form
        assert!(INT32_TEXT.contains(&b"-2147483648".as_slice()));
        assert!(INT32_TEXT.contains(&b"2147483648".as_slice()));
        assert!(INT64_TEXT.contains(&b"-9223372036854775808".as_slice()));
        assert!(INT64_TEXT.contains(&b"9223372036854775808".as_slice()));
        assert!(EMPTY_DEGENERATE.contains(&b"".as_slice())); // the empty-slice class
        assert!(EMPTY_DEGENERATE.contains(&b"{}".as_slice()));
        assert!(EMPTY_DEGENERATE.contains(&b"\\x".as_slice()));

        // Value banks carry MIN/MAX and the negate-overflow point.
        assert_eq!(I32_EDGES[0], i32::MIN);
        assert!(I64_EDGES.contains(&i64::MAX));
        assert!(TYPMOD_EDGES.contains(&i32::MIN));
        assert!(TYPMOD_EDGES.contains(&i32::MAX));

        // Union dedups and is non-trivial.
        let all = all_edge_texts();
        assert!(all.len() > 40, "edge corpus unexpectedly small: {}", all.len());

        // Huge bank actually allocates length-extreme inputs.
        let huge = huge_texts();
        assert!(huge.iter().any(|v| v.len() >= 100_000));
    }

    /// Seed-encoding sanity — always green.
    #[test]
    fn edge_seed_encoding() {
        let s = edge_seed(3, b"-2147483648");
        assert_eq!(s[0], 3);
        assert_eq!(&s[1..], b"-2147483648");

        // Spray hits every selector × payload.
        let seeds = spray_seeds(12, [b"".as_slice(), b"32768".as_slice()]);
        assert_eq!(seeds.len(), 12 * 2);
        assert!(seeds.iter().any(|s| s[0] == 11));

        // campaign_seeds is several-thousand strong across selectors.
        let cs = campaign_seeds(12);
        assert!(cs.len() > 2000, "campaign corpus too small: {}", cs.len());
    }

    /// The full cross-cutting differential campaign. `#[ignore]` because it is
    /// the heavy sweep (runs the vendored C oracle for every case) and, on a
    /// genuine divergence, is EXPECTED to surface a [`Finding`] rather than
    /// stay silent. Run explicitly:
    ///   cargo test -p decoder_fuzz edge_campaign_full -- --ignored --nocapture
    #[test]
    #[ignore = "heavy differential sweep; run explicitly to hunt edge divergences"]
    fn edge_campaign_full() {
        let (cases, findings) = run_campaign(14);
        eprintln!("edge campaign: {cases} cases across {} drivers", edge_drivers().len());
        for f in &findings {
            eprintln!(
                "FINDING driver={} selector={} payload={:?} :: {}",
                f.driver,
                f.selector,
                String::from_utf8_lossy(&f.payload),
                f.message
            );
        }
        eprintln!("edge campaign: {} finding(s)", findings.len());
        // The sweep must actually run a substantial number of cases.
        assert!(cases > 20_000, "campaign ran too few cases: {cases}");
    }

    /// Wire-frame bank sanity — always green.
    #[test]
    fn wire_bank_sanity() {
        // COPY NULL sentinel and the illegal-negative it must NOT be confused
        // with are both present.
        assert!(COPY_FIELD_LEN.contains(&-1)); // legal NULL marker
        assert!(COPY_FIELD_LEN.contains(&-2)); // illegal — framing error
        assert!(COPY_FIELD_LEN.contains(&i32::MIN));
        assert!(COPY_FIELD_LEN.contains(&i32::MAX));

        // Truncated bank carries the empty message (recv-off-nothing class).
        assert!(WIRE_TRUNCATED.contains(&b"".as_slice()));

        let frames = wire_frames();
        // Non-trivial corpus.
        assert!(frames.len() > 200, "wire corpus too small: {}", frames.len());
        // Contains the ST3 INT32_MIN big-endian length word as a leading field.
        let min_be = i32::MIN.to_be_bytes();
        assert!(
            frames.iter().any(|f| f.starts_with(&min_be)),
            "INT32_MIN big-endian length word missing from wire bank"
        );
        // Contains an empty frame (the Q8-F1 empty-slice class).
        assert!(frames.iter().any(|f| f.is_empty()));
    }

    /// Wire campaign seed-count sanity — always green.
    #[test]
    fn wire_campaign_seed_count() {
        let cs = wire_campaign_seeds(34);
        assert!(cs.len() > 5000, "wire campaign corpus too small: {}", cs.len());
        // Every selector represented.
        assert!(cs.iter().any(|s| s[0] == 33));
    }

    /// The full wire/binary-receive differential campaign. `#[ignore]`: heavy
    /// (runs the vendored C oracle for every frame) and EXPECTED to surface a
    /// [`Finding`] on a genuine length-field divergence. Run explicitly:
    ///   cargo test -p decoder_fuzz wire_campaign_full -- --ignored --nocapture
    #[test]
    #[ignore = "heavy wire differential sweep; run explicitly to hunt length-field divergences"]
    fn wire_campaign_full() {
        let (cases, findings, stats) = run_wire_campaign(34);
        eprintln!(
            "wire campaign: {cases} cases across {} drivers",
            wire_drivers().len()
        );
        eprintln!("--- per-driver execution (us/case near 0 => vacuous, driver no-oped) ---");
        for s in &stats {
            eprintln!(
                "  {:24} cases={:7} elapsed_us={:9} us/case={:8.2}{}",
                s.driver,
                s.cases,
                s.elapsed_us,
                s.us_per_case(),
                if s.stopped_early { "  [STOPPED: finding]" } else { "" }
            );
        }
        for f in &findings {
            eprintln!(
                "FINDING driver={} selector={} payload={:02x?} :: {}",
                f.driver, f.selector, f.payload, f.message
            );
        }
        eprintln!("wire campaign: {} finding(s)", findings.len());
        assert!(cases > 20_000, "wire campaign ran too few cases: {cases}");
    }

    /// DETECTION-POWER CONTROL (must-fail control) — always green, cheap.
    ///
    /// A 0-finding sweep is only evidence if the sweep CAN report a finding.
    /// This plants a driver that panics precisely on the ST3 shape (a leading
    /// big-endian INT32_MIN length word) and asserts the campaign machinery
    /// surfaces it as a [`Finding`] with the offending payload intact. Without
    /// this control, "0 findings" is indistinguishable from a harness that
    /// captures nothing.
    #[test]
    fn wire_campaign_detects_planted_panic() {
        fn st3_canary(data: &[u8]) {
            // Panic exactly the way the real ST3 bug did: wrapping arithmetic
            // on an attacker-controlled big-endian length word.
            if let Some(payload) = data.get(1..) {
                if payload.len() >= 4 {
                    let len = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    // In debug this overflow-panics at INT32_MIN, mirroring
                    // `i32::from_be_bytes(len) - 4`. Assert it explicitly so the
                    // control holds under release too.
                    assert!(
                        len.checked_sub(4).is_some(),
                        "planted ST3 canary: length word {len} underflows on -4"
                    );
                }
            }
        }

        let (cases, findings, stats) =
            run_wire_campaign_over(4, vec![("st3_canary", st3_canary as fn(&[u8]))]);

        assert!(cases > 0, "control sweep ran no cases");
        assert_eq!(
            findings.len(),
            1,
            "detection-power control FAILED: campaign did not report the planted panic"
        );
        let f = &findings[0];
        assert_eq!(f.driver, "st3_canary");
        assert!(
            f.message.contains("underflows on -4"),
            "finding lost the panic message: {}",
            f.message
        );
        // The captured payload must be the INT32_MIN length word that tripped it.
        assert_eq!(
            &f.payload[..4],
            &i32::MIN.to_be_bytes(),
            "finding did not carry the offending length word"
        );
        // And the driver must be recorded as stopped early.
        assert!(stats[0].stopped_early, "stop-on-first-finding not recorded");
    }

    /// rowtypes_diff (record_recv — the binary COLUMN-COUNT field, a prime
    /// target) pins process-global seams and no-ops when a sibling diff
    /// module claimed them first. Swept in ISOLATION so it owns the
    /// environment: run as the only test in its process —
    ///   cargo test -p decoder_fuzz --lib wire_campaign_rowtypes -- --ignored --nocapture
    #[test]
    #[ignore = "heavy wire sweep; MUST run as the only test in its process (seam ownership)"]
    fn wire_campaign_rowtypes() {
        let (cases, findings, stats) = run_wire_campaign_over(
            34,
            vec![("rowtypes_diff", crate::rowtypes_diff as fn(&[u8]))],
        );
        for s in &stats {
            eprintln!(
                "  {:24} cases={:7} elapsed_us={:9} us/case={:8.2}{}",
                s.driver,
                s.cases,
                s.elapsed_us,
                s.us_per_case(),
                if s.stopped_early { "  [STOPPED: finding]" } else { "" }
            );
        }
        for f in &findings {
            eprintln!(
                "FINDING driver={} selector={} payload={:02x?} :: {}",
                f.driver, f.selector, f.payload, f.message
            );
        }
        eprintln!("rowtypes wire campaign: {cases} cases, {} finding(s)", findings.len());
        assert!(cases > 5_000, "rowtypes wire sweep ran too few cases: {cases}");
    }
}
