//! tupaccess_diff: differential fuzz driver — shipped Rust heaptuple/tupdesc
//! (+ the deform half of types_tuple) vs vendored PostgreSQL 18.3
//! (Stamp-18.3, upstream sha 62d6c7d3df) C (csrc/pg_tupaccess_io.c).
//! Crates under test: crates/backend/access/common/heaptuple,
//! crates/backend/access/common/tupdesc,
//! crates/_support/types/types_tuple (heap_deform_tuple, heap_getattr,
//! fastgetattr, nocachegetattr, heap_attisnull, getmissingattr).
//!
//! Comparison planes per arm: value/image bytes BIT-EXACT (tuple images,
//! byval Datums as raw u64 words — never value-equal; byref pointee bytes per
//! attlen semantics), descriptor field plane (every FormData_pg_attribute
//! field + every CompactAttribute field except the stateful attcacheoff +
//! constr contents), verdict planes (equalTupleDescs / equalRowTypes /
//! hashRowType / attmap identity), error verdict + errcode class (sqlstate
//! mapped to the oracle's class constants; message text out of scope).
//!
//! HARNESS CONTRACT: the descriptor/value wire formats are documented in the
//! C oracle's SECTION D header; the encoders here and the decoders there are
//! transcriptions of each other — asymmetry is a harness bug, never a
//! divergence. Both sides stage byval Datums SIGN-EXTENDED from the column
//! width (C CharGetDatum/Int16GetDatum/... semantics).
//!
//! ENVIRONMENT PINS (data, not computation):
//!   - type menu (12 entries; see MENU below == C pg_ta_menu),
//!   - SearchSysCache1(TYPEOID)/lookup_pg_type_shape pinned to the menu,
//!   - attnullability staged VALID/INVALID whenever attnotnull (relcache
//!     resolution; also satisfies the C-side live asserts),
//!   - dynahash missing_cache -> linear table (C side).
//!
//! RATIFIED 2026-08-01 (Michael): platform non-surface — width-1 byval Datum upper 56
//! bits (C fetch_att `*((char *) T)`, tupmacs.h; char signedness is
//! platform-defined — signed on macOS-aarch64/x86_64-Linux, UNSIGNED on
//! Linux-aarch64 — consumers truncate via DatumGetChar). Both datum
//! serializers and the round-trip self-check compare width-1 words masked
//! to the low 8 bits; found by the first fleet CONFIRM (Linux-aarch64,
//! input 000100000000 0001ff). Widths 2/4/8 are signed on all platforms:
//! NOT masked. The equalTupleDescs missing-value plane needs no mask or
//! generator constraint: datumIsEqual's byval word compare runs same-side
//! only (C-vs-C / Rust-vs-Rust) over stagings injective in the low byte,
//! so its verdict is platform-stable.
//!
//! CARVES (documented; each has a reason):
//!   - heap_getsysattr / attnum <= 0: system columns need xact state; the
//!     driver never passes attnum <= 0.
//!   - heap_copy_tuple_as_datum on HeapTupleHasExternal tuples: needs the
//!     TOAST flattener; both sides skip that arm (the has_external VERDICT
//!     itself is compared).
//!   - TupleDescGetDefault: stringToNode unported; the defval plane is
//!     compared through the field-plane serializer + equalTupleDescs.
//!   - heap_modify_tuple_by_cols invalid-column arm: C elogs, Rust panics —
//!     the driver only sends valid column numbers.
//!   - MaxTupleAttributeNumber: generator caps natts at 40, so the error arm
//!     is exercised by the DEDICATED op 1 spot-check (natts = 1665), not by
//!     the generator (preferred option per charter).
//!
//! No dictionary file: the input is a dense binary spec (op/menu/flag bytes,
//! no magic tokens); libFuzzer's CMP tracing finds the few interesting
//! constants (menu indexes, natts boundaries), and the seed corpus pins the
//! bitmap/width/witness shapes directly.
//!
//! INJECTION SWEEP AT CREATION (2026-08-01, scratchpad inject.py; every
//! planted defect FAILED the seed-replay test, then was reverted;
//! plane -> planted defect -> verdict):
//!   form image          -> flip byte 0 of the Rust image           -> CAUGHT
//!   deform round-trip   -> xor 1 into first serialized deform byte -> CAUGHT
//!   getattr (cold)      -> serialize cold result with isnull=true  -> CAUGHT
//!   getattr (warm pair) -> serialize warm result with isnull=true  -> CAUGHT
//!   minimal chain       -> truncate Rust minimal image by 1 byte   -> CAUGHT
//!   modify              -> skip doReplace for att index 1          -> CAUGHT
//!   copy                -> xor 0xff into byte 4 of the Rust copy   -> CAUGHT
//!   expand              -> drop last byte of Rust expanded image   -> CAUGHT
//!   tupdesc field-plane -> flip serialized attndims bit 0          -> CAUGHT
//!   equal/hash verdicts -> invert Rust equalRowTypes verdict       -> CAUGHT
//!   attmap              -> add 1 to every Rust attmap entry        -> CAUGHT
//!   error-verdict       -> map DATATYPE_MISMATCH to class 1        -> CAUGHT
//! Post-carve re-check (width-1 mask must hide ONLY the upper 56 bits):
//!   deform width-1 low byte -> xor 1 into the masked byval word     -> CAUGHT

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::ffi::c_int;

extern crate alloc;
extern crate std;

use datum::Datum;
use types_core::Oid;
use types_error::SqlState;
use mcx::PgVec;
use types_tuple::TupleDescData;
use std::sync::atomic::Ordering::Relaxed;

// Constructed-state builder facility (stub:tupdesc): the descriptor/value
// spec decode, wire contract and both-sides staging live in the shared
// module; this target is the migration demo that consumes it.
use crate::stub_tupdesc::*;

extern "C" {
    fn pg_diff_errcode_get() -> i32;
    fn pg_ta_form(spec: *const u8, speclen: c_int, vals: *const u8, valslen: c_int,
                  out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
    fn pg_ta_form_toomany() -> c_int;
    fn pg_ta_minimal(spec: *const u8, speclen: c_int, vals: *const u8, valslen: c_int,
                     out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
    fn pg_ta_getattr(spec: *const u8, speclen: c_int, src_natts: c_int, attnum: c_int,
                     vals: *const u8, valslen: c_int,
                     out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
    fn pg_ta_modify(spec: *const u8, speclen: c_int, vals: *const u8, valslen: c_int,
                    repl: *const u8, repllen: c_int, by_cols: c_int,
                    out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
    fn pg_ta_copy(spec: *const u8, speclen: c_int, vals: *const u8, valslen: c_int,
                  out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
    fn pg_ta_expand(spec: *const u8, speclen: c_int, src_natts: c_int,
                    vals: *const u8, valslen: c_int,
                    out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
    fn pg_ta_desc_cmp(spec1: *const u8, len1: c_int, spec2: *const u8, len2: c_int,
                      out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
    fn pg_ta_desc_copy(spec: *const u8, speclen: c_int, which: c_int,
                       arg1: c_int, arg2: c_int,
                       out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
    fn pg_ta_desc_init(es: *const u8, eslen: c_int,
                       out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
    fn pg_ta_attmap(spec_in: *const u8, ilen: c_int, spec_out: *const u8, olen: c_int,
                    which: c_int, vals: *const u8, valslen: c_int,
                    out: *mut u8, outcap: c_int, outlen: *mut c_int) -> c_int;
}

const OUTCAP: usize = 1 << 19;

// ---------------------------------------------------------------------------
// C-result reader + serializers (mirrors of the C SECTION D/E writers)
// ---------------------------------------------------------------------------

struct Rd<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Rd<'a> {
    fn u8(&mut self) -> u8 {
        let v = self.b[self.i];
        self.i += 1;
        v
    }
    fn u16(&mut self) -> u16 {
        let v = u16::from_le_bytes(self.b[self.i..self.i + 2].try_into().unwrap());
        self.i += 2;
        v
    }
    fn u32(&mut self) -> u32 {
        let v = u32::from_le_bytes(self.b[self.i..self.i + 4].try_into().unwrap());
        self.i += 4;
        v
    }
    fn bytes(&mut self, n: usize) -> &'a [u8] {
        let v = &self.b[self.i..self.i + n];
        self.i += n;
        v
    }
    fn image(&mut self) -> &'a [u8] {
        let n = self.u32() as usize;
        self.bytes(n)
    }
    fn done(&self) -> bool {
        self.i == self.b.len()
    }
}

/// serialize a fetched datum exactly like C pg_ta_put_datum
fn ser_datum(w: &mut Vec<u8>, d: Datum, isnull: bool, attlen: i16, attbyval: bool) {
    w.push(u8::from(isnull));
    if isnull {
        return;
    }
    if attbyval {
        w.push(0);
        // width-1 masked: see byval_word (platform non-surface carve)
        w.extend_from_slice(&byval_word(d, attlen).to_le_bytes());
    } else {
        let p = d.as_usize() as *const u8;
        // SAFETY: a live by-ref datum fetched from a tuple image or a
        // descriptor missing value built by this harness.
        let n = unsafe {
            if attlen > 0 {
                attlen as usize
            } else if attlen == -1 {
                types_tuple::varatt::varsize_any(p)
            } else {
                let mut k = 0usize;
                while *p.add(k) != 0 {
                    k += 1;
                }
                k + 1
            }
        };
        w.push(1);
        w.extend_from_slice(&(n as u32).to_le_bytes());
        // SAFETY: n readable bytes per attlen semantics.
        w.extend_from_slice(unsafe { core::slice::from_raw_parts(p, n) });
    }
}

/// full descriptor field-plane serializer (mirror of C pg_ta_put_desc_plane)
fn ser_desc_plane(w: &mut Vec<u8>, d: &TupleDescData<'_>) {
    w.extend_from_slice(&(d.natts as u32).to_le_bytes());
    w.extend_from_slice(&d.tdtypeid.to_le_bytes());
    w.extend_from_slice(&(d.tdtypmod as u32).to_le_bytes());
    for i in 0..d.natts as usize {
        let a = d.attr(i);
        w.extend_from_slice(&a.attrelid.to_le_bytes());
        w.extend_from_slice(&a.attname.data);
        w.extend_from_slice(&a.atttypid.to_le_bytes());
        w.extend_from_slice(&(a.attlen as u16).to_le_bytes());
        w.extend_from_slice(&(a.attnum as u16).to_le_bytes());
        w.extend_from_slice(&(a.atttypmod as u32).to_le_bytes());
        w.extend_from_slice(&(a.attndims as u16).to_le_bytes());
        w.push(u8::from(a.attbyval));
        w.push(a.attalign as u8);
        w.push(a.attstorage as u8);
        w.push(a.attcompression as u8);
        w.push(u8::from(a.attnotnull));
        w.push(u8::from(a.atthasdef));
        w.push(u8::from(a.atthasmissing));
        w.push(a.attidentity as u8);
        w.push(a.attgenerated as u8);
        w.push(u8::from(a.attisdropped));
        w.push(u8::from(a.attislocal));
        w.extend_from_slice(&(a.attinhcount as u16).to_le_bytes());
        w.extend_from_slice(&a.attcollation.to_le_bytes());
    }
    for i in 0..d.natts as usize {
        let ca = d.compact_attr(i);
        w.extend_from_slice(&(ca.attlen as u16).to_le_bytes());
        w.push(u8::from(ca.attbyval));
        w.push(u8::from(ca.attispackable));
        w.push(u8::from(ca.atthasmissing));
        w.push(u8::from(ca.attisdropped));
        w.push(u8::from(ca.attgenerated));
        w.push(ca.attnullability as u8);
        w.push(ca.attalignby);
    }
    match d.constr.as_deref() {
        None => w.push(0),
        Some(cs) => {
            w.push(1);
            w.push(u8::from(cs.has_not_null));
            w.push(u8::from(cs.has_generated_stored));
            w.push(u8::from(cs.has_generated_virtual));
            w.extend_from_slice(&cs.num_defval.to_le_bytes());
            for dv in cs.defval[..cs.num_defval as usize].iter() {
                let b: &[u8] = dv.adbin.as_ref().map(|s| s.as_bytes()).unwrap_or(&[]);
                w.extend_from_slice(&(dv.adnum as u16).to_le_bytes());
                w.extend_from_slice(&(b.len() as u16).to_le_bytes());
                w.extend_from_slice(b);
            }
            w.extend_from_slice(&cs.num_check.to_le_bytes());
            for ck in cs.check[..cs.num_check as usize].iter() {
                let nm: &[u8] = ck.ccname.as_ref().map(|s| s.as_bytes()).unwrap_or(&[]);
                let b: &[u8] = ck.ccbin.as_ref().map(|s| s.as_bytes()).unwrap_or(&[]);
                w.extend_from_slice(&(nm.len() as u16).to_le_bytes());
                w.extend_from_slice(nm);
                w.extend_from_slice(&(b.len() as u16).to_le_bytes());
                w.extend_from_slice(b);
                w.push(u8::from(ck.ccenforced));
                w.push(u8::from(ck.ccvalid));
                w.push(u8::from(ck.ccnoinherit));
            }
            if cs.missing.is_empty() {
                w.push(0);
            } else {
                w.push(1);
                for (i, ms) in cs.missing.iter().enumerate() {
                    w.push(u8::from(ms.am_present));
                    if ms.am_present {
                        let ca = d.compact_attr(i);
                        if ca.attbyval {
                            let word = ms.am_value.as_u64().to_le_bytes();
                            w.extend_from_slice(&(ca.attlen as u16).to_le_bytes());
                            w.extend_from_slice(&word[..ca.attlen as usize]);
                        } else {
                            let p = ms.am_value.as_usize() as *const u8;
                            // SAFETY: descriptor-owned missing value.
                            let n = unsafe {
                                if ca.attlen > 0 {
                                    ca.attlen as usize
                                } else if ca.attlen == -1 {
                                    types_tuple::varatt::varsize_any(p)
                                } else {
                                    let mut k = 0usize;
                                    while *p.add(k) != 0 {
                                        k += 1;
                                    }
                                    k + 1
                                }
                            };
                            w.extend_from_slice(&(n as u16).to_le_bytes());
                            // SAFETY: n readable bytes.
                            w.extend_from_slice(unsafe { core::slice::from_raw_parts(p, n) });
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Error-class mapping (mirror of the oracle's class constants)
// ---------------------------------------------------------------------------

fn class_of(ss: SqlState) -> i32 {
    use types_error as te;
    if ss == te::ERRCODE_DATATYPE_MISMATCH {
        3
    } else if ss == te::ERRCODE_TOO_MANY_COLUMNS {
        6
    } else {
        7
    }
}

fn c_errcode() -> i32 {
    // SAFETY: plain TLS read.
    unsafe { pg_diff_errcode_get() }
}

// ---------------------------------------------------------------------------
// Seam installation (environment; set-once per process)
// ---------------------------------------------------------------------------

static INSTALL: std::sync::Once = std::sync::Once::new();
static OWNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn menu_shape(oid: Oid) -> Option<types_tuple::PgTypeShape> {
    MENU.iter().find(|m| m.typid == oid).map(|m| types_tuple::PgTypeShape {
        typlen: m.attlen,
        typbyval: m.attbyval,
        typalign: m.attalign,
        typstorage: m.attstorage,
        typcollation: m.attcollation,
    })
}

fn install() -> bool {
    INSTALL.call_once(|| {
        if syscache_seams::lookup_pg_type_shape::is_installed()
            || syscache_seams::lookup_pg_type_typcache_shape::is_installed()
            || syscache_seams::pg_type_io_shape::is_installed()
            || catalog_seams::is_catalog_relation_oid::is_installed()
        {
            return; // another diff module owns the environment
        }
        OWNED.store(true, Relaxed);
        // TupleDescInitEntry catalog pin (menu DATA; unknown oid -> None ->
        // the cache-lookup-failed error arm)
        syscache_seams::lookup_pg_type_shape::set(|typid| Ok(menu_shape(typid)));
        // format_type_be feeds error DETAIL text only (out of scope); any
        // defined shape keeps the DATATYPE_MISMATCH class intact.
        syscache_seams::lookup_pg_type_typcache_shape::set(|_typid| {
            Ok(Some(syscache_seams::PgTypeTypcacheShape {
                typname: Default::default(),
                typlen: -1,
                typbyval: false,
                typalign: b'i' as i8,
                typstorage: b'x' as i8,
                typtype: b'b' as i8,
                typisdefined: true,
                typrelid: types_core::primitive::InvalidOid,
                typsubscript: types_core::primitive::InvalidOid,
                typelem: types_core::primitive::InvalidOid,
                typarray: types_core::primitive::InvalidOid,
                typcollation: types_core::primitive::InvalidOid,
            }))
        });
        // fabricated attrs carry attrelid = 0: never a catalog relation
        catalog_seams::is_catalog_relation_oid::set(|_relid| false);
        // format_type_be (error DETAIL construction only) probes visibility
        namespace_seams::type_is_visible::set(|_typid| Ok(true));
        // print_typmod (typmod >= 0 error DETAIL) probes typmodout; an
        // InvalidOid typmodout keeps the generic "(n)" print, no fmgr call
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok(Some(syscache_seams::PgTypeIoShape {
                oid: typid,
                typinput: types_core::primitive::InvalidOid,
                typoutput: types_core::primitive::InvalidOid,
                typreceive: types_core::primitive::InvalidOid,
                typsend: types_core::primitive::InvalidOid,
                typmodin: types_core::primitive::InvalidOid,
                typmodout: types_core::primitive::InvalidOid,
                typelem: types_core::primitive::InvalidOid,
                typlen: -1,
                typbyval: false,
                typalign: b'i' as i8,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
    });
    OWNED.load(Relaxed)
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn tupaccess_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if data.is_empty() {
        return; // clean no-op on the empty input
    }
    if !install() {
        return; // seams owned by a sibling diff module in this process
    }
    let mut cur = Cursor { b: &data[1..], i: 0 };
    let op = (data[0] % 12) as usize;
    DIV_OPS[op].fetch_add(1, Relaxed);
    match op {
        0 => op_form(&mut cur),
        1 => op_toomany(),
        2 => op_minimal(&mut cur),
        3 => op_getattr(&mut cur),
        4 => op_modify(&mut cur, false),
        5 => op_modify(&mut cur, true),
        6 => op_copy(&mut cur),
        7 => op_expand(&mut cur),
        8 => op_desc_cmp(&mut cur),
        9 => op_desc_copy(&mut cur),
        10 => op_desc_init(&mut cur),
        11 => op_attmap(&mut cur),
        _ => unreachable!(),
    }
}

fn run_c(
    f: impl FnOnce(*mut u8, c_int, *mut c_int) -> c_int,
) -> (c_int, i32, Vec<u8>) {
    let mut out = alloc::vec![0u8; OUTCAP];
    let mut outlen: c_int = 0;
    let st = f(out.as_mut_ptr(), OUTCAP as c_int, &mut outlen);
    assert!(st >= 0, "C harness internal failure {st}");
    out.truncate(outlen as usize);
    (st, c_errcode(), out)
}

/// stub:tupdesc CONTROL HOOK (tests only): C descriptor field-plane built
/// from an arbitrary WIRE vs the Rust plane built from `spec` — the wire may
/// deliberately differ from the spec so the must-fail controls can plant a
/// ONE-SIDE-ONLY construction difference and prove the differential sees it.
#[cfg(test)]
pub(crate) fn desc_control_planes(wire: &[u8], spec: &DescSpec) -> (Vec<u8>, Vec<u8>) {
    // SAFETY: buffers live for the call.
    let (st, _, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_desc_copy(wire.as_ptr(), wire.len() as c_int, 0, 0, 0, o, cap, ol)
    });
    assert_eq!(st, 0);
    let ctx = mcx::MemoryContext::new("stub_tupdesc_control");
    let mcx = ctx.mcx();
    let d = build_rust_desc(mcx, spec);
    let r = tupdesc::CreateTupleDescCopy(mcx, &d).expect("copy");
    let mut rser = Vec::new();
    ser_desc_plane(&mut rser, &r);
    rser.push(u8::from(tupdesc::equalTupleDescs(&d, &r)));
    rser.push(u8::from(tupdesc::equalRowTypes(&d, &r)));
    (rser, cbuf)
}

#[cfg(test)]
pub(crate) fn control_install() -> bool {
    install()
}

fn op_form(cur: &mut Cursor<'_>) {
    let spec = decode_desc(cur);
    let vals = decode_values(cur, &spec, spec.natts());
    let sw = spec_wire(&spec);
    let vw = values_wire(&vals);
    // SAFETY: buffers live for the call.
    let (st, _, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_form(sw.as_ptr(), sw.len() as c_int, vw.as_ptr(), vw.len() as c_int, o, cap, ol)
    });
    assert_eq!(st, 0, "C heap_form_tuple errored on a generator-legal row");

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let desc = build_rust_desc(mcx, &spec);
    let (values, isnull) = stage_values(mcx, &spec, &vals);
    let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &isnull)
        .expect("Rust heap_form_tuple errored where C succeeded");
    let rimg = tuple.image();

    let mut rd = Rd { b: &cbuf, i: 0 };
    let cimg = rd.image();
    assert_eq!(rimg, cimg, "heap_form_tuple image divergence");

    // deform round-trip, compared attribute-by-attribute against C
    let natts = spec.natts();
    let mut dv = alloc::vec![Datum::null(); natts];
    let mut dn = alloc::vec![true; natts];
    types_tuple::heap_deform_tuple(tuple.as_tuple(), &desc, &mut dv, &mut dn);
    let mut rser = Vec::new();
    for i in 0..natts {
        let ca = desc.compact_attr(i);
        ser_datum(&mut rser, dv[i], dn[i], ca.attlen, ca.attbyval);
    }
    let cser = &cbuf[rd.i..];
    assert_eq!(rser.as_slice(), cser, "heap_deform_tuple divergence");

    // self-checking oracle: the round-trip must reproduce the inputs
    for i in 0..natts {
        match &vals[i] {
            None => assert!(dn[i], "deform lost a NULL (att {i})"),
            Some(b) => {
                assert!(!dn[i], "deform fabricated a NULL (att {i})");
                let m = &MENU[spec.atts[i].menu as usize];
                if m.attbyval {
                    let expect = stage_datum(mcx, m.attlen, m.attbyval, b);
                    // width-1 compared under the byval_word mask (platform
                    // non-surface carve; see ser_datum)
                    assert_eq!(
                        byval_word(dv[i], m.attlen),
                        byval_word(expect, m.attlen),
                        "byval round-trip datum mismatch (att {i}, width {})",
                        m.attlen
                    );
                } else if m.attlen > 0 {
                    let p = dv[i].as_usize() as *const u8;
                    // SAFETY: fixed byref datum points into the live image.
                    let got = unsafe { core::slice::from_raw_parts(p, m.attlen as usize) };
                    let mut expect = alloc::vec![0u8; m.attlen as usize];
                    let n = b.len().min(m.attlen as usize);
                    expect[..n].copy_from_slice(&b[..n]);
                    assert_eq!(got, expect.as_slice(), "fixed byref round-trip (att {i})");
                } else if m.attlen == -1 {
                    let p = dv[i].as_usize() as *const u8;
                    // SAFETY: varlena datum points into the live image.
                    unsafe {
                        if types_tuple::varatt::varatt_is_1b_e(p) {
                            let total = types_tuple::varatt::varsize_any(p);
                            let got = core::slice::from_raw_parts(p, total);
                            assert_eq!(got, b.as_slice(), "external round-trip (att {i})");
                        } else {
                            let total = types_tuple::varatt::varsize_any(p);
                            let hdr = if types_tuple::varatt::varatt_is_1b(p) { 1 } else { 4 };
                            let got = core::slice::from_raw_parts(p.add(hdr), total - hdr);
                            let ihdr = if b[0] & 0x01 == 0x01 { 1 } else { 4 };
                            assert_eq!(got, &b[ihdr..], "varlena payload round-trip (att {i})");
                        }
                    }
                } else {
                    let p = dv[i].as_usize() as *const u8;
                    // SAFETY: NUL-terminated cstring in the live image.
                    let got = unsafe { core::ffi::CStr::from_ptr(p.cast()) }.to_bytes();
                    assert_eq!(got, b.as_slice(), "cstring round-trip (att {i})");
                }
            }
        }
    }
}

fn op_toomany() {
    // C side
    // SAFETY: no buffers.
    let cst = unsafe { pg_ta_form_toomany() };
    assert_eq!(cst, 1, "C too-many-columns arm did not error");
    assert_eq!(c_errcode(), 6, "C too-many-columns errcode class");
    // Rust side
    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let n = (types_tuple::MaxTupleAttributeNumber + 1) as usize;
    let desc = tupdesc::CreateTemplateTupleDesc(mcx, n as i32).expect("template");
    let values = alloc::vec![Datum::null(); n];
    let isnull = alloc::vec![true; n];
    let err = match heaptuple::heap_form_tuple(mcx, &desc, &values, &isnull) {
        Ok(_) => panic!("Rust heap_form_tuple accepted 1665 columns"),
        Err(e) => e,
    };
    assert_eq!(class_of(err.sqlstate), 6, "Rust too-many-columns errcode class");
}

fn op_minimal(cur: &mut Cursor<'_>) {
    let spec = decode_desc(cur);
    let vals = decode_values(cur, &spec, spec.natts());
    let sw = spec_wire(&spec);
    let vw = values_wire(&vals);
    // SAFETY: buffers live for the call.
    let (st, _, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_minimal(sw.as_ptr(), sw.len() as c_int, vw.as_ptr(), vw.len() as c_int, o, cap, ol)
    });
    assert_eq!(st, 0, "C minimal chain errored on a generator-legal row");
    let mut rd = Rd { b: &cbuf, i: 0 };
    let c_form = rd.image();
    let c_copy = rd.image();
    let c_heap = rd.image();
    let c_fromheap = rd.image();
    assert!(rd.done());

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let desc = build_rust_desc(mcx, &spec);
    let (values, isnull) = stage_values(mcx, &spec, &vals);

    let mt = heaptuple::heap_form_minimal_tuple(mcx, &desc, &values, &isnull, 0)
        .expect("heap_form_minimal_tuple");
    assert_eq!(mt.as_bytes(), c_form, "heap_form_minimal_tuple image divergence");

    // planned fast path: when the plan exists AND the row is all non-null,
    // its image must equal both the unplanned Rust image and the C image
    match heaptuple::MinimalFormPlan::try_new(&desc) {
        Some(plan) => {
            DIV_PLAN[0].fetch_add(1, Relaxed);
            if !isnull.contains(&true) && desc.natts > 0 {
                let pt = heaptuple::heap_form_minimal_tuple_planned(mcx, &plan, &values, 0)
                    .expect("planned form");
                assert_eq!(pt.as_bytes(), c_form, "planned minimal image divergence");
            }
        }
        None => {
            DIV_PLAN[1].fetch_add(1, Relaxed);
        }
    }

    let mt2 = heaptuple::heap_copy_minimal_tuple(mcx, mt.as_bytes(), 0)
        .expect("heap_copy_minimal_tuple");
    assert_eq!(mt2.as_bytes(), c_copy, "heap_copy_minimal_tuple image divergence");

    let ht = heaptuple::heap_tuple_from_minimal_tuple(mcx, mt.as_bytes())
        .expect("heap_tuple_from_minimal_tuple");
    assert_eq!(ht.image(), c_heap, "heap_tuple_from_minimal_tuple image divergence");

    let ht2 = heaptuple::heap_form_tuple(mcx, &desc, &values, &isnull).expect("form");
    let mt3 = heaptuple::minimal_tuple_from_heap_tuple(mcx, ht2.as_tuple(), 0)
        .expect("minimal_tuple_from_heap_tuple");
    assert_eq!(mt3.as_bytes(), c_fromheap, "minimal_tuple_from_heap_tuple image divergence");
}

fn op_getattr(cur: &mut Cursor<'_>) {
    let spec = decode_desc(cur);
    let natts = spec.natts();
    if natts == 0 {
        return;
    }
    let src_natts = cur.u8() as usize % (natts + 1);
    let attnum = 1 + (cur.u8() as usize % natts) as i32;
    let vals = decode_values(cur, &spec, src_natts);
    let sw = spec_wire(&spec);
    let vw = values_wire(&vals);
    // SAFETY: buffers live for the call.
    let (st, _, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_getattr(sw.as_ptr(), sw.len() as c_int, src_natts as c_int, attnum,
                      vw.as_ptr(), vw.len() as c_int, o, cap, ol)
    });
    assert_eq!(st, 0, "C getattr errored on a generator-legal probe");
    let mut rd = Rd { b: &cbuf, i: 0 };
    let cimg = rd.image();
    let cdatum = &cbuf[rd.i..cbuf.len() - 1];
    let cisnull_att = cbuf[cbuf.len() - 1];

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let desc = build_rust_desc(mcx, &spec);

    // Rust reads run over the C-FORMED image (heap_getattr's own image plane
    // is op 0); cross-check the Rust-formed image first.
    let src = if src_natts < natts {
        tupdesc::CreateTupleDescTruncatedCopy(mcx, &desc, src_natts as i32).expect("trunc")
    } else {
        build_rust_desc(mcx, &spec)
    };
    let (values, isnull) = stage_values(mcx, &spec, &vals);
    let rt = heaptuple::heap_form_tuple(mcx, &src, &values[..src_natts], &isnull[..src_natts])
        .expect("form");
    assert_eq!(rt.image(), cimg, "source image divergence (getattr)");

    // SAFETY: cimg is a live, MAXALIGN'd (Vec<u8> from a fresh allocation is
    // at least 8-aligned on this platform via the global allocator) heap
    // tuple image produced by the C oracle's heap_form_tuple.
    let ctup = unsafe {
        types_tuple::HeapTupleData::from_raw_parts(
            cimg.as_ptr(),
            cimg.len() as u32,
            types_tuple::ItemPointerData::invalid(),
            types_core::primitive::InvalidOid,
        )
    };

    let ca = desc.compact_attr(attnum as usize - 1);
    let (attlen, attbyval) = (ca.attlen, ca.attbyval);

    // cold pass (fresh attcacheoff), then warm pass on the same descriptor
    let mut rser_cold = Vec::new();
    let mut n1 = false;
    // SAFETY: attnum in 1..=natts; descriptor matches the image.
    let d1 = unsafe { types_tuple::heap_getattr(&ctup, attnum, &desc, &mut n1) };
    ser_datum(&mut rser_cold, d1, n1, attlen, attbyval);
    let mut rser_warm = Vec::new();
    let mut n2 = false;
    // SAFETY: as above (attcacheoff now warmed).
    let d2 = unsafe { types_tuple::heap_getattr(&ctup, attnum, &desc, &mut n2) };
    ser_datum(&mut rser_warm, d2, n2, attlen, attbyval);
    assert_eq!(rser_cold, rser_warm, "heap_getattr cold/warm drift (Rust)");
    assert_eq!(rser_cold.as_slice(), cdatum, "heap_getattr divergence");

    // fastgetattr + nocachegetattr agreement (attribute present in the tuple)
    if attnum as usize <= src_natts {
        let mut fser = Vec::new();
        let mut fn1 = false;
        // SAFETY: attnum <= tuple natts (checked); desc matches image.
        let fd = unsafe { types_tuple::fastgetattr(&ctup, attnum, &desc, &mut fn1) };
        ser_datum(&mut fser, fd, fn1, attlen, attbyval);
        assert_eq!(fser.as_slice(), cdatum, "fastgetattr divergence");
        if !fn1 {
            // nocachegetattr: cold descriptor then warmed, both vs C
            let cold_desc = build_rust_desc(mcx, &spec);
            let mut nser = Vec::new();
            // SAFETY: non-null, present attribute (nocachegetattr contract).
            let nd = unsafe { types_tuple::nocachegetattr(&ctup, attnum, &cold_desc) };
            ser_datum(&mut nser, nd, false, attlen, attbyval);
            assert_eq!(nser.as_slice(), cdatum, "nocachegetattr (cold) divergence");
            let mut nser2 = Vec::new();
            // SAFETY: as above; cold_desc attcacheoff now warmed.
            let nd2 = unsafe { types_tuple::nocachegetattr(&ctup, attnum, &cold_desc) };
            ser_datum(&mut nser2, nd2, false, attlen, attbyval);
            assert_eq!(nser2.as_slice(), cdatum, "nocachegetattr (warm) divergence");
        }
    }

    let risnull = types_tuple::heap_attisnull(&ctup, attnum, Some(&desc));
    assert_eq!(u8::from(risnull), cisnull_att, "heap_attisnull divergence");
}

fn op_modify(cur: &mut Cursor<'_>, by_cols: bool) {
    let spec = decode_desc(cur);
    let natts = spec.natts();
    let vals = decode_values(cur, &spec, natts);
    // decode the replacement plan from fuzz bytes (kept in normalized form so
    // the C wire below and the Rust arrays are the same data)
    enum Repl {
        PerAtt(Vec<Option<Option<Vec<u8>>>>), // outer: do_replace, inner: value
        ByCols(Vec<(u8, Option<Vec<u8>>)>),
    }
    let repl = if !by_cols {
        let mut v = Vec::with_capacity(natts);
        for a in &spec.atts {
            if cur.u8() & 1 != 0 {
                let nul = a.aflags & 0x01 != 0 || cur.u8() & 0x03 == 0;
                if nul {
                    v.push(Some(None));
                } else {
                    v.push(Some(Some(gen_value(cur, a.menu as usize))));
                }
            } else {
                v.push(None);
            }
        }
        Repl::PerAtt(v)
    } else {
        if natts == 0 {
            return;
        }
        let ncols = (cur.u8() as usize % natts.min(8)) + 1;
        let mut v = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let col = 1 + cur.u8() % natts as u8;
            let a = &spec.atts[col as usize - 1];
            let nul = a.aflags & 0x01 != 0 || cur.u8() & 0x03 == 0;
            let val = if nul { None } else { Some(gen_value(cur, a.menu as usize)) };
            v.push((col, val));
        }
        Repl::ByCols(v)
    };

    // C wire for the replacement plan
    let mut rw = Vec::new();
    match &repl {
        Repl::PerAtt(v) => {
            for e in v {
                match e {
                    None => rw.push(0),
                    Some(None) => {
                        rw.push(1);
                        rw.push(1);
                    }
                    Some(Some(b)) => {
                        rw.push(1);
                        rw.push(0);
                        rw.extend_from_slice(&(b.len() as u16).to_le_bytes());
                        rw.extend_from_slice(b);
                    }
                }
            }
        }
        Repl::ByCols(v) => {
            rw.push(v.len() as u8);
            for (col, val) in v {
                rw.push(*col);
                match val {
                    None => rw.push(1),
                    Some(b) => {
                        rw.push(0);
                        rw.extend_from_slice(&(b.len() as u16).to_le_bytes());
                        rw.extend_from_slice(b);
                    }
                }
            }
        }
    }

    let sw = spec_wire(&spec);
    let vw = values_wire(&vals);
    // SAFETY: buffers live for the call.
    let (st, _, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_modify(sw.as_ptr(), sw.len() as c_int, vw.as_ptr(), vw.len() as c_int,
                     rw.as_ptr(), rw.len() as c_int, c_int::from(by_cols), o, cap, ol)
    });
    assert_eq!(st, 0, "C modify errored on a generator-legal row");
    let mut rd = Rd { b: &cbuf, i: 0 };
    let cimg = rd.image();

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let desc = build_rust_desc(mcx, &spec);
    let (values, isnull) = stage_values(mcx, &spec, &vals);
    let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &isnull).expect("form");

    let newimg = match &repl {
        Repl::PerAtt(v) => {
            let mut rv = alloc::vec![Datum::null(); natts];
            let mut rn = alloc::vec![true; natts];
            let mut dorepl = alloc::vec![false; natts];
            for (i, e) in v.iter().enumerate() {
                if let Some(val) = e {
                    dorepl[i] = true;
                    if let Some(b) = val {
                        let m = &MENU[spec.atts[i].menu as usize];
                        rv[i] = stage_datum(mcx, m.attlen, m.attbyval, b);
                        rn[i] = false;
                    }
                }
            }
            heaptuple::heap_modify_tuple(mcx, tuple.as_tuple(), &desc, &rv, &rn, &dorepl)
                .expect("heap_modify_tuple")
        }
        Repl::ByCols(v) => {
            let cols: Vec<i32> = v.iter().map(|(c, _)| *c as i32).collect();
            let mut rv = Vec::with_capacity(v.len());
            let mut rn = Vec::with_capacity(v.len());
            for (col, val) in v {
                match val {
                    None => {
                        rv.push(Datum::null());
                        rn.push(true);
                    }
                    Some(b) => {
                        let m = &MENU[spec.atts[*col as usize - 1].menu as usize];
                        rv.push(stage_datum(mcx, m.attlen, m.attbyval, b));
                        rn.push(false);
                    }
                }
            }
            heaptuple::heap_modify_tuple_by_cols(mcx, tuple.as_tuple(), &desc, &cols, &rv, &rn)
                .expect("heap_modify_tuple_by_cols")
        }
    };
    assert_eq!(newimg.image(), cimg, "heap_modify_tuple image divergence");
}

fn op_copy(cur: &mut Cursor<'_>) {
    let spec = decode_desc(cur);
    let vals = decode_values(cur, &spec, spec.natts());
    let sw = spec_wire(&spec);
    let vw = values_wire(&vals);
    // SAFETY: buffers live for the call.
    let (st, _, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_copy(sw.as_ptr(), sw.len() as c_int, vw.as_ptr(), vw.len() as c_int, o, cap, ol)
    });
    assert_eq!(st, 0, "C copy errored on a generator-legal row");
    let mut rd = Rd { b: &cbuf, i: 0 };
    let c1 = rd.image();
    let c2 = rd.image();
    let c_has_ext = rd.u8();

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let desc = build_rust_desc(mcx, &spec);
    let (values, isnull) = stage_values(mcx, &spec, &vals);
    let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &isnull).expect("form");

    let rc = heaptuple::heap_copytuple(mcx, tuple.as_tuple()).expect("heap_copytuple");
    assert_eq!(rc.image(), c1, "heap_copytuple image divergence");
    // C heap_copytuple_with_tuple produces the same image bytes; compared
    // against the Rust whole-tuple copy (Rust has no _with_tuple variant:
    // the management-struct split dissolves into HeapTuple ownership)
    assert_eq!(rc.image(), c2, "heap_copytuple_with_tuple image divergence");

    let r_has_ext = tuple.as_tuple().has_external();
    assert_eq!(u8::from(r_has_ext), c_has_ext, "has_external verdict divergence");
    if !r_has_ext {
        let c3 = rd.image();
        let d = heaptuple::heap_copy_tuple_as_datum(mcx, tuple.as_tuple(), &desc)
            .expect("heap_copy_tuple_as_datum");
        let p = d.as_usize() as *const u8;
        // SAFETY: composite datum with a 4B header; length = datum length.
        let rimg = unsafe {
            let total = types_tuple::varatt::varsize_any(p);
            core::slice::from_raw_parts(p, total)
        };
        assert_eq!(rimg, c3, "heap_copy_tuple_as_datum image divergence");
    }
    assert!(rd.done() || r_has_ext);
}

fn op_expand(cur: &mut Cursor<'_>) {
    let spec = decode_desc(cur);
    let natts = spec.natts();
    if natts == 0 {
        return;
    }
    let src_natts = cur.u8() as usize % natts; // 0..natts-1 (strictly narrower)
    let vals = decode_values(cur, &spec, src_natts);
    let sw = spec_wire(&spec);
    let vw = values_wire(&vals);
    // SAFETY: buffers live for the call.
    let (st, _, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_expand(sw.as_ptr(), sw.len() as c_int, src_natts as c_int,
                     vw.as_ptr(), vw.len() as c_int, o, cap, ol)
    });
    assert_eq!(st, 0, "C expand errored on a generator-legal row");
    let mut rd = Rd { b: &cbuf, i: 0 };
    let c_src = rd.image();
    let c_heap = rd.image();
    let c_min = rd.image();
    assert!(rd.done());

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let desc = build_rust_desc(mcx, &spec);
    let src = tupdesc::CreateTupleDescTruncatedCopy(mcx, &desc, src_natts as i32).expect("trunc");
    let (values, isnull) = stage_values(mcx, &spec, &vals);
    let tuple = heaptuple::heap_form_tuple(mcx, &src, &values[..src_natts], &isnull[..src_natts])
        .expect("form");
    assert_eq!(tuple.image(), c_src, "expand source image divergence");

    let he = heaptuple::heap_expand_tuple(mcx, tuple.as_tuple(), &desc).expect("heap_expand_tuple");
    assert_eq!(he.image(), c_heap, "heap_expand_tuple image divergence");
    let me = heaptuple::minimal_expand_tuple(mcx, tuple.as_tuple(), &desc)
        .expect("minimal_expand_tuple");
    assert_eq!(me.as_bytes(), c_min, "minimal_expand_tuple image divergence");
}

/// single-field spec mutation for equal/hash witness pairs
fn mutate_spec(s: &mut DescSpec, cur: &mut Cursor<'_>) {
    let field = cur.u8() % 20;
    let natts = s.natts();
    if natts == 0 || field >= 16 {
        match field % 4 {
            0 => s.dflags ^= 0x10,                       // tdtypeid
            1 => s.dflags ^= 0x02,                       // constr has_not_null
            2 => {
                if let Some((_, b)) = s.defvals.first_mut() {
                    b.push(b'x'); // defval adbin
                } else {
                    s.dflags ^= 0x04; // has_generated_stored
                }
            }
            _ => {
                if let Some(a) = s.atts.iter_mut().find(|a| a.aflags & 0x04 != 0) {
                    a.missing.push(0x11); // missing value bytes
                } else {
                    s.dflags ^= 0x08; // has_generated_virtual
                }
            }
        }
        return;
    }
    let a = &mut s.atts[cur.u8() as usize % natts];
    match field {
        0 => a.nameidx ^= 0x20,             // attname case
        1 => a.nameidx = (a.nameidx & 0xe0) | ((a.nameidx.wrapping_add(1)) & 0x1f),
        2 => a.xflags ^= 0x08,              // atttypid alias
        3 => {
            // menu swap between the attlen-only witness pair when possible
            a.menu = match a.menu { 5 => 11, 11 => 5, m => (m + 1) % NMENU as u8 };
            if a.menu as usize >= NMENU { a.menu = 0; }
            a.missing.clear();
            a.aflags &= !0x04;
        }
        4 => a.aflags ^= 0x02,              // attnotnull
        5 => a.aflags ^= 0x08,              // attnullability (when notnull)
        6 => a.aflags ^= 0x20,              // attndims
        7 => a.aflags ^= 0x40,              // atttypmod
        8 => a.aflags ^= 0x80,              // attcollation
        9 => {
            a.aflags ^= 0x01;               // attisdropped
            a.missing.clear();
            a.aflags &= !0x04;
        }
        10 => a.aflags ^= 0x10,             // attislocal
        11 => a.xflags ^= 0x01,             // atthasdef
        12 => a.xflags ^= 0x02,             // attidentity
        13 => a.xflags ^= 0x04,             // attgenerated
        14 => a.xflags ^= 0x10,             // attinhcount
        _ => a.xflags ^= 0x20,              // attcompression
    }
}

fn op_desc_cmp(cur: &mut Cursor<'_>) {
    let spec1 = decode_desc(cur);
    let mode = cur.u8();
    let spec2 = if mode & 1 != 0 {
        let mut s = spec1.clone();
        mutate_spec(&mut s, cur);
        s
    } else {
        decode_desc(cur)
    };
    let sw1 = spec_wire(&spec1);
    let sw2 = spec_wire(&spec2);
    // SAFETY: buffers live for the call.
    let (st, _, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_desc_cmp(sw1.as_ptr(), sw1.len() as c_int, sw2.as_ptr(), sw2.len() as c_int,
                       o, cap, ol)
    });
    assert_eq!(st, 0);
    let mut rd = Rd { b: &cbuf, i: 0 };
    let c_eq = rd.u8();
    let c_eqrow = rd.u8();
    let c_h1 = rd.u32();
    let c_h2 = rd.u32();

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let d1 = build_rust_desc(mcx, &spec1);
    let d2 = build_rust_desc(mcx, &spec2);
    assert_eq!(u8::from(tupdesc::equalTupleDescs(&d1, &d2)), c_eq, "equalTupleDescs divergence");
    assert_eq!(u8::from(tupdesc::equalRowTypes(&d1, &d2)), c_eqrow, "equalRowTypes divergence");
    assert_eq!(tupdesc::hashRowType(&d1), c_h1, "hashRowType(d1) divergence");
    assert_eq!(tupdesc::hashRowType(&d2), c_h2, "hashRowType(d2) divergence");
}

fn op_desc_copy(cur: &mut Cursor<'_>) {
    let spec = decode_desc(cur);
    let natts = spec.natts();
    let which = (cur.u8() % 5) as i32;
    if which == 4 && natts == 0 {
        return;
    }
    let arg1: i32 = match which {
        1 => (cur.u8() as usize % (natts + 1)) as i32,
        4 => 1 + (cur.u8() as usize % natts) as i32,
        _ => 0,
    };
    let arg2: i32 = if which == 4 { 1 + (cur.u8() as usize % natts) as i32 } else { 0 };
    let sw = spec_wire(&spec);
    // SAFETY: buffers live for the call.
    let (st, _, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_desc_copy(sw.as_ptr(), sw.len() as c_int, which, arg1, arg2, o, cap, ol)
    });
    assert_eq!(st, 0);

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let d = build_rust_desc(mcx, &spec);
    let r: TupleDescData<'_> = match which {
        0 => tupdesc::CreateTupleDescCopy(mcx, &d).expect("copy"),
        1 => tupdesc::CreateTupleDescTruncatedCopy(mcx, &d, arg1).expect("trunc"),
        2 => tupdesc::CreateTupleDescCopyConstr(mcx, &d).expect("copyconstr"),
        3 => {
            let mut dst = tupdesc::CreateTemplateTupleDesc(mcx, d.natts).expect("template");
            tupdesc::TupleDescCopy(&mut dst, &d);
            dst
        }
        _ => {
            let mut dst = tupdesc::CreateTupleDescCopy(mcx, &d).expect("copy");
            tupdesc::TupleDescCopyEntry(&mut dst, arg1 as i16, &d, arg2 as i16);
            dst
        }
    };
    let mut rser = Vec::new();
    ser_desc_plane(&mut rser, &r);
    rser.push(u8::from(tupdesc::equalTupleDescs(&d, &r)));
    rser.push(u8::from(tupdesc::equalRowTypes(&d, &r)));
    assert_eq!(rser.as_slice(), &cbuf[..], "tupdesc copy field-plane divergence (which={which})");
}

fn op_desc_init(cur: &mut Cursor<'_>) {
    let mode = cur.u8() & 1;
    let n = (cur.u8() % 9) as usize;
    #[derive(Clone)]
    struct Ent {
        kind: u8,
        code: u8,
        nameidx: u8,
        tm: u8,
        dim: u8,
    }
    let mut ents = Vec::with_capacity(n);
    for _ in 0..n {
        ents.push(Ent {
            kind: cur.u8(),
            code: cur.u8(),
            nameidx: cur.u8(),
            tm: cur.u8(),
            dim: cur.u8(),
        });
    }
    // wire
    let mut es = Vec::with_capacity(2 + 5 * n);
    es.push(mode);
    es.push(n as u8);
    for e in &ents {
        es.extend_from_slice(&[e.kind, e.code, e.nameidx, e.tm, e.dim]);
    }
    // SAFETY: buffers live for the call.
    let (st, cclass, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_desc_init(es.as_ptr(), es.len() as c_int, o, cap, ol)
    });

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    const BUILTIN: [Oid; 6] = [25, 16, 23, 20, 26, 1009]; // text/bool/int4/int8/oid/text[]

    let rres: Result<TupleDescData<'_>, alloc::boxed::Box<types_error::PgError>> = (|| {
        if mode == 1 {
            let names: Vec<String> = ents
                .iter()
                .map(|e| {
                    format!("{}{}", if e.nameidx & 0x20 != 0 { 'C' } else { 'c' }, e.nameidx & 0x1f)
                })
                .collect();
            let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let types: Vec<Oid> = ents.iter().map(|e| MENU[e.code as usize % NMENU].typid).collect();
            let typmods: Vec<i32> = ents.iter().map(|e| e.tm as i32 - 1).collect();
            let colls: Vec<Oid> =
                ents.iter().map(|e| if e.code & 0x40 != 0 { 999 } else { 0 }).collect();
            tupdesc::BuildDescFromLists(mcx, &name_refs, &types, &typmods, &colls)
        } else {
            let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, n as i32)?;
            for (i, e) in ents.iter().enumerate() {
                let attnum = (i + 1) as i16;
                let nm =
                    format!("{}{}", if e.nameidx & 0x20 != 0 { 'C' } else { 'c' }, e.nameidx & 0x1f);
                let name = if e.nameidx == 0xFF { None } else { Some(nm.as_str()) };
                let tm = e.tm as i32 - 1;
                let dim = (e.dim & 1) as i32;
                match e.kind % 5 {
                    0 => tupdesc::TupleDescInitEntry(
                        &mut desc, attnum, name, MENU[e.code as usize % NMENU].typid, tm, dim,
                    )?,
                    1 => tupdesc::TupleDescInitEntry(&mut desc, attnum, name, 4242, tm, dim)?,
                    2 => tupdesc::TupleDescInitBuiltinEntry(
                        &mut desc, attnum, &nm, BUILTIN[e.code as usize % 6], tm, dim,
                    )?,
                    3 => tupdesc::TupleDescInitBuiltinEntry(&mut desc, attnum, &nm, 4242, tm, dim)?,
                    _ => {
                        tupdesc::TupleDescInitEntry(
                            &mut desc, attnum, name, MENU[e.code as usize % NMENU].typid, tm, dim,
                        )?;
                        tupdesc::TupleDescInitEntryCollation(&mut desc, attnum, 999);
                    }
                }
            }
            Ok(desc)
        }
    })();

    match rres {
        Ok(desc) => {
            assert_eq!(st, 0, "desc_init verdict divergence (C errored, Rust ok)");
            let mut rser = Vec::new();
            ser_desc_plane(&mut rser, &desc);
            assert_eq!(rser.as_slice(), &cbuf[..], "desc_init field-plane divergence");
        }
        Err(e) => {
            assert_eq!(st, 1, "desc_init verdict divergence (Rust errored, C ok)");
            assert_eq!(class_of(e.sqlstate), cclass, "desc_init errcode class divergence");
        }
    }
}

fn op_attmap(cur: &mut Cursor<'_>) {
    let spec_in = decode_desc(cur);
    let mode = cur.u8();
    let spec_out = if mode & 1 != 0 {
        // related pair: single-field mutation keeps by-name matching likely
        let mut s = spec_in.clone();
        mutate_spec(&mut s, cur);
        s
    } else {
        decode_desc(cur)
    };
    let which = (cur.u8() % 7) as i32;
    let vals = decode_values(cur, &spec_in, spec_in.natts());
    let sw1 = spec_wire(&spec_in);
    let sw2 = spec_wire(&spec_out);
    let vw = values_wire(&vals);
    // SAFETY: buffers live for the call.
    let (st, cclass, cbuf) = run_c(|o, cap, ol| unsafe {
        pg_ta_attmap(sw1.as_ptr(), sw1.len() as c_int, sw2.as_ptr(), sw2.len() as c_int,
                     which, vw.as_ptr(), vw.len() as c_int, o, cap, ol)
    });

    let ctx = mcx::MemoryContext::new("tupaccess_diff");
    let mcx = ctx.mcx();
    let indesc = build_rust_desc(mcx, &spec_in);
    let outdesc = build_rust_desc(mcx, &spec_out);

    // Rust result: Ok(Some(map)) | Ok(None = identity) | Err(class)
    let rres: Result<Option<PgVec<'_, i16>>, alloc::boxed::Box<types_error::PgError>> =
        match which {
            0 => tupdesc::build_attrmap_by_name(mcx, &indesc, &outdesc).map(Some),
            1 => tupdesc::build_attrmap_by_name_missing_ok(mcx, &indesc, &outdesc).map(Some),
            2 => tupdesc::build_attrmap_by_name_if_req(mcx, &indesc, &outdesc, false),
            3 => tupdesc::build_attrmap_by_name_if_req(mcx, &indesc, &outdesc, true),
            4 => tupdesc::build_attrmap_by_position(mcx, &indesc, &outdesc, "pg_ta position mismatch"),
            5 => tupdesc::convert_tuples_by_name(mcx, &indesc, &outdesc),
            _ => tupdesc::convert_tuples_by_position(mcx, &indesc, &outdesc, "pg_ta position mismatch"),
        };

    match rres {
        Err(e) => {
            assert_eq!(st, 1, "attmap verdict divergence (Rust errored, C ok; which={which})");
            assert_eq!(class_of(e.sqlstate), cclass, "attmap errcode class divergence");
        }
        Ok(rmap) => {
            assert_eq!(st, 0, "attmap verdict divergence (C errored, Rust ok; which={which})");
            let mut rd = Rd { b: &cbuf, i: 0 };
            let ckind = rd.u8();
            match rmap {
                None => assert_eq!(ckind, 1, "attmap identity verdict divergence (which={which})"),
                Some(map) => {
                    assert_eq!(ckind, 0, "attmap identity verdict divergence (which={which})");
                    let maplen = rd.u16() as usize;
                    assert_eq!(maplen, map.len(), "attmap maplen divergence");
                    for (i, &m) in map.iter().enumerate() {
                        let cm = rd.u16();
                        assert_eq!(m as u16, cm, "attmap entry {i} divergence");
                    }
                    if which >= 5 {
                        // conversion executed over the source tuple
                        let (values, isnull) = stage_values(mcx, &spec_in, &vals);
                        let tuple =
                            heaptuple::heap_form_tuple(mcx, &indesc, &values, &isnull)
                                .expect("form");
                        let ct = heaptuple::execute_attr_map_tuple(
                            mcx, tuple.as_tuple(), &indesc, &outdesc, &map,
                        )
                        .expect("execute_attr_map_tuple");
                        let cimg = rd.image();
                        assert_eq!(ct.image(), cimg, "execute_attr_map_tuple image divergence");
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Seed corpus generator: builders that mirror the fuzz-input decoders above
// (decode_desc / decode_values / op payloads). Run
// `cargo test -p decoder_fuzz tupaccess -- --ignored write_seed_corpus`
// after changing the input format, then commit fuzz/corpus/tupaccess_diff.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod seedgen {
    use super::*;
    use std::collections::BTreeMap;
    use std::vec::Vec as StdVec;

    pub struct B(pub Vec<u8>);

    impl B {
        pub fn new(op: u8) -> B {
            B(alloc::vec![op])
        }
        pub fn u8(mut self, v: u8) -> B {
            self.0.push(v);
            self
        }
        pub fn raw(mut self, v: &[u8]) -> B {
            self.0.extend_from_slice(v);
            self
        }
        /// descriptor header: natts + dflags
        pub fn desc(self, natts: u8, dflags: u8) -> B {
            self.u8(natts).u8(dflags)
        }
        /// one attribute spec (no missing value)
        pub fn att(self, menu: u8, aflags: u8, nameidx: u8, xflags: u8) -> B {
            self.u8(menu).u8(aflags).u8(nameidx).u8(xflags)
        }
        /// gen_value input for a byval column of width w
        pub fn vbyval(self, bytes: &[u8]) -> B {
            self.raw(bytes)
        }
        /// gen_value input for a 4B-header varlena of payload len
        pub fn v4b(self, payload: &[u8]) -> B {
            self.u8(0) // mode < 5
                .raw(&(payload.len() as u16).to_le_bytes())
                .raw(payload)
        }
        /// gen_value input for a short-header varlena of payload len (<=126)
        pub fn vshort(self, payload: &[u8]) -> B {
            self.u8(5)
                .raw(&((payload.len() + 1) as u16).to_le_bytes())
                .raw(payload)
        }
        /// gen_value input for a TOAST pointer
        pub fn vext(self, body16: &[u8; 16]) -> B {
            self.u8(7).raw(body16)
        }
        /// gen_value input for a cstring
        pub fn vcstr(self, payload: &[u8]) -> B {
            self.u8(payload.len() as u8).raw(payload)
        }
        /// values-list entry markers
        pub fn nonnull(self) -> B {
            self.u8(1)
        }
        pub fn null(self) -> B {
            self.u8(0)
        }
    }

    /// all-byval-int4 descriptor with `natts` columns, values all non-null
    pub fn int4_row(op: u8, natts: u8, with_null_at: Option<u8>) -> Vec<u8> {
        let mut b = B::new(op).desc(natts, 0);
        for i in 0..natts {
            b = b.att(2, 0, i % 32, 0);
        }
        for i in 0..natts {
            b = if Some(i) == with_null_at {
                b.null()
            } else {
                b.nonnull().vbyval(&[i.wrapping_mul(17), 0, 0, 0x80])
            };
        }
        b.0
    }

    pub fn corpus() -> BTreeMap<std::string::String, StdVec<u8>> {
        let mut m: BTreeMap<std::string::String, StdVec<u8>> = BTreeMap::new();
        let mut put = |name: &str, v: Vec<u8>| {
            m.insert(name.into(), v);
        };

        // byval widths with the HIGH BIT SET (the fetch_att zero-extension
        // defect class), one per width, plus small-negative patterns
        for (i, (menu, w)) in [(0u8, 1usize), (1, 2), (2, 4), (3, 8)].iter().enumerate() {
            let hb = {
                let mut v = alloc::vec![0u8; *w];
                v[*w - 1] = 0x80;
                v
            };
            put(&format!("byval_highbit_w{w}"),
                B::new(0).desc(1, 0).att(*menu, 0, i as u8, 0).nonnull().vbyval(&hb).0);
            put(&format!("byval_allff_w{w}"),
                B::new(0).desc(1, 0).att(*menu, 0, i as u8, 0).nonnull().vbyval(&alloc::vec![0xFF; *w]).0);
        }

        // fleet Linux-aarch64 char-signedness regression (the exact crash
        // input) + multi-attribute width-1 high-bit siblings
        put("seed-w1-highbit-linux-char",
            B::new(0).desc(1, 0).att(0, 0, 0, 0).nonnull().vbyval(&[0xFF]).0);
        put("seed-w1-highbit-multi",
            B::new(0).desc(3, 0).att(0, 0, 0, 0).att(2, 0, 1, 0).att(0, 0, 2, 0)
                .nonnull().vbyval(&[0x80])
                .nonnull().vbyval(&[1, 2, 3, 4])
                .nonnull().vbyval(&[0xFF])
                .0);
        put("seed-w1-highbit-getattr",
            B::new(3).desc(2, 0).att(0, 0, 0, 0).att(0, 0, 1, 0)
                .u8(2).u8(0) // src_natts=2, attnum=1
                .nonnull().vbyval(&[0xFF])
                .nonnull().vbyval(&[0x80])
                .0);

        // NULL-bitmap byte boundaries, with one null to force the bitmap,
        // plus all-null / none-null / exactly-one-null shapes
        for natts in [1u8, 7, 8, 9, 15, 16, 17, 32, 33] {
            put(&format!("bitmap_natts{natts}"), int4_row(0, natts, Some(natts / 2)));
        }
        put("nonull_natts9", int4_row(0, 9, None));
        {
            let mut b = B::new(0).desc(3, 0).att(2, 0, 0, 0).att(2, 0, 1, 0).att(2, 0, 2, 0);
            for _ in 0..3 {
                b = b.null();
            }
            put("allnull_natts3", b.0);
        }
        put("natts0", B::new(0).desc(0, 0).0);

        // varlena header boundaries: short 126/127 total, 4B 126/127/128 total
        put("var_short_totals",
            B::new(0).desc(2, 0).att(7, 0, 0, 0).att(7, 0, 1, 0)
                .nonnull().vshort(&[b'a'; 125]) // total 126
                .nonnull().vshort(&[b'b'; 126]) // total 127
                .0);
        put("var_4b_totals",
            B::new(0).desc(3, 0).att(7, 0, 0, 0).att(7, 0, 1, 0).att(7, 0, 2, 0)
                .nonnull().v4b(&[b'c'; 122]) // total 126: convertible
                .nonnull().v4b(&[b'd'; 123]) // total 127: boundary
                .nonnull().v4b(&[b'e'; 124]) // total 128: not short-able
                .0);
        put("var_plain_storage",
            B::new(0).desc(2, 0).att(8, 0, 0, 0).att(9, 0, 1, 0)
                .nonnull().v4b(b"plain-keeps-4b")
                .nonnull().v4b(b"dblalign")
                .0);
        put("var_empty_and_cstr",
            B::new(0).desc(3, 0).att(7, 0, 0, 0).att(10, 0, 1, 0).att(10, 0, 2, 0)
                .nonnull().v4b(b"")
                .nonnull().vcstr(b"")
                .nonnull().vcstr(b"high\xc3\xa9bytes\xff")
                .0);

        // TOAST pointer attribute (HEAP_HASEXTERNAL fill path + copy carve)
        put("var_external",
            B::new(0).desc(2, 0).att(7, 0, 0, 0).att(2, 0, 1, 0)
                .nonnull().vext(&[7u8; 16])
                .nonnull().vbyval(&[1, 2, 3, 4])
                .0);
        put("copy_external",
            B::new(6).desc(1, 0).att(7, 0, 0, 0).nonnull().vext(&[9u8; 16]).0);

        // byref fixed widths + dropped column mid-descriptor
        put("fixed_byref_row",
            B::new(0).desc(4, 0).att(4, 0, 0, 0).att(5, 0, 1, 0).att(6, 0, 2, 0).att(11, 0, 3, 0)
                .nonnull().vbyval(&[0x80, 1, 2, 3, 4, 5, 6, 0xFF])
                .nonnull().raw(&[9; 12])
                .nonnull().raw(&[b'n'; 64])
                .nonnull().raw(&[3; 16])
                .0);
        put("dropped_mid",
            B::new(0).desc(3, 0).att(2, 0, 0, 0).att(7, 1, 1, 0).att(2, 0, 2, 0)
                .nonnull().vbyval(&[1, 0, 0, 0])
                // dropped column: decode_values forces NULL and reads NO bytes
                .nonnull().vbyval(&[2, 0, 0, 0x80])
                .0);

        // ops 1..7 basics
        put("toomany", B::new(1).0);
        put("minimal_plan_some", int4_row(2, 4, None));
        put("minimal_plan_none",
            B::new(2).desc(2, 0).att(7, 0, 0, 0).att(2, 0, 1, 0)
                .nonnull().vshort(b"mini")
                .nonnull().vbyval(&[0x80, 0x80, 0x80, 0x80])
                .0);
        put("minimal_nulls", int4_row(2, 9, Some(3)));
        put("getattr_basic", {
            let mut v = int4_row(3, 9, Some(4));
            // src_natts byte + attnum byte appended after the desc, before
            // values? No: op3 reads desc, then src_natts, then attnum, then
            // values — rebuild explicitly.
            v.clear();
            let mut b = B::new(3).desc(9, 0);
            for i in 0..9 {
                b = b.att(2, 0, i, 0);
            }
            b = b.u8(9).u8(4); // src_natts=9, attnum=1+4%9
            for i in 0..9u8 {
                b = if i == 4 { b.null() } else { b.nonnull().vbyval(&[i, 0, 0, 0x80]) };
            }
            v.extend_from_slice(&b.0);
            v
        });
        put("getattr_missing_arm", {
            // truncated source (src_natts=1) read under natts=3 with a
            // missing value on att 3: getmissingattr arm
            B::new(3).desc(3, 1)
                .att(2, 0, 0, 0)
                .att(2, 0x04, 1, 0).vbyval(&[0x11, 0, 0, 0x80]) // missing value
                .att(7, 0x04, 2, 0).v4b(b"missing-varlena")
                .u8(0) // ndefval
                .u8(0) // ncheck
                .u8(1) // src_natts = 1
                .u8(1) // attnum = 1 + 1%3 = 2
                .nonnull().vbyval(&[5, 0, 0, 0])
                .0
        });
        put("getattr_varlena_slowpath",
            B::new(3).desc(3, 0).att(7, 0, 0, 0).att(2, 0, 1, 0).att(10, 0, 2, 0)
                .u8(3).u8(2) // src=3, attnum=3
                .nonnull().vshort(b"vv")
                .nonnull().vbyval(&[1, 2, 3, 4])
                .nonnull().vcstr(b"cs")
                .0);
        put("modify_basic", {
            let mut b = B::new(4).desc(3, 0).att(2, 0, 0, 0).att(7, 0, 1, 0).att(3, 0, 2, 0);
            b = b
                .nonnull().vbyval(&[1, 0, 0, 0])
                .nonnull().v4b(b"before")
                .nonnull().vbyval(&[0xFF; 8]);
            // repl: replace att2 with a new varlena, keep others
            b = b.u8(0)
                .u8(1).nonnull().v4b(b"after-repl")
                .u8(0);
            b.0
        });
        put("modify_by_cols", {
            let mut b = B::new(5).desc(3, 0).att(2, 0, 0, 0).att(7, 0, 1, 0).att(0, 0, 2, 0);
            b = b
                .nonnull().vbyval(&[1, 0, 0, 0])
                .nonnull().v4b(b"x")
                .nonnull().vbyval(&[0x80]);
            // ncols byte -> (b % min(3,8)) + 1 = 2 cols
            b = b.u8(1)
                .u8(0) // col = 1 + 0%3 = 1
                .nonnull().vbyval(&[0x80, 0, 0, 0x80])
                .u8(1) // col = 2
                .null();
            b.0
        });
        put("copy_basic", int4_row(6, 5, Some(2)));
        put("expand_missing", {
            // full natts=4 with missing on atts 3,4 (byval + varlena);
            // src_natts=2
            B::new(7).desc(4, 1)
                .att(2, 0, 0, 0)
                .att(7, 0, 1, 0)
                .att(2, 0x04, 2, 0).vbyval(&[0x80, 0, 0, 0x80])
                .att(7, 0x04, 3, 0).v4b(b"filled-in")
                .u8(0).u8(0) // constr: no defval/check
                .u8(2)       // src_natts = 2%4
                .nonnull().vbyval(&[1, 2, 3, 4])
                .nonnull().vshort(b"src")
                .0
        });
        put("expand_nulls",
            B::new(7).desc(3, 0).att(2, 0, 0, 0).att(2, 0, 1, 0).att(2, 0, 2, 0)
                .u8(1) // src_natts = 1
                .nonnull().vbyval(&[9, 9, 9, 9])
                .0);

        // op 8: single-field witness pairs, EACH field, BOTH orders (the
        // xor-mutations are symmetric: a base with the bit clear covers one
        // order, a base with the bit set covers the other)
        let witness_fields: &[(u8, &str)] = &[
            (0, "attname_case"), (1, "attname_idx"), (2, "atttypid"),
            (3, "menu_attlen"), (4, "attnotnull"), (5, "attnullability"),
            (6, "attndims"), (7, "atttypmod"), (8, "attcollation"),
            (9, "attisdropped"), (10, "attislocal"), (11, "atthasdef"),
            (12, "attidentity"), (13, "attgenerated"), (14, "attinhcount"),
            (15, "attcompression"), (16, "tdtypeid"), (17, "constr_notnull"),
            (18, "defval_adbin"), (19, "missing_value"),
        ];
        for &(f, name) in witness_fields {
            for (order, base_aflags, base_xflags) in [
                (0u8, 0u8, 0u8),
                // reverse order: start with the mutated bit already set
                (1, match f { 4 => 0x02, 5 => 0x02 | 0x08, 6 => 0x20, 7 => 0x40,
                              8 => 0x80, 9 => 0x01, 10 => 0x10, _ => 0 },
                    match f { 2 => 0x08, 11 => 0x01, 12 => 0x02, 13 => 0x04,
                              14 => 0x10, 15 => 0x20, _ => 0 }),
            ] {
                // base: 2 attributes; constr present with one defval and one
                // missing so fields 17..19 have material to mutate
                let menu0 = if f == 3 { 5u8 } else { 2 };
                let aflags1 = if f == 5 { 0x02 | base_aflags } else { base_aflags };
                let b = B::new(8)
                    .desc(2, 1 | if f == 16 && order == 1 { 0x10 } else { 0 })
                    .att(menu0, aflags1, 0, base_xflags)
                    .att(7, 0x04, 1, 0).v4b(b"miss")
                    .u8(1).u8(0).u8(4).raw(b"expr") // 1 defval: adnum=1, len 4
                    .u8(0) // no checks
                    .u8(1) // mode: mutate
                    .u8(f) // field selector
                    .u8(0); // att index selector
                put(&format!("witness_{name}_o{order}"), b.0);
            }
        }
        put("desc_cmp_independent", {
            let mut b = B::new(8).desc(2, 0).att(2, 0, 0, 0).att(7, 0, 1, 0);
            b = b.u8(0); // mode: independent second spec
            b = b.desc(3, 0).att(2, 0, 0, 0).att(7, 0, 1, 0).att(0, 0, 2, 0);
            b.u8(0).0
        });

        // op 9: every copy shape over a constr-rich descriptor
        for which in 0u8..5 {
            let b = B::new(9)
                .desc(3, 0x0f)
                .att(2, 0x02, 0, 0x01)
                .att(7, 0x04, 1, 0x22).v4b(b"m1")
                .att(0, 0x04, 2, 0).vbyval(&[0x80])
                .u8(2).u8(0).u8(3).raw(b"dv1").u8(1).u8(2).raw(b"dv")
                .u8(1).u8(0x07).u8(2).raw(b"cn").u8(3).raw(b"exp")
                .u8(which) // which
                .u8(1)     // arg1 material
                .u8(2);    // arg2 material
            put(&format!("desc_copy_which{which}"), b.0);
        }

        // op 10: init entries — every kind incl. both error arms + lists mode
        put("init_entries", {
            let mut b = B::new(10).u8(0).u8(5);
            b = b.raw(&[0, 7, 1, 5, 0]);   // InitEntry menu7
            b = b.raw(&[4, 2, 2, 0, 1]);   // InitEntry + collation
            b = b.raw(&[2, 0, 3, 9, 0]);   // builtin text
            b = b.raw(&[2, 1, 4, 0, 0]);   // builtin bool
            b = b.raw(&[0, 0, 0xFF, 0, 0]); // NULL name InitEntry
            b.0
        });
        put("init_err_lookup", B::new(10).u8(0).u8(1).raw(&[1, 0, 1, 0, 0]).0);
        put("init_err_unsupported", B::new(10).u8(0).u8(1).raw(&[3, 0, 1, 0, 0]).0);
        put("init_lists", {
            let mut b = B::new(10).u8(1).u8(3);
            b = b.raw(&[0, 7, 1, 5, 0]);
            b = b.raw(&[0, 0x42, 2, 0, 0]); // code bit6 -> collation 999
            b = b.raw(&[0, 2, 3, 1, 0]);
            b.0
        });

        // op 11: attmap shapes — identity, rename, reorder, dropped,
        // type-mismatch (error), by-position, executed conversions
        let two_col = |op: u8, n1: u8, x1: u8, n2: u8, x2: u8| {
            B::new(op).desc(2, 0).att(2, 0, n1, x1).att(7, 0, n2, x2)
        };
        for which in 0u8..7 {
            // identity pair (same names/types both sides)
            let b = two_col(11, 0, 0, 1, 0)
                .u8(0) // independent
                .desc(2, 0).att(2, 0, 0, 0).att(7, 0, 1, 0)
                .u8(which)
                .nonnull().vbyval(&[1, 2, 3, 0x80])
                .nonnull().v4b(b"conv");
            put(&format!("attmap_identity_w{which}"), b.0);
        }
        put("attmap_rename",
            two_col(11, 0, 0, 1, 0).u8(0)
                .desc(2, 0).att(2, 0, 3, 0).att(7, 0, 1, 0)
                .u8(1) // by_name missing_ok
                .nonnull().vbyval(&[1, 0, 0, 0]).nonnull().v4b(b"x").0);
        put("attmap_rename_err",
            two_col(11, 0, 0, 1, 0).u8(0)
                .desc(2, 0).att(2, 0, 3, 0).att(7, 0, 1, 0)
                .u8(0) // by_name strict -> DATATYPE_MISMATCH error
                .nonnull().vbyval(&[1, 0, 0, 0]).nonnull().v4b(b"x").0);
        put("attmap_reorder",
            two_col(11, 0, 0, 1, 0).u8(0)
                .desc(2, 0).att(7, 0, 1, 0).att(2, 0, 0, 0)
                .u8(5) // convert_tuples_by_name + execute
                .nonnull().vbyval(&[0x80, 0, 0, 0x80]).nonnull().v4b(b"swap").0);
        put("attmap_dropped",
            B::new(11).desc(3, 0).att(2, 0, 0, 0).att(7, 1, 1, 0).att(0, 0, 2, 0)
                .u8(0)
                .desc(3, 0).att(2, 0, 0, 0).att(7, 1, 9, 0).att(0, 0, 2, 0)
                .u8(2) // if_req: dropped-compatible identity check
                .nonnull().vbyval(&[1, 0, 0, 0]).nonnull().vbyval(&[0x80]).0);
        put("attmap_typemismatch",
            two_col(11, 0, 0, 1, 0).u8(0)
                .desc(2, 0).att(3, 0, 0, 0).att(7, 0, 1, 0)
                .u8(0)
                .nonnull().vbyval(&[1, 0, 0, 0]).nonnull().v4b(b"x").0);
        put("attmap_position",
            two_col(11, 0, 0, 1, 0).u8(0)
                .desc(2, 0).att(2, 0, 9, 0).att(7, 0, 8, 0)
                .u8(6) // convert_tuples_by_position + execute (renames OK)
                .nonnull().vbyval(&[2, 0, 0, 0]).nonnull().vshort(b"pos").0);
        put("attmap_position_err",
            two_col(11, 0, 0, 1, 0).u8(0)
                .desc(1, 0).att(3, 0, 0, 0)
                .u8(4) // by_position type mismatch -> error
                .nonnull().vbyval(&[1, 0, 0, 0]).nonnull().v4b(b"x").0);
        put("attmap_mutated_pair",
            two_col(11, 0, 0, 1, 0)
                .u8(1) // mutate mode
                .u8(1) // field: attname idx
                .u8(0) // att 0
                .u8(0) // which = by_name strict
                .nonnull().vbyval(&[1, 0, 0, 0]).nonnull().v4b(b"x").0);

        // a couple of constr-flavored form rows so op0 sees constr descs too
        put("form_with_constr",
            B::new(0).desc(2, 0x03)
                .att(2, 0x02, 0, 0)
                .att(7, 0x04, 1, 0).vshort(b"mv")
                .u8(1).u8(0).u8(2).raw(b"df")
                .u8(1).u8(0x03).u8(1).raw(b"c").u8(2).raw(b"cb")
                .nonnull().vbyval(&[0x7f, 0xff, 0xff, 0xff])
                .null()
                .0);

        m
    }

    #[test]
    #[ignore = "writes fuzz/corpus/tupaccess_diff; run once after format changes"]
    fn write_seed_corpus() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tupaccess_diff");
        std::fs::create_dir_all(dir).unwrap();
        for (name, bytes) in corpus() {
            std::fs::write(std::format!("{dir}/{name}"), &bytes).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(bytes: &[u8]) {
        tupaccess_diff(bytes);
    }

    /// Replay every checked-in seed, then assert the diversity buckets are
    /// all nonzero (the generator/seed obligation of the charter).
    #[test]
    fn seed_corpus_replays_clean_and_diverse() {
        let _serial = crate::c_oracle_serial();
        if !install() {
            return; // sibling module owns the seams in this test process
        }
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tupaccess_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/tupaccess_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                if std::env::var_os("TUPACCESS_SEED_TRACE").is_some() {
                    std::eprintln!("SEED {}", p.display());
                }
                run(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 40, "expected >=40 seeds, found {n}");
        for (name, count) in diversity_report() {
            assert!(count > 0, "diversity bucket {name} never hit over the seed corpus");
        }
    }

    /// The empty input must be a clean no-op (crash-da39a3ee class).
    #[test]
    fn empty_input_is_noop() {
        run(&[]);
        run(&[0]);
        run(&[1]);
    }
}
