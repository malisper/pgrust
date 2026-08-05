//! stub:tupdesc — shared CONSTRUCTED-STATE builder: TupleDesc + heap-tuple
//! specs decoded from fuzz bytes and constructed IDENTICALLY on the Rust
//! side (build_rust_desc / stage_values) and on the C-oracle side (the
//! SECTION D wire decoder in csrc/pg_tupaccess_io.c consumes spec_wire /
//! values_wire output). Factored verbatim from the p1-tupaccess harness
//! (fuzz/core/src/tupaccess_diff.rs), which is the migration demo target.
//!
//! BOTH-SIDES DISCIPLINE: the constructed descriptor/values are part of the
//! compared input. Rust builds from the decoded DescSpec; C builds from the
//! wire encoding of the SAME spec. Neither side defaults anything: every
//! field either comes from the spec bytes or is a documented pin (attrelid=0,
//! attnum=i+1, name from att_name).
//!
//! CLAMPS (part of the compared-input contract; identical both sides because
//! they act on the SPEC before either side builds):
//!   - natts        : u8 % 41            (0..=40; the MaxTupleAttributeNumber
//!                                        error arm is a dedicated op)
//!   - menu index   : u8 % 12            (the 12-entry pinned type menu)
//!   - cstring len  : u8 % 121, NULs stripped
//!   - varlena      : short total 1..=127, 4B payload < 300, TOAST ptr 16B
//!   - defvals      : <= 3 entries, adnum strictly increasing, <= natts
//!   - checks       : <= 2 entries, ASCII NUL-free, sorted+deduped by name
//!   - hasmissing   : masked off on dropped/cstring columns and when the
//!                    descriptor has no constr (C getmissingattr Asserts)
//!
//! CONSTRUCTOR-AUDIT: a builder bug here fabricates agreement (both sides
//! consume the same wrong structure). The must-fail controls in
//! stub_controls_tests.rs plant one-side-only construction differences and
//! assert the differential catches them; the seed-replay + diversity test in
//! the demo target is the structural validator.

extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use datum::Datum;
use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgString, PgVec};
use types_core::Oid;
use types_tuple::{
    AttrDefault, AttrMissing, ConstrCheck, FormData_pg_attribute, TupleConstr, TupleDescData,
    ATTNULLABLE_INVALID, ATTNULLABLE_VALID,
};

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

// ---------------------------------------------------------------------------
// Type menu (== C pg_ta_menu; the harness contract)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub struct MenuEnt {
    pub typid: Oid,
    pub attlen: i16,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
    pub attcollation: Oid,
}

pub const NMENU: usize = 12;
pub const MENU: [MenuEnt; NMENU] = [
    MenuEnt { typid: 91101, attlen: 1, attbyval: true, attalign: b'c' as i8, attstorage: b'p' as i8, attcollation: 0 },
    MenuEnt { typid: 91102, attlen: 2, attbyval: true, attalign: b's' as i8, attstorage: b'p' as i8, attcollation: 0 },
    MenuEnt { typid: 91103, attlen: 4, attbyval: true, attalign: b'i' as i8, attstorage: b'p' as i8, attcollation: 0 },
    MenuEnt { typid: 91104, attlen: 8, attbyval: true, attalign: b'd' as i8, attstorage: b'p' as i8, attcollation: 0 },
    MenuEnt { typid: 91105, attlen: 8, attbyval: false, attalign: b'd' as i8, attstorage: b'p' as i8, attcollation: 0 },
    MenuEnt { typid: 91106, attlen: 12, attbyval: false, attalign: b'i' as i8, attstorage: b'p' as i8, attcollation: 0 },
    MenuEnt { typid: 91107, attlen: 64, attbyval: false, attalign: b'c' as i8, attstorage: b'p' as i8, attcollation: 0 },
    MenuEnt { typid: 91108, attlen: -1, attbyval: false, attalign: b'i' as i8, attstorage: b'x' as i8, attcollation: 100 },
    MenuEnt { typid: 91109, attlen: -1, attbyval: false, attalign: b'i' as i8, attstorage: b'p' as i8, attcollation: 0 },
    MenuEnt { typid: 91110, attlen: -1, attbyval: false, attalign: b'd' as i8, attstorage: b'x' as i8, attcollation: 100 },
    MenuEnt { typid: 91111, attlen: -2, attbyval: false, attalign: b'c' as i8, attstorage: b'p' as i8, attcollation: 0 },
    // differs from entry 5 ONLY in attlen (single-field witness)
    MenuEnt { typid: 91112, attlen: 16, attbyval: false, attalign: b'i' as i8, attstorage: b'p' as i8, attcollation: 0 },
];

// ---------------------------------------------------------------------------
// Diversity counters (asserted nonzero over the seed corpus by the committed
// test at the bottom; a generator emitting only flat non-null int rows would
// yield a meaningless 100%)
// ---------------------------------------------------------------------------

pub static DIV_NATTS: [AtomicUsize; 11] = [const { AtomicUsize::new(0) }; 11];
pub static DIV_MENU: [AtomicUsize; NMENU] = [const { AtomicUsize::new(0) }; NMENU];
/// null, dropped, missing, notnull, constr
pub static DIV_FLAGS: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
/// varlena input header classes: 4B, short, external, 4B-convertible-to-short
pub static DIV_VARHDR: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
/// high-bit-set byval input per width 1/2/4/8
pub static DIV_HIGHBIT: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
/// MinimalFormPlan gate: Some taken, None taken
pub static DIV_PLAN: [AtomicUsize; 2] = [const { AtomicUsize::new(0) }; 2];
pub static DIV_OPS: [AtomicUsize; 12] = [const { AtomicUsize::new(0) }; 12];

pub fn natts_bucket(natts: usize) -> usize {
    match natts {
        0 => 0,
        1 => 1,
        7 => 2,
        8 => 3,
        9 => 4,
        15 => 5,
        16 => 6,
        17 => 7,
        32 => 8,
        33 => 9,
        _ => 10,
    }
}

pub fn diversity_report() -> Vec<(String, usize)> {
    let mut v = Vec::new();
    let names0 = ["natts0", "natts1", "natts7", "natts8", "natts9", "natts15",
                  "natts16", "natts17", "natts32", "natts33", "nattsother"];
    for (i, n) in names0.iter().enumerate() {
        v.push(((*n).into(), DIV_NATTS[i].load(Relaxed)));
    }
    for i in 0..NMENU {
        v.push((format!("menu{i}"), DIV_MENU[i].load(Relaxed)));
    }
    for (i, n) in ["null", "dropped", "missing", "notnull", "constr"].iter().enumerate() {
        v.push((format!("flag_{n}"), DIV_FLAGS[i].load(Relaxed)));
    }
    for (i, n) in ["hdr4b", "hdrshort", "hdrexternal", "hdrconvertible"].iter().enumerate() {
        v.push((format!("var_{n}"), DIV_VARHDR[i].load(Relaxed)));
    }
    for (i, n) in [1, 2, 4, 8].iter().enumerate() {
        v.push((format!("highbit{n}"), DIV_HIGHBIT[i].load(Relaxed)));
    }
    v.push(("plan_some".into(), DIV_PLAN[0].load(Relaxed)));
    v.push(("plan_none".into(), DIV_PLAN[1].load(Relaxed)));
    for i in 0..12 {
        v.push((format!("op{i}"), DIV_OPS[i].load(Relaxed)));
    }
    v
}

// ---------------------------------------------------------------------------
// Fuzz-input cursor
// ---------------------------------------------------------------------------

pub struct Cursor<'a> {
    pub b: &'a [u8],
    pub i: usize,
}

impl<'a> Cursor<'a> {
    pub fn u8(&mut self) -> u8 {
        let v = self.b.get(self.i).copied().unwrap_or(0);
        self.i += 1;
        v
    }
    pub fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }
    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        let start = self.i.min(self.b.len());
        let end = (self.i + n).min(self.b.len());
        self.i = self.i.saturating_add(n);
        let mut v = self.b[start..end].to_vec();
        v.resize(n, 0);
        v
    }
}

// ---------------------------------------------------------------------------
// Descriptor spec (normalized; encoded identically for both sides)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AttSpec {
    pub menu: u8,
    pub aflags: u8, // bit0 dropped, 1 notnull, 2 hasmissing, 3 null-invalid,
                // 4 !islocal, 5 ndims1, 6 typmod77, 7 coll999
    pub nameidx: u8, // low5 idx, bit5 uppercase
    pub xflags: u8, // bit0 hasdef, 1 identity, 2 generated, 3 typid+100000,
                // 4 inhcount1, 5 compression
    pub missing: Vec<u8>, // datum image bytes (only when hasmissing)
}

#[derive(Clone)]
pub struct DescSpec {
    pub dflags: u8, // bit0 has_constr, 1 has_not_null, 2 gen_stored, 3 gen_virtual,
                // 4 tdtypeid alt
    pub tdtypmod: i32,
    pub atts: Vec<AttSpec>,
    pub defvals: Vec<(u8, Vec<u8>)>,      // (adnum, adbin) adnum strictly increasing
    pub checks: Vec<(u8, Vec<u8>, Vec<u8>)>, // (cflags, ccname, ccbin) sorted by name
}

impl DescSpec {
    pub fn natts(&self) -> usize {
        self.atts.len()
    }
    pub fn has_constr(&self) -> bool {
        self.dflags & 1 != 0
    }
}

/// generate one datum-image byte payload for a menu entry; records diversity
pub fn gen_value(cur: &mut Cursor<'_>, menu: usize) -> Vec<u8> {
    let m = &MENU[menu];
    if m.attbyval {
        let w = m.attlen as usize;
        let v = cur.bytes(w);
        if v[w - 1] & 0x80 != 0 {
            DIV_HIGHBIT[match w { 1 => 0, 2 => 1, 4 => 2, _ => 3 }].fetch_add(1, Relaxed);
        }
        return v;
    }
    if m.attlen > 0 {
        return cur.bytes(m.attlen as usize);
    }
    if m.attlen == -2 {
        // cstring: NULs stripped, length <= 120
        let n = cur.u8() as usize % 121;
        let mut v = cur.bytes(n);
        v.retain(|&b| b != 0);
        return v;
    }
    // varlena: mode selects the input header class
    let mode = cur.u8() % 8;
    if mode == 7 {
        // TOAST pointer: 0x01 tag byte, va_tag = 18 (VARTAG_ONDISK), 16 bytes
        DIV_VARHDR[2].fetch_add(1, Relaxed);
        let mut v = alloc::vec![0x01u8, 18u8];
        v.extend_from_slice(&cur.bytes(16));
        return v;
    }
    if mode >= 5 {
        // short-header input (1B): total size 1..=127 (payload 0..=126)
        let len = cur.u16() as usize % 127;
        let total = 1 + len;
        DIV_VARHDR[1].fetch_add(1, Relaxed);
        let mut v = alloc::vec![((total as u8) << 1) | 0x01];
        v.extend_from_slice(&cur.bytes(len));
        return v;
    }
    // 4B-header input; payload 0..~300 with the 122/123 boundary reachable
    // (VARATT_CAN_MAKE_SHORT: total-4+1+1 <= 127 i.e. payload <= 122... the
    // exact boundary is compared, not assumed)
    let len = cur.u16() as usize % 300;
    let total = 4 + len;
    DIV_VARHDR[0].fetch_add(1, Relaxed);
    if total <= 126 {
        DIV_VARHDR[3].fetch_add(1, Relaxed);
    }
    let mut v = datum::varlena::set_varsize_4b(total).to_vec();
    v.extend_from_slice(&cur.bytes(len));
    v
}

pub fn decode_att(cur: &mut Cursor<'_>, has_constr: bool) -> AttSpec {
    let menu = cur.u8() % NMENU as u8;
    let mut aflags = cur.u8();
    let nameidx = cur.u8();
    let xflags = cur.u8() & 0x3f;
    let dropped = aflags & 1 != 0;
    if dropped || !has_constr || MENU[menu as usize].attlen == -2 {
        // hasmissing only with constr, never on dropped columns, and never
        // on cstring columns (C getmissingattr Asserts attlen > 0 || -1:
        // cstring is not a table-column type, so no attmissingval exists)
        aflags &= !0x04;
    }
    let missing = if aflags & 0x04 != 0 {
        gen_value(cur, menu as usize)
    } else {
        Vec::new()
    };
    DIV_MENU[menu as usize].fetch_add(1, Relaxed);
    if dropped {
        DIV_FLAGS[1].fetch_add(1, Relaxed);
    }
    if aflags & 0x04 != 0 {
        DIV_FLAGS[2].fetch_add(1, Relaxed);
    }
    if aflags & 0x02 != 0 {
        DIV_FLAGS[3].fetch_add(1, Relaxed);
    }
    AttSpec { menu, aflags, nameidx, xflags, missing }
}

pub fn decode_desc(cur: &mut Cursor<'_>) -> DescSpec {
    let natts = (cur.u8() % 41) as usize;
    let dflags = cur.u8() & 0x1f;
    let tdtypmod = if dflags & 0x10 != 0 { 7 } else { -1 };
    let has_constr = dflags & 1 != 0;
    let mut atts = Vec::with_capacity(natts);
    for _ in 0..natts {
        atts.push(decode_att(cur, has_constr));
    }
    let mut defvals = Vec::new();
    let mut checks = Vec::new();
    if has_constr {
        DIV_FLAGS[4].fetch_add(1, Relaxed);
        let nd = (cur.u8() % 4).min(natts as u8);
        let mut adnum = 0u8;
        for _ in 0..nd {
            adnum = adnum + 1 + cur.u8() % 3;
            if adnum as usize > natts {
                break;
            }
            let blen = cur.u8() as usize % 16;
            let mut b = cur.bytes(blen);
            // constr strings are nodeToString outputs: ASCII in practice, and
            // the Rust TupleConstr carries them as PgString (UTF-8) — keep
            // the generator to NUL-free ASCII on both sides
            b.retain(|&x| x != 0 && x < 0x80);
            defvals.push((adnum, b));
        }
        let nc = cur.u8() % 3;
        for _ in 0..nc {
            let cflags = cur.u8() & 0x07;
            let nlen = cur.u8() as usize % 8;
            let mut nm = cur.bytes(nlen);
            nm.retain(|&x| x != 0 && x < 0x80);
            let blen = cur.u8() as usize % 12;
            let mut b = cur.bytes(blen);
            b.retain(|&x| x != 0 && x < 0x80);
            checks.push((cflags, nm, b));
        }
        // equalTupleDescs assumes ConstrCheck entries sorted by name
        checks.sort_by(|a, b| a.1.cmp(&b.1));
        checks.dedup_by(|a, b| a.1 == b.1);
    }
    DIV_NATTS[natts_bucket(natts)].fetch_add(1, Relaxed);
    DescSpec { dflags, tdtypmod, atts, defvals, checks }
}

/// wire-encode the spec for the C oracle (SECTION D contract)
pub fn spec_wire(s: &DescSpec) -> Vec<u8> {
    let mut w = Vec::with_capacity(64 + 8 * s.natts());
    w.push(s.natts() as u8);
    w.push(s.dflags);
    w.extend_from_slice(&s.tdtypmod.to_le_bytes());
    for a in &s.atts {
        w.push(a.menu);
        w.push(a.aflags);
        w.push(a.nameidx);
        w.push(a.xflags);
        if a.aflags & 0x04 != 0 {
            w.extend_from_slice(&(a.missing.len() as u16).to_le_bytes());
            w.extend_from_slice(&a.missing);
        }
    }
    if s.has_constr() {
        w.push(s.defvals.len() as u8);
        for (adnum, b) in &s.defvals {
            w.push(*adnum);
            w.push(b.len() as u8);
            w.extend_from_slice(b);
        }
        w.push(s.checks.len() as u8);
        for (cflags, nm, b) in &s.checks {
            w.push(*cflags);
            w.push(nm.len() as u8);
            w.extend_from_slice(nm);
            w.push(b.len() as u8);
            w.extend_from_slice(b);
        }
    }
    w
}

// C's pg_ta_build_desc reads the constr defval/check sections with its own
// count bytes; keep encoder + decoder in lockstep: the C decoder reads
// EXACTLY ndefval entries, so the wire count must equal what we emit (the
// normalization above may break early, hence len() is authoritative).

pub fn att_name(a: &AttSpec) -> String {
    format!("{}{}", if a.nameidx & 0x20 != 0 { 'C' } else { 'c' }, a.nameidx & 0x1f)
}

/// stage a datum from image bytes (mirror of C pg_ta_stage)
pub fn stage_datum(mcx: Mcx<'_>, attlen: i16, attbyval: bool, bytes: &[u8]) -> Datum {
    if attbyval {
        let mut w = [0u8; 8];
        let n = bytes.len().min(attlen as usize);
        w[..n].copy_from_slice(&bytes[..n]);
        let word = u64::from_le_bytes(w);
        return match attlen {
            1 => Datum::from_i8(word as u8 as i8),
            2 => Datum::from_i16(word as u16 as i16),
            4 => Datum::from_i32(word as u32 as i32),
            _ => Datum::from_i64(word as i64),
        };
    }
    let buf: Vec<u8> = if attlen > 0 {
        let mut v = alloc::vec![0u8; attlen as usize];
        let n = bytes.len().min(attlen as usize);
        v[..n].copy_from_slice(&bytes[..n]);
        v
    } else if attlen == -1 {
        if bytes.is_empty() { alloc::vec![0u8] } else { bytes.to_vec() }
    } else {
        let mut v = bytes.to_vec();
        v.push(0);
        v
    };
    let mut img = vec_with_capacity_in(mcx, buf.len()).expect("mcx alloc");
    mcx::vec_append_bytes(&mut img, &buf).expect("mcx append");
    let d = Datum::from_usize(img.as_ptr() as usize);
    core::mem::forget(img);
    d
}

/// build the Rust descriptor from the spec (same staging rules as the C
/// oracle's pg_ta_build_desc; the crate under test does the populating)
pub fn build_rust_desc<'m>(mcx: Mcx<'m>, s: &DescSpec) -> TupleDescData<'m> {
    let natts = s.natts();
    let mut attrs: Vec<FormData_pg_attribute> = Vec::with_capacity(natts);
    for (i, a) in s.atts.iter().enumerate() {
        let m = &MENU[a.menu as usize];
        let mut att = FormData_pg_attribute::default();
        att.attname.namestrcpy(&att_name(a));
        att.attrelid = 0;
        att.attnum = (i + 1) as i16;
        att.atttypid = if a.aflags & 0x01 != 0 {
            0
        } else {
            m.typid + if a.xflags & 0x08 != 0 { 100000 } else { 0 }
        };
        att.attlen = m.attlen;
        att.attbyval = m.attbyval;
        att.attalign = m.attalign;
        att.attstorage = m.attstorage;
        att.attcompression = if a.xflags & 0x20 != 0 { b'l' as i8 } else { 0 };
        att.attcollation = if a.aflags & 0x80 != 0 { 999 } else { m.attcollation };
        att.atttypmod = if a.aflags & 0x40 != 0 { 77 } else { -1 };
        att.attndims = if a.aflags & 0x20 != 0 { 1 } else { 0 };
        att.attisdropped = a.aflags & 0x01 != 0;
        att.attnotnull = a.aflags & 0x02 != 0;
        att.atthasmissing = a.aflags & 0x04 != 0;
        att.attislocal = a.aflags & 0x10 == 0;
        att.attinhcount = if a.xflags & 0x10 != 0 { 1 } else { 0 };
        att.atthasdef = a.xflags & 0x01 != 0;
        att.attidentity = if a.xflags & 0x02 != 0 { b'a' as i8 } else { 0 };
        att.attgenerated = if a.xflags & 0x04 != 0 { b's' as i8 } else { 0 };
        attrs.push(att);
    }
    let mut desc = tupdesc::CreateTupleDesc(mcx, &attrs).expect("CreateTupleDesc");
    desc.tdtypeid = if s.dflags & 0x10 != 0 { 424242 } else { types_core::catalog::RECORDOID };
    desc.tdtypmod = s.tdtypmod;
    for (i, a) in s.atts.iter().enumerate() {
        if a.aflags & 0x02 != 0 {
            desc.compact_attrs[i].attnullability = if a.aflags & 0x08 != 0 {
                ATTNULLABLE_INVALID
            } else {
                ATTNULLABLE_VALID
            };
        }
    }
    if s.has_constr() {
        let mut defval: PgVec<'m, AttrDefault<'m>> = PgVec::new_in(mcx);
        for (adnum, b) in &s.defvals {
            defval.push(AttrDefault {
                adnum: *adnum as i16,
                adbin: Some(
                    PgString::from_str_in(core::str::from_utf8(b).expect("non-ASCII adbin"), mcx).unwrap(),
                ),
            });
        }
        let mut check: PgVec<'m, ConstrCheck<'m>> = PgVec::new_in(mcx);
        for (cflags, nm, b) in &s.checks {
            check.push(ConstrCheck {
                ccname: Some(
                    PgString::from_str_in(core::str::from_utf8(nm).expect("non-ASCII ccname"), mcx).unwrap(),
                ),
                ccbin: Some(
                    PgString::from_str_in(core::str::from_utf8(b).expect("non-ASCII adbin"), mcx).unwrap(),
                ),
                ccenforced: cflags & 1 != 0,
                ccvalid: cflags & 2 != 0,
                ccnoinherit: cflags & 4 != 0,
            });
        }
        let any_missing = s.atts.iter().any(|a| a.aflags & 0x04 != 0);
        let mut missing: PgVec<'m, AttrMissing> =
            vec_with_capacity_in(mcx, if any_missing { natts } else { 0 }).unwrap();
        if any_missing {
            for a in &s.atts {
                if a.aflags & 0x04 != 0 {
                    let m = &MENU[a.menu as usize];
                    missing.push(AttrMissing {
                        am_present: true,
                        am_value: stage_datum(mcx, m.attlen, m.attbyval, &a.missing),
                    });
                } else {
                    missing.push(AttrMissing { am_present: false, am_value: Datum::null() });
                }
            }
        }
        let num_defval = defval.len() as u16;
        let num_check = check.len() as u16;
        desc.constr = Some(
            alloc_in(
                mcx,
                TupleConstr {
                    defval,
                    check,
                    missing,
                    num_defval,
                    num_check,
                    has_not_null: s.dflags & 0x02 != 0,
                    has_generated_stored: s.dflags & 0x04 != 0,
                    has_generated_virtual: s.dflags & 0x08 != 0,
                },
            )
            .expect("alloc constr"),
        );
    }
    desc
}

/// decode the VALUES list for the first nvals attributes (dropped -> null),
/// returning the per-att image bytes (None = SQL null)
pub fn decode_values(cur: &mut Cursor<'_>, s: &DescSpec, nvals: usize) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::with_capacity(nvals);
    for a in s.atts.iter().take(nvals) {
        let nul = a.aflags & 0x01 != 0 || cur.u8() & 0x03 == 0; // 25% null
        if nul {
            DIV_FLAGS[0].fetch_add(1, Relaxed);
            out.push(None);
        } else {
            out.push(Some(gen_value(cur, a.menu as usize)));
        }
    }
    out
}

pub fn values_wire(vals: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut w = Vec::new();
    for v in vals {
        match v {
            None => w.push(1),
            Some(b) => {
                w.push(0);
                w.extend_from_slice(&(b.len() as u16).to_le_bytes());
                w.extend_from_slice(b);
            }
        }
    }
    w
}

pub fn stage_values<'m>(
    mcx: Mcx<'m>,
    s: &DescSpec,
    vals: &[Option<Vec<u8>>],
) -> (Vec<Datum>, Vec<bool>) {
    let mut values = Vec::with_capacity(vals.len());
    let mut isnull = Vec::with_capacity(vals.len());
    for (a, v) in s.atts.iter().zip(vals) {
        match v {
            None => {
                values.push(Datum::null());
                isnull.push(true);
            }
            Some(b) => {
                let m = &MENU[a.menu as usize];
                values.push(stage_datum(mcx, m.attlen, m.attbyval, b));
                isnull.push(false);
            }
        }
    }
    (values, isnull)
}


/// RATIFIED 2026-08-01 (Michael): platform non-surface — width-1 byval Datum upper 56
/// bits. PG's fetch_att for attlen==1 is `*((char *) T)` (tupmacs.h) and C
/// `char` signedness is platform-defined — SIGNED on macOS-aarch64 /
/// x86_64-Linux (0xFF sign-extends), UNSIGNED on Linux-aarch64 (0xFF
/// zero-extends) — so the vendored C itself produces different upper Datum
/// bits per platform; within C every consumer truncates via DatumGetChar or
/// a 1-byte store. Widths 2/4/8 use int16/int32/int64 (signed everywhere)
/// and are NOT masked.
#[inline]
pub fn byval_word(d: Datum, attlen: i16) -> u64 {
    if attlen == 1 {
        d.as_u64() & 0xff
    } else {
        d.as_u64()
    }
}

