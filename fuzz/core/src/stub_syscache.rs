//! stub:syscache-row — shared CONSTRUCTED-STATE builder: catalog rows
//! supplied as fuzz input, constructed identically on the Rust side (a
//! thread-local row store answering the `syscache_seams` probes the shipped
//! `lsyscache` layer reads) and on the C-oracle side
//! (csrc/pg_stub_syscache.c: the same store loaded from the same wire, a
//! SearchSysCacheN/GetSysCacheOidN interception layer, and VERBATIM 18.3
//! lsyscache consumer bodies compiled over it).
//!
//! WHY: PostgreSQL's `lsyscache` layer is hundreds of tiny helpers
//! (`get_atttype`, `get_typlenbyval`, `get_opclass_family`,
//! `get_func_rettype`, ...) that each read exactly ONE catalog row —
//! transitively OPEN (syscache → catcache → relation → buffers → disk), but
//! pure over (arguments, that row) once the row is input.  Ratified as the
//! ninth stub facility 2026-08-01 (docs/design/phase2-plan.md §8 Q7); it
//! ABSORBS the two hand-rolled pinned menus built the same day
//! (proofs/p1-waveb-bloom's amproc menu, proofs/p1-waveb-mm's
//! amop/amproc/operator menus + `slow_path_ok()` fallback) — the pg_qsort
//! consolidation rule.
//!
//! COVERED CACHES (extensible BY TABLE, never per lane): pg_amop
//! (AMOPSTRATEGY + AMOPOPID keys), pg_amproc (AMPROCNUM), pg_operator
//! (OPEROID), pg_opclass (CLAOID), pg_type (TYPEOID), pg_attribute
//! (ATTNUM), pg_proc (PROCOID).  Row fields carried are exactly the wire
//! fields below; anything else (row oids of amop/amproc, non-att names,
//! pg_proc cost columns, ...) is NOT covered — zero on the C side, absent
//! from the Rust shapes.
//!
//! CLAMPS (part of the compared-input contract; applied before either side
//! builds):
//!   - rows per cache: `% (MAX_ROWS_PER_CACHE+1)` = 0..=16
//!   - FIRST matching row wins on BOTH sides (duplicate keys are legal
//!     input; both sides scan in wire order)
//!   - attname: 64 raw bytes, byte 63 forced 0 (NameData shape)
//!   - all other fields: raw LE integers, NOT normalized — catalog
//!     invariants (an amproc row's amproc naming a real proc row, ...) are
//!     things a consumer may rely on; the builder never fabricates them
//!
//! UNREACHABLE-STATE HAZARD (band-2, `harness-audit:required`): a supplied
//! row can be INCONSISTENT with the catalog it was not supplied alongside —
//! real PostgreSQL only reaches catalog-consistent states, so a verdict
//! over invented rows can exercise states no real execution reaches.
//! Mitigations (both committed): the derivation menu and seed rows are
//! HARVESTED from a live catalog (stub_syscache_harvest.rs, dev server
//! initdb'd from the 18.3 .dat pins), and the constructor is
//! injection-swept (fuzz/STUBS.md table).
//!
//! SHARED-BINARY COLLISION BEHAVIOR (lifted from the two absorbed lanes):
//! `install_seams()` claims the facility's `syscache_seams` slots
//! first-install-wins and returns whether the facility owns ALL of them.
//! In the shared `cargo test` binary another target's oracle may own some
//! (arrayfuncs/rowtypes/tupaccess install their own pins); consumers must
//! then downgrade like the absorbed lanes did (bloom: Lazy→Pinned mode;
//! mm: cache pre-seeding) — `authoritative()` is the probe.  Store-direct
//! probes (`rows_*` below) and the construction plane never depend on seam
//! ownership, so the facility's own controls are collision-immune.  In a
//! one-target fuzz binary the facility always owns its seams.
//!
//! WIRE (== the C decoder in pg_stub_syscache.c; keep in lockstep; all LE):
//!   [u8 n_amop]      n × { u32 fam, u32 left, u32 right, i16 strat,
//!                          u8 purpose, u32 opr, u32 method, u32 sortfam }
//!   [u8 n_amproc]    n × { u32 fam, u32 left, u32 right, i16 procnum,
//!                          u32 proc }
//!   [u8 n_operator]  n × { u32 oid, u32 nsp, u32 left, u32 right,
//!                          u32 result, u32 com, u32 negate, u32 code,
//!                          u32 rest, u32 join, u8 canmerge, u8 canhash }
//!   [u8 n_opclass]   n × { u32 oid, u32 method, u32 family, u32 intype,
//!                          u32 keytype }
//!   [u8 n_type]      n × { u32 oid, i16 typlen, u8 byval, u8 align,
//!                          u8 storage, u32 collation, u32 input,
//!                          u32 output, u32 receive, u32 send, u32 modin,
//!                          u32 modout, u32 elem, u8 delim, u8 isdefined }
//!   [u8 n_attribute] n × { u32 relid, i16 attnum, name[64], u32 typid,
//!                          i32 typmod, u32 collation, u8 generated }
//!   [u8 n_proc]      n × { u32 oid, u32 nsp, u32 rettype, u32 variadic,
//!                          u32 support, u32 lang, i16 nargs, u8 kind,
//!                          u8 volatile, u8 parallel, u8 retset,
//!                          u8 isstrict, u8 leakproof, u8 secdef,
//!                          u8 configisnull }

extern crate alloc;

use alloc::vec::Vec;
use core::cell::RefCell;

use syscache_seams::{
    PgAmopShape, PgAttributeLsShape, PgOpclassShape, PgOperatorShape, PgProcShape, PgTypeIoShape,
};
use types_core::Oid;
use types_tuple::{NameData, PgTypeShape};

use crate::stub_tupdesc::Cursor;

/// Per-cache row-count ceiling (identical both sides; documented clamp).
pub const MAX_ROWS_PER_CACHE: usize = 16;

/// NameData from raw bytes (≤63 used; NUL-padded). Const so the harvest
/// table can embed real attnames.
pub const fn attname_from_bytes(b: &[u8]) -> NameData {
    let mut data = [0u8; 64];
    let mut i = 0;
    while i < b.len() && i < 63 {
        data[i] = b[i];
        i += 1;
    }
    NameData { data }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AmopRow {
    pub amopfamily: Oid,
    pub amoplefttype: Oid,
    pub amoprighttype: Oid,
    pub amopstrategy: i16,
    pub amoppurpose: u8, // b's' search / b'o' ordering
    pub amopopr: Oid,
    pub amopmethod: Oid,
    pub amopsortfamily: Oid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AmprocRow {
    pub amprocfamily: Oid,
    pub amproclefttype: Oid,
    pub amprocrighttype: Oid,
    pub amprocnum: i16,
    pub amproc: Oid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperatorRow {
    pub oid: Oid,
    pub oprnamespace: Oid,
    pub oprleft: Oid,
    pub oprright: Oid,
    pub oprresult: Oid,
    pub oprcom: Oid,
    pub oprnegate: Oid,
    pub oprcode: Oid,
    pub oprrest: Oid,
    pub oprjoin: Oid,
    pub oprcanmerge: bool,
    pub oprcanhash: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpclassRow {
    pub oid: Oid,
    pub opcmethod: Oid,
    pub opcfamily: Oid,
    pub opcintype: Oid,
    pub opckeytype: Oid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypeRow {
    pub oid: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub typstorage: u8,
    pub typcollation: Oid,
    pub typinput: Oid,
    pub typoutput: Oid,
    pub typreceive: Oid,
    pub typsend: Oid,
    pub typmodin: Oid,
    pub typmodout: Oid,
    pub typelem: Oid,
    pub typdelim: u8,
    pub typisdefined: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttributeRow {
    pub attrelid: Oid,
    pub attnum: i16,
    pub attname: NameData,
    pub atttypid: Oid,
    pub atttypmod: i32,
    pub attcollation: Oid,
    pub attgenerated: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcRow {
    pub oid: Oid,
    pub pronamespace: Oid,
    pub prorettype: Oid,
    pub provariadic: Oid,
    pub prosupport: Oid,
    pub prolang: Oid,
    pub pronargs: i16,
    pub prokind: u8,
    pub provolatile: u8,
    pub proparallel: u8,
    pub proretset: bool,
    pub proisstrict: bool,
    pub proleakproof: bool,
    pub prosecdef: bool,
    pub proconfig_isnull: bool,
}

/// The supplied catalog — the compared input. Populate programmatically
/// (migrated lanes push their harvested rows) or via `decode_rows` (fuzz
/// derivation, menu-anchored on the harvest); then `set_rows` loads BOTH
/// sides.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SysCacheRows {
    pub amop: Vec<AmopRow>,
    pub amproc: Vec<AmprocRow>,
    pub operator: Vec<OperatorRow>,
    pub opclass: Vec<OpclassRow>,
    pub typ: Vec<TypeRow>,
    pub attribute: Vec<AttributeRow>,
    pub proc: Vec<ProcRow>,
}

// ---------------------------------------------------------------------
// fuzz derivation: menu-anchored on HARVESTED rows + one field mutation
// per row, so the explored domain stays anchored to catalog-reachable
// shapes while the differential still sees drifted fields.
// ---------------------------------------------------------------------

fn le_u32(cur: &mut Cursor<'_>) -> u32 {
    u32::from_le_bytes([cur.u8(), cur.u8(), cur.u8(), cur.u8()])
}

macro_rules! decode_table {
    ($cur:ident, $menu:expr, $mutate:expr) => {{
        let n = $cur.u8() as usize % (MAX_ROWS_PER_CACHE + 1);
        let mut rows = Vec::with_capacity(n);
        for _ in 0..n {
            let idx = $cur.u8() as usize % $menu.len();
            let mut row = $menu[idx];
            let sel = $cur.u8();
            let val = le_u32($cur);
            $mutate(&mut row, sel, val);
            rows.push(row);
        }
        rows
    }};
}

/// Decode a SysCacheRows from fuzz bytes (missing bytes read as 0, Cursor
/// contract — identical both sides because only the WIRE reaches C).
/// Each row = a harvested menu row (index clamped `% menu len`) with at
/// most ONE mutated field (`sel % (nfields+1)`; 0 = pristine).
pub fn decode_rows(cur: &mut Cursor<'_>) -> SysCacheRows {
    use crate::stub_syscache_harvest as h;
    SysCacheRows {
        amop: decode_table!(cur, h::HARVEST_AMOP, |r: &mut AmopRow, sel: u8, v: u32| {
            match sel % 9 {
                0 => {}
                1 => r.amopfamily = v,
                2 => r.amoplefttype = v,
                3 => r.amoprighttype = v,
                4 => r.amopstrategy = v as i16,
                5 => r.amoppurpose = if v as u8 & 1 == 0 { b's' } else { b'o' },
                6 => r.amopopr = v,
                7 => r.amopmethod = v,
                _ => r.amopsortfamily = v,
            }
        }),
        amproc: decode_table!(cur, h::HARVEST_AMPROC, |r: &mut AmprocRow, sel: u8, v: u32| {
            match sel % 6 {
                0 => {}
                1 => r.amprocfamily = v,
                2 => r.amproclefttype = v,
                3 => r.amprocrighttype = v,
                4 => r.amprocnum = v as i16,
                _ => r.amproc = v,
            }
        }),
        operator: decode_table!(cur, h::HARVEST_OPERATOR, |r: &mut OperatorRow, sel: u8, v: u32| {
            match sel % 13 {
                0 => {}
                1 => r.oid = v,
                2 => r.oprnamespace = v,
                3 => r.oprleft = v,
                4 => r.oprright = v,
                5 => r.oprresult = v,
                6 => r.oprcom = v,
                7 => r.oprnegate = v,
                8 => r.oprcode = v,
                9 => r.oprrest = v,
                10 => r.oprjoin = v,
                11 => r.oprcanmerge = v & 1 != 0,
                _ => r.oprcanhash = v & 1 != 0,
            }
        }),
        opclass: decode_table!(cur, h::HARVEST_OPCLASS, |r: &mut OpclassRow, sel: u8, v: u32| {
            match sel % 6 {
                0 => {}
                1 => r.oid = v,
                2 => r.opcmethod = v,
                3 => r.opcfamily = v,
                4 => r.opcintype = v,
                _ => r.opckeytype = v,
            }
        }),
        typ: decode_table!(cur, h::HARVEST_TYPE, |r: &mut TypeRow, sel: u8, v: u32| {
            match sel % 16 {
                0 => {}
                1 => r.oid = v,
                2 => r.typlen = v as i16,
                3 => r.typbyval = v & 1 != 0,
                4 => r.typalign = v as u8,
                5 => r.typstorage = v as u8,
                6 => r.typcollation = v,
                7 => r.typinput = v,
                8 => r.typoutput = v,
                9 => r.typreceive = v,
                10 => r.typsend = v,
                11 => r.typmodin = v,
                12 => r.typmodout = v,
                13 => r.typelem = v,
                14 => r.typdelim = v as u8,
                _ => r.typisdefined = v & 1 != 0,
            }
        }),
        attribute: decode_table!(cur, h::HARVEST_ATTRIBUTE, |r: &mut AttributeRow,
                                                             sel: u8,
                                                             v: u32| {
            match sel % 7 {
                0 => {}
                1 => r.attrelid = v,
                2 => r.attnum = v as i16,
                3 => r.attname.data[0] = (v as u8) | 1, // stays non-NUL
                4 => r.atttypid = v,
                5 => r.atttypmod = v as i32,
                _ => r.attcollation = v,
            }
        }),
        proc: decode_table!(cur, h::HARVEST_PROC, |r: &mut ProcRow, sel: u8, v: u32| {
            match sel % 16 {
                0 => {}
                1 => r.oid = v,
                2 => r.pronamespace = v,
                3 => r.prorettype = v,
                4 => r.provariadic = v,
                5 => r.prosupport = v,
                6 => r.prolang = v,
                7 => r.pronargs = v as i16,
                8 => r.prokind = v as u8,
                9 => r.provolatile = v as u8,
                10 => r.proparallel = v as u8,
                11 => r.proretset = v & 1 != 0,
                12 => r.proisstrict = v & 1 != 0,
                13 => r.proleakproof = v & 1 != 0,
                14 => r.prosecdef = v & 1 != 0,
                _ => r.proconfig_isnull = v & 1 != 0,
            }
        }),
    }
}

// ---------------------------------------------------------------------
// wire encoding (-> the C decoder; see WIRE above)
// ---------------------------------------------------------------------

fn clamp_check(rows: &SysCacheRows) {
    assert!(
        rows.amop.len() <= MAX_ROWS_PER_CACHE
            && rows.amproc.len() <= MAX_ROWS_PER_CACHE
            && rows.operator.len() <= MAX_ROWS_PER_CACHE
            && rows.opclass.len() <= MAX_ROWS_PER_CACHE
            && rows.typ.len() <= MAX_ROWS_PER_CACHE
            && rows.attribute.len() <= MAX_ROWS_PER_CACHE
            && rows.proc.len() <= MAX_ROWS_PER_CACHE,
        "stub:syscache-row clamp violation (>16 rows in a cache): harness bug"
    );
}

/// Wire-encode the rows for the C shim.
pub fn rows_wire(rows: &SysCacheRows) -> Vec<u8> {
    clamp_check(rows);
    let mut w = Vec::with_capacity(512);
    w.push(rows.amop.len() as u8);
    for r in &rows.amop {
        w.extend_from_slice(&r.amopfamily.to_le_bytes());
        w.extend_from_slice(&r.amoplefttype.to_le_bytes());
        w.extend_from_slice(&r.amoprighttype.to_le_bytes());
        w.extend_from_slice(&r.amopstrategy.to_le_bytes());
        w.push(r.amoppurpose);
        w.extend_from_slice(&r.amopopr.to_le_bytes());
        w.extend_from_slice(&r.amopmethod.to_le_bytes());
        w.extend_from_slice(&r.amopsortfamily.to_le_bytes());
    }
    w.push(rows.amproc.len() as u8);
    for r in &rows.amproc {
        w.extend_from_slice(&r.amprocfamily.to_le_bytes());
        w.extend_from_slice(&r.amproclefttype.to_le_bytes());
        w.extend_from_slice(&r.amprocrighttype.to_le_bytes());
        w.extend_from_slice(&r.amprocnum.to_le_bytes());
        w.extend_from_slice(&r.amproc.to_le_bytes());
    }
    w.push(rows.operator.len() as u8);
    for r in &rows.operator {
        w.extend_from_slice(&r.oid.to_le_bytes());
        w.extend_from_slice(&r.oprnamespace.to_le_bytes());
        w.extend_from_slice(&r.oprleft.to_le_bytes());
        w.extend_from_slice(&r.oprright.to_le_bytes());
        w.extend_from_slice(&r.oprresult.to_le_bytes());
        w.extend_from_slice(&r.oprcom.to_le_bytes());
        w.extend_from_slice(&r.oprnegate.to_le_bytes());
        w.extend_from_slice(&r.oprcode.to_le_bytes());
        w.extend_from_slice(&r.oprrest.to_le_bytes());
        w.extend_from_slice(&r.oprjoin.to_le_bytes());
        w.push(u8::from(r.oprcanmerge));
        w.push(u8::from(r.oprcanhash));
    }
    w.push(rows.opclass.len() as u8);
    for r in &rows.opclass {
        w.extend_from_slice(&r.oid.to_le_bytes());
        w.extend_from_slice(&r.opcmethod.to_le_bytes());
        w.extend_from_slice(&r.opcfamily.to_le_bytes());
        w.extend_from_slice(&r.opcintype.to_le_bytes());
        w.extend_from_slice(&r.opckeytype.to_le_bytes());
    }
    w.push(rows.typ.len() as u8);
    for r in &rows.typ {
        w.extend_from_slice(&r.oid.to_le_bytes());
        w.extend_from_slice(&r.typlen.to_le_bytes());
        w.push(u8::from(r.typbyval));
        w.push(r.typalign);
        w.push(r.typstorage);
        w.extend_from_slice(&r.typcollation.to_le_bytes());
        w.extend_from_slice(&r.typinput.to_le_bytes());
        w.extend_from_slice(&r.typoutput.to_le_bytes());
        w.extend_from_slice(&r.typreceive.to_le_bytes());
        w.extend_from_slice(&r.typsend.to_le_bytes());
        w.extend_from_slice(&r.typmodin.to_le_bytes());
        w.extend_from_slice(&r.typmodout.to_le_bytes());
        w.extend_from_slice(&r.typelem.to_le_bytes());
        w.push(r.typdelim);
        w.push(u8::from(r.typisdefined));
    }
    w.push(rows.attribute.len() as u8);
    for r in &rows.attribute {
        w.extend_from_slice(&r.attrelid.to_le_bytes());
        w.extend_from_slice(&r.attnum.to_le_bytes());
        let mut name = r.attname.data;
        name[63] = 0; // NameData clamp
        w.extend_from_slice(&name);
        w.extend_from_slice(&r.atttypid.to_le_bytes());
        w.extend_from_slice(&r.atttypmod.to_le_bytes());
        w.extend_from_slice(&r.attcollation.to_le_bytes());
        w.push(r.attgenerated);
    }
    w.push(rows.proc.len() as u8);
    for r in &rows.proc {
        w.extend_from_slice(&r.oid.to_le_bytes());
        w.extend_from_slice(&r.pronamespace.to_le_bytes());
        w.extend_from_slice(&r.prorettype.to_le_bytes());
        w.extend_from_slice(&r.provariadic.to_le_bytes());
        w.extend_from_slice(&r.prosupport.to_le_bytes());
        w.extend_from_slice(&r.prolang.to_le_bytes());
        w.extend_from_slice(&r.pronargs.to_le_bytes());
        w.push(r.prokind);
        w.push(r.provolatile);
        w.push(r.proparallel);
        w.push(u8::from(r.proretset));
        w.push(u8::from(r.proisstrict));
        w.push(u8::from(r.proleakproof));
        w.push(u8::from(r.prosecdef));
        w.push(u8::from(r.proconfig_isnull));
    }
    w
}

// ---------------------------------------------------------------------
// the Rust-side store + both-sides loading
// ---------------------------------------------------------------------

thread_local! {
    static STORE: RefCell<SysCacheRows> = RefCell::new(SysCacheRows::default());
}

extern "C" {
    fn pg_stub_syscache_load(wire: *const u8, wirelen: core::ffi::c_int) -> core::ffi::c_int;
    fn pg_stub_syscache_plane(
        out: *mut u8,
        outcap: core::ffi::c_int,
        outlen: *mut core::ffi::c_int,
    ) -> core::ffi::c_int;
    fn pg_stub_syscache_get_opfamily_proc(
        opfamily: u32,
        lefttype: u32,
        righttype: u32,
        procnum: i16,
    ) -> u32;
    fn pg_stub_syscache_get_opfamily_member(
        opfamily: u32,
        lefttype: u32,
        righttype: u32,
        strategy: i16,
    ) -> u32;
    fn pg_stub_syscache_get_opcode(opno: u32) -> u32;
    fn pg_stub_syscache_get_atttype(relid: u32, attnum: i16) -> u32;
    fn pg_stub_syscache_try_get_opclass_family(
        opclass: u32,
        out: *mut u32,
    ) -> core::ffi::c_int;
    fn pg_stub_syscache_try_get_typlenbyval(
        typid: u32,
        typlen: *mut i16,
        typbyval: *mut u8,
    ) -> core::ffi::c_int;
    fn pg_stub_syscache_try_get_func_rettype(funcid: u32, out: *mut u32) -> core::ffi::c_int;
}

/// Load the supplied rows on BOTH sides (Rust thread-local store + the C
/// shim's thread-local store, through ONE wire derivation). A supplied row
/// set is part of the compared input — never let one side default.
pub fn set_rows(rows: &SysCacheRows) {
    clamp_check(rows);
    let wire = rows_wire(rows);
    // SAFETY: buffer lives for the call.
    let st = unsafe { pg_stub_syscache_load(wire.as_ptr(), wire.len() as core::ffi::c_int) };
    assert!(st == 0, "C syscache store loader internal failure {st}");
    STORE.with(|s| *s.borrow_mut() = rows.clone());
}

fn with_store<R>(f: impl FnOnce(&SysCacheRows) -> R) -> R {
    STORE.with(|s| f(&s.borrow()))
}

// ---------------------------------------------------------------------
// store-direct probes (what the seam impls delegate to; also the
// collision-immune plane the facility's own controls use). FIRST matching
// row wins — the C shim scans identically.
// ---------------------------------------------------------------------

pub fn rows_amproc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: i16) -> Oid {
    with_store(|s| {
        s.amproc
            .iter()
            .find(|r| {
                r.amprocfamily == opfamily
                    && r.amproclefttype == lefttype
                    && r.amprocrighttype == righttype
                    && r.amprocnum == procnum
            })
            .map(|r| r.amproc)
            .unwrap_or(0)
    })
}

pub fn rows_amop_by_strategy(opfamily: Oid, lefttype: Oid, righttype: Oid, strategy: i16) -> Oid {
    with_store(|s| {
        s.amop
            .iter()
            .find(|r| {
                r.amopfamily == opfamily
                    && r.amoplefttype == lefttype
                    && r.amoprighttype == righttype
                    && r.amopstrategy == strategy
            })
            .map(|r| r.amopopr)
            .unwrap_or(0)
    })
}

pub fn rows_amop_by_operator(opno: Oid, purpose: u8, opfamily: Oid) -> Option<PgAmopShape> {
    with_store(|s| {
        s.amop
            .iter()
            .find(|r| r.amopopr == opno && r.amoppurpose == purpose && r.amopfamily == opfamily)
            .map(|r| PgAmopShape {
                amopstrategy: r.amopstrategy,
                amopsortfamily: r.amopsortfamily,
                amoplefttype: r.amoplefttype,
                amoprighttype: r.amoprighttype,
            })
    })
}

pub fn rows_operator_shape(opno: Oid) -> Option<PgOperatorShape> {
    with_store(|s| {
        s.operator.iter().find(|r| r.oid == opno).map(|r| PgOperatorShape {
            oprnamespace: r.oprnamespace,
            oprleft: r.oprleft,
            oprright: r.oprright,
            oprresult: r.oprresult,
            oprcom: r.oprcom,
            oprnegate: r.oprnegate,
            oprcode: r.oprcode,
            oprrest: r.oprrest,
            oprjoin: r.oprjoin,
            oprcanmerge: r.oprcanmerge,
            oprcanhash: r.oprcanhash,
        })
    })
}

pub fn rows_opclass_shape(opclass: Oid) -> Option<PgOpclassShape> {
    with_store(|s| {
        s.opclass.iter().find(|r| r.oid == opclass).map(|r| PgOpclassShape {
            opcmethod: r.opcmethod,
            opcfamily: r.opcfamily,
            opcintype: r.opcintype,
            opckeytype: r.opckeytype,
        })
    })
}

pub fn rows_type_shape(typid: Oid) -> Option<PgTypeShape> {
    with_store(|s| {
        s.typ.iter().find(|r| r.oid == typid).map(|r| PgTypeShape {
            typlen: r.typlen,
            typbyval: r.typbyval,
            typalign: r.typalign as i8,
            typstorage: r.typstorage as i8,
            typcollation: r.typcollation,
        })
    })
}

pub fn rows_type_io_shape(typid: Oid) -> Option<PgTypeIoShape> {
    with_store(|s| {
        s.typ.iter().find(|r| r.oid == typid).map(|r| PgTypeIoShape {
            oid: r.oid,
            typinput: r.typinput,
            typoutput: r.typoutput,
            typreceive: r.typreceive,
            typsend: r.typsend,
            typmodin: r.typmodin,
            typmodout: r.typmodout,
            typelem: r.typelem,
            typlen: r.typlen,
            typbyval: r.typbyval,
            typalign: r.typalign as i8,
            typdelim: r.typdelim as i8,
            typisdefined: r.typisdefined,
        })
    })
}

pub fn rows_attribute_shape(relid: Oid, attnum: i16) -> Option<PgAttributeLsShape> {
    with_store(|s| {
        s.attribute
            .iter()
            .find(|r| r.attrelid == relid && r.attnum == attnum)
            .map(|r| PgAttributeLsShape {
                attname: r.attname,
                atttypid: r.atttypid,
                atttypmod: r.atttypmod,
                attcollation: r.attcollation,
                attgenerated: r.attgenerated as i8,
            })
    })
}

pub fn rows_proc_shape(funcid: Oid) -> Option<PgProcShape> {
    with_store(|s| {
        s.proc.iter().find(|r| r.oid == funcid).map(|r| PgProcShape {
            pronamespace: r.pronamespace,
            prorettype: r.prorettype,
            provariadic: r.provariadic,
            prosupport: r.prosupport,
            prolang: r.prolang,
            pronargs: r.pronargs,
            prokind: r.prokind as i8,
            provolatile: r.provolatile as i8,
            proparallel: r.proparallel as i8,
            proretset: r.proretset,
            proisstrict: r.proisstrict,
            proleakproof: r.proleakproof,
            prosecdef: r.prosecdef,
            proconfig_isnull: r.proconfig_isnull,
        })
    })
}

// ---------------------------------------------------------------------
// seam installation (the shipped lsyscache layer then answers from the
// store) — first-install-wins with the shared-binary collision fallback
// both absorbed lanes solved independently.
// ---------------------------------------------------------------------

/// Install the facility's `syscache_seams` impls (idempotent). Returns
/// `authoritative()`: whether the facility owns ALL of its seams. False in
/// a shared test binary where a foreign oracle claimed one first —
/// consumers must then downgrade (bloom Lazy→Pinned pattern) or pre-seed
/// their caches (mm pattern); the store-direct `rows_*` probes and the
/// construction plane keep working regardless.
pub fn install_seams() -> bool {
    static AUTH: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *AUTH.get_or_init(|| {
        let mut all = true;
        macro_rules! claim {
            ($seam:path, $impl_:expr) => {{
                use $seam as seam;
                all &= std::panic::catch_unwind(|| seam::set($impl_)).is_ok();
            }};
        }
        claim!(syscache_seams::lookup_pg_amproc, |f, l, r, n| Ok(rows_amproc(f, l, r, n)));
        claim!(syscache_seams::lookup_pg_amop_by_strategy, |f, l, r, s| {
            Ok(rows_amop_by_strategy(f, l, r, s))
        });
        claim!(syscache_seams::lookup_pg_amop_by_operator, |o, p, f| {
            Ok(rows_amop_by_operator(o, p, f))
        });
        claim!(syscache_seams::lookup_pg_operator_shape, |o| Ok(rows_operator_shape(o)));
        claim!(syscache_seams::lookup_pg_opclass_shape, |o| Ok(rows_opclass_shape(o)));
        claim!(syscache_seams::lookup_pg_type_shape, |t| Ok(rows_type_shape(t)));
        claim!(syscache_seams::pg_type_io_shape, |t| Ok(rows_type_io_shape(t)));
        claim!(syscache_seams::lookup_pg_attribute_shape, |r, a| {
            Ok(rows_attribute_shape(r, a))
        });
        claim!(syscache_seams::lookup_pg_proc_shape, |f| Ok(rows_proc_shape(f)));
        all
    })
}

/// Whether the facility owns ALL of its seams (== the memoized
/// `install_seams` result; false until `install_seams` ran).
pub fn authoritative() -> bool {
    install_seams()
}

// ---------------------------------------------------------------------
// construction plane (SECTION-Y): both sides serialize their CONSTRUCTED
// stores; a builder asymmetry is a caught divergence, never silent
// agreement.
// ---------------------------------------------------------------------

/// Field-plane serializer over the Rust store — MUST stay in lockstep with
/// the C SECTION-Y writer (pg_stub_syscache.c). Serializes the STORE, not
/// the wire, so one-side construction drift is visible.
pub fn ser_syscache_plane(w: &mut Vec<u8>) {
    with_store(|s| {
        let wire_of_store = rows_wire(s);
        w.extend_from_slice(&wire_of_store);
    });
}

/// C-side plane (the C shim serializes ITS constructed store).
pub fn c_syscache_plane() -> Vec<u8> {
    let mut out = alloc::vec![0u8; 8192];
    let mut outlen: core::ffi::c_int = 0;
    // SAFETY: buffers live for the call.
    let st = unsafe {
        pg_stub_syscache_plane(out.as_mut_ptr(), out.len() as core::ffi::c_int, &mut outlen)
    };
    assert!(st == 0, "C syscache plane internal failure {st}");
    out.truncate(outlen as usize);
    out
}

/// The dual-construction differential: load both sides from the same rows
/// and panic on any store-plane difference. Consumer targets call this once
/// per exec before computing over the supplied catalog.
pub fn assert_syscache_construction_agrees(rows: &SysCacheRows) {
    set_rows(rows);
    let cplane = c_syscache_plane();
    let mut rplane = Vec::new();
    ser_syscache_plane(&mut rplane);
    assert_eq!(
        rplane, cplane,
        "stub:syscache-row construction divergence (rows = {rows:?})"
    );
}

// ---------------------------------------------------------------------
// safe wrappers over the VERBATIM C consumers (SECTION-V) — the C half of
// the lsyscache differential plane. Status 1 on the try_ variants = the
// miss-path elog fired ("cache lookup failed" class).
// ---------------------------------------------------------------------

pub fn c_get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: i16) -> Oid {
    // SAFETY: pure over the thread-local C store.
    unsafe { pg_stub_syscache_get_opfamily_proc(opfamily, lefttype, righttype, procnum) }
}

pub fn c_get_opfamily_member(opfamily: Oid, lefttype: Oid, righttype: Oid, strategy: i16) -> Oid {
    // SAFETY: pure over the thread-local C store.
    unsafe { pg_stub_syscache_get_opfamily_member(opfamily, lefttype, righttype, strategy) }
}

pub fn c_get_opcode(opno: Oid) -> Oid {
    // SAFETY: pure over the thread-local C store.
    unsafe { pg_stub_syscache_get_opcode(opno) }
}

pub fn c_get_atttype(relid: Oid, attnum: i16) -> Oid {
    // SAFETY: pure over the thread-local C store.
    unsafe { pg_stub_syscache_get_atttype(relid, attnum) }
}

/// Err(()) = the verbatim body's "cache lookup failed" elog fired.
pub fn c_get_opclass_family(opclass: Oid) -> Result<Oid, ()> {
    let mut out = 0u32;
    // SAFETY: out lives for the call.
    match unsafe { pg_stub_syscache_try_get_opclass_family(opclass, &mut out) } {
        0 => Ok(out),
        _ => Err(()),
    }
}

/// Err(()) = the verbatim body's "cache lookup failed" elog fired.
pub fn c_get_typlenbyval(typid: Oid) -> Result<(i16, bool), ()> {
    let mut typlen = 0i16;
    let mut typbyval = 0u8;
    // SAFETY: outs live for the call.
    match unsafe { pg_stub_syscache_try_get_typlenbyval(typid, &mut typlen, &mut typbyval) } {
        0 => Ok((typlen, typbyval != 0)),
        _ => Err(()),
    }
}

/// Err(()) = the verbatim body's "cache lookup failed" elog fired.
pub fn c_get_func_rettype(funcid: Oid) -> Result<Oid, ()> {
    let mut out = 0u32;
    // SAFETY: out lives for the call.
    match unsafe { pg_stub_syscache_try_get_func_rettype(funcid, &mut out) } {
        0 => Ok(out),
        _ => Err(()),
    }
}

/// Load the C store DIRECTLY from arbitrary wire bytes (control tests
/// only: plants one-side-only differences that `set_rows` can never
/// produce).
#[cfg(test)]
pub(crate) fn c_load_raw(wire: &[u8]) -> i32 {
    // SAFETY: buffer lives for the call.
    unsafe { pg_stub_syscache_load(wire.as_ptr(), wire.len() as core::ffi::c_int) as i32 }
}

/// A tiny fixed row set (one row per cache, harvested values) for tests.
pub fn demo_rows() -> SysCacheRows {
    use crate::stub_syscache_harvest as h;
    SysCacheRows {
        amop: alloc::vec![h::HARVEST_AMOP[0]],
        amproc: alloc::vec![h::HARVEST_AMPROC[0]],
        operator: alloc::vec![h::HARVEST_OPERATOR[0]],
        opclass: alloc::vec![h::HARVEST_OPCLASS[0]],
        typ: alloc::vec![h::HARVEST_TYPE[0]],
        attribute: alloc::vec![h::HARVEST_ATTRIBUTE[0]],
        proc: alloc::vec![h::HARVEST_PROC[0]],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stub_syscache_harvest as h;

    /// Baseline agreement: the fixed demo set plus 500 seeded pseudo-random
    /// decode_rows derivations construct identical stores on both sides.
    #[test]
    fn syscache_construction_agrees() {
        let _serial = crate::c_oracle_serial();
        assert_syscache_construction_agrees(&demo_rows());

        // xorshift64* stream — deterministic, no external dep.
        let mut x = 0x9e3779b97f4a7c15u64;
        for _ in 0..500 {
            let mut bytes = alloc::vec![0u8; 512];
            for b in bytes.iter_mut() {
                x ^= x >> 12;
                x ^= x << 25;
                x ^= x >> 27;
                *b = (x.wrapping_mul(0x2545F4914F6CDD1D) >> 56) as u8;
            }
            let mut cur = Cursor { b: &bytes, i: 0 };
            let rows = decode_rows(&mut cur);
            assert_syscache_construction_agrees(&rows);
        }
    }

    /// REACHABILITY WITNESS (band-2 obligation): every harvested REAL
    /// catalog row round-trips through both constructors (chunked to the
    /// 16-row store clamp) AND the VERBATIM 18.3 lsyscache consumers answer
    /// identically to the Rust store probes over it.
    #[test]
    fn syscache_harvest_rows_all_agree() {
        let _serial = crate::c_oracle_serial();

        for chunk in h::HARVEST_AMPROC.chunks(MAX_ROWS_PER_CACHE) {
            let rows = SysCacheRows { amproc: chunk.to_vec(), ..Default::default() };
            assert_syscache_construction_agrees(&rows);
            for r in chunk {
                let rust = rows_amproc(r.amprocfamily, r.amproclefttype, r.amprocrighttype, r.amprocnum);
                let c = c_get_opfamily_proc(r.amprocfamily, r.amproclefttype, r.amprocrighttype, r.amprocnum);
                assert_eq!(rust, c, "get_opfamily_proc parity over {r:?}");
            }
        }
        for chunk in h::HARVEST_AMOP.chunks(MAX_ROWS_PER_CACHE) {
            let rows = SysCacheRows { amop: chunk.to_vec(), ..Default::default() };
            assert_syscache_construction_agrees(&rows);
            for r in chunk {
                let rust = rows_amop_by_strategy(r.amopfamily, r.amoplefttype, r.amoprighttype, r.amopstrategy);
                let c = c_get_opfamily_member(r.amopfamily, r.amoplefttype, r.amoprighttype, r.amopstrategy);
                assert_eq!(rust, c, "get_opfamily_member parity over {r:?}");
            }
        }
        for chunk in h::HARVEST_OPERATOR.chunks(MAX_ROWS_PER_CACHE) {
            let rows = SysCacheRows { operator: chunk.to_vec(), ..Default::default() };
            assert_syscache_construction_agrees(&rows);
            for r in chunk {
                let rust = rows_operator_shape(r.oid).map(|s| s.oprcode).unwrap_or(0);
                assert_eq!(rust, c_get_opcode(r.oid), "get_opcode parity over {r:?}");
            }
        }
        for chunk in h::HARVEST_OPCLASS.chunks(MAX_ROWS_PER_CACHE) {
            let rows = SysCacheRows { opclass: chunk.to_vec(), ..Default::default() };
            assert_syscache_construction_agrees(&rows);
            for r in chunk {
                let rust = rows_opclass_shape(r.oid).map(|s| s.opcfamily);
                assert_eq!(rust, c_get_opclass_family(r.oid).ok(), "get_opclass_family parity over {r:?}");
            }
        }
        for chunk in h::HARVEST_TYPE.chunks(MAX_ROWS_PER_CACHE) {
            let rows = SysCacheRows { typ: chunk.to_vec(), ..Default::default() };
            assert_syscache_construction_agrees(&rows);
            for r in chunk {
                let rust = rows_type_shape(r.oid).map(|s| (s.typlen, s.typbyval));
                assert_eq!(rust, c_get_typlenbyval(r.oid).ok(), "get_typlenbyval parity over {r:?}");
            }
        }
        for chunk in h::HARVEST_ATTRIBUTE.chunks(MAX_ROWS_PER_CACHE) {
            let rows = SysCacheRows { attribute: chunk.to_vec(), ..Default::default() };
            assert_syscache_construction_agrees(&rows);
            for r in chunk {
                let rust = rows_attribute_shape(r.attrelid, r.attnum).map(|s| s.atttypid).unwrap_or(0);
                assert_eq!(rust, c_get_atttype(r.attrelid, r.attnum), "get_atttype parity over {r:?}");
            }
        }
        for chunk in h::HARVEST_PROC.chunks(MAX_ROWS_PER_CACHE) {
            let rows = SysCacheRows { proc: chunk.to_vec(), ..Default::default() };
            assert_syscache_construction_agrees(&rows);
            for r in chunk {
                let rust = rows_proc_shape(r.oid).map(|s| s.prorettype);
                assert_eq!(rust, c_get_func_rettype(r.oid).ok(), "get_func_rettype parity over {r:?}");
            }
        }
    }

    /// Miss parity: absent keys miss identically on both sides, including
    /// the elog-on-miss consumers (C status 1 == Rust None).
    #[test]
    fn syscache_miss_parity() {
        let _serial = crate::c_oracle_serial();
        set_rows(&demo_rows());

        assert_eq!(rows_amproc(9999, 23, 23, 11), 0);
        assert_eq!(c_get_opfamily_proc(9999, 23, 23, 11), 0);
        assert_eq!(rows_amop_by_strategy(9999, 23, 23, 1), 0);
        assert_eq!(c_get_opfamily_member(9999, 23, 23, 1), 0);
        assert_eq!(rows_operator_shape(9999).map(|s| s.oprcode).unwrap_or(0), 0);
        assert_eq!(c_get_opcode(9999), 0);
        assert!(rows_opclass_shape(9999).is_none());
        assert!(c_get_opclass_family(9999).is_err());
        assert!(rows_type_shape(9999).is_none());
        assert!(c_get_typlenbyval(9999).is_err());
        assert_eq!(rows_attribute_shape(9999, 1).map(|s| s.atttypid).unwrap_or(0), 0);
        assert_eq!(c_get_atttype(9999, 1), 0);
        assert!(rows_proc_shape(9999).is_none());
        assert!(c_get_func_rettype(9999).is_err());
    }

    /// FIRST-match-wins is a pinned contract on BOTH sides: two rows with
    /// the same key but different payloads resolve to the first, and the
    /// verbatim C consumer agrees.
    #[test]
    fn syscache_duplicate_key_first_match_pins_order() {
        let _serial = crate::c_oracle_serial();
        let mut rows = SysCacheRows::default();
        let mut a = h::HARVEST_AMPROC[0];
        let mut b = a;
        a.amproc = 1111;
        b.amproc = 2222;
        rows.amproc = alloc::vec![a, b];
        set_rows(&rows);
        let rust = rows_amproc(a.amprocfamily, a.amproclefttype, a.amprocrighttype, a.amprocnum);
        assert_eq!(rust, 1111, "Rust store must resolve FIRST match");
        assert_eq!(
            c_get_opfamily_proc(a.amprocfamily, a.amproclefttype, a.amprocrighttype, a.amprocnum),
            1111,
            "C store must resolve FIRST match"
        );
    }

    /// The derivation clamps are pinned contracts, not silent drift: the
    /// row-count clamp is % 17 (0..=16) and the menu-index clamp is % menu
    /// len (a count byte of 33 means 16 rows; menu index len(menu) means
    /// row 0).
    #[test]
    fn syscache_decode_clamps_are_pinned() {
        // count byte 33 -> 33 % 17 = 16 rows; every row byte trio pinned to
        // menu[0], no mutation.
        let mut bytes = alloc::vec![33u8];
        for _ in 0..16 {
            bytes.push(h::HARVEST_AMOP.len() as u8); // menu idx: len % len = 0
            bytes.push(0); // sel 0 = pristine
            bytes.extend_from_slice(&[0, 0, 0, 0]);
        }
        let mut cur = Cursor { b: &bytes, i: 0 };
        let rows = decode_rows(&mut cur);
        assert_eq!(rows.amop.len(), MAX_ROWS_PER_CACHE, "count clamp drifted from % 17");
        assert!(rows.amop.iter().all(|r| *r == h::HARVEST_AMOP[0]), "menu clamp drifted");
        // exhausted-cursor tables decode empty (Cursor zero-fill contract)
        assert!(rows.proc.is_empty());
    }
}
