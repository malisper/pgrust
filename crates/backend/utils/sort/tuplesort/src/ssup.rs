use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{direct_function_call2_coll_in, PGFunction};
use ::lsyscache::COMPARE_GT;
use ::types_nbtree::{BTORDER_PROC, BTSORTSUPPORT_PROC};

// pg_proc.dat oids for the sortsupport routines with a live comparator arm.
const F_BTINT2SORTSUPPORT: Oid = 3129;
const F_BTINT4SORTSUPPORT: Oid = 3130;
const F_BTOIDSORTSUPPORT: Oid = 3134;
const F_BTINT8SORTSUPPORT: Oid = 3131;
const F_DATE_SORTSUPPORT: Oid = 3136;
const F_TIMESTAMP_SORTSUPPORT: Oid = 3137;
const F_BTTEXTSORTSUPPORT: Oid = 3255;
const F_BTTEXT_PATTERN_SORTSUPPORT: Oid = 3332;
const F_BTBPCHAR_PATTERN_SORTSUPPORT: Oid = 3333;
const F_UUID_SORTSUPPORT: Oid = 3300;
const F_NETWORK_SORTSUPPORT: Oid = 5033;
const F_RANGE_SORTSUPPORT: Oid = 6391;
const F_MACADDR_SORTSUPPORT: Oid = 3359;
const F_INTERVAL_CMP: Oid = 1315;
const F_BPCHAR_SORTSUPPORT: Oid = 3328;
const F_BTNAMESORTSUPPORT: Oid = 3135;
const F_BYTEA_SORTSUPPORT: Oid = 3331;
const F_NUMERIC_SORTSUPPORT: Oid = 3283;
const F_BTFLOAT4SORTSUPPORT: Oid = 3132;
const F_BTFLOAT8SORTSUPPORT: Oid = 3133;
// btboolcmp: bool has no BTSORTSUPPORT_PROC, only this BTORDER_PROC; it's as
// mcx-free as btint4cmp (PG_GETARG_BOOL, no palloc) so it gets its own arm
// instead of falling into the comparison shim.
const F_BTBOOLCMP: Oid = 1693;

/// C's `ssup->comparator` fn pointer as a closed enum: identity is switchable
/// (tuplesort_sort_memtuples specialization dispatch) and calls monomorphize.
#[derive(Clone, Copy, Debug)]
pub enum SortComparator {
    /// `ssup_datum_unsigned_cmp` (abbreviated-key comparisons).
    Unsigned,
    /// `ssup_datum_signed_cmp` (btint8/timestamp sortsupport).
    SignedI64,
    /// `ssup_datum_int32_cmp` (btint4/date sortsupport).
    Int32,
    /// `btint2fastcmp` (btint2sortsupport).
    Int16,
    /// `btoidfastcmp` (btoidsortsupport): unsigned 32-bit.
    Uint32,
    /// `btboolcmp` direct (C shims BTORDER_PROC 1693); no sortsupport routine
    /// exists for bool, but the proc itself needs no mcx.
    Bool,
    /// `btfloat4fastcmp` (btfloat4sortsupport): NaN-aware total order.
    Float32,
    /// `btfloat8fastcmp` (btfloat8sortsupport): NaN-aware total order.
    Float64,
    /// `varstrfastcmp_c`, no abbreviation (bttextsortsupport, collate-is-C
    /// only); datums must point at live untoasted varlenas.
    TextC,
    /// interval_cmp direct (C shims BTORDER_PROC 1315); live 16-byte images.
    Interval,
    /// `bpcharfastcmp_c` (varstr_sortsupport bpchar arm, collate-is-C only).
    BpcharC,
    /// `namefastcmp_c` (btnamesortsupport); no abbreviation (sort-perf lane).
    NameC,
    /// `varlenafastcmp_locale`: resolved-once locale, no abbreviation (C too:
    /// pg_strxfrm_enabled false for libc), no last-pair result cache (order-
    /// identical; extra strcoll on repeated pairs — CATALOG watch).
    TextLocale(&'static pg_locale::PgLocale),
    /// `varlenafastcmp_locale` bpchar arm (bpchartruelen trim).
    BpcharLocale(&'static pg_locale::PgLocale),
    /// `uuid_fast_cmp` (uuid_sortsupport); datums point at live 16-byte uuids.
    Uuid,
    /// `network_fast_cmp` (network_sortsupport); live untoasted inet varlenas.
    Network,
    /// `numeric_fast_cmp` (numeric_sortsupport); live untoasted numeric
    /// varlenas (short or long numeric format).
    Numeric,
    /// `numeric_cmp_abbrev`: intentionally backwards signed compare — numeric
    /// abbreviations are negated so NaN (i64::MIN) sorts highest.
    NumericAbbrev,
    /// `gist_bbox_zorder_cmp` (gist_point_sortsupport, gistproc.c:1681):
    /// z-order over the leaf-key box's low corner; datums point at live
    /// 32-byte BOX images.
    GistPointZorder,
    /// `PrepareSortSupportComparisonShim`: the opfamily's BTORDER_PROC,
    /// resolved once, invoked per comparison (C builds an fcinfo per call
    /// too). Needs an mcx-threaded apply; the mcx-less lane panics.
    Shim(ShimCmp),
    /// Extension gist opclass comparator (btree_gist sortsupport procs),
    /// installed through `types_gist::GistSortSupportShim`. mcx-threaded
    /// lane only, like Shim.
    GistOpclass(::types_gist::GistSsupCmp),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShimCmp {
    pub fn_addr: PGFunction,
    pub fn_oid: Oid,
}

#[derive(Clone, Copy, Debug)]
pub struct SortSupport {
    pub ssup_collation: Oid,
    pub ssup_reverse: bool,
    pub ssup_nulls_first: bool,
    pub ssup_attno: i16,
    pub comparator: SortComparator,
}

#[inline(always)]
pub fn apply_cmp(cmp: SortComparator, x: Datum, y: Datum) -> i32 {
    match cmp {
        SortComparator::Unsigned => {
            let (x, y) = (x.as_u64(), y.as_u64());
            (x > y) as i32 - (x < y) as i32
        }
        SortComparator::SignedI64 => {
            let (x, y) = (x.as_i64(), y.as_i64());
            (x > y) as i32 - (x < y) as i32
        }
        SortComparator::Int32 => {
            let (x, y) = (x.as_i32(), y.as_i32());
            (x > y) as i32 - (x < y) as i32
        }
        SortComparator::Int16 => {
            let (x, y) = (x.as_i16(), y.as_i16());
            (x > y) as i32 - (x < y) as i32
        }
        SortComparator::Uint32 => {
            let (x, y) = (x.as_u32(), y.as_u32());
            (x > y) as i32 - (x < y) as i32
        }
        SortComparator::Bool => x.as_bool() as i32 - y.as_bool() as i32,
        SortComparator::Float32 => ::adt_float::float4_cmp_internal(x.as_f32(), y.as_f32()),
        SortComparator::Float64 => ::adt_float::float8_cmp_internal(x.as_f64(), y.as_f64()),
        // SAFETY: TextC contract (enum doc) — both datums are live varlena
        // pointers owned by the sort's tuplecontext.
        SortComparator::TextC => unsafe {
            with_varlena_payload(x, |a| {
                with_varlena_payload(y, |b| varlena::varstrfastcmp_c(a, b))
            })
        },
        // SAFETY: Interval contract (enum doc) — live 16-byte images.
        SortComparator::Interval => unsafe {
            let a = &*(x.as_usize() as *const adt_datetime::consts::Interval);
            let b = &*(y.as_usize() as *const adt_datetime::consts::Interval);
            adt_timestamp::interval::interval_cmp_internal(a, b)
        },
        // SAFETY: as TextC (all three arms).
        SortComparator::BpcharC => unsafe {
            with_varlena_payload(x, |a| {
                with_varlena_payload(y, |b| varlena::bpcharfastcmp_c(a, b))
            })
        },
        SortComparator::NameC => {
            // SAFETY: name datums point at live 64-byte NameData blocks.
            let (a, b) = unsafe {
                (
                    &*(x.as_usize() as *const [u8; 64]),
                    &*(y.as_usize() as *const [u8; 64]),
                )
            };
            namefastcmp_c(a, b)
        }
        // SAFETY: as TextC.
        SortComparator::TextLocale(locale) => unsafe {
            with_varlena_payload(x, |a| {
                with_varlena_payload(y, |b| varstrfastcmp_locale(a, b, locale, false))
            })
        },
        // SAFETY: as TextC.
        SortComparator::BpcharLocale(locale) => unsafe {
            with_varlena_payload(x, |a| {
                with_varlena_payload(y, |b| varstrfastcmp_locale(a, b, locale, true))
            })
        },
        // SAFETY: Uuid contract (enum doc) — live 16-byte images.
        SortComparator::Uuid => unsafe {
            let a = &*(x.as_usize() as *const ::adt_uuid::PgUuid);
            let b = &*(y.as_usize() as *const ::adt_uuid::PgUuid);
            ::adt_uuid::uuid_internal_cmp(a, b)
        },
        // SAFETY: Network contract (enum doc) — live inet varlenas.
        SortComparator::Network => unsafe {
            with_varlena_payload(x, |a| {
                with_varlena_payload(y, |b| {
                    ::adt_network::network_cmp_internal(
                        ::adt_network::InetRef::from_payload(a),
                        ::adt_network::InetRef::from_payload(b),
                    )
                })
            })
        },
        // SAFETY: Numeric contract (enum doc) — live numeric varlenas (short
        // or long numeric format).
        SortComparator::Numeric => unsafe {
            with_varlena_payload(x, |a| {
                with_varlena_payload(y, |b| ::adt_numeric::sortsupport::numeric_fast_cmp(a, b))
            })
        },
        SortComparator::NumericAbbrev => {
            let (x, y) = (x.as_i64(), y.as_i64());
            (x < y) as i32 - (x > y) as i32
        }
        // SAFETY: GistPointZorder contract (enum doc) — live 32-byte BOX
        // images (gist_point_compress leaf keys).
        SortComparator::GistPointZorder => unsafe {
            let a = ::types_core::geo::BOX::from_datum_bytes(core::slice::from_raw_parts(
                x.as_usize() as *const u8,
                32,
            ));
            let b = ::types_core::geo::BOX::from_datum_bytes(core::slice::from_raw_parts(
                y.as_usize() as *const u8,
                32,
            ));
            ::types_core::geo::gist_bbox_zorder_cmp(&a, &b)
        },
        SortComparator::Shim(shim) => panic!(
            "comparison shim (proc {}) reached an mcx-less comparator lane \
             (use apply_sort_comparator_in)",
            shim.fn_oid
        ),
        SortComparator::GistOpclass(_) => panic!(
            "gist opclass comparator reached an mcx-less comparator lane \
             (use apply_sort_comparator_in)"
        ),
    }
}

// C namefastcmp_c: strncmp(NameStr, NameStr, NAMEDATALEN).
#[inline]
fn namefastcmp_c(a: &[u8; 64], b: &[u8; 64]) -> i32 {
    for i in 0..64 {
        let (x, y) = (a[i], b[i]);
        if x != y {
            return if x < y { -1 } else { 1 };
        }
        if x == 0 {
            return 0;
        }
    }
    0
}

std::thread_local! {
    // ManuallyDrop: fn_extra Drop reaches other TLS registries, whose
    // destruction order at thread exit is unspecified — a panic in a TLS
    // dtor aborts the process. Leaks with the backend, like C's exit free.
    static SHIM_FLINFO: core::cell::RefCell<
        core::mem::ManuallyDrop<Option<(Oid, ::types_fmgr::FmgrInfo)>>,
    > = const { core::cell::RefCell::new(core::mem::ManuallyDrop::new(None)) };
}

// TLS carrier = C's ssup_cxt-lived flinfo (record_cmp memoizes in fn_extra);
// out of line so apply_cmp_in's fast arms don't pay for it.
#[inline(never)]
fn shim_cmp(shim: ShimCmp, x: Datum, y: Datum, collation: Oid, mcx: Mcx<'_>) -> i32 {
    let call = SHIM_FLINFO.with(|c| {
        let mut slot = c.borrow_mut();
        if !matches!(&**slot, Some((o, _)) if *o == shim.fn_oid) {
            **slot = Some((shim.fn_oid, ::fmgr_seams::fmgr_info::call(shim.fn_oid)?));
        }
        let (_, fl) = slot.as_mut().expect("just filled");
        ::types_fmgr::function_call2_coll_in(fl, collation, mcx, x, y)
    });
    match call {
        Ok(d) => d.as_i32(),
        // The comparator sits under infallible qsort plumbing; C's
        // ereport-out-of-sort unwinds here as a PgError panic so the original
        // error (e.g. array_cmp's missing-comparison-proc) surfaces verbatim.
        Err(e) => std::panic::panic_any(e),
    }
}

#[inline(always)]
pub fn apply_cmp_in(cmp: SortComparator, x: Datum, y: Datum, collation: Oid, mcx: Mcx<'_>) -> i32 {
    match cmp {
        SortComparator::Shim(shim) => shim_cmp(shim, x, y, collation, mcx),
        SortComparator::GistOpclass(f) => match f(x, y, collation, mcx) {
            Ok(r) => r,
            // As shim_cmp: surface the PgError through the infallible qsort
            // plumbing verbatim.
            Err(e) => std::panic::panic_any(e),
        },
        other => apply_cmp(other, x, y),
    }
}

// varstrfastcmp_locale (varlena.c); the tie-break memcmp+len equals C's
// strcmp (text carries no NULs).
fn varstrfastcmp_locale(a1: &[u8], a2: &[u8], locale: &pg_locale::PgLocale, bpchar: bool) -> i32 {
    if a1.len() == a2.len() && a1 == a2 {
        return 0;
    }
    let (a1, a2) = if bpchar {
        (bpchartruelen(a1), bpchartruelen(a2))
    } else {
        (a1, a2)
    };
    let result = locale.pg_strncoll(a1, a2);
    if result == 0 && locale.deterministic {
        return varlena::varstrfastcmp_c(a1, a2);
    }
    result
}

fn bpchartruelen(s: &[u8]) -> &[u8] {
    &s[..s.len() - s.iter().rev().take_while(|&&b| b == b' ').count()]
}

/// # Safety
/// `d` points at a live varlena of any form, readable through its header.
/// `DatumGetVarStringPP` + `VARDATA_ANY` (varlena.c fastcmp/abbrev entries):
/// plain and short forms borrow in place; compressed/external detoast into a
/// scratch context dropped on return — C's comparator-local pfree.
#[inline]
pub(crate) unsafe fn with_varlena_payload<R>(d: Datum, f: impl FnOnce(&[u8]) -> R) -> R {
    use ::types_tuple::varatt::{varatt_is_1b, varatt_is_1b_e, varsize_1b, varsize_4b};
    let p = d.as_usize() as *const u8;
    if varatt_is_1b_e(p) || (*p & 0x03) == 0x02 {
        return detoast_payload(p, f);
    }
    if varatt_is_1b(p) {
        f(core::slice::from_raw_parts(p.add(1), varsize_1b(p) - 1))
    } else {
        f(core::slice::from_raw_parts(p.add(4), varsize_4b(p) - 4))
    }
}

/// # Safety
/// `p` heads a live external or inline-compressed varlena image.
#[cold]
unsafe fn detoast_payload<R>(p: *const u8, f: impl FnOnce(&[u8]) -> R) -> R {
    let scratch = ::mcx::MemoryContext::new_bump("varstr cmp detoast");
    let raw = core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p));
    let img = ::detoast_seams::detoast_attr::call(scratch.mcx(), raw)
        // Infallible qsort plumbing above; a detoast failure is C's
        // ereport-out-of-sort.
        .unwrap_or_else(|e| panic!("sort key detoast failed: {}", e.message()));
    f(&img[4..])
}

/// `ApplySortComparator` (sortsupport.h); `cmp` is passed separately so each
/// qsort specialization instantiates with a constant comparator, as C does.
#[inline(always)]
pub fn apply_sort_comparator_as(
    cmp: SortComparator,
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: &SortSupport,
) -> i32 {
    if isnull1 {
        if isnull2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if isnull2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let compare = apply_cmp(cmp, datum1, datum2);
        if ssup.ssup_reverse {
            -compare
        } else {
            compare
        }
    }
}

#[inline(always)]
pub fn apply_sort_comparator(
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: &SortSupport,
) -> i32 {
    apply_sort_comparator_as(ssup.comparator, datum1, isnull1, datum2, isnull2, ssup)
}

/// `ApplySortComparator` with the comparison-shim arm live (needs an mcx for
/// the shim'd proc's per-call allocations).
#[inline(always)]
pub fn apply_sort_comparator_as_in(
    cmp: SortComparator,
    mcx: Mcx<'_>,
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: &SortSupport,
) -> i32 {
    if isnull1 {
        if isnull2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if isnull2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let compare = apply_cmp_in(cmp, datum1, datum2, ssup.ssup_collation, mcx);
        if ssup.ssup_reverse {
            -compare
        } else {
            compare
        }
    }
}

#[inline(always)]
pub fn apply_sort_comparator_in(
    mcx: Mcx<'_>,
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: &SortSupport,
) -> i32 {
    apply_sort_comparator_as_in(ssup.comparator, mcx, datum1, isnull1, datum2, isnull2, ssup)
}

/// The `abbrev_converter` identities (closed set); mutable converter state
/// lives in the sort's `AbbrevState`, not in the Copy SortSupport.
#[derive(Clone, Copy, Debug)]
pub enum AbbrevKind {
    VarStrC,
    BpcharC,
    /// varstr_abbrev_convert non-C arm over pg_strnxfrm(_prefix) sort keys
    /// (C: abbreviation stays on when pg_strxfrm_enabled — ICU).
    VarStrXfrm {
        locale: &'static pg_locale::PgLocale,
        bpchar: bool,
    },
    Uuid,
    Network,
    Numeric,
}

/// Armed abbreviation: the key's `comparator` becomes `Unsigned` over
/// converted datum1 words; ties re-compare the ORIGINAL datums (refetched
/// from the stored tuple — datum1 no longer holds them) with
/// `full_comparator` (C `abbrev_full_comparator`).
#[derive(Clone, Copy, Debug)]
pub struct AbbrevArm {
    pub kind: AbbrevKind,
    pub full_comparator: SortComparator,
}

impl AbbrevArm {
    /// The armed comparator over converted datum1 words: `ssup_datum_unsigned_cmp`
    /// for byte-image abbrevs, `numeric_cmp_abbrev` for numeric.
    pub fn abbrev_comparator(&self) -> SortComparator {
        match self.kind {
            AbbrevKind::Numeric => SortComparator::NumericAbbrev,
            _ => SortComparator::Unsigned,
        }
    }
}

/// `PrepareSortSupportFromOrderingOp` (sortsupport.c), `abbreviate=false` arm
/// (nodeSetOp et al.); out-of-enum sortsupport routines panic loudly.
pub fn prepare_sort_support_from_ordering_op(
    ordering_op: Oid,
    ssup: &SortSupportInit,
) -> PgResult<SortSupport> {
    Ok(prepare_sort_support_abbrev(ordering_op, ssup, false)?.0)
}

pub fn prepare_sort_support_abbrev(
    ordering_op: Oid,
    ssup: &SortSupportInit,
    abbreviate: bool,
) -> PgResult<(SortSupport, Option<AbbrevArm>)> {
    let Some((opfamily, opcintype, cmptype)) =
        lsyscache::get_ordering_op_properties(ordering_op)?
    else {
        panic!("operator {ordering_op} is not a valid ordering operator");
    };
    let ssup_reverse = cmptype == COMPARE_GT;
    let comparator =
        comparator_for_opfamily(opfamily, opcintype, opcintype, ssup.ssup_collation)?;
    let abbrev = if abbreviate {
        abbrev_arm_for(comparator)
    } else {
        None
    };

    Ok((
        SortSupport {
            ssup_collation: ssup.ssup_collation,
            ssup_reverse,
            ssup_nulls_first: ssup.ssup_nulls_first,
            ssup_attno: ssup.ssup_attno,
            comparator: match &abbrev {
                Some(arm) => arm.abbrev_comparator(),
                None => comparator,
            },
        },
        abbrev,
    ))
}

fn abbrev_arm_for(comparator: SortComparator) -> Option<AbbrevArm> {
    let kind = match comparator {
        SortComparator::TextC => AbbrevKind::VarStrC,
        SortComparator::BpcharC => AbbrevKind::BpcharC,
        SortComparator::TextLocale(locale) if locale.pg_strxfrm_enabled() => {
            AbbrevKind::VarStrXfrm { locale, bpchar: false }
        }
        SortComparator::BpcharLocale(locale) if locale.pg_strxfrm_enabled() => {
            AbbrevKind::VarStrXfrm { locale, bpchar: true }
        }
        SortComparator::Uuid => AbbrevKind::Uuid,
        SortComparator::Network => AbbrevKind::Network,
        SortComparator::Numeric => AbbrevKind::Numeric,
        _ => return None,
    };
    Some(AbbrevArm { kind, full_comparator: comparator })
}

/// The MJExamineQuals (nodeMergejoin.c) comparator resolve: BTSORTSUPPORT_PROC
/// for (lefttype,righttype), else the BTORDER_PROC shim — which, like every
/// out-of-enum sortsupport routine, panics loudly.
pub fn comparator_for_opfamily(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    collation: Oid,
) -> PgResult<SortComparator> {
    let sort_support_function =
        lsyscache::get_opfamily_proc(opfamily, lefttype, righttype, BTSORTSUPPORT_PROC as i16)?;
    Ok(match sort_support_function {
        F_BTINT4SORTSUPPORT | F_DATE_SORTSUPPORT => SortComparator::Int32,
        F_BTFLOAT4SORTSUPPORT => SortComparator::Float32,
        F_BTFLOAT8SORTSUPPORT => SortComparator::Float64,
        F_BTINT2SORTSUPPORT => SortComparator::Int16,
        // btoidfastcmp: unsigned; the zero-extended datum word compares exact.
        F_BTOIDSORTSUPPORT => SortComparator::Unsigned,
        F_BTINT8SORTSUPPORT | F_TIMESTAMP_SORTSUPPORT => SortComparator::SignedI64,
        F_BTINT2SORTSUPPORT => SortComparator::Int16,
        F_BTTEXTSORTSUPPORT | F_BPCHAR_SORTSUPPORT => {
            varstr_comparator(sort_support_function == F_BPCHAR_SORTSUPPORT, collation)?
        }
        F_BTTEXT_PATTERN_SORTSUPPORT => SortComparator::TextC,
        F_BTBPCHAR_PATTERN_SORTSUPPORT => SortComparator::BpcharC,
        F_BTNAMESORTSUPPORT => SortComparator::NameC,
        // bytea_sortsupport forces the C collation (byteas carry NULs).
        F_BYTEA_SORTSUPPORT => SortComparator::TextC,
        F_UUID_SORTSUPPORT => SortComparator::Uuid,
        F_NETWORK_SORTSUPPORT => SortComparator::Network,
        F_NUMERIC_SORTSUPPORT => SortComparator::Numeric,
        // C DIVERGENCE: range_sortsupport 6391 + macaddr_sortsupport 3359 fast
        // cmps unported; the BTORDER_PROC shim is order-identical (perf watch).
        0 | F_RANGE_SORTSUPPORT | F_MACADDR_SORTSUPPORT => {
            // C: PrepareSortSupportComparisonShim — fmgr_info the BTORDER_PROC
            // once; comparisons go through the resolved fn pointer.
            let sort_function =
                lsyscache::get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC as i16)?;
            if sort_function == 0 {
                panic!(
                    "missing support function {}({lefttype},{righttype}) in opfamily {opfamily}",
                    BTORDER_PROC
                );
            }
            if sort_function == F_INTERVAL_CMP {
                SortComparator::Interval
            } else if sort_function == F_BTBOOLCMP {
                SortComparator::Bool
            } else {
                let flinfo = ::fmgr_seams::fmgr_info::call(sort_function)?;
                SortComparator::Shim(ShimCmp {
                    fn_addr: flinfo.fn_addr,
                    fn_oid: sort_function,
                })
            }
        }
        other => panic!(
            "sortsupport routine {other} (opfamily {opfamily}) has no comparator arm; \
             abbreviated-key sortsupport (e.g. bttextsortsupport) not ported"
        ),
    })
}

// gist.h GIST_SORTSUPPORT_PROC; pg_proc.dat range_cmp.
const GIST_SORTSUPPORT_PROC_NUM: i16 = 11;
const F_RANGE_CMP: Oid = 3870;
const F_GIST_POINT_SORTSUPPORT: Oid = 3435;

/// PrepareSortSupportFromGistIndexRel's comparator resolve over the closed
/// proc-11 set. C DIVERGENCE (recorded): range_sortsupport's range_fast_cmp
/// is order-identical to range_cmp (rangetypes.c: same empty-first +
/// range_cmp_bounds logic), so it rides the shim. C DIVERGENCE:
/// gist_point_sortsupport's abbreviation (z-order Datum, ssup_datum_unsigned_cmp)
/// is skipped like every index-build abbrev; gist_bbox_zorder_cmp is the
/// abbrev arm's full tie-breaker, so the order is identical.
pub fn comparator_for_gist_index_col(opfamily: Oid, opcintype: Oid) -> PgResult<SortComparator> {
    let ssup_proc = lsyscache::get_opfamily_proc(
        opfamily,
        opcintype,
        opcintype,
        GIST_SORTSUPPORT_PROC_NUM,
    )?;
    match ssup_proc {
        F_GIST_POINT_SORTSUPPORT => Ok(SortComparator::GistPointZorder),
        F_RANGE_SORTSUPPORT => {
            let flinfo = ::fmgr_seams::fmgr_info::call(F_RANGE_CMP)?;
            Ok(SortComparator::Shim(ShimCmp {
                fn_addr: flinfo.fn_addr,
                fn_oid: F_RANGE_CMP,
            }))
        }
        0 => panic!(
            "missing support function {GIST_SORTSUPPORT_PROC_NUM}({opcintype},{opcintype}) \
             in opfamily {opfamily}"
        ),
        // Extension opclass (btree_gist): invoke the proc once with a shim
        // "SortSupport"; it installs a leaf-key comparator fn.
        other => {
            let mut flinfo = ::fmgr_seams::fmgr_info::call(other)?;
            let mut shim = ::types_gist::GistSortSupportShim {
                ssup_collation: 0,
                comparator: None,
            };
            let temp = ::mcx::MemoryContext::new("gist sortsupport resolve");
            let mut frame = ::types_fmgr::LocalFcinfo::<1>::new(0);
            frame.rearm(0);
            // SAFETY: temp outlives the call; nothing retained from it.
            unsafe { frame.set_result_mcx(temp.mcx()) };
            frame.set_arg(
                0,
                Datum::from_usize(&mut shim as *mut ::types_gist::GistSortSupportShim as usize),
            );
            flinfo.invoke(&mut frame)?;
            match shim.comparator {
                Some(cmp) => Ok(SortComparator::GistOpclass(cmp)),
                None => panic!("gist sortsupport proc {other} installed no comparator"),
            }
        }
    }
}

// varstr_sortsupport (varlena.c) comparator selection.
fn varstr_comparator(bpchar: bool, collation: Oid) -> PgResult<SortComparator> {
    varlena::check_collation_set(collation)?;
    let locale = pg_locale::pg_newlocale_from_collation(collation)?;
    Ok(match (locale.collate_is_c, bpchar) {
        (true, false) => SortComparator::TextC,
        (true, true) => SortComparator::BpcharC,
        (false, false) => SortComparator::TextLocale(locale),
        (false, true) => SortComparator::BpcharLocale(locale),
    })
}

/// The caller-filled prefix of C's zeroed SortSupportData.
pub struct SortSupportInit {
    pub ssup_collation: Oid,
    pub ssup_nulls_first: bool,
    pub ssup_attno: i16,
}

/// `PrepareSortSupportFromIndexRel` comparator resolve, btree arm.
pub fn comparator_for_index_col(
    opfamily: Oid,
    opcintype: Oid,
    collation: Oid,
) -> PgResult<SortComparator> {
    let ssup_proc =
        lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, BTSORTSUPPORT_PROC as i16)?;
    Ok(match ssup_proc {
        F_BTINT4SORTSUPPORT | F_DATE_SORTSUPPORT => SortComparator::Int32,
        F_BTFLOAT4SORTSUPPORT => SortComparator::Float32,
        F_BTFLOAT8SORTSUPPORT => SortComparator::Float64,
        F_BTINT8SORTSUPPORT | F_TIMESTAMP_SORTSUPPORT => SortComparator::SignedI64,
        F_BTINT2SORTSUPPORT => SortComparator::Int16,
        F_BTOIDSORTSUPPORT => SortComparator::Uint32,
        F_BTNAMESORTSUPPORT => SortComparator::NameC,
        F_BTTEXTSORTSUPPORT | F_BPCHAR_SORTSUPPORT => {
            varstr_comparator(ssup_proc == F_BPCHAR_SORTSUPPORT, collation)?
        }
        // C forces the "C" collation for the *_pattern_ops sortsupports.
        F_BTTEXT_PATTERN_SORTSUPPORT => SortComparator::TextC,
        F_BTBPCHAR_PATTERN_SORTSUPPORT => SortComparator::BpcharC,
        F_BYTEA_SORTSUPPORT => SortComparator::TextC,
        F_UUID_SORTSUPPORT => SortComparator::Uuid,
        F_NETWORK_SORTSUPPORT => SortComparator::Network,
        F_NUMERIC_SORTSUPPORT => SortComparator::Numeric,
        // C DIVERGENCE: range_sortsupport 6391 + macaddr_sortsupport 3359 fast
        // cmps unported; the BTORDER_PROC shim is order-identical (perf watch).
        0 | F_RANGE_SORTSUPPORT | F_MACADDR_SORTSUPPORT => {
            let sort_function =
                lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, BTORDER_PROC as i16)?;
            if sort_function == 0 {
                panic!(
                    "missing support function {}({opcintype},{opcintype}) in opfamily {opfamily}",
                    BTORDER_PROC
                );
            }
            if sort_function == F_INTERVAL_CMP {
                SortComparator::Interval
            } else if sort_function == F_BTBOOLCMP {
                SortComparator::Bool
            } else {
                let flinfo = ::fmgr_seams::fmgr_info::call(sort_function)?;
                SortComparator::Shim(ShimCmp {
                    fn_addr: flinfo.fn_addr,
                    fn_oid: sort_function,
                })
            }
        }
        other => panic!(
            "sortsupport routine {other} (opfamily {opfamily}) has no comparator arm; \
             abbreviated-key sortsupport not ported"
        ),
    })
}
