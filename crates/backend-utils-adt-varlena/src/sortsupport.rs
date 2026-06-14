//! FAMILY: SortSupport + abbreviated keys.
//!
//! `bttextsortsupport`/`bttext_pattern_sortsupport`/`bytea_sortsupport`, the
//! `varstr_sortsupport` installer, the comparator cores (`varstrfastcmp_c`,
//! `bpcharfastcmp_c`, `namefastcmp_c`, the `*_locale` variants), and the
//! abbreviated-key machinery (`varstr_abbrev_convert`/`varstr_abbrev_abort`).
//!
//! ## Layered shape
//!
//! The C `varstr_sortsupport` installs *function pointers* (`ssup->comparator`,
//! `ssup->abbrev_converter`, `ssup->abbrev_abort`,
//! `ssup->abbrev_full_comparator`) and a `VarStringSortSupport` scratch struct
//! into the C-ABI `SortSupportData` node, then drives the comparators/abbrev
//! through those pointers. In this repo's layered model the
//! [`types_sortsupport::SortSupportData`] is trimmed: `comparator` is a `Copy`
//! [`SortComparatorId`](types_sortsupport::SortComparatorId) token the
//! sortsupport owner interprets, and the `ssup_extra`/abbreviation-hook slots
//! are NOT carried (they are filled/read only inside the comparator-providing
//! unit). So `varstr_sortsupport` here resolves the collation, builds the
//! [`VarStringSortSupport`](crate::keystone) scratch the comparators run
//! against, and reports the decision (which comparator family, whether to
//! abbreviate). The function-pointer *install into the C-ABI node* is the
//! `utils/sort/sortsupport.c` / fmgr substrate; the algorithmic guts of every
//! comparator and the abbreviation converter/abort are ported here in full and
//! are directly callable.
//!
//! The pure comparator cores operate on the already-detoasted payload bytes
//! (`VARDATA_ANY`/`VARSIZE_ANY_EXHDR`); a `name` operand is its fixed-width
//! 64-byte NUL-padded buffer. The locale comparators and the abbreviated-key
//! converter read/write the scratch and reach the locale providers
//! (`pg_strcoll`/`pg_strxfrm`/`pg_strxfrm_prefix` and their `*_enabled` guards)
//! through `backend-utils-adt-pg-locale-seams` (pg_locale.c — genuinely
//! external collation/ICU owner). HyperLogLog cardinality goes through
//! `backend-lib-hyperloglog-seams` and the abbreviated-key hashing through
//! `common-hashfn-seams`.
//!
//! Depends on the keystone for [`VarStringSortSupport`](crate::keystone) and
//! [`check_collation_set`](crate::keystone::check_collation_set).

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_sortsupport::SortSupportData;

use backend_lib_hyperloglog_seams as hll;
use backend_utils_adt_pg_locale_seams as loc;
use common_hashfn_seams as hashfn;

use crate::keystone::{check_collation_set, VarStringSortSupport, TEXTBUFLEN};

// C: built-in type OIDs (`pg_type.dat`). Defined locally (the layered
// `types-core` does not centralize the row-type OIDs); values mirror the C
// `*OID` macros.
/// C: `BYTEAOID` (`pg_type.dat` oid 17).
pub const BYTEAOID: Oid = 17;
/// C: `NAMEOID` (`pg_type.dat` oid 19).
pub const NAMEOID: Oid = 19;
/// C: `TEXTOID` (`pg_type.dat` oid 25).
pub const TEXTOID: Oid = 25;
/// C: `BPCHAROID` (`pg_type.dat` oid 1042).
pub const BPCHAROID: Oid = 1042;

/// C: `C_COLLATION_OID` (`pg_collation.dat` oid 950).
pub const C_COLLATION_OID: Oid = types_core::catalog::C_COLLATION_OID;

/// C: `NAMEDATALEN` (c.h) — fixed `name` width.
pub const NAMEDATALEN: usize = crate::keystone::NAMEDATALEN;

/// C: `PG_CACHE_LINE_SIZE` (pg_config_manual.h) — the cache-line size the
/// abbreviated-key hash caps the hashed prefix at.
const PG_CACHE_LINE_SIZE: usize = 128;

/// C: `sizeof(Datum)` — the abbreviated-key prefix width (8 on a 64-bit build).
const SIZEOF_DATUM: usize = core::mem::size_of::<usize>();

/// The decision `varstr_sortsupport` returns: which authoritative comparator
/// family applies, and whether the abbreviated-key optimization is in play.
/// (In C this is recorded as installed function pointers on the `SortSupport`
/// node; the trimmed [`SortSupportData`] cannot carry those slots, so the
/// installer hands the decision back for the caller's comparator dispatch.)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VarStringComparator {
    /// `varstrfastcmp_c` — C-collation text/varchar/bytea memcmp core.
    FastCmpC,
    /// `bpcharfastcmp_c` — C-collation bpchar (trailing-blank-trimmed) core.
    BpCharFastCmpC,
    /// `namefastcmp_c` — C-collation NAME strncmp core.
    NameFastCmpC,
    /// `varlenafastcmp_locale` — locale-aware text/varchar/bpchar/bytea core.
    VarLenaFastCmpLocale,
    /// `namefastcmp_locale` — locale-aware NAME core.
    NameFastCmpLocale,
}

/// What `varstr_sortsupport` resolved: the authoritative comparator family,
/// whether to abbreviate, and the freshly-built `VarStringSortSupport` scratch
/// (present iff abbreviation or a non-C collation needs it — C's
/// `if (abbreviate || !collate_c)` branch).
#[derive(Debug)]
pub struct VarStrSortSupport<'mcx> {
    /// The authoritative comparator (C: `ssup->comparator`, or
    /// `ssup->abbrev_full_comparator` when abbreviating).
    pub comparator: VarStringComparator,
    /// Whether the abbreviated-key optimization is planned (C: the final value
    /// of `abbreviate`).
    pub abbreviate: bool,
    /// The `ssup->ssup_extra` scratch, when allocated.
    pub extra: Option<VarStringSortSupport<'mcx>>,
}

// ===========================================================================
// bttextsortsupport / bttext_pattern_sortsupport / bytea_sortsupport
// ===========================================================================

/// C: `bttextsortsupport(PG_FUNCTION_ARGS)` (varlena.c:1956-1971) — install the
/// generic string SortSupport for `text` under the function's collation.
///
/// C switches into `ssup->ssup_cxt` (the scratch context) before allocating;
/// here that context is `ssup.ssup_cxt`, which [`varstr_sortsupport`] charges
/// the scratch buffers / HLL state to.
pub fn bttextsortsupport<'mcx>(
    ssup: &mut SortSupportData<'mcx>,
    collid: Oid,
) -> PgResult<VarStrSortSupport<'mcx>> {
    // C:1965-1966 varstr_sortsupport(ssup, TEXTOID, collid);
    varstr_sortsupport(ssup, TEXTOID, collid)
}

/// C: `bttext_pattern_sortsupport(PG_FUNCTION_ARGS)` (varlena.c:3001-3012) —
/// `text_pattern_ops` sort support: always the C collation core.
pub fn bttext_pattern_sortsupport<'mcx>(
    ssup: &mut SortSupportData<'mcx>,
) -> PgResult<VarStrSortSupport<'mcx>> {
    // C:3009-3010 varstr_sortsupport(ssup, TEXTOID, C_COLLATION_OID);
    varstr_sortsupport(ssup, TEXTOID, C_COLLATION_OID)
}

/// C: `bytea_sortsupport(PG_FUNCTION_ARGS)` (varlena.c:4121-4132) — bytea sort
/// support: always the C collation core (bytea is non-collatable).
pub fn bytea_sortsupport<'mcx>(
    ssup: &mut SortSupportData<'mcx>,
) -> PgResult<VarStrSortSupport<'mcx>> {
    // C:4129-4130 varstr_sortsupport(ssup, BYTEAOID, C_COLLATION_OID);
    varstr_sortsupport(ssup, BYTEAOID, C_COLLATION_OID)
}

// ===========================================================================
// varstr_sortsupport — the generic installer
// ===========================================================================

/// C: `varstr_sortsupport(SortSupport ssup, Oid typid, Oid collid)`
/// (varlena.c:1983-2115) — generic sortsupport for the character/byte types.
///
/// Resolves the collation, selects the authoritative comparator family, and —
/// when abbreviation or a non-C collation needs scratch — builds the
/// [`VarStringSortSupport`] state (the C `ssup->ssup_extra`) and arms the
/// abbreviated-key counters. The C install of the comparator/abbrev *function
/// pointers* into the C-ABI node is the sortsupport/fmgr substrate; this
/// returns the decision instead (see [`VarStrSortSupport`]).
pub fn varstr_sortsupport<'mcx>(
    ssup: &mut SortSupportData<'mcx>,
    typid: Oid,
    collid: Oid,
) -> PgResult<VarStrSortSupport<'mcx>> {
    // C:1986 bool abbreviate = ssup->abbreviate;
    let mut abbreviate = ssup.abbreviate;
    // C:1987 bool collate_c = false;
    let mut collate_c = false;

    // C:1991 check_collation_set(collid);
    check_collation_set(collid)?;

    // C:1993 locale = pg_newlocale_from_collation(collid);
    let locale = loc::pg_newlocale_from_collation::call(ssup.ssup_cxt, collid)?;

    // C:2008-2054 select the comparator family.
    let comparator = if locale.collate_is_c {
        // C:2008-2022 C-locale fast comparators.
        let cmp = if typid == BPCHAROID {
            // C:2011 ssup->comparator = bpcharfastcmp_c;
            VarStringComparator::BpCharFastCmpC
        } else if typid == NAMEOID {
            // C:2014-2016 namefastcmp_c; abbreviation disabled for NAME.
            abbreviate = false;
            VarStringComparator::NameFastCmpC
        } else {
            // C:2019 ssup->comparator = varstrfastcmp_c;
            VarStringComparator::FastCmpC
        };
        // C:2021 collate_c = true;
        collate_c = true;
        cmp
    } else {
        // C:2024-2053 locale-aware comparators.
        let cmp = if typid == NAMEOID {
            // C:2030-2032 namefastcmp_locale; abbreviation disabled for NAME.
            abbreviate = false;
            VarStringComparator::NameFastCmpLocale
        } else {
            // C:2035 ssup->comparator = varlenafastcmp_locale;
            VarStringComparator::VarLenaFastCmpLocale
        };
        // C:2052-2053 abbreviation for non-C collations is gated on
        // pg_strxfrm_enabled().
        if !loc::pg_strxfrm_enabled::call(collid) {
            abbreviate = false;
        }
        cmp
    };

    // C:2063-2114 build the scratch state when abbreviating or non-C.
    let extra = if abbreviate || !collate_c {
        // C:2065-2069 sss = palloc; buf1/buf2 = palloc(TEXTBUFLEN); buflen = TEXTBUFLEN.
        let buf1 = mcx::vec_with_capacity_in(ssup.ssup_cxt, TEXTBUFLEN)?;
        let buf2 = mcx::vec_with_capacity_in(ssup.ssup_cxt, TEXTBUFLEN)?;
        let mut sss = VarStringSortSupport {
            buf1,
            buf2,
            // C:2071-2072 Start with invalid values.
            last_len1: -1,
            last_len2: -1,
            // C:2074 last_returned = 0.
            last_returned: 0,
            // C:2092-2094 Arbitrarily initialize cache_blob to true.
            cache_blob: true,
            // C:2095 sss->collate_c = collate_c;
            collate_c,
            // C:2096 sss->typid = typid;
            typid,
            prop_card: 0.0,
            // C:2075-2078 locale = NULL when collate_c, else the resolved locale.
            locale: if collate_c { None } else { Some(collid) },
            // The C comparator reads `sss->locale->deterministic`.
            locale_deterministic: if collate_c { false } else { locale.deterministic },
            abbr_card: None,
            full_card: None,
        };

        // C:2104-2113 arm the abbreviated-key optimization.
        if abbreviate {
            // C:2106 sss->prop_card = 0.20;
            sss.prop_card = 0.20;
            // C:2107-2108 initHyperLogLog(&sss->abbr_card, 10);
            //             initHyperLogLog(&sss->full_card, 10);
            sss.abbr_card = Some(hll::init_hyper_log_log::call(10));
            sss.full_card = Some(hll::init_hyper_log_log::call(10));
            // C:2109-2112 abbrev_full_comparator = comparator;
            //             comparator = ssup_datum_unsigned_cmp;
            //             abbrev_converter = varstr_abbrev_convert;
            //             abbrev_abort = varstr_abbrev_abort;
            // (The install of the unsigned-cmp comparator + abbrev hooks into the
            // C-ABI node is the sortsupport substrate; the authoritative
            // comparator is reported in `comparator` below.)
        }

        Some(sss)
    } else {
        None
    };

    Ok(VarStrSortSupport {
        comparator,
        abbreviate,
        extra,
    })
}

// ===========================================================================
// C-collation comparator cores (pure)
// ===========================================================================

/// C: `varstrfastcmp_c(Datum x, Datum y, SortSupport ssup)`
/// (varlena.c:2120-2148) — C-collation varlena compare: `memcmp` + length
/// tiebreak. Pure on the `VARDATA_ANY`/`VARSIZE_ANY_EXHDR` payload bytes (the
/// `Datum` unwrap/detoast and leak-avoidance `pfree` are the fmgr boundary).
pub fn varstrfastcmp_c(a: &[u8], b: &[u8]) -> i32 {
    // C:2134-2135 len1/len2 = VARSIZE_ANY_EXHDR.
    let len1 = a.len();
    let len2 = b.len();
    // C:2137 result = memcmp(a1p, a2p, Min(len1, len2));
    let min = len1.min(len2);
    let mut result = memcmp(&a[..min], &b[..min]);
    // C:2138-2139 length tiebreak.
    if result == 0 && len1 != len2 {
        result = if len1 < len2 { -1 } else { 1 };
    }
    result
}

/// C: `bpcharfastcmp_c(Datum x, Datum y, SortSupport ssup)`
/// (varlena.c:2157-2185) — bpchar core: trims trailing blanks before the
/// C-collation compare.
pub fn bpcharfastcmp_c(a: &[u8], b: &[u8]) -> i32 {
    // C:2171-2172 len = bpchartruelen(VARDATA_ANY, VARSIZE_ANY_EXHDR);
    let len1 = bpchartruelen(a);
    let len2 = bpchartruelen(b);
    // C:2174 result = memcmp(a1p, a2p, Min(len1, len2));
    let min = len1.min(len2);
    let mut result = memcmp(&a[..min], &b[..min]);
    // C:2175-2176 length tiebreak.
    if result == 0 && len1 != len2 {
        result = if len1 < len2 { -1 } else { 1 };
    }
    result
}

/// C: the bpchar trailing-blank-trim helper (`bpchartruelen`, varchar.c) used
/// by [`bpcharfastcmp_c`] and the locale path. Returns the byte length with
/// trailing ASCII spaces removed; relies on `' '` being a singleton unit in
/// every supported encoding.
pub fn bpchartruelen(s: &[u8]) -> usize {
    // C: while (i >= 0) { if (s[i] != ' ') break; i--; } return i + 1;
    let mut i = s.len();
    while i > 0 {
        if s[i - 1] != b' ' {
            break;
        }
        i -= 1;
    }
    i
}

/// C: `namefastcmp_c(Datum x, Datum y, SortSupport ssup)`
/// (varlena.c:2190-2197) — `name` `strncmp` over the fixed-width NUL-terminated
/// buffers.
pub fn namefastcmp_c(a: &[u8; NAMEDATALEN], b: &[u8; NAMEDATALEN]) -> i32 {
    // C:2196 return strncmp(NameStr(*arg1), NameStr(*arg2), NAMEDATALEN);
    strncmp(a, b, NAMEDATALEN)
}

// ===========================================================================
// Locale comparator cores
// ===========================================================================

/// C: `varlenafastcmp_locale(Datum x, Datum y, SortSupport ssup)`
/// (varlena.c:2202-2228) — locale-aware varlena compare. On the byte-slice
/// surface the `DatumGetVarStringPP`/`VARDATA_ANY` unwrap collapses to the
/// payload slices, so this just forwards to [`varstrfastcmp_locale`].
pub fn varlenafastcmp_locale(
    sss: &mut VarStringSortSupport<'_>,
    mcx: Mcx<'_>,
    a: &[u8],
    b: &[u8],
) -> PgResult<i32> {
    // C:2219 result = varstrfastcmp_locale(a1p, len1, a2p, len2, ssup);
    varstrfastcmp_locale(sss, mcx, a, b)
}

/// C: `namefastcmp_locale(Datum x, Datum y, SortSupport ssup)`
/// (varlena.c:2233-2242) — locale-aware NAME compare: uses the NUL-bounded
/// logical length (`strlen(NameStr(*arg))`) of each fixed-width buffer.
pub fn namefastcmp_locale(
    sss: &mut VarStringSortSupport<'_>,
    mcx: Mcx<'_>,
    a: &[u8; NAMEDATALEN],
    b: &[u8; NAMEDATALEN],
) -> PgResult<i32> {
    // C:2239-2241 varstrfastcmp_locale(NameStr, strlen(NameStr), ..., ssup);
    let a = &a[..name_strlen(a)];
    let b = &b[..name_strlen(b)];
    varstrfastcmp_locale(sss, mcx, a, b)
}

/// C: `varstrfastcmp_locale(char *a1p, int len1, char *a2p, int len2,
/// SortSupport ssup)` (varlena.c:2247-2338) — the locale comparison core with
/// the equality fast-path and the buf1/buf2 caching that lets repeated
/// comparisons of the same strings reuse the last `pg_strcoll` result.
pub fn varstrfastcmp_locale(
    sss: &mut VarStringSortSupport<'_>,
    _mcx: Mcx<'_>,
    a1p: &[u8],
    a2p: &[u8],
) -> PgResult<i32> {
    let mut len1 = a1p.len();
    let mut len2 = a2p.len();

    // C:2254-2271 Fast pre-check for equality.
    if len1 == len2 && memcmp(a1p, a2p) == 0 {
        return Ok(0);
    }

    // C:2273-2278 BpChar: trim trailing spaces.
    if sss.typid == BPCHAROID {
        len1 = bpchartruelen(a1p);
        len2 = bpchartruelen(a2p);
    }
    let a1 = &a1p[..len1];
    let a2 = &a2p[..len2];

    // C:2280-2289 grow buf1/buf2 as needed (PgVec owns its capacity; the
    // explicit repalloc heuristic is the C allocator's, not load-bearing on
    // the result — we just need room for the data + NUL terminator).

    // C:2301-2308 cache buf1.
    let mut arg1_match = true;
    if (len1 as i32) != sss.last_len1 || &sss.buf1[..] != a1 {
        arg1_match = false;
        // memcpy(sss->buf1, a1p, len1); buf1[len1] = '\0'; last_len1 = len1;
        sss.buf1.clear();
        sss.buf1.extend_from_slice(a1);
        sss.last_len1 = len1 as i32;
    }

    // C:2316-2326 cache buf2; reuse cached result when both match.
    if (len2 as i32) != sss.last_len2 || &sss.buf2[..] != a2 {
        sss.buf2.clear();
        sss.buf2.extend_from_slice(a2);
        sss.last_len2 = len2 as i32;
    } else if arg1_match && !sss.cache_blob {
        // C:2322-2325 Use result cached following last actual strcoll() call.
        return Ok(sss.last_returned);
    }

    // C:2328 result = pg_strcoll(sss->buf1, sss->buf2, sss->locale);
    let collid = sss.locale.expect("locale comparator requires a resolved locale");
    let buf1: &[u8] = &sss.buf1;
    let buf2: &[u8] = &sss.buf2;
    let mut result = loc::pg_strcoll::call(collid, buf1, buf2)?;

    // C:2330-2332 if (result == 0 && sss->locale->deterministic)
    //                 result = strcmp(sss->buf1, sss->buf2);
    if result == 0 && sss.locale_deterministic {
        result = strcmp(buf1, buf2);
    }

    // C:2334-2336 cache result.
    sss.cache_blob = false;
    sss.last_returned = result;
    Ok(result)
}

// ===========================================================================
// Abbreviated-key conversion / abort
// ===========================================================================

/// C: `varstr_abbrev_convert(Datum original, SortSupport ssup)`
/// (varlena.c:2347-2538) — convert an authoritative key to its abbreviated
/// representation: pack the first `sizeof(Datum)` bytes of the (C-collation:
/// raw / non-C: strxfrm) blob into a `Datum`, byteswapping so an unsigned
/// integer compare matches a `memcmp` of the prefix.
///
/// `original_data` is the `VARDATA_ANY(authoritative)` payload bytes.
pub fn varstr_abbrev_convert<'mcx>(
    sss: &mut VarStringSortSupport<'mcx>,
    mcx: Mcx<'mcx>,
    original_data: &[u8],
) -> PgResult<usize> {
    // C:2350 const size_t max_prefix_bytes = sizeof(Datum);
    let max_prefix_bytes = SIZEOF_DATUM;

    // C:2361-2363 memset(pres, 0, max_prefix_bytes); — `res` starts all-NUL.
    let mut pres = [0u8; SIZEOF_DATUM];
    // C:2364 len = VARSIZE_ANY_EXHDR(authoritative);
    let mut len = original_data.len();

    // C:2366-2368 BpChar: ignore trailing spaces.
    if sss.typid == BPCHAROID {
        len = bpchartruelen(original_data);
    }
    let data = &original_data[..len];

    // Whether a cache hit skipped the cardinality hashing (C `goto done`).
    let mut skip_hash = false;

    if sss.collate_c {
        // C:2397-2398 memcpy(pres, authoritative_data, Min(len, max_prefix_bytes));
        let n = len.min(max_prefix_bytes);
        pres[..n].copy_from_slice(&data[..n]);
    } else {
        // C:2400-2482 strxfrm / ICU path.
        let collid = sss.locale.expect("abbrev convert requires a resolved locale");

        // C:2416-2422 maybe reuse the strxfrm() blob from the last call.
        if sss.last_len1 == len as i32
            && sss.cache_blob
            && &sss.buf1[..] == data
        {
            // memcpy(pres, sss->buf2, Min(max_prefix_bytes, sss->last_len2));
            let n = max_prefix_bytes.min(sss.last_len2.max(0) as usize);
            let n = n.min(sss.buf2.len());
            pres[..n].copy_from_slice(&sss.buf2[..n]);
            skip_hash = true;
        } else {
            // C:2424-2430 memcpy(sss->buf1, authoritative_data, len);
            //             buf1[len] = '\0'; last_len1 = len;
            sss.buf1.clear();
            sss.buf1.extend_from_slice(data);
            sss.last_len1 = len as i32;

            let blob = if loc::pg_strxfrm_prefix_enabled::call(collid) {
                // C:2432-2444 prefix transform of the first max_prefix_bytes.
                let buf1: &[u8] = &sss.buf1;
                let prefix = loc::pg_strxfrm_prefix::call(mcx, collid, buf1, max_prefix_bytes)?;
                // C:2443 sss->last_len2 = bsize;
                sss.last_len2 = prefix.len() as i32;
                prefix
            } else {
                // C:2446-2470 full transform (the C loop grows the buffer until
                // the whole blob fits; the seam returns the complete blob).
                let buf1: &[u8] = &sss.buf1;
                let blob = loc::pg_strxfrm::call(mcx, collid, buf1)?;
                // C:2459 sss->last_len2 = bsize;
                sss.last_len2 = blob.len() as i32;
                blob
            };

            // C:2481 memcpy(pres, sss->buf2, Min(max_prefix_bytes, bsize));
            let n = max_prefix_bytes.min(blob.len());
            pres[..n].copy_from_slice(&blob[..n]);

            // Cache the blob in buf2 (C reuses buf2 as the strxfrm scratch).
            sss.buf2.clear();
            sss.buf2.extend_from_slice(&blob);
        }
    }

    // The packed prefix as the native-endian Datum word.
    let mut res = usize::from_ne_bytes(pres);

    if !skip_hash {
        // C:2495-2501 hash the authoritative key (capped at one cache line),
        // mixing in length when truncated, into full_card.
        let hashed = &data[..len.min(PG_CACHE_LINE_SIZE)];
        let mut hash = hashfn::hash_bytes::call(hashed);
        if len > PG_CACHE_LINE_SIZE {
            // C:2498-2499 hash ^= hash_uint32((uint32) len);
            hash ^= hashfn::hash_bytes_uint32::call(len as u32);
        }
        // C:2501 addHyperLogLog(&sss->full_card, hash);
        if let Some(h) = sss.full_card {
            hll::add_hyper_log_log::call(h, hash);
        }

        // C:2504-2515 hash the abbreviated key into abbr_card.
        let abbr_hash = if SIZEOF_DATUM == 8 {
            // C:2506-2511 lohalf/hihalf of res, XORed, then hash_uint32.
            let lohalf = res as u32;
            let hihalf = (res >> 32) as u32;
            hashfn::hash_bytes_uint32::call(lohalf ^ hihalf)
        } else {
            // C:2514 hash_uint32((uint32) res);
            hashfn::hash_bytes_uint32::call(res as u32)
        };
        // C:2517 addHyperLogLog(&sss->abbr_card, abbr_hash);
        if let Some(h) = sss.abbr_card {
            hll::add_hyper_log_log::call(h, abbr_hash);
        }

        // C:2520 sss->cache_blob = true;
        sss.cache_blob = true;
    }

    // C:2531 res = DatumBigEndianToNative(res);
    res = datum_big_endian_to_native(res);

    // C:2537 return res;
    // The abbreviated key is a packed machine word the sort machinery compares
    // by unsigned-integer order (the audited ABI/storage edge); it stays a bare
    // `usize`, never reframed as a value-`Datum`.
    Ok(res)
}

/// C: `varstr_abbrev_abort(int memtupcount, SortSupport ssup)`
/// (varlena.c:2545-2653) — heuristic deciding whether to abandon the
/// abbreviated-key optimization based on the HyperLogLog cardinality estimates.
pub fn varstr_abbrev_abort(sss: &mut VarStringSortSupport<'_>, memtupcount: i32) -> bool {
    // C:2554-2556 Have a little patience.
    if memtupcount < 100 {
        return false;
    }

    // C:2558-2559 estimate cardinalities.
    let mut abbrev_distinct = sss
        .abbr_card
        .map(|h| hll::estimate_hyper_log_log::call(h))
        .unwrap_or(0.0);
    let mut key_distinct = sss
        .full_card
        .map(|h| hll::estimate_hyper_log_log::call(h))
        .unwrap_or(0.0);

    // C:2566-2570 clamp to at least one distinct value.
    if abbrev_distinct <= 1.0 {
        abbrev_distinct = 1.0;
    }
    if key_distinct <= 1.0 {
        key_distinct = 1.0;
    }

    // C:2603 if (abbrev_distinct > key_distinct * sss->prop_card)
    if abbrev_distinct > key_distinct * sss.prop_card {
        // C:2630-2631 decay required cardinality past 10,000 tuples.
        if memtupcount > 10000 {
            sss.prop_card *= 0.65;
        }
        // C:2633 return false;
        return false;
    }

    // C:2652 return true; — abort abbreviation.
    true
}

// ===========================================================================
// Small C-library equivalents.
// ===========================================================================

/// C: `memcmp(a, b, n)` over equal-length slices, returning the sign as C does.
fn memcmp(a: &[u8], b: &[u8]) -> i32 {
    match a.cmp(b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// C: `strcmp(a, b)` over NUL-terminated buffers (here the buffers hold no
/// embedded NUL, so this is the lexicographic byte compare of the whole slices,
/// matching the C tiebreak semantics).
fn strcmp(a: &[u8], b: &[u8]) -> i32 {
    memcmp(a, b)
}

/// C: `strncmp(a, b, n)` over NUL-terminated buffers bounded by `n` bytes.
fn strncmp(a: &[u8], b: &[u8], n: usize) -> i32 {
    let mut i = 0;
    while i < n {
        let ca = a.get(i).copied().unwrap_or(0);
        let cb = b.get(i).copied().unwrap_or(0);
        if ca != cb {
            return if ca < cb { -1 } else { 1 };
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
    0
}

/// C: `strlen(NameStr(*name))` — logical length of a fixed-width `name` buffer,
/// up to the first NUL (or `NAMEDATALEN` if unterminated).
fn name_strlen(name: &[u8; NAMEDATALEN]) -> usize {
    name.iter().position(|&c| c == 0).unwrap_or(NAMEDATALEN)
}

/// C: `DatumBigEndianToNative(x)` (pg_bswap.h) — on a little-endian build this
/// is `BSWAP64(x)` (so that an unsigned-integer compare of the packed prefix
/// matches a big-endian / `memcmp` byte order); on big-endian it is the
/// identity.
fn datum_big_endian_to_native(x: usize) -> usize {
    #[cfg(target_endian = "little")]
    {
        if SIZEOF_DATUM == 8 {
            (x as u64).swap_bytes() as usize
        } else {
            (x as u32).swap_bytes() as usize
        }
    }
    #[cfg(target_endian = "big")]
    {
        x
    }
}
