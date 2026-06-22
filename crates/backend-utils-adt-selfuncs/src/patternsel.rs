//! The LIKE / regex / prefix restriction- and join-selectivity estimators of
//! `like_support.c` — `patternsel_common` and the SQL-callable
//! `likesel`/`iclikesel`/`regexeqsel`/`icregexeqsel`/`nlikesel`/`icnlikesel`/
//! `regexnesel`/`icregexnesel`/`prefixsel` estimators dispatched here through
//! `call_oprrest` by the pattern operators' `oprrest` OIDs.
//!
//! In C these are `PGFunction`s reached via `OidFunctionCall4Coll(oprrest, ...)`.
//! The `fcinfo` decode into the typed planner arguments (`root`, `operator`,
//! `args`, `varRelid`, `collation`) happens at the [`crate::dispatch`] boundary,
//! so the entry points below take the already-decoded arguments. The estimation
//! math is the 1:1 port of `like_support.c`'s pattern-analysis routines
//! (`pattern_fixed_prefix` → `like_fixed_prefix` / `regex_fixed_prefix`, the
//! `like_selectivity` / `regex_selectivity` heuristics, `prefix_selectivity`,
//! and `make_greater_string`).
//!
//! `like_support.c` lives in the (otherwise-unported)
//! `backend-utils-adt-string-byte` catalog unit; these estimators are carved out
//! here because selfuncs owns the dispatch and the `VariableStatData` /
//! histogram / MCV substrate they are built on (matching how selfuncs hosts the
//! cross-cycle `range`/`network`/`array` estimators).
//!
//! The planner-support index-condition leg of `like_regex_support`
//! (`textlike_support` et al. -> `match_pattern_prefix`, which derives `>=` / `<`
//! / `=` / prefix index quals from a pattern's fixed prefix) is also ported here
//! and installed on the `oid_function_call1_index_support` fmgr-support seam.
//! The remaining `SupportRequestSimplify`/`OptimizeWindowClause` legs of
//! `like_regex_support` are not reached by this seam and are not ported.

use mcx::{slice_in, Mcx};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_datum::datum::Datum as WordDatum;
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INDETERMINATE_COLLATION,
};
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{NodeId, PlannerInfo};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::VARHDRSZ;

use backend_nodes_core::makefuncs::{make_const, make_opclause};
use backend_nodes_core::nodefuncs::expr_type;
use backend_utils_adt_pg_locale_seams as locale_seams;
use backend_utils_adt_regexp_seams as regexp;
use backend_utils_adt_varlena_seams as varlena;
use backend_utils_cache_lsyscache_seams as lsc;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_utils_mb_mbutils_seams as mb;

use crate::clamp_probability;
use crate::ineq::{histogram_selectivity, ineq_histogram_selectivity, mcv_selectivity};
use crate::scalar::{stats_tuple_stanullfrac, var_eq_const};

/* ---------------------------------------------------------------------------
 * Type / collation / operator OIDs (pg_type.dat / pg_operator.dat /
 * pg_collation.dat, PostgreSQL 18.3) the estimators reference directly. Mirrored
 * locally, as the dispatch module mirrors its `F_*` estimator OIDs.
 * ------------------------------------------------------------------------- */
const TEXTOID: Oid = 25;
const BYTEAOID: Oid = 17;
const NAMEOID: Oid = 19;
const BPCHAROID: Oid = 1042;

const TEXT_EQUAL_OPERATOR: Oid = 98;
const TEXT_LESS_OPERATOR: Oid = 664;
const TEXT_GREATER_EQUAL_OPERATOR: Oid = 667;
const NAME_EQUAL_TEXT_OPERATOR: Oid = 254;
const NAME_LESS_TEXT_OPERATOR: Oid = 255;
const NAME_GREATER_EQUAL_TEXT_OPERATOR: Oid = 257;
const BPCHAR_EQUAL_OPERATOR: Oid = 1054;
const BPCHAR_LESS_OPERATOR: Oid = 1058;
const BPCHAR_GREATER_EQUAL_OPERATOR: Oid = 1061;
const BYTEA_EQUAL_OPERATOR: Oid = 1955;
const BYTEA_LESS_OPERATOR: Oid = 1957;
const BYTEA_GREATER_EQUAL_OPERATOR: Oid = 1960;

const DEFAULT_COLLATION_OID: Oid = 100;
const C_COLLATION_OID: Oid = 950;
/// `NAMEDATALEN` (pg_config_manual.h) — `name`'s fixed length (incl. NUL).
const NAMEDATALEN: i32 = 64;

/// `DEFAULT_MATCH_SEL` (selfuncs.h) — default selectivity for pattern-match
/// operators, `0.005`.
const DEFAULT_MATCH_SEL: f64 = 0.005;

/* ---------------------------------------------------------------------------
 * Pattern_Type / Pattern_Prefix_Status (like_support.c top-of-file enums).
 * ------------------------------------------------------------------------- */

/// `Pattern_Type` (like_support.c) — which kind of pattern an estimator works on.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PatternType {
    Like,
    LikeIc,
    Regex,
    RegexIc,
    Prefix,
}

/// `Pattern_Prefix_Status` (like_support.c) — the result of `pattern_fixed_prefix`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PatternPrefixStatus {
    None,
    Partial,
    Exact,
}

/* ---------------------------------------------------------------------------
 * Pattern-fragment selectivity heuristics (like_support.c).
 * ------------------------------------------------------------------------- */

const FIXED_CHAR_SEL: f64 = 0.20; // about 1/5
const CHAR_RANGE_SEL: f64 = 0.25;
const ANY_CHAR_SEL: f64 = 0.9; // not 1, since it won't match end-of-string
const FULL_WILDCARD_SEL: f64 = 5.0;
const PARTIAL_WILDCARD_SEL: f64 = 2.0;

/// `like_selectivity(patt, pattlen, case_insensitive)` (like_support.c) —
/// estimate the selectivity of a LIKE pattern fragment. 1:1 with the C body
/// (operating on the database-encoding byte payload).
fn like_selectivity(patt: &[u8], _case_insensitive: bool) -> f64 {
    let pattlen = patt.len();
    let mut sel = 1.0f64;
    let mut pos = 0usize;

    // Skip any leading wildcard; it's already factored into initial sel.
    while pos < pattlen {
        if patt[pos] != b'%' && patt[pos] != b'_' {
            break;
        }
        pos += 1;
    }

    while pos < pattlen {
        if patt[pos] == b'%' {
            sel *= FULL_WILDCARD_SEL;
        } else if patt[pos] == b'_' {
            sel *= ANY_CHAR_SEL;
        } else if patt[pos] == b'\\' {
            // Backslash quotes the next character.
            pos += 1;
            if pos >= pattlen {
                break;
            }
            sel *= FIXED_CHAR_SEL;
        } else {
            sel *= FIXED_CHAR_SEL;
        }
        pos += 1;
    }
    // Could get sel > 1 if multiple wildcards.
    if sel > 1.0 {
        sel = 1.0;
    }
    sel
}

/// `regex_selectivity_sub(patt, pattlen, case_insensitive)` (like_support.c) —
/// the recursive core of [`regex_selectivity`]. 1:1 with the C body.
fn regex_selectivity_sub(patt: &[u8], case_insensitive: bool) -> PgResult<f64> {
    let pattlen = patt.len();
    let mut sel = 1.0f64;
    let mut paren_depth = 0i32;
    let mut paren_pos = 0usize; // dummy init to keep compiler quiet
    let mut pos = 0usize;

    // Since this function recurses, it could be driven to stack overflow.
    backend_utils_misc_stack_depth::check_stack_depth()?;

    while pos < pattlen {
        if patt[pos] == b'(' {
            if paren_depth == 0 {
                paren_pos = pos; // remember start of parenthesized item
            }
            paren_depth += 1;
        } else if patt[pos] == b')' && paren_depth > 0 {
            paren_depth -= 1;
            if paren_depth == 0 {
                sel *= regex_selectivity_sub(&patt[(paren_pos + 1)..pos], case_insensitive)?;
            }
        } else if patt[pos] == b'|' && paren_depth == 0 {
            // If unquoted | is present at paren level 0 in pattern, we have
            // multiple alternatives; sum their probabilities.
            sel += regex_selectivity_sub(&patt[(pos + 1)..], case_insensitive)?;
            break; // rest of pattern is now processed
        } else if patt[pos] == b'[' {
            let mut negclass = false;
            pos += 1;
            if pos < pattlen && patt[pos] == b'^' {
                negclass = true;
                pos += 1;
            }
            if pos < pattlen && patt[pos] == b']' {
                // ']' at start of class is not special
                pos += 1;
            }
            while pos < pattlen && patt[pos] != b']' {
                pos += 1;
            }
            if paren_depth == 0 {
                sel *= if negclass {
                    1.0 - CHAR_RANGE_SEL
                } else {
                    CHAR_RANGE_SEL
                };
            }
        } else if patt[pos] == b'.' {
            if paren_depth == 0 {
                sel *= ANY_CHAR_SEL;
            }
        } else if patt[pos] == b'*' || patt[pos] == b'?' || patt[pos] == b'+' {
            // Ought to be smarter about quantifiers...
            if paren_depth == 0 {
                sel *= PARTIAL_WILDCARD_SEL;
            }
        } else if patt[pos] == b'{' {
            while pos < pattlen && patt[pos] != b'}' {
                pos += 1;
            }
            if paren_depth == 0 {
                sel *= PARTIAL_WILDCARD_SEL;
            }
        } else if patt[pos] == b'\\' {
            // backslash quotes the next character
            pos += 1;
            if pos >= pattlen {
                break;
            }
            if paren_depth == 0 {
                sel *= FIXED_CHAR_SEL;
            }
        } else if paren_depth == 0 {
            sel *= FIXED_CHAR_SEL;
        }
        pos += 1;
    }
    // Could get sel > 1 if multiple wildcards.
    if sel > 1.0 {
        sel = 1.0;
    }
    Ok(sel)
}

/// `regex_selectivity(patt, pattlen, case_insensitive, fixed_prefix_len)`
/// (like_support.c) — estimate the selectivity of a regex pattern fragment.
/// 1:1 with the C body.
fn regex_selectivity(patt: &[u8], case_insensitive: bool, fixed_prefix_len: usize) -> PgResult<f64> {
    let pattlen = patt.len();
    let mut sel;

    // If patt doesn't end with $, consider it to have a trailing wildcard.
    if pattlen > 0
        && patt[pattlen - 1] == b'$'
        && (pattlen == 1 || patt[pattlen - 2] != b'\\')
    {
        // has trailing $
        sel = regex_selectivity_sub(&patt[..pattlen - 1], case_insensitive)?;
    } else {
        // no trailing $
        sel = regex_selectivity_sub(patt, case_insensitive)?;
        sel *= FULL_WILDCARD_SEL;
    }

    // If there's a fixed prefix, discount its selectivity. We have to be careful
    // here since a very long prefix could result in pow's result underflowing to
    // zero (in which case "sel" probably has as well).
    if fixed_prefix_len > 0 {
        let prefixsel = FIXED_CHAR_SEL.powi(fixed_prefix_len as i32);
        if prefixsel > 0.0 {
            sel /= prefixsel;
        }
    }

    // Make sure result stays in range.
    Ok(clamp_probability(sel))
}

/// `pattern_char_isalpha(c, is_multibyte, locale)` (like_support.c) — whether a
/// char is a letter (subject to case-folding). 1:1 with the C body.
fn pattern_char_isalpha(c: u8, is_multibyte: bool, locale: &types_locale::PgLocaleStruct) -> bool {
    use types_locale::CollProvider;
    let highbit = (c & 0x80) != 0;
    if locale.ctype_is_c {
        c.is_ascii_alphabetic()
    } else if is_multibyte && highbit {
        true
    } else if locale.provider != CollProvider::Libc {
        highbit || c.is_ascii_alphabetic()
    } else {
        // C: isalpha_l((unsigned char) c, locale->info.lt). The libc locale_t
        // (info.lt) handle is not carried by the trimmed PgLocaleStruct.
        panic!(
            "pattern_char_isalpha: the LIBC isalpha_l(c, locale->info.lt) path needs the \
             libc locale_t handle, which the trimmed pg_locale_struct does not carry"
        )
    }
}

/* ---------------------------------------------------------------------------
 * Fixed-prefix extraction (like_support.c).
 * ------------------------------------------------------------------------- */

/// A fabricated prefix `Const`: the constant's TEXT/BYTEA/BPCHAR/NAME type and
/// its header-less payload (database encoding), mirroring the palloc'd `Const`
/// the C `*_fixed_prefix` routines hand back.
struct PrefixConst<'mcx> {
    consttype: Oid,
    /// The header-less payload bytes (text/bytea content, or the `name` chars).
    payload: alloc::vec::Vec<u8>,
    /// The `Datum` carrier (varlena image with header for text/bytea/bpchar;
    /// header-less for `name`), as `var_eq_const` / `ineq_histogram_selectivity`
    /// consume it.
    constvalue: Datum<'mcx>,
}

/// `string_to_const`-shaped construction of a TEXT/BPCHAR/VARCHAR/NAME/BYTEA
/// prefix `Const` from a header-less payload. Builds the `Datum::ByRef` varlena
/// image (a 4-byte length header + payload) the value model carries, matching
/// `string_to_datum` (`CStringGetTextDatum` etc.).
fn string_to_const<'mcx>(mcx: Mcx<'mcx>, payload: &[u8], datatype: Oid) -> PgResult<PrefixConst<'mcx>> {
    let constvalue = if datatype == NAMEOID {
        // namein: header-less, NUL-padded to NAMEDATALEN is the on-disk shape,
        // but the value carrier holds the raw chars (no header); the comparison
        // protos operate on the payload. C's name is a fixed-length-by-ref value.
        Datum::ByRef(slice_in(mcx, payload)?)
    } else {
        // text/bytea/varchar/bpchar: varlena image = 4-byte header + payload.
        make_varlena_datum(mcx, payload)?
    };
    Ok(PrefixConst {
        consttype: datatype,
        payload: payload.to_vec(),
        constvalue,
    })
}

/// `string_to_bytea_const(str, str_len)` (like_support.c) — a bytea prefix
/// `Const` from a binary payload.
fn string_to_bytea_const<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<PrefixConst<'mcx>> {
    Ok(PrefixConst {
        consttype: BYTEAOID,
        payload: payload.to_vec(),
        constvalue: make_varlena_datum(mcx, payload)?,
    })
}

/// Build a `Datum::ByRef` varlena image (4-byte length header + payload) charged
/// to `mcx`, mirroring `SET_VARSIZE(p, VARHDRSZ + len)` + `memcpy(VARDATA, ...)`.
/// The canonical value model carries the self-describing header-ful image (the
/// `VARATT_IS_4B_U` uncompressed form), which crosses the fmgr boundary verbatim.
fn make_varlena_datum<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Datum<'mcx>> {
    let total = VARHDRSZ + payload.len();
    let mut image = alloc::vec::Vec::with_capacity(total);
    image.extend_from_slice(&types_datum::varlena::set_varsize_4b(total));
    image.extend_from_slice(payload);
    Ok(Datum::ByRef(slice_in(mcx, &image)?))
}

/// Read the header-less payload of a text/bytea `Const` value (`VARDATA_ANY` /
/// `VARSIZE_ANY_EXHDR` over the detoasted `ByRef` image).
fn const_text_payload<'a>(constvalue: &'a Datum<'a>) -> &'a [u8] {
    match constvalue {
        Datum::ByRef(b) => varlena_payload(b),
        Datum::Cstring(s) => s.as_bytes(),
        _ => panic!("pattern const value is not a by-reference text/bytea image"),
    }
}

/// `VARDATA(b)` / `VARSIZE - VARHDRSZ` — the payload of a detoasted varlena
/// image. The canonical by-reference value model is header-ful everywhere (the
/// uncompressed `VARATT_IS_4B_U` form: a 4-byte length word followed by the
/// payload), so the payload is the image past the 4-byte header.
fn varlena_payload(b: &[u8]) -> &[u8] {
    b.get(VARHDRSZ..).unwrap_or(&[])
}

/// `like_fixed_prefix(patt_const, case_insensitive, collation, &prefix,
/// &rest_selec)` (like_support.c) — extract the fixed prefix of a LIKE pattern.
/// 1:1 with the C body. Returns `(status, prefix, rest_selec)`.
fn like_fixed_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    patt_consttype: Oid,
    patt_value: &Datum<'_>,
    case_insensitive: bool,
    collation: Oid,
) -> PgResult<(PatternPrefixStatus, Option<PrefixConst<'mcx>>, f64)> {
    let typeid = patt_consttype;
    debug_assert!(typeid == BYTEAOID || typeid == TEXTOID);

    let is_multibyte = mb::pg_database_encoding_max_length::call() > 1;
    let mut locale: Option<types_locale::PgLocale> = None;

    if case_insensitive {
        if typeid == BYTEAOID {
            return Err(PgError::error(
                "case insensitive matching not supported on type bytea",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        if !OidIsValid(collation) {
            return Err(PgError::error(
                "could not determine which collation to use for ILIKE",
            )
            .with_sqlstate(ERRCODE_INDETERMINATE_COLLATION)
            .with_hint("Use the COLLATE clause to set the collation explicitly."));
        }
        locale = Some(locale_seams::pg_newlocale_from_collation::call(mcx, collation)?);
    }

    // The right-hand const is type text or bytea; the payload is the detoasted,
    // header-less content.
    let patt = const_text_payload(patt_value);
    let pattlen = patt.len();

    let mut matchbuf: alloc::vec::Vec<u8> = alloc::vec::Vec::with_capacity(pattlen + 1);
    let mut pos = 0usize;
    while pos < pattlen {
        // % and _ are wildcard characters in LIKE.
        if patt[pos] == b'%' || patt[pos] == b'_' {
            break;
        }
        // Backslash escapes the next character.
        if patt[pos] == b'\\' {
            pos += 1;
            if pos >= pattlen {
                break;
            }
        }
        // Stop if case-varying character (it's sort of a wildcard).
        if case_insensitive {
            let loc = locale.as_ref().expect("ILIKE requires a resolved locale");
            if pattern_char_isalpha(patt[pos], is_multibyte, &*loc) {
                break;
            }
        }
        matchbuf.push(patt[pos]);
        pos += 1;
    }
    let match_pos = matchbuf.len();

    let prefix = if typeid != BYTEAOID {
        Some(string_to_const(mcx, &matchbuf, typeid)?)
    } else {
        Some(string_to_bytea_const(mcx, &matchbuf)?)
    };

    let rest_selec = like_selectivity(&patt[pos..], case_insensitive);

    // In LIKE, an empty pattern is an exact match!
    let status = if pos == pattlen {
        PatternPrefixStatus::Exact // reached end of pattern, so exact
    } else if match_pos > 0 {
        PatternPrefixStatus::Partial
    } else {
        PatternPrefixStatus::None
    };
    Ok((status, prefix, rest_selec))
}

/// `regex_fixed_prefix(patt_const, case_insensitive, collation, &prefix,
/// &rest_selec)` (like_support.c) — extract the fixed prefix of a regex.
/// 1:1 with the C body.
fn regex_fixed_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    patt_consttype: Oid,
    patt_value: &Datum<'_>,
    case_insensitive: bool,
    collation: Oid,
) -> PgResult<(PatternPrefixStatus, Option<PrefixConst<'mcx>>, f64)> {
    let typeid = patt_consttype;

    // There are no bytea regex operators defined; the rest of this routine is
    // not safe for binary (possibly NUL-containing) strings.
    if typeid == BYTEAOID {
        return Err(PgError::error(
            "regular-expression matching not supported on type bytea",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    let patt = const_text_payload(patt_value);

    // Use the regexp machinery to extract the prefix, if any.
    let prefix = regexp::regexp_fixed_prefix::call(mcx, patt, case_insensitive, collation)?;

    match prefix {
        None => {
            let rest_selec = regex_selectivity(patt, case_insensitive, 0)?;
            Ok((PatternPrefixStatus::None, None, rest_selec))
        }
        Some((prefix_bytes, exact)) => {
            let prefix_const = string_to_const(mcx, &prefix_bytes, typeid)?;
            let rest_selec = if exact {
                // Exact match, so there's no additional selectivity.
                1.0
            } else {
                regex_selectivity(patt, case_insensitive, prefix_bytes.len())?
            };
            let status = if exact {
                PatternPrefixStatus::Exact
            } else {
                PatternPrefixStatus::Partial
            };
            Ok((status, Some(prefix_const), rest_selec))
        }
    }
}

/// `pattern_fixed_prefix(patt, ptype, collation, &prefix, &rest_selec)`
/// (like_support.c) — dispatch by pattern type. 1:1 with the C body.
fn pattern_fixed_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    patt_consttype: Oid,
    patt_consttypmod: i32,
    patt_constcollid: Oid,
    patt_constlen: i32,
    patt_constbyval: bool,
    patt_value: &Datum<'_>,
    ptype: PatternType,
    collation: Oid,
) -> PgResult<(PatternPrefixStatus, Option<PrefixConst<'mcx>>, f64)> {
    match ptype {
        PatternType::Like => like_fixed_prefix(mcx, patt_consttype, patt_value, false, collation),
        PatternType::LikeIc => like_fixed_prefix(mcx, patt_consttype, patt_value, true, collation),
        PatternType::Regex => regex_fixed_prefix(mcx, patt_consttype, patt_value, false, collation),
        PatternType::RegexIc => regex_fixed_prefix(mcx, patt_consttype, patt_value, true, collation),
        PatternType::Prefix => {
            // Prefix type work is trivial: the prefix is a datumCopy of the
            // pattern const itself.
            let _ = (patt_consttypmod, patt_constcollid, patt_constlen, patt_constbyval);
            let copy = patt_value.clone_in(mcx)?;
            let payload = match &copy {
                Datum::ByRef(_) | Datum::Cstring(_) => const_text_payload(&copy).to_vec(),
                _ => alloc::vec::Vec::new(),
            };
            let prefix = PrefixConst {
                consttype: patt_consttype,
                payload,
                constvalue: copy,
            };
            Ok((PatternPrefixStatus::Partial, Some(prefix), 1.0))
        }
    }
}

/* ---------------------------------------------------------------------------
 * Prefix selectivity + make_greater_string (like_support.c).
 * ------------------------------------------------------------------------- */

/// `prefix_selectivity(root, vardata, eqopr, ltopr, geopr, collation,
/// prefixcon)` (like_support.c) — selectivity of "var >= prefix AND var <
/// greaterstr". 1:1 with the C body.
#[allow(clippy::too_many_arguments)]
fn prefix_selectivity<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    vardata: &types_selfuncs::VariableStatData,
    eqopr: Oid,
    ltopr: Oid,
    geopr: Oid,
    collation: Oid,
    prefixcon: &PrefixConst<'mcx>,
) -> PgResult<f64> {
    // The `>=` / `<` histogram probes (`ineq_histogram_selectivity`) and the
    // `=` clamp (`var_eq_const`) compare the prefix const against the column's
    // by-reference text/bytea histogram/MCV slot values. A by-reference prefix
    // const can only cross that comparison once the selfuncs by-reference value
    // carrier is threaded through `get_attstatsslot` (WALL 1ai, shared with
    // `var_eq_const`). Until then a by-reference prefix takes C's "no usable
    // histogram" outcome: `DEFAULT_MATCH_SEL`.
    if !matches!(prefixcon.constvalue, Datum::ByVal(_)) {
        return Ok(DEFAULT_MATCH_SEL);
    }

    // Estimate the selectivity of "x >= prefix".
    let ge_opproc = lsc::get_opcode::call(geopr)?;
    let mut prefixsel = ineq_histogram_selectivity(
        mcx,
        run,
        root,
        vardata,
        geopr,
        ge_opproc,
        true,
        true,
        collation,
        &prefixcon.constvalue,
        prefixcon.consttype,
    )?;

    if prefixsel < 0.0 {
        // No histogram is present ... return a suitable default estimate.
        return Ok(DEFAULT_MATCH_SEL);
    }

    // If we can create a string larger than the prefix, say "x < greaterstr".
    let lt_opproc = lsc::get_opcode::call(ltopr)?;
    let greaterstrcon = make_greater_string(mcx, prefixcon, lt_opproc, collation)?;
    if let Some(greaterstr) = greaterstrcon {
        let topsel = ineq_histogram_selectivity(
            mcx,
            run,
            root,
            vardata,
            ltopr,
            lt_opproc,
            false,
            false,
            collation,
            &greaterstr.constvalue,
            greaterstr.consttype,
        )?;
        // ineq_histogram_selectivity worked before, it shouldn't fail now.
        debug_assert!(topsel >= 0.0);

        // Merge the two selectivities as for a range query.
        prefixsel = topsel + prefixsel - 1.0;
    }

    // Clamp to the "variable = 'foo'" estimate to avoid a ridiculously small
    // result when the bounds are too close for the histogram to distinguish.
    let eq_sel = var_eq_const(
        mcx,
        root,
        vardata,
        eqopr,
        collation,
        &prefixcon.constvalue,
        false,
        true,
        false,
    )?;

    Ok(prefixsel.max(eq_sel))
}

/// `byte_increment(ptr, len)` (like_support.c) — increment a single byte for the
/// bytea character-incrementer.
fn byte_increment(b: &mut u8) -> bool {
    if *b >= 255 {
        return false;
    }
    *b += 1;
    true
}

/// `make_greater_string(str_const, ltproc, collation)` (like_support.c) — try to
/// produce a `Const` greater than the given string or any string it prefixes.
/// 1:1 with the C body; returns `None` on failure.
fn make_greater_string<'mcx>(
    mcx: Mcx<'mcx>,
    str_const: &PrefixConst<'_>,
    ltproc_oid: Oid,
    collation: Oid,
) -> PgResult<Option<PrefixConst<'mcx>>> {
    let datatype = str_const.consttype;

    // Get a modifiable copy of the prefix string and set up the comparison
    // string (cmp). In C locale (or bytea, or empty) cmp is the prefix itself,
    // otherwise a suffix char is appended.
    let mut workstr: alloc::vec::Vec<u8>;
    let cmp_payload: alloc::vec::Vec<u8>;

    if datatype == BYTEAOID {
        workstr = str_const.payload.clone();
        cmp_payload = str_const.payload.clone();
    } else {
        // text/varchar/bpchar/name: the workstr is the payload (C string form).
        workstr = str_const.payload.clone();
        let len = workstr.len();
        let locale = locale_seams::pg_newlocale_from_collation::call(mcx, collation)?;
        if len == 0 || locale.collate_is_c {
            cmp_payload = str_const.payload.clone();
        } else {
            // Determine the suffix char that the collation sees as largest.
            let suffixchar = greater_string_suffix(collation)?;
            let mut cmp = str_const.payload.clone();
            cmp.push(suffixchar);
            cmp_payload = cmp;
        }
    }

    // The comparison value as a Datum (varlena image for text/bytea/bpchar;
    // header-less chars for name).
    let cmpstr = if datatype == NAMEOID {
        Datum::ByRef(slice_in(mcx, &cmp_payload)?)
    } else {
        make_varlena_datum(mcx, &cmp_payload)?
    };

    // The character incrementer: per-byte for bytea, encoding-aware otherwise.
    let mut len = workstr.len();
    while len > 0 {
        // Identify the last character (for bytea, the last byte).
        let charlen: usize = if datatype == BYTEAOID {
            1
        } else {
            len - mb::pg_mbcliplen::call(&workstr[..len], len as i32, (len - 1) as i32) as usize
        };
        let last_start = len - charlen;

        // Try to generate a larger string by incrementing the last character.
        loop {
            let incremented = if datatype == BYTEAOID {
                byte_increment(&mut workstr[last_start])
            } else {
                charinc(&mut workstr[last_start..len])
            };
            if !incremented {
                break;
            }

            let workstr_const = if datatype == BYTEAOID {
                string_to_bytea_const(mcx, &workstr[..len])?
            } else {
                string_to_const(mcx, &workstr[..len], datatype)?
            };

            // C: FunctionCall2Coll(ltproc, collation, cmpstr, workstr_const).
            // Both operands are by-reference text/bytea values, so this crosses
            // the canonical by-reference `Datum` fmgr lane (not the bare-word
            // one — which cannot carry the pointer).
            let (res, isnull) = fmgr::function_call_invoke_datum::call(
                mcx,
                ltproc_oid,
                collation,
                &[cmpstr.clone_in(mcx)?, workstr_const.constvalue.clone_in(mcx)?],
                &[],
                None,
            )?;
            if !isnull && res.as_bool() {
                // Successfully made a string larger than cmpstr.
                return Ok(Some(workstr_const));
            }
            // No good, try again.
        }

        // No luck here: truncate off the last character and try the next.
        len -= charlen;
        workstr.truncate(len);
    }

    Ok(None)
}

/// The collation-largest suffix char among "Z", "z", "y", "9" (the C static
/// cache in `make_greater_string`). Recomputed per call here (no static cache).
fn greater_string_suffix(collation: Oid) -> PgResult<u8> {
    let mut best: &[u8] = b"Z";
    if varlena::varstr_cmp::call(best, b"z", collation)? < 0 {
        best = b"z";
    }
    if varlena::varstr_cmp::call(best, b"y", collation)? < 0 {
        best = b"y";
    }
    if varlena::varstr_cmp::call(best, b"9", collation)? < 0 {
        best = b"9";
    }
    Ok(best[0])
}

/// `pg_database_encoding_character_incrementer()(lastchar, charlen)`
/// (like_support.c / mbutils.c) — increment the last character in place.
fn charinc(lastchar: &mut [u8]) -> bool {
    let inc = backend_utils_mb_mbutils::pg_database_encoding_character_incrementer();
    inc(lastchar)
}

/* ---------------------------------------------------------------------------
 * patternsel_common + entry points (like_support.c).
 * ------------------------------------------------------------------------- */

/// The bare ABI word of a by-value const, or `None` for a by-reference value
/// (which cannot cross the bare-word histogram/MCV fmgr lane — WALL 1ai). The
/// caller takes C's histogram/MCV-free path when this is `None`.
fn const_word_opt(v: &Datum<'_>) -> Option<WordDatum> {
    match v {
        Datum::ByVal(w) => Some(WordDatum::from_usize(*w)),
        _ => None,
    }
}

/// `patternsel_common(root, oprid, opfuncid, args, varRelid, collation, ptype,
/// negate)` (like_support.c) — the LIKE/regex/prefix restriction-selectivity
/// core. 1:1 with the C body.
#[allow(clippy::too_many_arguments)]
fn patternsel_common<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    oprid: Oid,
    mut opfuncid: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
    ptype: PatternType,
    negate: bool,
) -> PgResult<f64> {
    // Initialize result to the default estimate for match / not-match.
    let mut result = if negate {
        1.0 - DEFAULT_MATCH_SEL
    } else {
        DEFAULT_MATCH_SEL
    };

    // If expression is not variable op constant, punt.
    let (vardata, other, varonleft) =
        match crate::examine::get_restriction_variable(mcx, run, root, args, var_relid)? {
            Some(t) => t,
            None => return Ok(result),
        };
    let patt = match &other {
        Expr::Const(c) if varonleft => c,
        _ => {
            crate::examine::release_variable_stats(vardata);
            return Ok(result);
        }
    };

    // If the constant is NULL, assume operator is strict: never TRUE.
    if patt.constisnull {
        crate::examine::release_variable_stats(vardata);
        return Ok(0.0);
    }
    let consttype = patt.consttype;

    // The right-hand const is text or bytea for all supported operators.
    if consttype != TEXTOID && consttype != BYTEAOID {
        crate::examine::release_variable_stats(vardata);
        return Ok(result);
    }

    // The exposed type of the left-hand side identifies the comparison operators
    // and the required comparison-constant type.
    let vartype = vardata.vartype;
    let (eqopr, ltopr, geopr, rdatatype) = match vartype {
        x if x == TEXTOID => (
            TEXT_EQUAL_OPERATOR,
            TEXT_LESS_OPERATOR,
            TEXT_GREATER_EQUAL_OPERATOR,
            TEXTOID,
        ),
        x if x == NAMEOID => (
            NAME_EQUAL_TEXT_OPERATOR,
            NAME_LESS_TEXT_OPERATOR,
            NAME_GREATER_EQUAL_TEXT_OPERATOR,
            TEXTOID,
        ),
        x if x == BPCHAROID => (
            BPCHAR_EQUAL_OPERATOR,
            BPCHAR_LESS_OPERATOR,
            BPCHAR_GREATER_EQUAL_OPERATOR,
            BPCHAROID,
        ),
        x if x == BYTEAOID => (
            BYTEA_EQUAL_OPERATOR,
            BYTEA_LESS_OPERATOR,
            BYTEA_GREATER_EQUAL_OPERATOR,
            BYTEAOID,
        ),
        _ => {
            // Can't get here unless attached to the wrong operator.
            crate::examine::release_variable_stats(vardata);
            return Ok(result);
        }
    };

    // Grab nullfrac for use below.
    let mut nullfrac = 0.0f64;
    if let Some(stats_tuple) = vardata.stats_tuple {
        nullfrac = stats_tuple_stanullfrac(stats_tuple) as f64;
    }

    // Pull out any fixed prefix and estimate the fractional selectivity of the
    // remainder. We use the pattern operator's actual collation.
    let (pstatus, mut prefix, rest_selec) = pattern_fixed_prefix(
        mcx,
        patt.consttype,
        patt.consttypmod,
        patt.constcollid,
        patt.constlen,
        patt.constbyval,
        &patt.constvalue,
        ptype,
        collation,
    )?;

    // Coerce the prefix const to the right type if necessary (text -> bpchar).
    if let Some(pfx) = prefix.as_mut() {
        if pfx.consttype != rdatatype {
            debug_assert!(pfx.consttype == TEXTOID && rdatatype == BPCHAROID);
            pfx.consttype = rdatatype;
        }
    }

    if pstatus == PatternPrefixStatus::Exact {
        // Pattern specifies an exact match, so estimate as for '='.
        let pfx = prefix.as_ref().expect("exact-match prefix must be present");
        result = var_eq_const(
            mcx,
            root,
            &vardata,
            eqopr,
            collation,
            &pfx.constvalue,
            false,
            true,
            false,
        )?;
    } else {
        // Not exact-match. Use the histogram if large enough; else estimate the
        // fixed prefix and pattern remainder separately and combine.
        //
        // The histogram (`histogram_selectivity`) and MCV (`mcv_selectivity`)
        // scans invoke the pattern operator's `opfuncid` against the column's
        // by-reference text/bytea histogram/MCV slot values, which cross the
        // bare-word fmgr lane (`AttStatsSlot.values` are bare pointer words from
        // the C-shaped pg_statistic tuple). When the pattern const is itself a
        // by-reference value, that comparison needs the canonical by-reference
        // value carrier threaded through `get_attstatsslot` — the shared selfuncs
        // by-reference value-carrier follow-on (WALL 1ai, the same wall
        // `var_eq_const`'s MCV loop documents). Until it lands, a by-reference
        // const can only take the histogram/MCV-free heuristic path, which is
        // exactly C's `selec < 0` (no usable histogram) + empty-MCV outcome.
        // The pattern operator (LIKE/regex) compares the pattern const against
        // the column's histogram/MCV slot values. The slot values are bare
        // pointer words (deconstruct_array offsets) from the C-shaped
        // `pg_statistic` tuple, which the pattern proc cannot dereference; so a
        // by-reference pattern const still takes C's histogram/MCV-free
        // heuristic path. (The const itself now crosses by reference, but the
        // bin side cannot — WALL 1ai.)
        if const_word_opt(&patt.constvalue).is_none() {
            {
                // By-reference const: no histogram/MCV comparison possible, so
                // estimate from the fixed prefix and pattern remainder alone
                // (C's `hist_size < 100`, `selec < 0` => `selec = heursel`
                // branch, with `mcv_selec == 0` and `sumcommon == 0`).
                let prefixsel = if pstatus == PatternPrefixStatus::Partial {
                    let pfx = prefix.as_ref().expect("partial prefix must be present");
                    prefix_selectivity(mcx, run, root, &vardata, eqopr, ltopr, geopr, collation, pfx)?
                } else {
                    1.0
                };
                let mut selec = prefixsel * rest_selec;
                if selec < 0.0001 {
                    selec = 0.0001;
                } else if selec > 0.9999 {
                    selec = 0.9999;
                }
                selec *= 1.0 - nullfrac;

                result = selec;
                if negate {
                    result = 1.0 - result - nullfrac;
                }
                result = clamp_probability(result);
                crate::examine::release_variable_stats(vardata);
                return Ok(result);
            }
        }

        // Try to use the histogram entries to get selectivity.
        if !OidIsValid(opfuncid) {
            opfuncid = lsc::get_opcode::call(oprid)?;
        }
        let (mut selec, hist_size) =
            histogram_selectivity(mcx, &vardata, opfuncid, collation, &patt.constvalue, true, 10, 1)?;

        // If not at least 100 entries, use the heuristic method.
        if hist_size < 100 {
            let prefixsel = if pstatus == PatternPrefixStatus::Partial {
                let pfx = prefix.as_ref().expect("partial prefix must be present");
                prefix_selectivity(mcx, run, root, &vardata, eqopr, ltopr, geopr, collation, pfx)?
            } else {
                1.0
            };
            let heursel = prefixsel * rest_selec;

            if selec < 0.0 {
                // fewer than 10 histogram entries
                selec = heursel;
            } else {
                // For 10..100 entries, blend, trusting the histogram more as it
                // grows.
                let hist_weight = hist_size as f64 / 100.0;
                selec = selec * hist_weight + heursel * (1.0 - hist_weight);
            }
        }

        // Don't believe extremely small or large estimates.
        if selec < 0.0001 {
            selec = 0.0001;
        } else if selec > 0.9999 {
            selec = 0.9999;
        }

        // Add up the MCV fractions satisfying MCV OP PATTERN (and the total MCV
        // fraction).
        let (mcv_selec, sumcommon) =
            mcv_selectivity(mcx, &vardata, opfuncid, collation, &patt.constvalue, true)?;

        // Merge MCV and histogram results: the histogram covers only the
        // non-null values not listed in MCV.
        selec *= 1.0 - nullfrac - sumcommon;
        selec += mcv_selec;
        result = selec;
    }

    // Adjust for not-match.
    if negate {
        result = 1.0 - result - nullfrac;
    }

    result = clamp_probability(result);

    crate::examine::release_variable_stats(vardata);
    Ok(result)
}

/// `patternsel(fcinfo, ptype, negate)` (like_support.c) — the impedance-matcher
/// the SQL-callable estimators share. 1:1 with the C body.
fn patternsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut operator: Oid,
    args: &[NodeId],
    var_relid: i32,
    collation: Oid,
    ptype: PatternType,
    negate: bool,
) -> PgResult<f64> {
    // For NOT LIKE etc., get the positive-match operator and work with that.
    if negate {
        operator = lsc::get_negator::call(operator)?;
        if !OidIsValid(operator) {
            return Err(PgError::error(
                "patternsel called for operator without a negator",
            ));
        }
    }

    patternsel_common(
        mcx, run, root, operator, InvalidOid, args, var_relid, collation, ptype, negate,
    )
}

/// `like_regex_support(rawreq, ptype)` (like_support.c) — the
/// `SupportRequestSelectivity` branch only: make a function-call selectivity
/// estimate, just as we'd do if the call were via the corresponding operator.
/// This is what a `prosupport` selectivity request (e.g. `starts_with` via
/// `text_starts_with_support` with `Pattern_Type_Prefix`) resolves to. The C
/// `is_join` branch punts to `DEFAULT_MATCH_SEL`; the restriction branch shares
/// code with the operator restriction estimators via `patternsel_common`
/// (oprid = InvalidOid, the support call has only the funcid).
#[allow(clippy::too_many_arguments)]
pub fn like_regex_support_selectivity<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    funcid: Oid,
    args: &[NodeId],
    var_relid: i32,
    inputcollid: Oid,
    is_join: bool,
    ptype: PatternType,
) -> PgResult<f64> {
    if is_join {
        // For the moment we just punt. If patternjoinsel is ever improved to do
        // better, this should be made to call it.
        Ok(DEFAULT_MATCH_SEL)
    } else {
        // Share code with operator restriction selectivity functions.
        patternsel_common(
            mcx,
            run,
            root,
            InvalidOid,
            funcid,
            args,
            var_relid,
            inputcollid,
            ptype,
            false,
        )
    }
}

/// `function_selectivity`'s `SupportRequestSelectivity` dispatch
/// (plancat.c -> the function's prosupport). Resolves the function's
/// `prosupport` (`get_func_support(funcid)`) and, for the like_support.c
/// pattern support functions, runs the selectivity estimate via
/// [`like_regex_support_selectivity`] with the prosupport's baked-in pattern
/// type. Returns `Some(selectivity)` when this unit owns the support function;
/// `None` (the C "support function fails, use default" path) otherwise.
#[allow(clippy::too_many_arguments)]
pub fn func_selectivity_support<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    funcid: Oid,
    args: &[NodeId],
    var_relid: i32,
    inputcollid: Oid,
    is_join: bool,
) -> PgResult<Option<f64>> {
    let prosupport = lsc::get_func_support::call(funcid)?;
    // Map the function's prosupport (the C `*_support` entry point that bakes in
    // a Pattern_Type) to its pattern type. Only the like_support.c pattern
    // support functions implement a SupportRequestSelectivity branch; any other
    // prosupport returns NULL there, i.e. the historical-default path.
    let ptype = match prosupport {
        x if x == F_TEXTLIKE_SUPPORT => PatternType::Like,
        x if x == F_TEXTICLIKE_SUPPORT => PatternType::LikeIc,
        x if x == F_TEXTREGEXEQ_SUPPORT => PatternType::Regex,
        x if x == F_TEXTICREGEXEQ_SUPPORT => PatternType::RegexIc,
        x if x == F_TEXT_STARTS_WITH_SUPPORT => PatternType::Prefix,
        _ => return Ok(None),
    };
    let sel = like_regex_support_selectivity(
        mcx,
        run,
        root,
        funcid,
        args,
        var_relid,
        inputcollid,
        is_join,
        ptype,
    )?;
    Ok(Some(sel))
}

macro_rules! patternsel_entry {
    ($name:ident, $ptype:expr, $negate:expr, $doc:literal) => {
        #[doc = $doc]
        pub fn $name<'mcx>(
            mcx: Mcx<'mcx>,
            run: &PlannerRun<'mcx>,
            root: &mut PlannerInfo,
            operator: Oid,
            args: &[NodeId],
            var_relid: i32,
            collation: Oid,
        ) -> PgResult<f64> {
            patternsel(mcx, run, root, operator, args, var_relid, collation, $ptype, $negate)
        }
    };
}

patternsel_entry!(regexeqsel, PatternType::Regex, false, "`regexeqsel` (like_support.c) — selectivity of `~`.");
patternsel_entry!(icregexeqsel, PatternType::RegexIc, false, "`icregexeqsel` (like_support.c) — selectivity of `~*`.");
patternsel_entry!(likesel, PatternType::Like, false, "`likesel` (like_support.c) — selectivity of `~~` (LIKE).");
patternsel_entry!(prefixsel, PatternType::Prefix, false, "`prefixsel` (like_support.c) — selectivity of the `^@` prefix operator.");
patternsel_entry!(iclikesel, PatternType::LikeIc, false, "`iclikesel` (like_support.c) — selectivity of `~~*` (ILIKE).");
patternsel_entry!(regexnesel, PatternType::Regex, true, "`regexnesel` (like_support.c) — selectivity of `!~`.");
patternsel_entry!(icregexnesel, PatternType::RegexIc, true, "`icregexnesel` (like_support.c) — selectivity of `!~*`.");
patternsel_entry!(nlikesel, PatternType::Like, true, "`nlikesel` (like_support.c) — selectivity of `!~~` (NOT LIKE).");
patternsel_entry!(icnlikesel, PatternType::LikeIc, true, "`icnlikesel` (like_support.c) — selectivity of `!~~*` (NOT ILIKE).");

/* ---------------------------------------------------------------------------
 * The planner-support index-condition path (like_support.c):
 * `textlike_support` et al. -> `like_regex_support`(SupportRequestIndexCondition)
 * -> `match_pattern_prefix`. Installed on the
 * `oid_function_call1_index_support` fmgr-planner-support seam (indxpath.c's
 * `get_index_clause_from_support` reaches it). The `SupportRequestSelectivity`
 * leg of `like_regex_support` is reached through the separate
 * `function_selectivity` -> `call_func_selectivity_support` boundary and shares
 * `patternsel_common` directly; it is not part of this index-condition seam.
 * ------------------------------------------------------------------------- */

/* opfamily / pattern-operator OIDs (pg_opfamily.dat / pg_operator.dat). */
const TEXT_PATTERN_BTREE_FAM_OID: Oid = 2095;
const BPCHAR_PATTERN_BTREE_FAM_OID: Oid = 2097;
const TEXT_SPGIST_FAM_OID: Oid = 4017;
const TEXT_PATTERN_LESS_OPERATOR: Oid = 2314;
const TEXT_PATTERN_GREATER_EQUAL_OPERATOR: Oid = 2317;
const TEXT_PREFIX_OPERATOR: Oid = 3877;
const BPCHAR_PATTERN_LESS_OPERATOR: Oid = 2326;
const BPCHAR_PATTERN_GREATER_EQUAL_OPERATOR: Oid = 2329;
const BOOLOID: Oid = 16;

/* Support-function OIDs (fmgroids.h). The `text*` and `name*`/`bpchar*` pattern
 * functions share one support function per pattern kind, so the dispatch keys on
 * the `prosupport` OID (mirroring `like_regex_support`'s per-support-function
 * `ptype` parameter), not on the underlying function. */
const F_TEXTLIKE_SUPPORT: Oid = 1023;
const F_TEXTICLIKE_SUPPORT: Oid = 1025;
const F_TEXTREGEXEQ_SUPPORT: Oid = 1364;
const F_TEXTICREGEXEQ_SUPPORT: Oid = 1024;
const F_TEXT_STARTS_WITH_SUPPORT: Oid = 6242;

/// `match_pattern_prefix(leftop, rightop, ptype, expr_coll, opfamily,
/// indexcollation)` (like_support.c) — try to generate index conditions for a
/// LIKE/regex/prefix clause. 1:1 with the C body. Returns the derived bare
/// index-condition `Expr`s (the C `List *`; empty ⇒ `NIL`).
#[allow(clippy::too_many_arguments)]
fn match_pattern_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    leftop: &Expr<'_>,
    rightop: &Expr<'_>,
    ptype: PatternType,
    expr_coll: Oid,
    opfamily: Oid,
    indexcollation: Oid,
) -> PgResult<alloc::vec::Vec<Expr<'mcx>>> {
    // Can't do anything with a non-constant or NULL pattern argument.
    let patt = match rightop {
        Expr::Const(c) if !c.constisnull => c,
        _ => return Ok(alloc::vec::Vec::new()),
    };

    // Try to extract a fixed prefix from the pattern.
    let (pstatus, mut prefix, _rest) = pattern_fixed_prefix(
        mcx,
        patt.consttype,
        patt.consttypmod,
        patt.constcollid,
        patt.constlen,
        patt.constbyval,
        &patt.constvalue,
        ptype,
        expr_coll,
    )?;

    // Fail if no fixed prefix.
    if pstatus == PatternPrefixStatus::None {
        return Ok(alloc::vec::Vec::new());
    }

    // Identify the operators based on the left-hand argument type (and the
    // legacy "pattern" opclasses). These determine the needed prefix type.
    let ldatatype = expr_type(Some(leftop))?;
    let mut preopr = InvalidOid;
    let (eqopr, ltopr, geopr, collation_aware, rdatatype) = match ldatatype {
        x if x == TEXTOID => {
            if opfamily == TEXT_PATTERN_BTREE_FAM_OID {
                (
                    TEXT_EQUAL_OPERATOR,
                    TEXT_PATTERN_LESS_OPERATOR,
                    TEXT_PATTERN_GREATER_EQUAL_OPERATOR,
                    false,
                    TEXTOID,
                )
            } else if opfamily == TEXT_SPGIST_FAM_OID {
                preopr = TEXT_PREFIX_OPERATOR;
                (
                    TEXT_EQUAL_OPERATOR,
                    TEXT_PATTERN_LESS_OPERATOR,
                    TEXT_PATTERN_GREATER_EQUAL_OPERATOR,
                    false,
                    TEXTOID,
                )
            } else {
                (
                    TEXT_EQUAL_OPERATOR,
                    TEXT_LESS_OPERATOR,
                    TEXT_GREATER_EQUAL_OPERATOR,
                    true,
                    TEXTOID,
                )
            }
        }
        x if x == NAMEOID => (
            NAME_EQUAL_TEXT_OPERATOR,
            NAME_LESS_TEXT_OPERATOR,
            NAME_GREATER_EQUAL_TEXT_OPERATOR,
            true,
            TEXTOID,
        ),
        x if x == BPCHAROID => {
            if opfamily == BPCHAR_PATTERN_BTREE_FAM_OID {
                (
                    BPCHAR_EQUAL_OPERATOR,
                    BPCHAR_PATTERN_LESS_OPERATOR,
                    BPCHAR_PATTERN_GREATER_EQUAL_OPERATOR,
                    false,
                    BPCHAROID,
                )
            } else {
                (
                    BPCHAR_EQUAL_OPERATOR,
                    BPCHAR_LESS_OPERATOR,
                    BPCHAR_GREATER_EQUAL_OPERATOR,
                    true,
                    BPCHAROID,
                )
            }
        }
        x if x == BYTEAOID => (
            BYTEA_EQUAL_OPERATOR,
            BYTEA_LESS_OPERATOR,
            BYTEA_GREATER_EQUAL_OPERATOR,
            false,
            BYTEAOID,
        ),
        // Can't get here unless attached to the wrong operator.
        _ => return Ok(alloc::vec::Vec::new()),
    };

    // Coerce the prefix const to the right type if necessary (text -> bpchar).
    {
        let pfx = prefix.as_mut().expect("non-None status implies a prefix");
        if pfx.consttype != rdatatype {
            debug_assert!(pfx.consttype == TEXTOID && rdatatype == BPCHAROID);
            pfx.consttype = rdatatype;
        }
    }
    let prefix = prefix.expect("non-None status implies a prefix");
    let prefix_expr = prefix_to_const_expr(mcx, &prefix)?;
    let left_expr = leftop.clone_in(mcx)?;

    // Exact-match pattern -> "=" indexqual (if the opclass supports it).
    if pstatus == PatternPrefixStatus::Exact {
        if !lsc::op_in_opfamily::call(eqopr, opfamily)? {
            return Ok(alloc::vec::Vec::new());
        }
        if indexcollation != expr_coll {
            return Ok(alloc::vec::Vec::new());
        }
        let expr = make_opclause(eqopr, BOOLOID, false, left_expr, Some(prefix_expr), InvalidOid, indexcollation);
        return Ok(alloc::vec![expr]);
    }

    // Non-exact is unsupported under a nondeterministic expression collation
    // (the optimized bytewise tests are inconsistent with it). expr_coll is
    // unset for non-collation-aware types such as bytea.
    if OidIsValid(expr_coll) && !locale_seams::collation_is_deterministic::call(expr_coll)? {
        return Ok(alloc::vec::Vec::new());
    }

    // A nonempty required prefix. Some opclasses support prefix checks directly.
    if OidIsValid(preopr) && lsc::op_in_opfamily::call(preopr, opfamily)? {
        let expr = make_opclause(preopr, BOOLOID, false, left_expr, Some(prefix_expr), InvalidOid, indexcollation);
        return Ok(alloc::vec![expr]);
    }

    // A range constraint only works reliably for a collation-insensitive / "C"
    // index collation.
    if collation_aware
        && !locale_seams::pg_newlocale_from_collation::call(mcx, indexcollation)?.collate_is_c
    {
        return Ok(alloc::vec::Vec::new());
    }

    // We can always say "x >= prefix".
    if !lsc::op_in_opfamily::call(geopr, opfamily)? {
        return Ok(alloc::vec::Vec::new());
    }
    let ge_expr = make_opclause(
        geopr,
        BOOLOID,
        false,
        left_expr.clone_in(mcx)?,
        Some(prefix_to_const_expr(mcx, &prefix)?),
        InvalidOid,
        indexcollation,
    );
    let mut result = alloc::vec![ge_expr];

    // If we can build a guaranteed-greater string, add "x < greaterstr".
    if !lsc::op_in_opfamily::call(ltopr, opfamily)? {
        return Ok(result);
    }
    let ltproc = lsc::get_opcode::call(ltopr)?;
    if let Some(greaterstr) = make_greater_string(mcx, &prefix, ltproc, indexcollation)? {
        let lt_expr = make_opclause(
            ltopr,
            BOOLOID,
            false,
            left_expr.clone_in(mcx)?,
            Some(prefix_to_const_expr(mcx, &greaterstr)?),
            InvalidOid,
            indexcollation,
        );
        result.push(lt_expr);
    }

    Ok(result)
}

/// Materialize a [`PrefixConst`] into an `Expr::Const` node (the `makeConst`
/// the C `string_to_const`/`make_greater_string` produced, threaded through the
/// `make_const` owner so the by-reference payload is `datumCopy`'d into the node
/// arena). `constcollid`/`constlen` follow `string_to_const`'s hard-wired
/// per-type properties.
fn prefix_to_const_expr<'mcx>(mcx: Mcx<'mcx>, pfx: &PrefixConst<'_>) -> PgResult<Expr<'mcx>> {
    // string_to_const property table (like_support.c): text/varchar/bpchar use
    // the default collation and constlen -1; name uses C collation, length
    // NAMEDATALEN; bytea uses InvalidOid, constlen -1.
    let (collid, constlen) = match pfx.consttype {
        x if x == NAMEOID => (C_COLLATION_OID, NAMEDATALEN),
        x if x == BYTEAOID => (InvalidOid, -1),
        // text / varchar / bpchar
        _ => (DEFAULT_COLLATION_OID, -1),
    };
    let value = pfx.constvalue.clone_in(mcx)?;
    let constbyval = false; // all supported prefix types are pass-by-ref
    Ok(Expr::Const(make_const(
        mcx,
        pfx.consttype,
        -1,
        collid,
        constlen,
        value,
        false,
        constbyval,
    )?))
}

/// `like_regex_support`'s `SupportRequestIndexCondition` leg
/// (like_support.c) — installed on the `oid_function_call1_index_support` seam.
/// `prosupport`/`funcid` identify the pattern operator family; the clause is the
/// `OpExpr`/`FuncExpr` node; `opfamily`/`indexcollation` come from the index
/// column.
fn index_condition_support(
    root: &PlannerInfo,
    prosupport: Oid,
    _funcid: Oid,
    clause: NodeId,
    indexarg: i32,
    index: &types_pathnodes::IndexOptInfo,
    indexcol: i32,
) -> (alloc::vec::Vec<Expr<'static>>, bool) {
    // Map the support function (shared by the text/name/bpchar variants of each
    // pattern operator) to its pattern type — the `ptype` C bakes into each
    // `*_support` entry point.
    let ptype = match prosupport {
        x if x == F_TEXTLIKE_SUPPORT => PatternType::Like,
        x if x == F_TEXTICLIKE_SUPPORT => PatternType::LikeIc,
        x if x == F_TEXTREGEXEQ_SUPPORT => PatternType::Regex,
        x if x == F_TEXTICREGEXEQ_SUPPORT => PatternType::RegexIc,
        x if x == F_TEXT_STARTS_WITH_SUPPORT => PatternType::Prefix,
        // Not a like_support.c pattern support function. The C
        // `get_index_clause_from_support` (indxpath.c) sends a
        // `SupportRequestIndexCondition` node to the function's prosupport via
        // `OidFunctionCall1`; a support function that does not handle that request
        // type (e.g. `range_contains_elem_support` / `elem_contained_by_range_support`,
        // which only answer `SupportRequestSimplify`) simply returns NULL, so the
        // derived-clause list is empty and the planner falls through to the
        // ordinary operator-class match. Decline here the same way (empty list,
        // not lossy) rather than aborting — this seam only knows the pattern
        // operators; any other support function's IndexCondition leg is a no-op
        // for our purposes.
        _ => return (alloc::vec::Vec::new(), false),
    };

    // We only consider the indexkey-on-left case (no reverse pattern operators).
    if indexarg != 0 {
        return (alloc::vec::Vec::new(), false);
    }

    let node = root.node(clause);
    // is_opclause / is_funcclause: the clause is an OpExpr or FuncExpr with two
    // args (indexkey, pattern).
    let (left, right, inputcollid) = match node {
        Expr::OpExpr(op) if op.args.len() == 2 => (&op.args[0], &op.args[1], op.inputcollid),
        Expr::FuncExpr(f) if f.args.len() == 2 => (&f.args[0], &f.args[1], f.inputcollid),
        _ => return (alloc::vec::Vec::new(), false),
    };

    let opfamily = index.opfamily[indexcol as usize];
    let indexcollation = index.indexcollations[indexcol as usize];

    // The estimator's prefix-extraction allocations live in a per-call context;
    // the produced Expr nodes are cloned out into the planner arena by the seam
    // caller (it allocs each into `root` and wraps in a RestrictInfo).
    let cx = mcx::MemoryContext::new("selfuncs match_pattern_prefix");
    let result = match match_pattern_prefix(
        cx.mcx(),
        left,
        right,
        ptype,
        inputcollid,
        opfamily,
        indexcollation,
    ) {
        Ok(v) => v,
        // C's support functions ereport on hard errors (e.g. ILIKE on bytea);
        // the seam returns a (clauses, lossy) pair with no error channel, so a
        // hard error surfaces as a panic (mirror-PG-and-abort), as for the other
        // index-support boundaries.
        Err(e) => panic!("match_pattern_prefix failed: {}", e.message()),
    };
    // The derived prefix quals are built in the transient `cx`; the seam contract
    // hands them back at the planner-arena `'static` lifetime (the caller
    // re-allocates each into `root`), so erase at this seam-handoff boundary.
    let result: alloc::vec::Vec<Expr<'static>> =
        result.into_iter().map(Expr::erase_lifetime).collect();

    // C `get_index_clause_from_support` sets `req.lossy = true` as the default
    // assumption (indxpath.c), and `like_regex_support` NEVER overrides it — so a
    // pattern-prefix index clause is ALWAYS lossy: the derived `>=`/`<`/`=`
    // prefix quals only bound the search, and the original LIKE/regex clause must
    // be retained as a recheck `Filter` (createplan.c keeps it because a lossy
    // IndexClause is skipped by `is_redundant_with_indexclauses`). Even the
    // exact-prefix `=` case stays lossy in C (regex `^abc$` may still differ from
    // `proname = 'abc'` for e.g. trailing newline semantics).
    (result, true)
}

/// Install the `oid_function_call1_index_support` planner-support seam with the
/// like_support.c pattern-operator index-condition support.
pub fn init_support_seam() {
    backend_utils_fmgr_support_seams::oid_function_call1_index_support::set(
        index_condition_support,
    );
}
