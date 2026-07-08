//! like_support.c planner slice: patternsel family (regexeqsel/likesel/... and
//! negators) over Var-op-Const, plus the SupportRequestIndexCondition leg
//! (match_pattern_prefix). C-collation lane; locale-aware arms are loud.

use datum::Datum;
use mcx::Mcx;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::FmgrInfo;

use crate::run::PlannerRun;
use crate::selfuncs::{
    self, clamp_probability, VariableStatData, DEFAULT_MATCH_SEL,
};
use types_pathnodes::NodeId;

pub use planner_seams::PatternType;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PrefixStatus {
    None,
    Partial,
    Exact,
}

// Planner-internal stand-in for the prefix Const node.
#[derive(Clone, Copy)]
struct PrefixConst {
    value: Datum,
    typ: Oid,
}

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

const FIXED_CHAR_SEL: f64 = 0.20;
const CHAR_RANGE_SEL: f64 = 0.25;
const ANY_CHAR_SEL: f64 = 0.9;
const FULL_WILDCARD_SEL: f64 = 5.0;
const PARTIAL_WILDCARD_SEL: f64 = 2.0;

// Pattern consts can be short-form (bound-param datumCopy preserves headers).
fn varlena_payload<'a>(d: Datum) -> &'a [u8] {
    selfuncs::varlena_datum_payload(d)
}

fn text_const<'mcx>(mcx: Mcx<'mcx>, s: &[u8], typ: Oid) -> PgResult<PrefixConst> {
    let total = datum::varlena::VARHDRSZ + s.len();
    let mut img = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut img, &datum::varlena::set_varsize_4b(total))?;
    mcx::vec_append_bytes(&mut img, s)?;
    Ok(PrefixConst { value: Datum::from_usize(img.leak().as_ptr() as usize), typ })
}

pub fn patternsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: Oid,
    args: &[NodeId],
    varrelid: i32,
    collation: Oid,
    ptype: PatternType,
    negate: bool,
) -> PgResult<f64> {
    let mut operator = operator;
    if negate {
        operator = lsyscache::get_negator(operator)?;
        if operator == 0 {
            return Ok(1.0 - DEFAULT_MATCH_SEL);
        }
    }
    patternsel_common(run, operator, 0, args, varrelid, collation, ptype, negate)
}

// C dual entry: operator path passes oprid (opfuncid 0, resolved lazily);
// prosupport SupportRequestSelectivity passes opfuncid with oprid 0.
pub(crate) fn patternsel_common<'mcx>(
    run: &mut PlannerRun<'mcx>,
    oprid: Oid,
    opfuncid: Oid,
    args: &[NodeId],
    varrelid: i32,
    collation: Oid,
    ptype: PatternType,
    negate: bool,
) -> PgResult<f64> {
    let default = if negate { 1.0 - DEFAULT_MATCH_SEL } else { DEFAULT_MATCH_SEL };

    let Some((vardata, other, varonleft)) =
        selfuncs::get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(default);
    };
    if !varonleft {
        return Ok(default);
    }
    let Some(c) = other.as_const() else {
        return Ok(default);
    };
    if c.constisnull {
        return Ok(0.0);
    }
    let constval = c.constvalue;
    let consttype = c.consttype;
    if consttype != TEXTOID && consttype != BYTEAOID {
        return Ok(default);
    }

    let (eqopr, ltopr, geopr, rdatatype) = match vardata.vartype {
        TEXTOID => (
            TEXT_EQUAL_OPERATOR,
            TEXT_LESS_OPERATOR,
            TEXT_GREATER_EQUAL_OPERATOR,
            TEXTOID,
        ),
        // RHS type must stay text so the comparison value is not truncated
        // to NAMEDATALEN.
        NAMEOID => (
            NAME_EQUAL_TEXT_OPERATOR,
            NAME_LESS_TEXT_OPERATOR,
            NAME_GREATER_EQUAL_TEXT_OPERATOR,
            TEXTOID,
        ),
        BPCHAROID => (
            BPCHAR_EQUAL_OPERATOR,
            BPCHAR_LESS_OPERATOR,
            BPCHAR_GREATER_EQUAL_OPERATOR,
            BPCHAROID,
        ),
        BYTEAOID => (
            BYTEA_EQUAL_OPERATOR,
            BYTEA_LESS_OPERATOR,
            BYTEA_GREATER_EQUAL_OPERATOR,
            BYTEAOID,
        ),
        _ => return Ok(default),
    };

    let nullfrac = vardata.nullfrac();

    let (pstatus, mut prefix, rest_selec) =
        pattern_fixed_prefix(run.mcx, constval, consttype, ptype, collation)?;

    if let Some(p) = prefix.as_mut() {
        if p.typ != rdatatype {
            debug_assert!(p.typ == TEXTOID && rdatatype == BPCHAROID);
            p.typ = rdatatype;
        }
    }

    let mut result;
    if pstatus == PrefixStatus::Exact {
        result = selfuncs::var_eq_const(
            run,
            &vardata,
            eqopr,
            collation,
            prefix.expect("exact prefix").value,
            false,
            true,
            false,
        )?;
    } else {
        let opfuncid = if opfuncid != 0 { opfuncid } else { lsyscache::get_opcode(oprid)? };
        let mut opproc = fmgr_core::fmgr_info(opfuncid)?;

        let (mut selec, hist_size) = selfuncs::histogram_selectivity(
            run.mcx,
            &vardata, &mut opproc, collation, constval, true, 10, 1,
        )?;

        if hist_size < 100 {
            let prefixsel = if pstatus == PrefixStatus::Partial {
                prefix_selectivity(
                    run,
                    &vardata,
                    eqopr,
                    ltopr,
                    geopr,
                    collation,
                    prefix.expect("partial prefix"),
                )?
            } else {
                1.0
            };
            let heursel = prefixsel * rest_selec;
            if selec < 0.0 {
                selec = heursel;
            } else {
                let hist_weight = hist_size as f64 / 100.0;
                selec = selec * hist_weight + heursel * (1.0 - hist_weight);
            }
        }

        selec = selec.clamp(0.0001, 0.9999);

        let (mcv_selec, sumcommon) = selfuncs::mcv_selectivity(
            run, &vardata, &mut opproc, collation, constval, true,
        )?;

        selec *= 1.0 - nullfrac - sumcommon;
        selec += mcv_selec;
        result = selec;
    }

    if negate {
        result = 1.0 - result - nullfrac;
    }
    Ok(clamp_probability(result))
}

// Non-collatable comparisons (e.g. bytea) are always deterministic.
fn nondeterministic(coll: Oid) -> PgResult<bool> {
    Ok(coll != 0 && !lsyscache::get_collation_isdeterministic(coll)?)
}

fn pattern_fixed_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    patt: Datum,
    patt_type: Oid,
    ptype: PatternType,
    collation: Oid,
) -> PgResult<(PrefixStatus, Option<PrefixConst>, f64)> {
    match ptype {
        PatternType::Like => like_fixed_prefix(mcx, patt, patt_type, false, collation),
        PatternType::LikeIc => like_fixed_prefix(mcx, patt, patt_type, true, collation),
        PatternType::Regex => regex_fixed_prefix(mcx, patt, patt_type, false, collation),
        PatternType::RegexIc => regex_fixed_prefix(mcx, patt, patt_type, true, collation),
        PatternType::Prefix => {
            let prefix = text_const(mcx, varlena_payload(patt), patt_type)?;
            Ok((PrefixStatus::Partial, Some(prefix), 1.0))
        }
    }
}

// pattern_char_isalpha (like_support.c:1493).
fn pattern_char_isalpha(c: u8, is_multibyte: bool, locale: &pg_locale::PgLocale) -> bool {
    if locale.ctype_is_c {
        c.is_ascii_alphabetic()
    } else if is_multibyte && c >= 0x80 {
        true
    } else if locale.provider != pg_locale::COLLPROVIDER_LIBC {
        c >= 0x80 || c.is_ascii_alphabetic()
    } else {
        locale.isalpha_l(c)
    }
}

// like_fixed_prefix (like_support.c).
fn like_fixed_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    patt_const: Datum,
    typeid: Oid,
    case_insensitive: bool,
    collation: Oid,
) -> PgResult<(PrefixStatus, Option<PrefixConst>, f64)> {
    debug_assert!(typeid == BYTEAOID || typeid == TEXTOID);
    let is_multibyte = mbutils::pg_database_encoding_max_length() > 1;
    let mut locale: Option<&'static pg_locale::PgLocale> = None;

    if case_insensitive {
        if typeid == BYTEAOID {
            return Err(Box::new(PgError::error(
                "case insensitive matching not supported on type bytea".to_string(),
            )));
        }
        if collation == 0 {
            return Err(Box::new(PgError::error(
                "could not determine which collation to use for ILIKE".to_string(),
            )));
        }
        locale = Some(pg_locale::pg_newlocale_from_collation(collation)?);
    }

    let patt = varlena_payload(patt_const);
    let pattlen = patt.len();

    let mut match_buf = mcx::vec_with_capacity_in(mcx, pattlen)?;
    let mut pos = 0usize;
    while pos < pattlen {
        if patt[pos] == b'%' || patt[pos] == b'_' {
            break;
        }
        if patt[pos] == b'\\' {
            pos += 1;
            if pos >= pattlen {
                break;
            }
        }
        if case_insensitive && pattern_char_isalpha(patt[pos], is_multibyte, locale.unwrap()) {
            break;
        }
        match_buf.push(patt[pos]);
        pos += 1;
    }

    let prefix = text_const(mcx, &match_buf, typeid)?;
    let rest_selec = like_selectivity(&patt[pos..], case_insensitive);

    if pos == pattlen {
        // In LIKE, an empty remainder means an exact match.
        return Ok((PrefixStatus::Exact, Some(prefix), rest_selec));
    }
    if !match_buf.is_empty() {
        return Ok((PrefixStatus::Partial, Some(prefix), rest_selec));
    }
    Ok((PrefixStatus::None, Some(prefix), rest_selec))
}

// regex_fixed_prefix (like_support.c); prefix extraction rides the ported
// regex engine through the regexp_fixed_prefix seam.
fn regex_fixed_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    patt_const: Datum,
    typeid: Oid,
    case_insensitive: bool,
    collation: Oid,
) -> PgResult<(PrefixStatus, Option<PrefixConst>, f64)> {
    if typeid == BYTEAOID {
        return Err(Box::new(PgError::error(
            "regular-expression matching not supported on type bytea".to_string(),
        )));
    }
    let patt = varlena_payload(patt_const);
    match regexp_seams::regexp_fixed_prefix::call(mcx, patt, case_insensitive, collation)? {
        None => {
            let rest = regex_selectivity(patt, case_insensitive, 0);
            Ok((PrefixStatus::None, None, rest))
        }
        Some((prefix_bytes, exact)) => {
            let prefix = text_const(mcx, &prefix_bytes, typeid)?;
            let rest = if exact {
                1.0
            } else {
                regex_selectivity(patt, case_insensitive, prefix_bytes.len())
            };
            let status =
                if exact { PrefixStatus::Exact } else { PrefixStatus::Partial };
            Ok((status, Some(prefix), rest))
        }
    }
}

// prefix_selectivity (like_support.c): "var >= prefix AND var < greaterstr".
fn prefix_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    eqopr: Oid,
    ltopr: Oid,
    geopr: Oid,
    collation: Oid,
    prefixcon: PrefixConst,
) -> PgResult<f64> {
    let mut opproc = selfuncs::opproc_for(geopr)?;
    let mut prefixsel = selfuncs::ineq_histogram_selectivity(
        run,
        vardata,
        geopr,
        &mut opproc,
        true,
        true,
        collation,
        prefixcon.value,
        prefixcon.typ,
    )?;
    if prefixsel < 0.0 {
        return Ok(DEFAULT_MATCH_SEL);
    }

    let mut ltproc = selfuncs::opproc_for(ltopr)?;
    if let Some(greaterstr) = make_greater_string(run.mcx, prefixcon, &mut ltproc, collation)? {
        let topsel = selfuncs::ineq_histogram_selectivity(
            run,
            vardata,
            ltopr,
            &mut ltproc,
            false,
            false,
            collation,
            greaterstr.value,
            greaterstr.typ,
        )?;
        debug_assert!(topsel >= 0.0);
        // Range-pair merge as in clauselist_selectivity; nulls are already
        // excluded by ineq_histogram_selectivity.
        prefixsel = topsel + prefixsel - 1.0;
    }

    let eq_sel = selfuncs::var_eq_const(
        run,
        vardata,
        eqopr,
        collation,
        prefixcon.value,
        false,
        true,
        false,
    )?;
    Ok(prefixsel.max(eq_sel))
}

fn byte_increment(ptr: &mut [u8]) -> bool {
    if ptr[0] >= 255 {
        return false;
    }
    ptr[0] += 1;
    true
}

// make_greater_string (like_support.c). Non-C collations need the
// suffix-and-varstr_cmp leg; loud until that lane lands.
fn make_greater_string<'mcx>(
    mcx: Mcx<'mcx>,
    str_const: PrefixConst,
    ltproc: &mut FmgrInfo,
    collation: Oid,
) -> PgResult<Option<PrefixConst>> {
    let datatype = str_const.typ;
    debug_assert!(datatype != NAMEOID);
    let src = varlena_payload(str_const.value);
    let mut workstr = mcx::vec_with_capacity_in(mcx, src.len())?;
    mcx::vec_append_bytes(&mut workstr, src)?;
    let mut len = workstr.len();
    let cmpstr = str_const.value;
    if datatype != BYTEAOID
        && len > 0
        && !pg_locale::pg_newlocale_from_collation(collation)?.collate_is_c
    {
        panic!("make_greater_string (like_support.c): non-C collation suffix leg; C-collation lane only");
    }

    let charinc: fn(&mut [u8]) -> bool = if datatype == BYTEAOID {
        byte_increment
    } else {
        mbutils::pg_database_encoding_character_incrementer()
    };

    while len > 0 {
        let charlen = if datatype == BYTEAOID {
            1
        } else {
            len - mbutils::pg_mbcliplen(&workstr[..len], len as i32, len as i32 - 1) as usize
        };
        while charinc(&mut workstr[len - charlen..len]) {
            let cand = text_const(mcx, &workstr[..len], datatype)?;
            if types_fmgr::function_call2_coll_in(ltproc, collation, mcx, cmpstr, cand.value)?
                .as_bool()
            {
                return Ok(Some(cand));
            }
        }
        len -= charlen;
    }
    Ok(None)
}

fn like_selectivity(patt: &[u8], _case_insensitive: bool) -> f64 {
    let mut sel = 1.0f64;
    let pattlen = patt.len();
    let mut pos = 0usize;
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
    if sel > 1.0 {
        sel = 1.0;
    }
    sel
}

fn regex_selectivity_sub(patt: &[u8], case_insensitive: bool) -> f64 {
    let mut sel = 1.0f64;
    let mut paren_depth = 0i32;
    let mut paren_pos = 0usize;
    let pattlen = patt.len();
    let mut pos = 0usize;
    while pos < pattlen {
        let c = patt[pos];
        if c == b'(' {
            if paren_depth == 0 {
                paren_pos = pos;
            }
            paren_depth += 1;
        } else if c == b')' && paren_depth > 0 {
            paren_depth -= 1;
            if paren_depth == 0 {
                sel *= regex_selectivity_sub(&patt[paren_pos + 1..pos], case_insensitive);
            }
        } else if c == b'|' && paren_depth == 0 {
            sel += regex_selectivity_sub(&patt[pos + 1..], case_insensitive);
            break;
        } else if c == b'[' {
            pos += 1;
            let negclass = pos < pattlen && patt[pos] == b'^';
            if negclass {
                pos += 1;
            }
            if pos < pattlen && patt[pos] == b']' {
                pos += 1;
            }
            while pos < pattlen && patt[pos] != b']' {
                pos += 1;
            }
            if paren_depth == 0 {
                sel *= if negclass { 1.0 - CHAR_RANGE_SEL } else { CHAR_RANGE_SEL };
            }
        } else if c == b'.' {
            if paren_depth == 0 {
                sel *= ANY_CHAR_SEL;
            }
        } else if c == b'*' || c == b'?' || c == b'+' {
            if paren_depth == 0 {
                sel *= PARTIAL_WILDCARD_SEL;
            }
        } else if c == b'{' {
            while pos < pattlen && patt[pos] != b'}' {
                pos += 1;
            }
            if paren_depth == 0 {
                sel *= PARTIAL_WILDCARD_SEL;
            }
        } else if c == b'\\' {
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
    if sel > 1.0 {
        sel = 1.0;
    }
    sel
}

fn regex_selectivity(patt: &[u8], case_insensitive: bool, fixed_prefix_len: usize) -> f64 {
    let pattlen = patt.len();
    let mut sel;
    if pattlen > 0
        && patt[pattlen - 1] == b'$'
        && (pattlen == 1 || patt[pattlen - 2] != b'\\')
    {
        sel = regex_selectivity_sub(&patt[..pattlen - 1], case_insensitive);
    } else {
        sel = regex_selectivity_sub(patt, case_insensitive);
        sel *= FULL_WILDCARD_SEL;
    }
    if fixed_prefix_len > 0 {
        let prefixsel = FIXED_CHAR_SEL.powf(fixed_prefix_len as f64);
        if prefixsel > 0.0 {
            sel /= prefixsel;
        }
    }
    clamp_probability(sel)
}

const TEXT_PATTERN_BTREE_FAM_OID: Oid = 2095;
const TEXT_SPGIST_FAM_OID: Oid = 4017;
const BPCHAR_PATTERN_BTREE_FAM_OID: Oid = 2097;
const TEXT_PATTERN_LESS_OPERATOR: Oid = 2314;
const TEXT_PATTERN_GREATER_EQUAL_OPERATOR: Oid = 2317;
const BPCHAR_PATTERN_LESS_OPERATOR: Oid = 2326;
const BPCHAR_PATTERN_GREATER_EQUAL_OPERATOR: Oid = 2329;
const TEXT_PREFIX_OPERATOR: Oid = 3877;
const DEFAULT_COLLATION_OID: Oid = 100;
const C_COLLATION_OID: Oid = 950;
const BOOLOID: Oid = 16;
const NAMEDATALEN: i32 = 64;

fn const_node<'mcx>(mcx: Mcx<'mcx>, pc: PrefixConst) -> PgResult<types_nodes::Node<'mcx>> {
    let (collation, constlen) = match pc.typ {
        TEXTOID | BPCHAROID => (DEFAULT_COLLATION_OID, -1),
        NAMEOID => (C_COLLATION_OID, NAMEDATALEN),
        BYTEAOID => (0, -1),
        other => panic!("string_to_const (like_support.c): datatype {other}"),
    };
    types_nodes::Node::mk(
        mcx,
        types_nodes::primnodes::Const {
            consttype: pc.typ,
            consttypmod: -1,
            constcollid: collation,
            constlen,
            constvalue: pc.value,
            constisnull: false,
            constbyval: false,
            location: -1,
        },
    )
}

// make_opclause (makefuncs.c) with the opfuncid resolved in place (C's
// set_opfuncid runs before execution anyway).
pub(crate) fn make_opclause<'mcx>(
    mcx: Mcx<'mcx>,
    opno: Oid,
    leftop: types_nodes::Node<'mcx>,
    rightop: types_nodes::Node<'mcx>,
    inputcollid: Oid,
) -> PgResult<types_nodes::Node<'mcx>> {
    types_nodes::Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno,
            opfuncid: lsyscache::get_opcode(opno)?,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: 0,
            inputcollid,
            args: types_nodes::NodeList::make2(mcx, leftop, rightop)?,
            location: -1,
        },
    )
}

// match_pattern_prefix (like_support.c): the SupportRequestIndexCondition
// leg. Returns the generated indexqual expressions, or None.
// NONDETERMINISTIC (like_support.c): non-collatable comparisons, e.g. for
// bytea, are always deterministic.
fn nondeterministic_coll(coll: types_core::Oid) -> PgResult<bool> {
    Ok(coll != types_core::InvalidOid && !lsyscache::get_collation_isdeterministic(coll)?)
}

pub fn match_pattern_prefix<'mcx>(
    run: &mut PlannerRun<'mcx>,
    leftop: types_nodes::Node<'mcx>,
    rightop: types_nodes::Node<'mcx>,
    ptype: PatternType,
    expr_coll: Oid,
    opfamily: Oid,
    indexcollation: Oid,
) -> PgResult<Option<mcx::PgVec<'mcx, types_nodes::Node<'mcx>>>> {
    let mcx = run.mcx;
    let Some(patt) = rightop.as_const() else { return Ok(None) };
    if patt.constisnull {
        return Ok(None);
    }
    let (pstatus, prefix, _rest) =
        pattern_fixed_prefix(mcx, patt.constvalue, patt.consttype, ptype, expr_coll)?;
    if pstatus == PrefixStatus::None {
        return Ok(None);
    }
    let mut prefix = prefix.expect("fixed prefix present");

    let ldatatype = crate::costsize::expr_type_typmod(leftop).0;
    let mut preopr: Oid = 0;
    let (eqopr, ltopr, geopr, collation_aware, rdatatype) = match ldatatype {
        TEXTOID => {
            if opfamily == TEXT_PATTERN_BTREE_FAM_OID {
                (TEXT_EQUAL_OPERATOR, TEXT_PATTERN_LESS_OPERATOR,
                 TEXT_PATTERN_GREATER_EQUAL_OPERATOR, false, TEXTOID)
            } else if opfamily == TEXT_SPGIST_FAM_OID {
                preopr = TEXT_PREFIX_OPERATOR;
                (TEXT_EQUAL_OPERATOR, TEXT_PATTERN_LESS_OPERATOR,
                 TEXT_PATTERN_GREATER_EQUAL_OPERATOR, false, TEXTOID)
            } else {
                (TEXT_EQUAL_OPERATOR, TEXT_LESS_OPERATOR,
                 TEXT_GREATER_EQUAL_OPERATOR, true, TEXTOID)
            }
        }
        NAMEOID => (
            NAME_EQUAL_TEXT_OPERATOR, NAME_LESS_TEXT_OPERATOR,
            NAME_GREATER_EQUAL_TEXT_OPERATOR, true, TEXTOID,
        ),
        BPCHAROID => {
            if opfamily == BPCHAR_PATTERN_BTREE_FAM_OID {
                (BPCHAR_EQUAL_OPERATOR, BPCHAR_PATTERN_LESS_OPERATOR,
                 BPCHAR_PATTERN_GREATER_EQUAL_OPERATOR, false, BPCHAROID)
            } else {
                (BPCHAR_EQUAL_OPERATOR, BPCHAR_LESS_OPERATOR,
                 BPCHAR_GREATER_EQUAL_OPERATOR, true, BPCHAROID)
            }
        }
        BYTEAOID => (
            BYTEA_EQUAL_OPERATOR, BYTEA_LESS_OPERATOR,
            BYTEA_GREATER_EQUAL_OPERATOR, false, BYTEAOID,
        ),
        _ => return Ok(None),
    };

    if prefix.typ != rdatatype {
        debug_assert!(prefix.typ == TEXTOID && rdatatype == BPCHAROID);
        prefix.typ = rdatatype;
    }

    if pstatus == PrefixStatus::Exact {
        if !lsyscache::op_in_opfamily(eqopr, opfamily)? {
            return Ok(None);
        }
        // A collation mismatch only disqualifies the "=" indexqual when the
        // expression collation is nondeterministic: all deterministic
        // collations agree on (bitwise) equality, and the lossy indexqual is
        // rechecked by the LIKE/regex operator anyway (C d0bb49e).
        if indexcollation != expr_coll && nondeterministic(expr_coll)? {
            return Ok(None);
        }
        let expr = make_opclause(mcx, eqopr, leftop, const_node(mcx, prefix)?, indexcollation)?;
        let mut out = mcx::PgVec::new_in(mcx);
        out.push(expr);
        return Ok(Some(out));
    }

    if nondeterministic(expr_coll)? {
        return Ok(None);
    }

    if preopr != 0 && lsyscache::op_in_opfamily(preopr, opfamily)? {
        let expr = make_opclause(mcx, preopr, leftop, const_node(mcx, prefix)?, indexcollation)?;
        let mut out = mcx::PgVec::new_in(mcx);
        out.push(expr);
        return Ok(Some(out));
    }

    if collation_aware
        && !pg_locale::pg_newlocale_from_collation(indexcollation)?.collate_is_c
    {
        return Ok(None);
    }

    if !lsyscache::op_in_opfamily(geopr, opfamily)? {
        return Ok(None);
    }
    let mut out = mcx::PgVec::new_in(mcx);
    out.push(make_opclause(mcx, geopr, leftop, const_node(mcx, prefix)?, indexcollation)?);

    if !lsyscache::op_in_opfamily(ltopr, opfamily)? {
        return Ok(Some(out));
    }
    let mut ltproc = selfuncs::opproc_for(ltopr)?;
    if let Some(greaterstr) = make_greater_string(mcx, prefix, &mut ltproc, indexcollation)? {
        out.push(make_opclause(
            mcx,
            ltopr,
            leftop,
            const_node(mcx, greaterstr)?,
            indexcollation,
        )?);
    }
    Ok(Some(out))
}
