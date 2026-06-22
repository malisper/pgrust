//! `utils/adt/ruleutils.c` — the SQL deparser, **F0a: the deparse
//! name-resolution engine** (the foundation the rest of ruleutils builds on).
//!
//! This is the first family of the ruleutils.c port (the file is 13.7k LOC and
//! cannot land in one pass). F0a delivers:
//!
//! 1. The four ruleutils-private types — [`DeparseContext`],
//!    [`DeparseNamespace`], [`DeparseColumns`], [`NameHashEntry`] — modeled
//!    field-for-field against `ruleutils.c` (110-313). They are *crate-private*
//!    (not in `types-*`): nothing outside ruleutils.c reads them.
//!
//! 2. The relation/column alias name-resolution engine (`ruleutils.c`
//!    3870-5139): [`set_rtable_names`], [`set_deparse_for_query`],
//!    [`set_simple_column_names`], [`has_dangerous_join_using`],
//!    [`set_using_names`], [`set_relation_column_names`],
//!    [`set_join_column_names`], [`colname_is_unique`], [`make_colname_unique`],
//!    [`expand_colnames_array_to`], the `*_names_hash` helpers,
//!    [`identify_join_columns`], [`get_rtable_name`], [`deparse_columns_fetch`],
//!    plus the pure frontends [`deparse_context_for`] and
//!    [`select_rtable_names_for_explain`].
//!
//! 3. The plan-navigation half ([`set_deparse_plan`] and friends) is gated on
//!    the planner producing owned `Plan` trees (issue #159) — those read `Plan`
//!    fields a real producer does not yet supply, so they are seam-and-panic
//!    (mirror-PG-and-panic) until the plan layer lands as ruleutils F0b.
//!
//! **F1 (the expression deparser) is now landed** in the [`expr_deparse`]
//! module: the precedence-aware `get_rule_expr` tree-walker and its per-node
//! deparsers (operators, functions, aggregates, window functions, constants,
//! coercions, CASE, ARRAY/ROW/COALESCE/MIN-MAX, NULL/boolean tests, sub-links,
//! subscripts, the `isSimpleNode` precedence oracle, and the Query-side
//! `get_variable`). F1 introduces the output buffer (`DeparseContext::buf`) the
//! engine renders into. The plan-tree-navigation arms (#159), the F2 query-tree
//! deparsers (`get_rule_orderby` / `get_rule_windowspec` / `get_query_def`), and
//! the catalog name generators (`generate_operator_name` /
//! `generate_function_name`) it reaches are seam-and-panic until those families
//! land.
//!
//! The query-tree deparsers (F2) and the catalog definition builders (F3) build
//! on F1 and are NOT in this family.
//!
//! C source: `src/backend/utils/adt/ruleutils.c`.
#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::format;

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_core::fmgr::NAMEDATALEN;
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_nodes::nodes::Node;
use types_nodes::parsenodes::{
    RangeTblEntry, RTE_FUNCTION, RTE_JOIN, RTE_RELATION, RTE_TABLEFUNC,
};
use types_nodes::rawnodes::{Alias, FromExpr, JoinExpr};

/// `quote_identifier(ident)` (ruleutils.c 13028-13104) — quote an identifier
/// only if needed for re-parse safety. Faithful port: an identifier is "safe"
/// (no quoting) iff it begins with a lowercase letter or underscore, contains
/// only lowercase letters, digits and underscores, is not a reserved/typish
/// keyword, and `quote_all_identifiers` is off. Otherwise it is wrapped in
/// double quotes with any embedded `"` doubled. The result is always copied
/// into `mcx` (C returns the input pointer in the safe case; the owned image
/// copies either way).
pub fn quote_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &str) -> PgResult<PgString<'mcx>> {
    let bytes = ident.as_bytes();

    // safe = ((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_')
    let mut safe = matches!(bytes.first(), Some(&c) if (c.is_ascii_lowercase() || c == b'_'));
    let mut nquotes = 0usize;

    for &ch in bytes {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == b'_' {
            // okay
        } else {
            safe = false;
            if ch == b'"' {
                nquotes += 1;
            }
        }
    }

    if seams::quote_all_identifiers() {
        safe = false;
    }

    if safe {
        // Check for keyword. We quote keywords except for unreserved ones.
        let kwnum =
            common_keywords::ScanKeywordLookup(ident, &common_keywords::ScanKeywords);
        if kwnum >= 0
            && common_keywords::keyword_category(kwnum as usize)
                != Some(common_keywords::KeywordCategory::Unreserved)
        {
            safe = false;
        }
    }

    if safe {
        // no change needed
        return PgString::from_str_in(ident, mcx);
    }

    // result = palloc(strlen(ident) + nquotes + 2 + 1); fill with doubled quotes.
    let mut out: PgVec<u8> = PgVec::new_in(mcx);
    out.try_reserve(bytes.len() + nquotes + 2)
        .map_err(|_| mcx.oom(0))?;
    out.push(b'"');
    for &ch in bytes {
        if ch == b'"' {
            out.push(b'"');
        }
        out.push(ch);
    }
    out.push(b'"');

    // The bytes are ASCII-quoted around an arbitrary identifier; the identifier
    // text came in as a `&str` so the whole image is valid UTF-8.
    let s = core::str::from_utf8(out.as_slice())
        .map_err(|_| PgError::error(alloc::string::String::from("quote_identifier: non-UTF-8 identifier")))?;
    PgString::from_str_in(s, mcx)
}

/// `quote_qualified_identifier(qualifier, ident)` (ruleutils.c 13112-13123) —
/// build `qualifier.ident`, or just `ident` when `qualifier` is `None`, quoting
/// each component with [`quote_identifier`] as needed. The result is allocated
/// in `mcx` (C pallocs via `StringInfo`).
pub fn quote_qualified_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    qualifier: Option<&str>,
    ident: &str,
) -> PgResult<PgString<'mcx>> {
    use alloc::string::String;

    let mut buf = String::new();
    if let Some(q) = qualifier {
        let q_quoted = quote_identifier(mcx, q)?;
        buf.push_str(q_quoted.as_str());
        buf.push('.');
    }
    let ident_quoted = quote_identifier(mcx, ident)?;
    buf.push_str(ident_quoted.as_str());
    PgString::from_str_in(&buf, mcx)
}

/// The catalog half of `generate_relation_name` (`ruleutils.c` 13160-13205).
///
/// The CTE-name-conflict scan over the deparse namespace stack is done in-crate
/// by the caller (`query_deparse::generate_relation_name`, which owns the
/// namespace list); this performs the remaining catalog work that C does after
/// `SearchSysCache1(RELOID)`: read `relname`/`relnamespace`, qualify the name
/// iff `force_qual` (the CTE conflict) or the relation is not visible in the
/// search path, and `quote_qualified_identifier`.
///
/// Installed as the ruleutils inward seam `generate_relation_name`.
pub fn generate_relation_name_catalog<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    force_qual: bool,
) -> PgResult<PgString<'mcx>> {
    // SearchSysCache1(RELOID, ...); elog(ERROR) on miss. The two GETSTRUCT
    // reads (relname, relnamespace) come off the same tuple in C; here they are
    // two lsyscache reads of the same cache entry.
    let relname = match backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, relid)? {
        Some(s) => s,
        None => {
            return Err(elog_error(alloc::format!(
                "cache lookup failed for relation {}",
                relid
            )))
        }
    };

    // need_qual = force_qual (CTE conflict, found by the caller) ||
    //             !RelationIsVisible(relid)
    let need_qual = if force_qual {
        true
    } else {
        !backend_catalog_namespace_seams::relation_is_visible::call(mcx, relid)?
    };

    let nspname: Option<PgString<'mcx>> = if need_qual {
        let relnamespace = backend_utils_cache_lsyscache_seams::get_rel_namespace::call(relid)?;
        backend_utils_cache_lsyscache_seams::get_namespace_name_or_temp::call(mcx, relnamespace)?
    } else {
        None
    };

    quote_qualified_identifier(mcx, nspname.as_deref(), relname.as_str())
}

/// `generate_function_name(funcid, nargs, argnames, argtypes, has_variadic,
/// use_variadic_p, inGroupBy)` (ruleutils.c 13256-13348): the
/// possibly-schema-qualified, quoted function name to display for `funcid`,
/// given the actual argument names/types of the call (which matter because of
/// ambiguous-function resolution), and whether `VARIADIC` should be printed.
///
/// Faithful port: read `proname`/`pronamespace` from `pg_proc`; force
/// qualification of `cube`/`rollup` inside GROUP BY; decide `use_variadic`;
/// then schema-qualify iff the unqualified name + argtypes + VARIADIC flag would
/// NOT re-resolve (via `func_get_detail`) to the same `funcid`. Returns
/// `(name, use_variadic)`.
///
/// Installed as the ruleutils inward seam `generate_function_name`.
pub fn generate_function_name_catalog<'mcx>(
    mcx: Mcx<'mcx>,
    funcid: Oid,
    nargs: i32,
    argnames: PgVec<'mcx, Option<PgString<'mcx>>>,
    argtypes: PgVec<'mcx, Oid>,
    has_variadic: bool,
    want_use_variadic: bool,
    in_group_by: bool,
) -> PgResult<(PgString<'mcx>, bool)> {
    use backend_utils_cache_lsyscache_seams as lsys;

    // proctup = SearchSysCache1(PROCOID, funcid); elog(ERROR) on miss.
    // proname = NameStr(procform->proname).
    let proname = match lsys::get_func_name::call(mcx, funcid)? {
        Some(s) => s,
        None => {
            return Err(elog_error(alloc::format!(
                "cache lookup failed for function {}",
                funcid
            )))
        }
    };

    // Due to parser hacks to avoid reserving CUBE, force qualification of some
    // function names within GROUP BY.
    let mut force_qualify = false;
    if in_group_by && (proname.as_str() == "cube" || proname.as_str() == "rollup") {
        force_qualify = true;
    }

    // Determine whether VARIADIC should be printed. Must be done first since it
    // affects the func_get_detail lookup rules.
    let use_variadic = if want_use_variadic {
        // Assert(!has_variadic || OidIsValid(procform->provariadic));
        debug_assert!(
            !has_variadic || lsys::get_func_variadictype::call(funcid).map(|v| v != Oid::from(0u32)).unwrap_or(true),
            "funcvariadic set but function is not variadic"
        );
        has_variadic
    } else {
        debug_assert!(!has_variadic);
        false
    };

    // Schema-qualify only if the parser would fail to resolve the correct
    // function from the unqualified name + argtypes + VARIADIC flag. If we
    // already decided to force qualification, skip the lookup and pretend the
    // lookup did not find our funcid.
    let resolves_to_same = if force_qualify {
        false
    } else {
        // func_get_detail(list_make1(makeString(proname)), NIL, argnames, nargs,
        //                 argtypes, !use_variadic, true, false, ...);
        // The seam carries argnames + expand_variadic (= !use_variadic) +
        // expand_defaults (= true, as C passes); include_out_arguments is fixed
        // false (C passes false). We pass the actual argument names through.
        // C's argnames is a List of String value-nodes (one per *named* arg, no
        // NULLs); flatten our Option list to the present names.
        let names: alloc::vec::Vec<alloc::string::String> =
            alloc::vec![alloc::string::String::from(proname.as_str())];
        let mut arg_names: alloc::vec::Vec<PgString<'mcx>> = alloc::vec::Vec::new();
        for o in argnames.iter() {
            if let Some(s) = o.as_ref() {
                arg_names.push(PgString::from_str_in(s.as_str(), mcx)?);
            }
        }
        let arg_types: alloc::vec::Vec<Oid> = argtypes.iter().copied().collect();
        let detail = backend_parser_parse_func_seams::func_get_detail::call(
            mcx,
            &names,
            &arg_names,
            nargs,
            &arg_types,
            !use_variadic,
            true,
        )?;
        use backend_parser_parse_func_seams::FuncDetailCode as FDC;
        matches!(
            detail.fdresult,
            FDC::Normal | FDC::Aggregate | FDC::WindowFunc
        ) && detail.funcid == funcid
    };

    let nspname: Option<PgString<'mcx>> = if resolves_to_same {
        None
    } else {
        let pronamespace = lsys::get_func_namespace::call(funcid)?;
        lsys::get_namespace_name_or_temp::call(mcx, pronamespace)?
    };

    let result = quote_qualified_identifier(mcx, nspname.as_deref(), proname.as_str())?;
    Ok((result, use_variadic))
}

/// `generate_operator_clause(buf, leftop, leftoptype, opoid, rightop,
/// rightoptype)` (ruleutils.c:13439) — append the schema-qualified, optionally
/// casted `leftop OPERATOR(nsp.opr) rightop` fragment to a fresh buffer and
/// return its raw bytes.  `ri_GenerateQual` (ri_triggers.c) is the sole caller.
pub fn generate_operator_clause_catalog<'mcx>(
    mcx: Mcx<'mcx>,
    leftop: &[u8],
    leftoptype: Oid,
    opoid: Oid,
    rightop: &[u8],
    rightoptype: Oid,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    // opertup = SearchSysCache1(OPEROID, opoid); elog(ERROR) on miss.
    let operform = match backend_utils_cache_syscache_seams::oper_row_by_oid::call(mcx, opoid)? {
        Some(o) => o,
        None => {
            return Err(elog_error(alloc::format!(
                "cache lookup failed for operator {}",
                opoid
            )))
        }
    };
    // Assert(operform->oprkind == 'b');
    debug_assert_eq!(operform.oprkind, b'b');

    // nspname = get_namespace_name(operform->oprnamespace);
    let nspname =
        backend_utils_cache_lsyscache_seams::get_namespace_name::call(mcx, operform.oprnamespace)?
            .ok_or_else(|| {
                elog_error(alloc::format!(
                    "cache lookup failed for namespace {}",
                    operform.oprnamespace
                ))
            })?;

    let mut buf: alloc::vec::Vec<u8> = alloc::vec::Vec::new();

    // appendStringInfoString(buf, leftop);
    buf.extend_from_slice(leftop);
    // if (leftoptype != operform->oprleft) add_cast_to(buf, operform->oprleft);
    if leftoptype != operform.oprleft {
        add_cast_to(mcx, &mut buf, operform.oprleft)?;
    }
    // appendStringInfo(buf, " OPERATOR(%s.", quote_identifier(nspname));
    buf.extend_from_slice(b" OPERATOR(");
    let qnsp = quote_identifier(mcx, nspname.as_str())?;
    buf.extend_from_slice(qnsp.as_bytes());
    buf.extend_from_slice(b".");
    // appendStringInfoString(buf, oprname);
    buf.extend_from_slice(operform.oprname.as_bytes());
    // appendStringInfo(buf, ") %s", rightop);
    buf.extend_from_slice(b") ");
    buf.extend_from_slice(rightop);
    // if (rightoptype != operform->oprright) add_cast_to(buf, operform->oprright);
    if rightoptype != operform.oprright {
        add_cast_to(mcx, &mut buf, operform.oprright)?;
    }

    mcx::slice_in(mcx, &buf)
}

/// `add_cast_to(buf, typid)` (ruleutils.c:13478) — append `::nsp.typ` for the
/// type (always schema-qualified, default typmod), spelled out the hard way
/// (not `format_type_be`) to avoid CHARACTER/BIT truncation corner cases.
fn add_cast_to<'mcx>(mcx: Mcx<'mcx>, buf: &mut alloc::vec::Vec<u8>, typid: Oid) -> PgResult<()> {
    // typetup = SearchSysCache1(TYPEOID, typid); elog(ERROR) on miss.
    let nm = backend_utils_cache_syscache_seams::type_namespace_and_name::call(mcx, typid)?
        .ok_or_else(|| elog_error(alloc::format!("cache lookup failed for type {}", typid)))?;
    // nspname = get_namespace_name_or_temp(typform->typnamespace);
    let nspname = backend_utils_cache_lsyscache_seams::get_namespace_name_or_temp::call(
        mcx, nm.namespace,
    )?;
    // appendStringInfo(buf, "::%s.%s", quote_identifier(nspname),
    //                  quote_identifier(typname));
    buf.extend_from_slice(b"::");
    let qnsp = quote_identifier(mcx, nspname.as_deref().unwrap_or(""))?;
    buf.extend_from_slice(qnsp.as_bytes());
    buf.extend_from_slice(b".");
    let qtyp = quote_identifier(mcx, nm.name.as_str())?;
    buf.extend_from_slice(qtyp.as_bytes());
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * Catalog name generators / option flatteners used by the index & constraint
 * definition deparsers (ruleutils.c 12861-13668).
 * -------------------------------------------------------------------------- */

/// `generate_collation_name(collid)` (ruleutils.c 13543-13567) — the
/// schema-qualified, quoted name of a collation. Installed as the ruleutils
/// `generate_collation_name` inward seam (the expression deparser reaches it
/// through that seam; the index deparser calls it directly).
pub fn generate_collation_name<'mcx>(mcx: Mcx<'mcx>, collid: Oid) -> PgResult<PgString<'mcx>> {
    // tp = SearchSysCache1(COLLOID, collid); elog(ERROR) on miss.
    let nm = backend_utils_cache_syscache_seams::collation_namespace_and_name::call(mcx, collid)?
        .ok_or_else(|| elog_error(format!("cache lookup failed for collation {collid}")))?;

    // nspname = CollationIsVisible(collid) ? NULL : get_namespace_name_or_temp(collnamespace).
    let nspname: Option<PgString<'mcx>> =
        if backend_catalog_namespace_seams::collation_is_visible::call(mcx, collid)? {
            None
        } else {
            backend_utils_cache_lsyscache_seams::get_namespace_name_or_temp::call(mcx, nm.namespace)?
        };

    // result = quote_qualified_identifier(nspname, collname);
    quote_qualified_identifier(mcx, nspname.as_deref(), nm.name.as_str())
}

/// `get_opclass_name(opclass, actual_datatype, buf)` (ruleutils.c 12861-12890) —
/// append ` <opclass-name>` to `buf`, but only when the opclass is not the
/// default for `actual_datatype` (or `actual_datatype` is `InvalidOid`). The
/// name is schema-qualified iff the opclass is not visible in the search path.
pub(crate) fn get_opclass_name<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut alloc::string::String,
    opclass: Oid,
    actual_datatype: Oid,
) -> PgResult<()> {
    // opcrec = SearchSysCache1(CLAOID, opclass); elog(ERROR) on miss.
    let (opcnamespace, opcmethod, opcname) =
        backend_utils_cache_syscache_seams::opclass_namespace_method_name::call(mcx, opclass)?
            .ok_or_else(|| elog_error(format!("cache lookup failed for opclass {opclass}")))?;

    // if (!OidIsValid(actual_datatype) ||
    //     GetDefaultOpClass(actual_datatype, opcrec->opcmethod) != opclass)
    let is_default = oid_is_valid(actual_datatype)
        && backend_utils_cache_lsyscache_seams::get_default_opclass::call(
            actual_datatype,
            opcmethod,
        )? == opclass;
    if !is_default {
        if backend_catalog_namespace_seams::opclass_is_visible::call(mcx, opclass)? {
            // appendStringInfo(buf, " %s", quote_identifier(opcname));
            let q = quote_identifier(mcx, opcname.as_str())?;
            buf.push(' ');
            buf.push_str(q.as_str());
        } else {
            // nspname = get_namespace_name_or_temp(opcrec->opcnamespace);
            // appendStringInfo(buf, " %s.%s", quote_identifier(nspname),
            //                  quote_identifier(opcname));
            let nspname =
                backend_utils_cache_lsyscache_seams::get_namespace_name_or_temp::call(
                    mcx,
                    opcnamespace,
                )?;
            let qnsp = quote_identifier(mcx, nspname.as_deref().unwrap_or(""))?;
            let qopc = quote_identifier(mcx, opcname.as_str())?;
            buf.push(' ');
            buf.push_str(qnsp.as_str());
            buf.push('.');
            buf.push_str(qopc.as_str());
        }
    }
    Ok(())
}

/// `generate_opclass_name(opclass)` (ruleutils.c 12898-12907) — the
/// schema-qualified opclass name (no leading space). Installed as the ruleutils
/// `generate_opclass_name` inward seam.
pub fn generate_opclass_name<'mcx>(mcx: Mcx<'mcx>, opclass: Oid) -> PgResult<PgString<'mcx>> {
    let mut buf = alloc::string::String::new();
    // get_opclass_name(opclass, InvalidOid, &buf);
    get_opclass_name(mcx, &mut buf, opclass, Oid::default())?;
    // return &buf.data[1];  /* get_opclass_name() prepends space */
    PgString::from_str_in(buf.strip_prefix(' ').unwrap_or(&buf), mcx)
}

/// `get_reloptions(buf, reloptions)` (ruleutils.c 13587-13637) — render a
/// `text[]` of `name=value` reloptions into `buf`.
fn get_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut alloc::string::String,
    reloptions: &[u8],
) -> PgResult<()> {
    use alloc::string::String;

    // deconstruct_array_builtin(DatumGetArrayTypeP(reloptions), TEXTOID, ...).
    let options =
        backend_utils_adt_arrayfuncs_seams::deconstruct_text_array::call(mcx, reloptions)?;

    for (i, option) in options.iter().enumerate() {
        let option = option.as_str();
        // name=value split on the first '='; missing '=' -> empty value.
        let (name, value) = match option.find('=') {
            Some(pos) => (&option[..pos], &option[pos + 1..]),
            None => (option, ""),
        };

        if i > 0 {
            buf.push_str(", ");
        }
        // appendStringInfo(buf, "%s=", quote_identifier(name));
        let qname = quote_identifier(mcx, name)?;
        buf.push_str(qname.as_str());
        buf.push('=');

        // if (quote_identifier(value) == value) append value else simple_quote_literal(value).
        // quote_identifier returns its argument verbatim iff no quoting is needed.
        let qval = quote_identifier(mcx, value)?;
        if qval.as_str() == value {
            buf.push_str(value);
        } else {
            simple_quote_literal_into(buf, value);
        }
    }
    let _ = String::new;
    Ok(())
}

/// `simple_quote_literal(buf, val)` (ruleutils.c 11963) rendered into an owned
/// `String` (the catalog-def builders accumulate into a plain `String`, not a
/// `DeparseContext` buffer). Doubles embedded single quotes / backslashes and
/// emits the `E''` prefix when a backslash is present.
fn simple_quote_literal_into(buf: &mut alloc::string::String, val: &str) {
    // Mirror simple_quote_literal: leading ' (with E if needed), escape ' and \\.
    let needs_e = val.contains('\\');
    if needs_e {
        buf.push('E');
    }
    buf.push('\'');
    for ch in val.chars() {
        if ch == '\'' || ch == '\\' {
            buf.push(ch);
        }
        buf.push(ch);
    }
    buf.push('\'');
}

/// `oid_is_valid` re-exported for the catalog-def modules.
pub(crate) fn oid_is_valid_pub(oid: Oid) -> bool {
    oid_is_valid(oid)
}

/// `simple_quote_literal(buf, val)` re-exported for the trigger-def module
/// (the `tgargs` literals render through the same `String`-accumulating helper).
pub(crate) fn simple_quote_literal_into_pub(buf: &mut alloc::string::String, val: &str) {
    simple_quote_literal_into(buf, val)
}

/// `GET_PRETTY_FLAGS(pretty)` (ruleutils.c 92) — re-exported for the trigger-def
/// module. `pretty ? (PAREN|INDENT|SCHEMA) : INDENT`.
pub(crate) fn get_pretty_flags_pub(pretty: bool) -> i32 {
    if pretty {
        PRETTYFLAG_PAREN | PRETTYFLAG_INDENT | PRETTYFLAG_SCHEMA
    } else {
        PRETTYFLAG_INDENT
    }
}

/// `get_reloptions(buf, reloptions)` re-exported for the index deparser (opclass
/// options rendering).
pub(crate) fn get_reloptions_pub<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut alloc::string::String,
    reloptions: &[u8],
) -> PgResult<()> {
    get_reloptions(mcx, buf, reloptions)
}

/// `flatten_reloptions(relid)` (ruleutils.c 13642-13669) — the relation's
/// reloptions as a `name=value, …` string, or `None` when unset.
pub(crate) fn flatten_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    // reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
    let token = backend_utils_cache_syscache_seams::fetch_class_reloptions::call(mcx, relid)?;
    if token.is_null {
        return Ok(None);
    }
    let mut buf = alloc::string::String::new();
    get_reloptions(mcx, &mut buf, &token.bytes)?;
    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}

mod seams;
pub use seams::init_seams;

mod expr_deparse;
pub use expr_deparse::{
    get_coercion_expr, get_const_expr, get_func_expr, get_oper_expr, get_parameter,
    get_rule_expr, get_rule_expr_funccall, get_rule_expr_paren, get_rule_expr_toplevel,
    get_rule_list_toplevel, get_sublink_expr, get_variable, isSimpleNode,
};

mod query_deparse;
pub use query_deparse::{
    get_query_def, get_rule_orderby, get_rule_windowspec,
};

/// `AccessShareLock` (`storage/lockdefs.h`) — the lock `deparse_context_for`
/// takes on its synthetic relation RTE.
const AccessShareLock: i32 = 1;

/// `RELKIND_RELATION` (`catalog/pg_class.h`) — ordinary table.
const RELKIND_RELATION: i8 = b'r' as i8;

/* -------------------------------------------------------------------------- *
 * Small helpers (errors, strings, list access).
 * -------------------------------------------------------------------------- */

/// `elog(ERROR, ...)` inside a deparse routine — produce an error `PgError`.
fn elog_error(msg: alloc::string::String) -> PgError {
    PgError::error(msg)
}

/// `strVal(node)` for a `Node::String` (the WITH/USING/colnames lists carry
/// their `char *` members as `String` value nodes). Returns the `&str`, or an
/// error if the node is not a `String` (which cannot happen for these lists
/// after parse analysis).
fn str_val<'a>(node: &'a Node<'_>) -> PgResult<&'a str> {
    node.as_string().map(|s| s.sval.as_str()).ok_or_else(|| {
        elog_error(format!(
            "expected String value node, got tag {}",
            node.tag().0
        ))
    })
}

/// `list_nth(list, n)` for a list of `String` value nodes (`char *` list) — the
/// `n`-th name, or an error if out of range. The list elements are boxed
/// (`List *` of `Node *`), so deref the box.
fn list_nth_str<'a, 'mcx>(
    list: &'a PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    n: usize,
) -> PgResult<&'a str> {
    match list.get(n) {
        Some(node) => str_val(node),
        None => Err(elog_error(format!("string list index {n} out of range"))),
    }
}

/// `rt_fetch(index, rtable)` — borrow the 1-based RTE. Mirrors the C macro
/// `rt_fetch`, which is `list_nth(rtable, index-1)`.
fn rt_fetch<'a, 'mcx>(
    index: i32,
    rtable: &'a [RangeTblEntry<'mcx>],
) -> PgResult<&'a RangeTblEntry<'mcx>> {
    let i = (index - 1) as usize;
    rtable.get(i).ok_or_else(|| {
        elog_error(format!(
            "rt_fetch: range-table index {index} out of range (len {})",
            rtable.len()
        ))
    })
}

/* -------------------------------------------------------------------------- *
 * The four ruleutils-private types (ruleutils.c 110-313).
 * -------------------------------------------------------------------------- */

/// `typedef struct { ... } deparse_context` (`ruleutils.c` 110-127).
///
/// The context info threaded through the recursive deparse routines. F0a uses
/// only `namespaces` (for [`get_rtable_name`]); the rest of the fields are
/// modeled field-for-field so F1/F2 fill them in without re-shaping the struct.
/// The `buf`/`StringInfo` output sink is deliberately not modeled in F0a (it is
/// introduced with the F2 query deparsers that actually emit text).
pub struct DeparseContext<'mcx> {
    /// `StringInfo buf` — output buffer to append to. F0a never emitted text, so
    /// the buffer is introduced with F1 (the expression deparser is the first
    /// family that renders SQL). Modeled as the owned [`types_stringinfo::StringInfo`]
    /// (the C `appendStringInfo*` family lives in `stringinfo.c`; the deparser's
    /// thin append helpers in `expr_deparse` wrap the byte buffer directly).
    pub buf: types_stringinfo::StringInfo<'mcx>,
    /// `List *namespaces` — list of `deparse_namespace` nodes.
    pub namespaces: PgVec<'mcx, DeparseNamespace<'mcx>>,
    /// `TupleDesc resultDesc` — if top level of a view, the view's tupdesc.
    /// Read by [`get_variable`]'s `varInOrderBy` column-name-match path.
    pub resultDesc: Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>,
    /// `List *targetList` — current query level's SELECT targetlist. Read by
    /// [`get_variable`]'s `varInOrderBy` path; set by the F2 query deparsers.
    pub targetList: PgVec<'mcx, types_nodes::primnodes::TargetEntry<'mcx>>,
    /// `List *windowClause` — current query level's WINDOW clause (list of
    /// `WindowClause`). Read by the `WindowFunc` `OVER` query-decompilation path.
    pub windowClause: PgVec<'mcx, types_nodes::rawnodes::WindowClause<'mcx>>,
    /// `int prettyFlags` — enabling of pretty-print functions.
    pub prettyFlags: i32,
    /// `int wrapColumn` — max line length, or -1 for no limit.
    pub wrapColumn: i32,
    /// `int indentLevel` — current indent level for pretty-print.
    pub indentLevel: i32,
    /// `bool varprefix` — true to print prefixes on Vars.
    pub varprefix: bool,
    /// `bool colNamesVisible` — do we care about output column names?
    pub colNamesVisible: bool,
    /// `bool inGroupBy` — deparsing GROUP BY clause?
    pub inGroupBy: bool,
    /// `bool varInOrderBy` — deparsing simple Var in ORDER BY?
    pub varInOrderBy: bool,
    /// `Bitmapset *appendparents` — if not null, map child Vars of these relids
    /// back to the parent rel.
    pub appendparents: Option<types_nodes::bitmapset::Bitmapset<'mcx>>,
}

/// `typedef struct { ... } deparse_namespace` (`ruleutils.c` 159-186).
///
/// One Var-namespace level per query/plan context. Carries both the Query-side
/// fields (rtable / rtable_names / rtable_columns / ctes / appendrels /
/// using_names) and the nine plan-only fields (plan / ancestors / outer_plan /
/// inner_plan / *_tlist / index_tlist). The plan-only fields stay at their
/// zero/None default until ruleutils F0b populates them; F0a never reads them.
pub struct DeparseNamespace<'mcx> {
    /// `List *rtable` — list of `RangeTblEntry` nodes (the query's range table).
    /// F0a borrows the Query's range table rather than owning a copy.
    pub rtable: PgVec<'mcx, RangeTblEntry<'mcx>>,
    /// `List *rtable_names` — parallel list of names for RTEs (`None` for
    /// nameless RTEs such as unnamed joins).
    pub rtable_names: PgVec<'mcx, Option<PgString<'mcx>>>,
    /// `List *rtable_columns` — parallel list of `deparse_columns` structs.
    pub rtable_columns: PgVec<'mcx, DeparseColumns<'mcx>>,
    /// `List *subplans` — list of `Plan` trees for SubPlans (PlannedStmt case).
    /// Plan-only; not populated until F0b (carried as the generic Node list).
    pub subplans: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `List *ctes` — list of `CommonTableExpr` nodes (Query case).
    pub ctes: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `AppendRelInfo **appendrels` — array indexed by child relid, or empty.
    /// Plan-only (PlannedStmt case); holds the trimmed plan-data carrier the
    /// child->parent Var mapping reads.
    pub appendrels: PgVec<'mcx, Option<types_nodes::appendrel_carrier::AppendRelInfoCarrier>>,
    /// `char *ret_old_alias` — alias for OLD in RETURNING list.
    pub ret_old_alias: Option<PgString<'mcx>>,
    /// `char *ret_new_alias` — alias for NEW in RETURNING list.
    pub ret_new_alias: Option<PgString<'mcx>>,
    /// `bool unique_using` — are we making USING names globally unique?
    pub unique_using: bool,
    /// `List *using_names` — list of assigned names for USING columns.
    pub using_names: PgVec<'mcx, PgString<'mcx>>,
    // --- Remaining fields used only when deparsing a Plan tree (F0b): ---
    /// `Plan *plan` — immediate parent of the current expression.
    pub plan: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `List *ancestors` — ancestors of `plan`.
    pub ancestors: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `Plan *outer_plan` — outer subnode, or None.
    pub outer_plan: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `Plan *inner_plan` — inner subnode, or None.
    pub inner_plan: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `List *outer_tlist` — referent for OUTER_VAR Vars.
    pub outer_tlist: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `List *inner_tlist` — referent for INNER_VAR Vars.
    pub inner_tlist: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    /// `List *index_tlist` — referent for INDEX_VAR Vars.
    pub index_tlist: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    // --- Special namespace representing a function signature (F1): ---
    /// `char *funcname`.
    pub funcname: Option<PgString<'mcx>>,
    /// `int numargs`.
    pub numargs: i32,
    /// `char **argnames`.
    pub argnames: PgVec<'mcx, Option<PgString<'mcx>>>,
}

/// `typedef struct { ... } deparse_columns` (`ruleutils.c` 228-313).
///
/// Per-relation column alias data. The string arrays are `Vec<Option<String>>`
/// where `None` is the C `char *` NULL (a dropped column / unassigned slot).
pub struct DeparseColumns<'mcx> {
    /// `int num_cols` — length of `colnames[]`.
    pub num_cols: i32,
    /// `char **colnames` — array of C strings and NULLs (indexed by varattno-1).
    pub colnames: PgVec<'mcx, Option<PgString<'mcx>>>,
    /// `int num_new_cols` — length of `new_colnames[]`.
    pub num_new_cols: i32,
    /// `char **new_colnames` — array of C strings (no dropped columns).
    pub new_colnames: PgVec<'mcx, Option<PgString<'mcx>>>,
    /// `bool *is_new_col` — which new_colnames are new since original parsing.
    pub is_new_col: PgVec<'mcx, bool>,
    /// `bool printaliases` — should we actually print a column alias list?
    pub printaliases: bool,
    /// `List *parentUsing` — names used as USING names in joins above this RTE.
    pub parentUsing: PgVec<'mcx, PgString<'mcx>>,
    /// `int leftrti` — rangetable index of left child (JOIN RTE only).
    pub leftrti: i32,
    /// `int rightrti` — rangetable index of right child (JOIN RTE only).
    pub rightrti: i32,
    /// `int *leftattnos` — left-child varattnos of join cols, or 0.
    pub leftattnos: PgVec<'mcx, i32>,
    /// `int *rightattnos` — right-child varattnos of join cols, or 0.
    pub rightattnos: PgVec<'mcx, i32>,
    /// `List *usingNames` — names assigned to merged columns.
    pub usingNames: PgVec<'mcx, PgString<'mcx>>,
    /// `HTAB *names_hash` — copies of all strings in this struct's colnames /
    /// new_colnames / parentUsing. Built only for sufficiently wide relations
    /// (>= 32 cols) and only during set_relation/set_join_column_names; `None`
    /// otherwise. C uses a string `HTAB`; the owned model is a `BTreeMap` set
    /// (the payload is the key itself).
    pub names_hash: Option<BTreeMap<alloc::string::String, ()>>,
}

impl<'mcx> DeparseColumns<'mcx> {
    /// Deep-clone the column info into `mcx` (F2 reads a colinfo by value while
    /// mutating the context buffer; the owned model has no shared pointers).
    pub(crate) fn clone_columns(&self, mcx: Mcx<'mcx>) -> PgResult<DeparseColumns<'mcx>> {
        let opt_str_vec = |v: &PgVec<'mcx, Option<PgString<'mcx>>>| -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>> {
            let mut out = PgVec::new_in(mcx);
            out.try_reserve(v.len()).map_err(|_| mcx.oom(0))?;
            for s in v.iter() {
                out.push(match s {
                    Some(p) => Some(pstrdup(mcx, p.as_str())?),
                    None => None,
                });
            }
            Ok(out)
        };
        let str_vec = |v: &PgVec<'mcx, PgString<'mcx>>| -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
            clone_str_vec(mcx, v)
        };
        let i32_vec = |v: &PgVec<'mcx, i32>| -> PgResult<PgVec<'mcx, i32>> {
            let mut out = PgVec::new_in(mcx);
            out.try_reserve(v.len()).map_err(|_| mcx.oom(0))?;
            for &x in v.iter() {
                out.push(x);
            }
            Ok(out)
        };
        let bool_vec = |v: &PgVec<'mcx, bool>| -> PgResult<PgVec<'mcx, bool>> {
            let mut out = PgVec::new_in(mcx);
            out.try_reserve(v.len()).map_err(|_| mcx.oom(0))?;
            for &x in v.iter() {
                out.push(x);
            }
            Ok(out)
        };
        Ok(DeparseColumns {
            num_cols: self.num_cols,
            colnames: opt_str_vec(&self.colnames)?,
            num_new_cols: self.num_new_cols,
            new_colnames: opt_str_vec(&self.new_colnames)?,
            is_new_col: bool_vec(&self.is_new_col)?,
            printaliases: self.printaliases,
            parentUsing: str_vec(&self.parentUsing)?,
            leftrti: self.leftrti,
            rightrti: self.rightrti,
            leftattnos: i32_vec(&self.leftattnos)?,
            rightattnos: i32_vec(&self.rightattnos)?,
            usingNames: str_vec(&self.usingNames)?,
            names_hash: self.names_hash.clone(),
        })
    }

    /// `palloc0(sizeof(deparse_columns))` — a zeroed `deparse_columns` (all
    /// arrays empty, all scalars zero, `names_hash` NULL).
    fn zeroed(mcx: Mcx<'mcx>) -> DeparseColumns<'mcx> {
        DeparseColumns {
            num_cols: 0,
            colnames: PgVec::new_in(mcx),
            num_new_cols: 0,
            new_colnames: PgVec::new_in(mcx),
            is_new_col: PgVec::new_in(mcx),
            printaliases: false,
            parentUsing: PgVec::new_in(mcx),
            leftrti: 0,
            rightrti: 0,
            leftattnos: PgVec::new_in(mcx),
            rightattnos: PgVec::new_in(mcx),
            usingNames: PgVec::new_in(mcx),
            names_hash: None,
        }
    }
}

impl<'mcx> DeparseNamespace<'mcx> {
    /// `memset(dpns, 0, sizeof(deparse_namespace))` — a zeroed namespace.
    pub(crate) fn zeroed(mcx: Mcx<'mcx>) -> DeparseNamespace<'mcx> {
        DeparseNamespace {
            rtable: PgVec::new_in(mcx),
            rtable_names: PgVec::new_in(mcx),
            rtable_columns: PgVec::new_in(mcx),
            subplans: PgVec::new_in(mcx),
            ctes: PgVec::new_in(mcx),
            appendrels: PgVec::new_in(mcx),
            ret_old_alias: None,
            ret_new_alias: None,
            unique_using: false,
            using_names: PgVec::new_in(mcx),
            plan: None,
            ancestors: PgVec::new_in(mcx),
            outer_plan: None,
            inner_plan: None,
            outer_tlist: PgVec::new_in(mcx),
            inner_tlist: PgVec::new_in(mcx),
            index_tlist: PgVec::new_in(mcx),
            funcname: None,
            numargs: 0,
            argnames: PgVec::new_in(mcx),
        }
    }
}

/// `typedef struct { char name[NAMEDATALEN]; int counter; } NameHashEntry`
/// (`ruleutils.c` 318-322) — an entry in `set_rtable_names`' hash table. In the
/// owned model the name is the `BTreeMap` key; this struct documents the C type
/// and pairs the key with its `counter` payload.
pub struct NameHashEntry {
    /// `char name[NAMEDATALEN]` — hash key.
    pub name: alloc::string::String,
    /// `int counter` — largest addition used so far for this name.
    pub counter: i32,
}

/// `str_val(node)` — crate-internal access for the F2 query-deparse module.
pub(crate) fn str_val_pub<'a>(node: &'a Node<'_>) -> PgResult<&'a str> {
    str_val(node)
}

/// `get_rtable_name(rtindex, context)` — crate-internal access for F2.
pub(crate) fn get_rtable_name_pub<'a, 'mcx>(
    rtindex: i32,
    context: &'a DeparseContext<'mcx>,
) -> PgResult<Option<&'a str>> {
    get_rtable_name(rtindex, context)
}

/// `get_rel_name(relid)` — crate-internal access for F2.
pub(crate) fn get_rel_name_pub<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    get_rel_name(mcx, relid)
}

/// Deep-clone a `DeparseNamespace` — crate-internal access for F2 (recursion
/// threads the parent namespace stack by value, as C does `list_copy`).
pub(crate) fn clone_namespace_pub<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &DeparseNamespace<'mcx>,
) -> PgResult<DeparseNamespace<'mcx>> {
    clone_namespace(mcx, dpns)
}

/// `format_type_with_typemod(type_oid, typemod)` — crate-internal access for F2.
pub(crate) fn format_type_with_typemod_pub<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    typemod: i32,
) -> PgResult<PgString<'mcx>> {
    match backend_utils_adt_format_type_seams::format_type_extended::call(mcx, type_oid, typemod, 0)? {
        Some(s) => Ok(s),
        None => Err(elog_error("format_type_with_typemod returned NULL".into())),
    }
}

/// `simple_quote_literal(buf, str)` — crate-internal access for F2.
pub(crate) fn simple_quote_literal_pub(
    context: &mut DeparseContext<'_>,
    val: &str,
) -> PgResult<()> {
    expr_deparse::simple_quote_literal_pub(context, val)
}

/// `deparse_columns_fetch(rangetable_index, dpns)` (`ruleutils.c` 315-317) —
/// borrow the `deparse_columns` for the 1-based range-table index.
fn deparse_columns_fetch<'a, 'mcx>(
    rangetable_index: i32,
    dpns: &'a DeparseNamespace<'mcx>,
) -> &'a DeparseColumns<'mcx> {
    &dpns.rtable_columns[(rangetable_index - 1) as usize]
}

/* -------------------------------------------------------------------------- *
 * OOM-safe list/string allocation helpers (charged to mcx).
 * -------------------------------------------------------------------------- */

/// `lappend(list, x)` — push, growing fallibly (palloc OOM surfaces as the
/// recoverable `PgError`).
fn lappend<T>(mcx: Mcx<'_>, list: &mut PgVec<'_, T>, x: T) -> PgResult<()> {
    list.try_reserve(1).map_err(|_| mcx.oom(0))?;
    list.push(x);
    Ok(())
}

/// Clone a `PgString` into `mcx` (C `pstrdup`).
fn pstrdup<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<PgString<'mcx>> {
    PgString::from_str_in(s, mcx)
}

/* -------------------------------------------------------------------------- *
 * set_rtable_names + the EXPLAIN frontend (ruleutils.c 3854-4020).
 * -------------------------------------------------------------------------- */

/// `get_rel_name(rte->relid)` through the lsyscache seam — the live relation
/// name, or `None` if the relation is gone (a deleted rel mid-deparse).
fn get_rel_name<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, relid)
}

/// `set_rtable_names: select RTE aliases to be used in printing a query`
/// (`ruleutils.c` 3883-4020).
///
/// Fills `dpns.rtable_names` one-for-one with `dpns.rtable`; each name is unique
/// among those in the new namespace plus the ancestor `parent_namespaces`. If
/// `rels_used` is `Some`, only RTE indexes in it are given aliases.
pub fn set_rtable_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    parent_namespaces: &[DeparseNamespace<'mcx>],
    rels_used: Option<&types_nodes::bitmapset::Bitmapset<'mcx>>,
) -> PgResult<()> {
    dpns.rtable_names = PgVec::new_in(mcx);
    // nothing more to do if empty rtable
    if dpns.rtable.is_empty() {
        return Ok(());
    }

    // We use a hash table to hold known names, so this is O(N) not O(N^2).
    // names_hash maps a known name to its NameHashEntry counter.
    let mut names_hash: BTreeMap<alloc::string::String, i32> = BTreeMap::new();

    // Preload the hash table with names appearing in parent_namespaces.
    for olddpns in parent_namespaces {
        for oldname in olddpns.rtable_names.iter() {
            let oldname = match oldname {
                Some(n) => n,
                None => continue,
            };
            // we do not complain about duplicate names in parent namespaces
            names_hash.insert(oldname.as_str().into(), 0);
        }
    }

    // Now we can scan the rtable.
    let mut rtindex: i32 = 1;
    // Index the rtable by position; the per-RTE refname can come from the
    // catalog (get_rel_name), so the loop is value-producing/fallible.
    let nrte = dpns.rtable.len();
    for i in 0..nrte {
        // CHECK_FOR_INTERRUPTS() is process-global and handled by the host loop.

        // Determine the candidate refname for this RTE.
        let mut refname: Option<PgString<'mcx>> = {
            let rte = &dpns.rtable[i];
            if rels_used.is_some()
                && !backend_nodes_core_seams::bms_is_member::call(rtindex, rels_used)
            {
                // Ignore unreferenced RTE.
                None
            } else if let Some(alias) = rte.alias.as_ref() {
                // If RTE has a user-defined alias, prefer that.
                match alias.aliasname.as_ref() {
                    Some(n) => Some(pstrdup(mcx, n.as_str())?),
                    None => None,
                }
            } else if rte.rtekind == RTE_RELATION {
                // Use the current actual name of the relation.
                get_rel_name(mcx, rte.relid)?
            } else if rte.rtekind == RTE_JOIN {
                // Unnamed join has no refname.
                None
            } else {
                // Otherwise use whatever the parser assigned.
                match rte.eref.as_ref().and_then(|e| e.aliasname.as_ref()) {
                    Some(n) => Some(pstrdup(mcx, n.as_str())?),
                    None => None,
                }
            }
        };

        // If the selected name isn't unique, append digits to make it so, and
        // make a new hash entry for it once we've got a unique name. For a very
        // long input name, we might have to truncate to stay within NAMEDATALEN.
        if let Some(rn) = refname.as_ref() {
            let key: alloc::string::String = rn.as_str().into();
            if let Some(counter) = names_hash.get(&key).copied() {
                // Name already in use, must choose a new one.
                let mut refnamelen = rn.len();
                let base: alloc::string::String = rn.as_str().into();
                let mut counter = counter;
                let modname: alloc::string::String;
                loop {
                    counter += 1;
                    let m = loop {
                        let candidate = format!("{}_{}", &base[..refnamelen], counter);
                        if (candidate.len() as i32) < NAMEDATALEN {
                            break candidate;
                        }
                        // drop chars from refname to keep all the digits
                        refnamelen = backend_utils_mb_mbutils_seams::pg_mbcliplen::call(
                            base.as_bytes(),
                            refnamelen as i32,
                            refnamelen as i32 - 1,
                        ) as usize;
                    };
                    if !names_hash.contains_key(&m) {
                        modname = m;
                        break;
                    }
                }
                // record the bumped counter against the original name's entry
                names_hash.insert(key, counter);
                // init new hash entry for the chosen modified name
                names_hash.insert(modname.clone(), 0);
                refname = Some(pstrdup(mcx, &modname)?);
            } else {
                // Name not previously used, need only initialize hentry.
                names_hash.insert(key, 0);
            }
        }

        lappend(mcx, &mut dpns.rtable_names, refname)?;
        rtindex += 1;
    }

    Ok(())
}

/// `select_rtable_names_for_explain(rtable, rels_used)` (`ruleutils.c`
/// 3854-3868) — choose the display alias for each RTE referenced in a plan
/// (`rels_used`). A frontend to [`set_rtable_names`]. Installed inward so
/// EXPLAIN can reach it across the cycle.
pub fn select_rtable_names_for_explain<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &PgVec<'mcx, RangeTblEntry<'mcx>>,
    rels_used: Option<&types_nodes::bitmapset::Bitmapset<'mcx>>,
) -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>> {
    let mut dpns = DeparseNamespace::zeroed(mcx);

    // dpns.rtable = rtable (the engine borrows by copying RTE images into mcx;
    // set_rtable_names only reads alias/eref/rtekind/relid off each RTE).
    dpns.rtable = clone_rtable(mcx, rtable)?;
    // subplans = NIL; ctes = NIL; appendrels = NULL — zeroed() already.
    // `rels_used` is threaded through as-is: when it is None (the C NULL
    // `*rels_used`, e.g. a dummy Result with no scan node), set_rtable_names
    // names every RTE so plan-targetlist Vars keep their relation prefixes.
    set_rtable_names(mcx, &mut dpns, &[], rels_used)?;
    // We needn't bother computing column aliases yet.

    Ok(dpns.rtable_names)
}

/// Re-home a range table's RTE images into `mcx` (the deparse namespace owns its
/// own `List *rtable` copy, as C does when it stores the Query/PlannedStmt
/// rtable pointer — here we deep-copy because the owned model has no shared
/// pointers). Uses the node-tree `RangeTblEntry::clone_in`.
fn clone_rtable<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &PgVec<'mcx, RangeTblEntry<'mcx>>,
) -> PgResult<PgVec<'mcx, RangeTblEntry<'mcx>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(rtable.len()).map_err(|_| mcx.oom(0))?;
    for rte in rtable.iter() {
        out.push(rte.clone_in(mcx)?);
    }
    Ok(out)
}

/* -------------------------------------------------------------------------- *
 * deparse_context_for — pure relation-only frontend (ruleutils.c 3700-3737).
 * -------------------------------------------------------------------------- */

/// `deparse_context_for(aliasname, relid)` (`ruleutils.c` 3700-3737) — build a
/// one-deep deparse namespace stack for a single relation, used by callers that
/// deparse a partial expression over one rel (CHECK / index predicate text).
pub fn deparse_context_for<'mcx>(
    mcx: Mcx<'mcx>,
    aliasname: &str,
    relid: Oid,
) -> PgResult<PgVec<'mcx, DeparseNamespace<'mcx>>> {
    let mut dpns = DeparseNamespace::zeroed(mcx);

    // Build a minimal RTE for the rel.
    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_RELATION;
    rte.relid = relid;
    rte.relkind = RELKIND_RELATION; // no need for exactness here
    rte.rellockmode = AccessShareLock;
    // rte->alias = makeAlias(aliasname, NIL);
    let alias = Alias {
        aliasname: Some(pstrdup(mcx, aliasname)?),
        colnames: PgVec::new_in(mcx),
    };
    rte.alias = Some(mcx::alloc_in(mcx, alias)?);
    // rte->eref = rte->alias (a second copy, since the owned model has no shared
    // pointers; both carry the same aliasname/colnames).
    let eref = Alias {
        aliasname: Some(pstrdup(mcx, aliasname)?),
        colnames: PgVec::new_in(mcx),
    };
    rte.eref = Some(mcx::alloc_in(mcx, eref)?);
    rte.lateral = false;
    rte.inh = false;
    rte.inFromCl = true;

    // Build one-element rtable.
    lappend(mcx, &mut dpns.rtable, rte)?;
    // subplans = NIL; ctes = NIL; appendrels = NULL — zeroed().
    set_rtable_names(mcx, &mut dpns, &[], None)?;
    set_simple_column_names(mcx, &mut dpns)?;

    // Return a one-deep namespace stack.
    let mut stack = PgVec::new_in(mcx);
    lappend(mcx, &mut stack, dpns)?;
    Ok(stack)
}

/// Build the two-deep `old`/`new` deparse namespace stack
/// `pg_get_triggerdef_worker` uses for a trigger WHEN qualification
/// (ruleutils.c 1075-1109): two minimal relation RTEs aliased `old`/`new` over
/// the same trigger relation, then `set_rtable_names` + `set_simple_column_names`.
/// Returns the one-deep namespace stack (`list_make1(&dpns)`), used with
/// `varprefix = true` so Vars render as `old.col` / `new.col`.
pub(crate) fn deparse_context_for_old_new<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relkind: i8,
) -> PgResult<PgVec<'mcx, DeparseNamespace<'mcx>>> {
    let mut dpns = DeparseNamespace::zeroed(mcx);

    let mut make_rte = |name: &str| -> PgResult<RangeTblEntry<'mcx>> {
        let mut rte = RangeTblEntry::new_in(mcx);
        rte.rtekind = RTE_RELATION;
        rte.relid = relid;
        rte.relkind = relkind;
        rte.rellockmode = AccessShareLock;
        let alias = Alias {
            aliasname: Some(pstrdup(mcx, name)?),
            colnames: PgVec::new_in(mcx),
        };
        rte.alias = Some(mcx::alloc_in(mcx, alias)?);
        let eref = Alias {
            aliasname: Some(pstrdup(mcx, name)?),
            colnames: PgVec::new_in(mcx),
        };
        rte.eref = Some(mcx::alloc_in(mcx, eref)?);
        rte.lateral = false;
        rte.inh = false;
        rte.inFromCl = true;
        Ok(rte)
    };

    // dpns.rtable = list_make2(oldrte, newrte);
    let oldrte = make_rte("old")?;
    let newrte = make_rte("new")?;
    lappend(mcx, &mut dpns.rtable, oldrte)?;
    lappend(mcx, &mut dpns.rtable, newrte)?;

    set_rtable_names(mcx, &mut dpns, &[], None)?;
    set_simple_column_names(mcx, &mut dpns)?;

    let mut stack = PgVec::new_in(mcx);
    lappend(mcx, &mut stack, dpns)?;
    Ok(stack)
}

/* -------------------------------------------------------------------------- *
 * set_deparse_for_query (ruleutils.c 4028-4085).
 * -------------------------------------------------------------------------- */

/// `set_deparse_for_query(dpns, query, parent_namespaces)` (`ruleutils.c`
/// 4028-4085) — initialize a `deparse_namespace` from scratch for deparsing a
/// `Query` tree: assign RTE aliases, zero the column structs, run the USING-name
/// pass over the jointree, then assign the remaining per-RTE column aliases.
pub fn set_deparse_for_query<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    query: &types_nodes::copy_query::Query<'mcx>,
    parent_namespaces: &[DeparseNamespace<'mcx>],
) -> PgResult<()> {
    // Initialize *dpns and fill rtable/ctes links.
    *dpns = DeparseNamespace::zeroed(mcx);
    dpns.rtable = clone_rtable(mcx, &query.rtable)?;
    // subplans = NIL; appendrels = NULL — zeroed().
    dpns.ctes = clone_node_vec(mcx, &query.cteList)?;
    dpns.ret_old_alias = match query.returningOldAlias.as_ref() {
        Some(s) => Some(pstrdup(mcx, s.as_str())?),
        None => None,
    };
    dpns.ret_new_alias = match query.returningNewAlias.as_ref() {
        Some(s) => Some(pstrdup(mcx, s.as_str())?),
        None => None,
    };

    // Assign a unique relation alias to each RTE.
    set_rtable_names(mcx, dpns, parent_namespaces, None)?;

    // Initialize dpns->rtable_columns to contain zeroed structs.
    dpns.rtable_columns = PgVec::new_in(mcx);
    while dpns.rtable_columns.len() < dpns.rtable.len() {
        lappend(mcx, &mut dpns.rtable_columns, DeparseColumns::zeroed(mcx))?;
    }

    // If it's a utility query, it won't have a jointree.
    if let Some(jointree) = query.jointree.as_ref() {
        // Detect whether global uniqueness of USING names is needed.
        let jt = Node::mk_from_expr(mcx, clone_fromexpr(mcx, jointree)?)?;
        dpns.unique_using = has_dangerous_join_using(mcx, dpns, &jt)?;

        // Select names for USING-merged columns via a recursive jointree pass.
        let empty: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
        set_using_names(mcx, dpns, &jt, &empty)?;
    }

    // Now assign remaining column aliases for each RTE. We do this in a linear
    // scan of the rtable, so as to process RTEs whether or not they are in the
    // jointree. JOIN RTEs must be processed after their children, which is OK
    // because they appear later in the rtable list than their children.
    let n = dpns.rtable.len();
    for i in 0..n {
        let is_join = dpns.rtable[i].rtekind == RTE_JOIN;
        // Detach the colinfo for this RTE so we can mutate it while reading
        // sibling colinfos/the rtable; reinsert it afterward (the C code mutates
        // *colinfo in place with the other structs reachable through dpns).
        let mut colinfo = core::mem::replace(
            &mut dpns.rtable_columns[i],
            DeparseColumns::zeroed(mcx),
        );
        if is_join {
            // need an owned RTE image for the by-value reads
            let rte = dpns.rtable[i].clone_in(mcx)?;
            set_join_column_names(mcx, dpns, &rte, &mut colinfo)?;
        } else {
            let rte = dpns.rtable[i].clone_in(mcx)?;
            set_relation_column_names(mcx, dpns, &rte, &mut colinfo)?;
        }
        dpns.rtable_columns[i] = colinfo;
    }

    Ok(())
}

/// `set_simple_column_names(dpns)` (`ruleutils.c` 4097-4118) — fill in column
/// aliases for non-query situations (EXPLAIN / relation-only RTEs). Join RTEs
/// are skipped (left all-zero).
pub fn set_simple_column_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
) -> PgResult<()> {
    // Initialize dpns->rtable_columns to contain zeroed structs.
    dpns.rtable_columns = PgVec::new_in(mcx);
    while dpns.rtable_columns.len() < dpns.rtable.len() {
        lappend(mcx, &mut dpns.rtable_columns, DeparseColumns::zeroed(mcx))?;
    }

    // Assign unique column aliases within each non-join RTE.
    let n = dpns.rtable.len();
    for i in 0..n {
        if dpns.rtable[i].rtekind != RTE_JOIN {
            let mut colinfo = core::mem::replace(
                &mut dpns.rtable_columns[i],
                DeparseColumns::zeroed(mcx),
            );
            let rte = dpns.rtable[i].clone_in(mcx)?;
            set_relation_column_names(mcx, dpns, &rte, &mut colinfo)?;
            dpns.rtable_columns[i] = colinfo;
        }
    }
    Ok(())
}

/* -------------------------------------------------------------------------- *
 * has_dangerous_join_using / set_using_names (ruleutils.c 4139-4365).
 * -------------------------------------------------------------------------- */

/// `has_dangerous_join_using(dpns, jtnode)` (`ruleutils.c` 4139-4191) — search
/// the jointree for an unnamed JOIN USING whose merged columns are not simple
/// Var references (which would force globally-unique USING aliases).
pub fn has_dangerous_join_using<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &DeparseNamespace<'mcx>,
    jtnode: &Node<'mcx>,
) -> PgResult<bool> {
    match jtnode.node_tag() {
        types_nodes::nodes::ntag::T_RangeTblRef => {
            // nothing to do here
            Ok(false)
        }
        types_nodes::nodes::ntag::T_FromExpr => {
            let f = jtnode.expect_fromexpr();
            for child in f.fromlist.iter() {
                if has_dangerous_join_using(mcx, dpns, child)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        types_nodes::nodes::ntag::T_JoinExpr => {
            let j = jtnode.expect_joinexpr();
            // Is it an unnamed JOIN with USING?
            if j.alias.is_none() && !j.usingClause.is_empty() {
                // Check each join alias var; if any merged col isn't a simple
                // reference to an underlying column, we have a dangerous case.
                let jrte = rt_fetch(j.rtindex, &dpns.rtable)?;
                for i in 0..(jrte.joinmergedcols as usize) {
                    let aliasvar = jrte.joinaliasvars.get(i).ok_or_else(|| {
                        elog_error(format!("joinaliasvars index {i} out of range"))
                    })?;
                    if !aliasvar.is_var() {
                        return Ok(true);
                    }
                }
            }

            // Nope, but inspect children.
            if let Some(larg) = j.larg.as_ref() {
                if has_dangerous_join_using(mcx, dpns, larg)? {
                    return Ok(true);
                }
            }
            if let Some(rarg) = j.rarg.as_ref() {
                if has_dangerous_join_using(mcx, dpns, rarg)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Err(elog_error(format!(
            "unrecognized node type: {}",
            jtnode.tag().0
        ))),
    }
}

/// `set_using_names(dpns, jtnode, parentUsing)` (`ruleutils.c` 4209-4365) —
/// select column aliases for USING-merged columns in a recursive descent of the
/// jointree. `dpns.unique_using` must already be set.
pub fn set_using_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    jtnode: &Node<'mcx>,
    parent_using: &PgVec<'mcx, PgString<'mcx>>,
) -> PgResult<()> {
    match jtnode.node_tag() {
        types_nodes::nodes::ntag::T_RangeTblRef => {
            // nothing to do now
            Ok(())
        }
        types_nodes::nodes::ntag::T_FromExpr => {
            let f = jtnode.expect_fromexpr();
            for child in f.fromlist.iter() {
                set_using_names(mcx, dpns, child, parent_using)?;
            }
            Ok(())
        }
        types_nodes::nodes::ntag::T_JoinExpr => {
            let j = jtnode.expect_joinexpr();
            let rtindex = j.rtindex;
            let rte = rt_fetch(rtindex, &dpns.rtable)?.clone_in(mcx)?;

            // Get info about the shape of the join — fills the join fields of
            // this RTE's colinfo (leftrti/rightrti/leftattnos/rightattnos).
            {
                let mut colinfo = core::mem::replace(
                    &mut dpns.rtable_columns[(rtindex - 1) as usize],
                    DeparseColumns::zeroed(mcx),
                );
                identify_join_columns(mcx, j, &rte, &mut colinfo)?;
                dpns.rtable_columns[(rtindex - 1) as usize] = colinfo;
            }

            let (leftrti, rightrti, num_cols) = {
                let colinfo = deparse_columns_fetch(rtindex, dpns);
                (colinfo.leftrti, colinfo.rightrti, colinfo.num_cols)
            };

            // If this join is unnamed, any name requirements pushed down to here
            // must be pushed down again to the children.
            if rte.alias.is_none() {
                for i in 0..(num_cols as usize) {
                    let colname = {
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        match colinfo.colnames.get(i).and_then(|c| c.as_ref()) {
                            Some(c) => pstrdup(mcx, c.as_str())?,
                            None => continue,
                        }
                    };
                    let (latt, ratt) = {
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        (colinfo.leftattnos[i], colinfo.rightattnos[i])
                    };
                    // Push down to left column, unless it's a system column.
                    if latt > 0 {
                        let leftcolinfo = &mut dpns.rtable_columns[(leftrti - 1) as usize];
                        expand_colnames_array_to(mcx, leftcolinfo, latt)?;
                        leftcolinfo.colnames[(latt - 1) as usize] =
                            Some(pstrdup(mcx, colname.as_str())?);
                    }
                    // Same on the righthand side.
                    if ratt > 0 {
                        let rightcolinfo = &mut dpns.rtable_columns[(rightrti - 1) as usize];
                        expand_colnames_array_to(mcx, rightcolinfo, ratt)?;
                        rightcolinfo.colnames[(ratt - 1) as usize] =
                            Some(pstrdup(mcx, colname.as_str())?);
                    }
                }
            }

            // The parentUsing list passed down to children. If there's a USING
            // clause, we extend a copy of it with the chosen merged names.
            let mut child_parent_using = clone_str_vec(mcx, parent_using)?;

            // If there's a USING clause, select the USING column names and push
            // those names down to the children.
            if !j.usingClause.is_empty() {
                // USING names must correspond to the first join output columns.
                {
                    let mut colinfo = core::mem::replace(
                        &mut dpns.rtable_columns[(rtindex - 1) as usize],
                        DeparseColumns::zeroed(mcx),
                    );
                    expand_colnames_array_to(mcx, &mut colinfo, j.usingClause.len() as i32)?;
                    dpns.rtable_columns[(rtindex - 1) as usize] = colinfo;
                }

                for i in 0..j.usingClause.len() {
                    let mut colname: PgString<'mcx> =
                        pstrdup(mcx, str_val(&j.usingClause[i])?)?;

                    // Assert it's a merged column.
                    debug_assert!({
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        colinfo.leftattnos[i] != 0 && colinfo.rightattnos[i] != 0
                    });

                    // Adopt passed-down name if any, else select a unique name.
                    let preassigned = {
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        colinfo.colnames.get(i).and_then(|c| c.as_ref()).map(|c| {
                            // can't pstrdup under the borrow; clone the str out
                            alloc::string::String::from(c.as_str())
                        })
                    };
                    if let Some(pre) = preassigned {
                        colname = pstrdup(mcx, &pre)?;
                    } else {
                        // Prefer user-written output alias if any.
                        if let Some(alias) = rte.alias.as_ref() {
                            if i < alias.colnames.len() {
                                colname = pstrdup(mcx, list_nth_str(&alias.colnames, i)?)?;
                            }
                        }
                        // Make it appropriately unique.
                        let unique = {
                            let colinfo = deparse_columns_fetch(rtindex, dpns);
                            make_colname_unique(mcx, colname.as_str(), dpns, colinfo)?
                        };
                        colname = unique;
                        if dpns.unique_using {
                            let c = pstrdup(mcx, colname.as_str())?;
                            lappend(mcx, &mut dpns.using_names, c)?;
                        }
                        // Save it as output column name, too.
                        let colinfo = &mut dpns.rtable_columns[(rtindex - 1) as usize];
                        colinfo.colnames[i] = Some(pstrdup(mcx, colname.as_str())?);
                    }

                    // Remember selected names for use later.
                    {
                        let c = pstrdup(mcx, colname.as_str())?;
                        let colinfo = &mut dpns.rtable_columns[(rtindex - 1) as usize];
                        lappend(mcx, &mut colinfo.usingNames, c)?;
                    }
                    {
                        let c = pstrdup(mcx, colname.as_str())?;
                        lappend(mcx, &mut child_parent_using, c)?;
                    }

                    // Push down to left column, unless it's a system column.
                    let (latt, ratt) = {
                        let colinfo = deparse_columns_fetch(rtindex, dpns);
                        (colinfo.leftattnos[i], colinfo.rightattnos[i])
                    };
                    if latt > 0 {
                        let leftcolinfo = &mut dpns.rtable_columns[(leftrti - 1) as usize];
                        expand_colnames_array_to(mcx, leftcolinfo, latt)?;
                        leftcolinfo.colnames[(latt - 1) as usize] =
                            Some(pstrdup(mcx, colname.as_str())?);
                    }
                    if ratt > 0 {
                        let rightcolinfo = &mut dpns.rtable_columns[(rightrti - 1) as usize];
                        expand_colnames_array_to(mcx, rightcolinfo, ratt)?;
                        rightcolinfo.colnames[(ratt - 1) as usize] =
                            Some(pstrdup(mcx, colname.as_str())?);
                    }
                }
            }

            // Mark child deparse_columns structs with correct parentUsing info.
            dpns.rtable_columns[(leftrti - 1) as usize].parentUsing =
                clone_str_vec(mcx, &child_parent_using)?;
            dpns.rtable_columns[(rightrti - 1) as usize].parentUsing =
                clone_str_vec(mcx, &child_parent_using)?;

            // Now recursively assign USING column names in children.
            let larg = j
                .larg
                .as_ref()
                .ok_or_else(|| elog_error("JoinExpr has no larg".into()))?;
            let rarg = j
                .rarg
                .as_ref()
                .ok_or_else(|| elog_error("JoinExpr has no rarg".into()))?;
            // Clone the children out so the recursive call can borrow dpns mutably.
            let larg = Node_clone(mcx, larg)?;
            let rarg = Node_clone(mcx, rarg)?;
            set_using_names(mcx, dpns, &larg, &child_parent_using)?;
            set_using_names(mcx, dpns, &rarg, &child_parent_using)?;
            Ok(())
        }
        _ => Err(elog_error(format!(
            "unrecognized node type: {}",
            jtnode.tag().0
        ))),
    }
}

/* -------------------------------------------------------------------------- *
 * set_relation_column_names / set_join_column_names (ruleutils.c 4374-4840).
 * -------------------------------------------------------------------------- */

/// `set_relation_column_names(dpns, rte, colinfo)` (`ruleutils.c` 4374-4566) —
/// select column aliases for a non-join RTE.
pub fn set_relation_column_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    colinfo: &mut DeparseColumns<'mcx>,
) -> PgResult<()> {
    // Construct an array of the current "real" column names of the RTE,
    // indexed by physical column number with None for dropped columns.
    let real_colnames: PgVec<'mcx, Option<PgString<'mcx>>> = if rte.rtekind == RTE_RELATION {
        // Relation --- look to the system catalogs for up-to-date info.
        // (relation_open + RelationGetDescr + per-attr attisdropped/attname; a
        // catalog-coupled read owned by the relcache/table-AM substrate.)
        backend_utils_adt_ruleutils_seams::ruleutils_relation_real_colnames::call(mcx, rte.relid)?
    } else if rte.rtekind == RTE_FUNCTION && !rte.functions.is_empty() {
        // Function returning composite: use expandRTE() (include dropped) so
        // dropped columns come back as empty strings -> None.
        backend_utils_adt_ruleutils_seams::ruleutils_expand_function_rte_colnames::call(mcx, rte)?
    } else {
        // Otherwise get the column names from eref. An empty string is a
        // dropped column, so change to None.
        let eref = rte
            .eref
            .as_ref()
            .ok_or_else(|| elog_error("RTE has no eref".into()))?;
        let mut out = PgVec::new_in(mcx);
        out.try_reserve(eref.colnames.len()).map_err(|_| mcx.oom(0))?;
        for cn in eref.colnames.iter() {
            let cname = str_val(cn)?;
            if cname.is_empty() {
                out.push(None);
            } else {
                out.push(Some(pstrdup(mcx, cname)?));
            }
        }
        out
    };
    let ncolumns = real_colnames.len() as i32;

    // Ensure colinfo->colnames has a slot for each column.
    expand_colnames_array_to(mcx, colinfo, ncolumns)?;
    debug_assert_eq!(colinfo.num_cols, ncolumns);

    // Make sufficiently large new_colnames and is_new_col arrays.
    // (num_new_cols stays 0 until after the loop so colname_is_unique won't
    // consult the not-yet-filled new_colnames.)
    colinfo.new_colnames = PgVec::new_in(mcx);
    colinfo.is_new_col = PgVec::new_in(mcx);
    for _ in 0..ncolumns {
        colinfo.new_colnames.push(None);
        colinfo.is_new_col.push(false);
    }

    // If the RTE is wide enough, use a hash table to avoid O(N^2) costs.
    build_colinfo_names_hash(colinfo);

    // Scan the columns, select a unique alias for each, store in colnames and
    // new_colnames. Mark new_colnames entries as new (beyond eref->colnames len).
    let noldcolumns = rte
        .eref
        .as_ref()
        .map(|e| e.colnames.len() as i32)
        .unwrap_or(0);
    let mut changed_any = false;
    let mut j: usize = 0;
    for i in 0..(ncolumns as usize) {
        let real_colname = real_colnames[i].as_ref();

        // Skip dropped columns.
        let real_colname = match real_colname {
            Some(rc) => rc,
            None => {
                debug_assert!(colinfo.colnames[i].is_none());
                continue;
            }
        };

        // If alias already assigned, that's what to use.
        let colname: PgString<'mcx> = if colinfo.colnames[i].is_none() {
            // If user wrote an alias, prefer that over real column name.
            let candidate: PgString<'mcx> = match rte.alias.as_ref() {
                Some(a) if i < a.colnames.len() => pstrdup(mcx, list_nth_str(&a.colnames, i)?)?,
                _ => pstrdup(mcx, real_colname.as_str())?,
            };
            // Unique-ify and insert into colinfo.
            let colname = make_colname_unique(mcx, candidate.as_str(), dpns, colinfo)?;
            colinfo.colnames[i] = Some(pstrdup(mcx, colname.as_str())?);
            add_to_names_hash(colinfo, colname.as_str());
            colname
        } else {
            pstrdup(mcx, colinfo.colnames[i].as_ref().unwrap().as_str())?
        };

        // Put names of non-dropped columns in new_colnames[] too.
        colinfo.new_colnames[j] = Some(pstrdup(mcx, colname.as_str())?);
        // And mark them as new or not.
        colinfo.is_new_col[j] = i as i32 >= noldcolumns;
        j += 1;

        // Remember if any assigned aliases differ from "real" name.
        if !changed_any && colname.as_str() != real_colname.as_str() {
            changed_any = true;
        }
    }

    // We're now done needing the colinfo's names_hash.
    destroy_colinfo_names_hash(colinfo);

    // Set correct length for new_colnames[] array.
    colinfo.num_new_cols = j as i32;

    // Decide whether to print the alias column list.
    colinfo.printaliases = if rte.rtekind == RTE_RELATION {
        changed_any
    } else if rte.rtekind == RTE_FUNCTION {
        true
    } else if rte.rtekind == RTE_TABLEFUNC {
        false
    } else if rte
        .alias
        .as_ref()
        .map(|a| !a.colnames.is_empty())
        .unwrap_or(false)
    {
        true
    } else {
        changed_any
    };

    Ok(())
}

/// `set_join_column_names(dpns, rte, colinfo)` (`ruleutils.c` 4577-4840) —
/// select column aliases for a join RTE. Both input RTEs must already be done.
pub fn set_join_column_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    colinfo: &mut DeparseColumns<'mcx>,
) -> PgResult<()> {
    let leftrti = colinfo.leftrti;
    let rightrti = colinfo.rightrti;

    // Ensure colinfo->colnames has a slot for each (old) column.
    let noldcolumns = rte
        .eref
        .as_ref()
        .map(|e| e.colnames.len() as i32)
        .unwrap_or(0);
    expand_colnames_array_to(mcx, colinfo, noldcolumns)?;
    debug_assert_eq!(colinfo.num_cols, noldcolumns);

    // If the RTE is wide enough, use a hash table to avoid O(N^2) costs.
    build_colinfo_names_hash(colinfo);

    // Scan the join output columns; set_using_names() already named the merged
    // (USING) columns, so start the loop after them.
    let mut changed_any = false;
    let using_count = colinfo.usingNames.len() as i32;
    for i in (using_count as usize)..(noldcolumns as usize) {
        let (latt, ratt) = (colinfo.leftattnos[i], colinfo.rightattnos[i]);
        // Join column must refer to at least one input column.
        debug_assert!(latt != 0 || ratt != 0);

        // Get the child column name.
        let real_colname: Option<PgString<'mcx>> = if latt > 0 {
            match dpns.rtable_columns[(leftrti - 1) as usize].colnames[(latt - 1) as usize].as_ref()
            {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            }
        } else if ratt > 0 {
            match dpns.rtable_columns[(rightrti - 1) as usize].colnames[(ratt - 1) as usize]
                .as_ref()
            {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            }
        } else {
            // We're joining system columns --- use eref name.
            let eref = rte
                .eref
                .as_ref()
                .ok_or_else(|| elog_error("join RTE has no eref".into()))?;
            Some(pstrdup(mcx, list_nth_str(&eref.colnames, i)?)?)
        };

        // If child col has been dropped, no need to assign a join colname.
        let real_colname = match real_colname {
            Some(rc) => rc,
            None => {
                colinfo.colnames[i] = None;
                continue;
            }
        };

        // In an unnamed join, just report child column names as-is.
        if rte.alias.is_none() {
            colinfo.colnames[i] = Some(pstrdup(mcx, real_colname.as_str())?);
            add_to_names_hash(colinfo, real_colname.as_str());
            continue;
        }

        // If alias already assigned, that's what to use.
        let colname: PgString<'mcx> = if colinfo.colnames[i].is_none() {
            let candidate: PgString<'mcx> = match rte.alias.as_ref() {
                Some(a) if i < a.colnames.len() => pstrdup(mcx, list_nth_str(&a.colnames, i)?)?,
                _ => pstrdup(mcx, real_colname.as_str())?,
            };
            let colname = make_colname_unique(mcx, candidate.as_str(), dpns, colinfo)?;
            colinfo.colnames[i] = Some(pstrdup(mcx, colname.as_str())?);
            add_to_names_hash(colinfo, colname.as_str());
            colname
        } else {
            pstrdup(mcx, colinfo.colnames[i].as_ref().unwrap().as_str())?
        };

        // Remember if any assigned aliases differ from "real" name.
        if !changed_any && colname.as_str() != real_colname.as_str() {
            changed_any = true;
        }
    }

    // Calculate number of columns the join would have if re-parsed now, and
    // create storage for the new_colnames and is_new_col arrays.
    let (left_num_new, right_num_new) = (
        dpns.rtable_columns[(leftrti - 1) as usize].num_new_cols,
        dpns.rtable_columns[(rightrti - 1) as usize].num_new_cols,
    );
    let nnewcolumns = left_num_new + right_num_new - colinfo.usingNames.len() as i32;
    colinfo.num_new_cols = nnewcolumns;
    colinfo.new_colnames = PgVec::new_in(mcx);
    colinfo.is_new_col = PgVec::new_in(mcx);
    for _ in 0..nnewcolumns {
        colinfo.new_colnames.push(None);
        colinfo.is_new_col.push(false);
    }

    // Generate the new_colnames array. Must match the parser column ordering:
    // merged columns first (USING order), then non-merged left (attnum order),
    // then non-merged right.
    let mut leftmerged: types_nodes::bitmapset::Bitmapset<'mcx> =
        types_nodes::bitmapset::Bitmapset { words: PgVec::new_in(mcx) };
    let mut rightmerged: types_nodes::bitmapset::Bitmapset<'mcx> =
        types_nodes::bitmapset::Bitmapset { words: PgVec::new_in(mcx) };

    // Handle merged columns; they are first and can't be new.
    let mut i: usize = 0;
    let mut j: usize = 0;
    while i < (noldcolumns as usize)
        && colinfo.leftattnos[i] != 0
        && colinfo.rightattnos[i] != 0
    {
        // column name is already determined and known unique
        colinfo.new_colnames[j] = match colinfo.colnames[i].as_ref() {
            Some(c) => Some(pstrdup(mcx, c.as_str())?),
            None => None,
        };
        colinfo.is_new_col[j] = false;

        // build bitmapsets of child attnums of merged columns
        if colinfo.leftattnos[i] > 0 {
            bms_add_member(mcx, &mut leftmerged, colinfo.leftattnos[i])?;
        }
        if colinfo.rightattnos[i] > 0 {
            bms_add_member(mcx, &mut rightmerged, colinfo.rightattnos[i])?;
        }
        i += 1;
        j += 1;
    }

    // Handle non-merged left-child columns.
    let mut ic: usize = 0;
    let left_num_new = dpns.rtable_columns[(leftrti - 1) as usize].num_new_cols;
    for jc in 0..(left_num_new as usize) {
        let is_new = dpns.rtable_columns[(leftrti - 1) as usize].is_new_col[jc];
        if !is_new {
            // Advance ic to next non-dropped old column of left child.
            while ic < dpns.rtable_columns[(leftrti - 1) as usize].num_cols as usize
                && dpns.rtable_columns[(leftrti - 1) as usize].colnames[ic].is_none()
            {
                ic += 1;
            }
            debug_assert!(ic < dpns.rtable_columns[(leftrti - 1) as usize].num_cols as usize);
            ic += 1;
            // If it is a merged column, we already processed it.
            if bms_is_member_local(&leftmerged, ic as i32) {
                continue;
            }
            // Else, advance i to the corresponding existing join column.
            while i < colinfo.num_cols as usize && colinfo.colnames[i].is_none() {
                i += 1;
            }
            debug_assert!(i < colinfo.num_cols as usize);
            debug_assert_eq!(ic as i32, colinfo.leftattnos[i]);
            // Use the already-assigned name of this column.
            colinfo.new_colnames[j] = match colinfo.colnames[i].as_ref() {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            };
            i += 1;
        } else {
            let child_colname = match dpns.rtable_columns[(leftrti - 1) as usize].new_colnames[jc]
                .as_ref()
            {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            };
            // Unique-ify the new child column name and assign, unless we're in
            // an unnamed join, in which case just copy.
            if rte.alias.is_some() {
                let base = child_colname
                    .as_ref()
                    .map(|c| alloc::string::String::from(c.as_str()))
                    .unwrap_or_default();
                let uniq = make_colname_unique(mcx, &base, dpns, colinfo)?;
                if !changed_any && uniq.as_str() != base.as_str() {
                    changed_any = true;
                }
                colinfo.new_colnames[j] = Some(pstrdup(mcx, uniq.as_str())?);
            } else {
                colinfo.new_colnames[j] = child_colname;
            }
            let added: Option<alloc::string::String> = colinfo.new_colnames[j]
                .as_ref()
                .map(|c| alloc::string::String::from(c.as_str()));
            if let Some(a) = added {
                add_to_names_hash(colinfo, &a);
            }
        }
        colinfo.is_new_col[j] = dpns.rtable_columns[(leftrti - 1) as usize].is_new_col[jc];
        j += 1;
    }

    // Handle non-merged right-child columns in exactly the same way.
    let mut ic: usize = 0;
    let right_num_new = dpns.rtable_columns[(rightrti - 1) as usize].num_new_cols;
    for jc in 0..(right_num_new as usize) {
        let is_new = dpns.rtable_columns[(rightrti - 1) as usize].is_new_col[jc];
        if !is_new {
            while ic < dpns.rtable_columns[(rightrti - 1) as usize].num_cols as usize
                && dpns.rtable_columns[(rightrti - 1) as usize].colnames[ic].is_none()
            {
                ic += 1;
            }
            debug_assert!(ic < dpns.rtable_columns[(rightrti - 1) as usize].num_cols as usize);
            ic += 1;
            if bms_is_member_local(&rightmerged, ic as i32) {
                continue;
            }
            while i < colinfo.num_cols as usize && colinfo.colnames[i].is_none() {
                i += 1;
            }
            debug_assert!(i < colinfo.num_cols as usize);
            debug_assert_eq!(ic as i32, colinfo.rightattnos[i]);
            colinfo.new_colnames[j] = match colinfo.colnames[i].as_ref() {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            };
            i += 1;
        } else {
            let child_colname = match dpns.rtable_columns[(rightrti - 1) as usize].new_colnames
                [jc]
                .as_ref()
            {
                Some(c) => Some(pstrdup(mcx, c.as_str())?),
                None => None,
            };
            if rte.alias.is_some() {
                let base = child_colname
                    .as_ref()
                    .map(|c| alloc::string::String::from(c.as_str()))
                    .unwrap_or_default();
                let uniq = make_colname_unique(mcx, &base, dpns, colinfo)?;
                if !changed_any && uniq.as_str() != base.as_str() {
                    changed_any = true;
                }
                colinfo.new_colnames[j] = Some(pstrdup(mcx, uniq.as_str())?);
            } else {
                colinfo.new_colnames[j] = child_colname;
            }
            let added: Option<alloc::string::String> = colinfo.new_colnames[j]
                .as_ref()
                .map(|c| alloc::string::String::from(c.as_str()));
            if let Some(a) = added {
                add_to_names_hash(colinfo, &a);
            }
        }
        colinfo.is_new_col[j] = dpns.rtable_columns[(rightrti - 1) as usize].is_new_col[jc];
        j += 1;
    }

    // Assert we processed the right number of columns (USE_ASSERT_CHECKING).
    #[cfg(debug_assertions)]
    {
        let mut i = i;
        while i < colinfo.num_cols as usize && colinfo.colnames[i].is_none() {
            i += 1;
        }
        debug_assert_eq!(i, colinfo.num_cols as usize);
        debug_assert_eq!(j, nnewcolumns as usize);
    }

    // We're now done needing the colinfo's names_hash.
    destroy_colinfo_names_hash(colinfo);

    // For a named join, print column aliases if we changed any from the child
    // names. Unnamed joins cannot print aliases.
    colinfo.printaliases = if rte.alias.is_some() { changed_any } else { false };

    Ok(())
}

/* -------------------------------------------------------------------------- *
 * colname_is_unique / make_colname_unique / expand_colnames_array_to
 * + the names_hash helpers (ruleutils.c 4847-5056).
 * -------------------------------------------------------------------------- */

/// `colname_is_unique(colname, dpns, colinfo)` (`ruleutils.c` 4847-4915).
fn colname_is_unique<'mcx>(
    colname: &str,
    dpns: &DeparseNamespace<'mcx>,
    colinfo: &DeparseColumns<'mcx>,
) -> bool {
    // If we have a hash table, consult that instead of linearly scanning.
    if let Some(h) = colinfo.names_hash.as_ref() {
        if h.contains_key(colname) {
            return false;
        }
    } else {
        // Check against already-assigned column aliases within RTE.
        for oldname in colinfo.colnames.iter() {
            if let Some(o) = oldname {
                if o.as_str() == colname {
                    return false;
                }
            }
        }
        // If we're building a new_colnames array, check that too.
        for oldname in colinfo.new_colnames.iter() {
            if let Some(o) = oldname {
                if o.as_str() == colname {
                    return false;
                }
            }
        }
        // Also check against names already assigned for parent-join USING cols.
        for oldname in colinfo.parentUsing.iter() {
            if oldname.as_str() == colname {
                return false;
            }
        }
    }

    // Also check against USING-column names that must be globally unique.
    for oldname in dpns.using_names.iter() {
        if oldname.as_str() == colname {
            return false;
        }
    }

    true
}

/// `make_colname_unique(colname, dpns, colinfo)` (`ruleutils.c` 4922-4954).
fn make_colname_unique<'mcx>(
    mcx: Mcx<'mcx>,
    colname: &str,
    dpns: &DeparseNamespace<'mcx>,
    colinfo: &DeparseColumns<'mcx>,
) -> PgResult<PgString<'mcx>> {
    if !colname_is_unique(colname, dpns, colinfo) {
        let mut colnamelen = colname.len();
        let mut i = 0i32;
        let modname: alloc::string::String;
        loop {
            i += 1;
            let m = loop {
                let candidate = format!("{}_{}", &colname[..colnamelen], i);
                if (candidate.len() as i32) < NAMEDATALEN {
                    break candidate;
                }
                // drop chars from colname to keep all the digits
                colnamelen = backend_utils_mb_mbutils_seams::pg_mbcliplen::call(
                    colname.as_bytes(),
                    colnamelen as i32,
                    colnamelen as i32 - 1,
                ) as usize;
            };
            if colname_is_unique(&m, dpns, colinfo) {
                modname = m;
                break;
            }
        }
        pstrdup(mcx, &modname)
    } else {
        pstrdup(mcx, colname)
    }
}

/// `expand_colnames_array_to(colinfo, n)` (`ruleutils.c` 4961-4972) — make
/// `colinfo.colnames` at least `n` items long, zero-filling the new entries.
fn expand_colnames_array_to<'mcx>(
    mcx: Mcx<'mcx>,
    colinfo: &mut DeparseColumns<'mcx>,
    n: i32,
) -> PgResult<()> {
    if n > colinfo.num_cols {
        colinfo
            .colnames
            .try_reserve((n - colinfo.num_cols) as usize)
            .map_err(|_| mcx.oom(0))?;
        while (colinfo.colnames.len() as i32) < n {
            colinfo.colnames.push(None);
        }
        colinfo.num_cols = n;
    }
    Ok(())
}

/// `build_colinfo_names_hash(colinfo)` (`ruleutils.c` 4977-5030) — build the
/// names_hash for RTEs with >= 32 columns, preloaded with any names already
/// present in colnames/new_colnames/parentUsing.
fn build_colinfo_names_hash(colinfo: &mut DeparseColumns<'_>) {
    // Use a hash table only for RTEs with at least 32 columns.
    if colinfo.num_cols < 32 {
        return;
    }
    let mut h: BTreeMap<alloc::string::String, ()> = BTreeMap::new();
    for oldname in colinfo.colnames.iter() {
        if let Some(o) = oldname {
            h.insert(o.as_str().into(), ());
        }
    }
    for oldname in colinfo.new_colnames.iter() {
        if let Some(o) = oldname {
            h.insert(o.as_str().into(), ());
        }
    }
    for oldname in colinfo.parentUsing.iter() {
        h.insert(oldname.as_str().into(), ());
    }
    colinfo.names_hash = Some(h);
}

/// `add_to_names_hash(colinfo, name)` (`ruleutils.c` 5035-5043) — add a string
/// to the names_hash, if one is in use.
fn add_to_names_hash(colinfo: &mut DeparseColumns<'_>, name: &str) {
    if let Some(h) = colinfo.names_hash.as_mut() {
        h.insert(name.into(), ());
    }
}

/// `destroy_colinfo_names_hash(colinfo)` (`ruleutils.c` 5048-5056).
fn destroy_colinfo_names_hash(colinfo: &mut DeparseColumns<'_>) {
    colinfo.names_hash = None;
}

/* -------------------------------------------------------------------------- *
 * identify_join_columns / get_rtable_name (ruleutils.c 5064-5139).
 * -------------------------------------------------------------------------- */

/// `identify_join_columns(j, jrte, colinfo)` (`ruleutils.c` 5064-5125) — figure
/// out where the columns of a join come from. Fills leftrti/rightrti and the
/// leftattnos/rightattnos arrays (usingNames is filled later).
fn identify_join_columns<'mcx>(
    mcx: Mcx<'mcx>,
    j: &JoinExpr<'mcx>,
    jrte: &RangeTblEntry<'mcx>,
    colinfo: &mut DeparseColumns<'mcx>,
) -> PgResult<()> {
    // Extract left/right child RT indexes.
    colinfo.leftrti = match j.larg.as_ref().map(|n| &**n) {
        Some(n) => match n.node_tag() {
            types_nodes::nodes::ntag::T_RangeTblRef => n.expect_rangetblref().rtindex,
            types_nodes::nodes::ntag::T_JoinExpr => n.expect_joinexpr().rtindex,
            _ => {
                return Err(elog_error(format!(
                    "unrecognized node type in jointree: {}",
                    n.tag().0
                )))
            }
        },
        None => return Err(elog_error("JoinExpr larg is NULL".into())),
    };
    colinfo.rightrti = match j.rarg.as_ref().map(|n| &**n) {
        Some(n) => match n.node_tag() {
            types_nodes::nodes::ntag::T_RangeTblRef => n.expect_rangetblref().rtindex,
            types_nodes::nodes::ntag::T_JoinExpr => n.expect_joinexpr().rtindex,
            _ => {
                return Err(elog_error(format!(
                    "unrecognized node type in jointree: {}",
                    n.tag().0
                )))
            }
        },
        None => return Err(elog_error("JoinExpr rarg is NULL".into())),
    };

    // Children are processed earlier than the join in the second pass.
    debug_assert!(colinfo.leftrti < j.rtindex);
    debug_assert!(colinfo.rightrti < j.rtindex);

    // Initialize result arrays with zeroes.
    let numjoincols = jrte.joinaliasvars.len();
    debug_assert_eq!(
        numjoincols,
        jrte.eref.as_ref().map(|e| e.colnames.len()).unwrap_or(0)
    );
    colinfo.leftattnos = PgVec::new_in(mcx);
    colinfo.rightattnos = PgVec::new_in(mcx);
    for _ in 0..numjoincols {
        colinfo.leftattnos.push(0);
        colinfo.rightattnos.push(0);
    }

    // Deconstruct joinleftcols/joinrightcols into the desired format. Merged
    // (USING) columns are the first columns of the join output.
    let mut jcolno: usize = 0;
    for &leftattno in jrte.joinleftcols.iter() {
        colinfo.leftattnos[jcolno] = leftattno;
        jcolno += 1;
    }
    let mut rcolno: i32 = 0;
    for &rightattno in jrte.joinrightcols.iter() {
        if rcolno < jrte.joinmergedcols {
            // merged column?
            colinfo.rightattnos[rcolno as usize] = rightattno;
        } else {
            colinfo.rightattnos[jcolno] = rightattno;
            jcolno += 1;
        }
        rcolno += 1;
    }
    debug_assert_eq!(jcolno, numjoincols);

    Ok(())
}

/// `get_rtable_name(rtindex, context)` (`ruleutils.c` 5132-5139) — the
/// previously-assigned alias for a 1-based RTE index in the topmost namespace.
pub fn get_rtable_name<'a, 'mcx>(
    rtindex: i32,
    context: &'a DeparseContext<'mcx>,
) -> PgResult<Option<&'a str>> {
    let dpns = context
        .namespaces
        .first()
        .ok_or_else(|| elog_error("deparse context has no namespace".into()))?;
    debug_assert!(rtindex > 0 && rtindex <= dpns.rtable_names.len() as i32);
    match dpns.rtable_names.get((rtindex - 1) as usize) {
        Some(opt) => Ok(opt.as_ref().map(|s| s.as_str())),
        None => Err(elog_error(format!(
            "get_rtable_name: index {rtindex} out of range"
        ))),
    }
}

/* -------------------------------------------------------------------------- *
 * The plan-navigation half (ruleutils.c 5151-5337) — F0b (plan-tree deparse).
 *
 * These read `Plan` fields (outerPlan/innerPlan/targetlist/Append/MergeAppend/
 * SubqueryScan/CteScan/WorkTableScan/ModifyTable/IndexOnlyScan/ForeignScan/
 * CustomScan/RecursiveUnion) of the now-owned `Plan` tree carried by the running
 * `PlannedStmt`. The deparse-namespace plan-only fields hold `'mcx` clones of
 * the source plan nodes / targetlists (the owned model has no shared pointers,
 * so the C `dpns->outer_plan = outerPlan(plan)` pointer-aliasing becomes a deep
 * clone into the deparse context's arena).
 * -------------------------------------------------------------------------- */

/// `outerPlan(node)` / `innerPlan(node)` (`plannodes.h`) — the left/right
/// subplan of a `Plan` header, cloned into `mcx` (`None` for `NULL`).
fn clone_subplan<'mcx>(
    mcx: Mcx<'mcx>,
    sub: &Option<PgBox<'_, Node<'_>>>,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    match sub {
        Some(b) => Ok(Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

/// Wrap a `List *targetlist` (`PgVec<TargetEntry>`) as the namespace tlist
/// representation `PgVec<PgBox<Node>>` (each TargetEntry boxed as a `Node`),
/// cloning into `mcx`. C aliases the plan's targetlist pointer; the owned model
/// clones each TargetEntry into the deparse arena.
fn tlist_as_node_vec<'mcx>(
    mcx: Mcx<'mcx>,
    tlist: &Option<PgVec<'_, types_nodes::primnodes::TargetEntry<'_>>>,
) -> PgResult<PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>> {
    let mut out = PgVec::new_in(mcx);
    if let Some(tl) = tlist {
        out.try_reserve(tl.len()).map_err(|_| mcx.oom(0))?;
        for tle in tl.iter() {
            out.push(mcx::alloc_in(mcx, Node::mk_target_entry(mcx, tle.clone_in(mcx)?)?)?);
        }
    }
    Ok(out)
}

/// `set_deparse_plan(dpns, plan)` (`ruleutils.c` 5151-5225) — set up the
/// namespace to deparse subexpressions of a given `Plan` node: point `dpns->plan`
/// at it, choose the OUTER/INNER referent subplans (with the Append / MergeAppend
/// / SubqueryScan / CteScan / WorkTableScan / ModifyTable special cases), and set
/// the OUTER/INNER/INDEX referent targetlists.
pub fn set_deparse_plan<'mcx, 'p>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    plan: &Node<'p>,
) -> PgResult<()> {
    use types_nodes::nodes::ntag;
    let tag = plan.node_tag();
    let header = plan.plan_head();

    // dpns->plan = plan;
    dpns.plan = Some(mcx::alloc_in(mcx, plan.clone_in(mcx)?)?);

    // OUTER referent: Append/MergeAppend special-case the first child.
    dpns.outer_plan = match tag {
        ntag::T_Append => match plan.as_append() {
            Some(a) => match a.appendplans.first() {
                Some(c) => Some(mcx::alloc_in(mcx, c.clone_in(mcx)?)?),
                None => None,
            },
            None => None,
        },
        ntag::T_MergeAppend => match plan.as_mergeappend() {
            Some(m) => match m.mergeplans.first() {
                Some(c) => Some(mcx::alloc_in(mcx, c.clone_in(mcx)?)?),
                None => None,
            },
            None => None,
        },
        // dpns->outer_plan = outerPlan(plan)
        _ => clone_subplan(mcx, &header.lefttree)?,
    };

    dpns.outer_tlist = match dpns.outer_plan.as_ref() {
        Some(op) => tlist_as_node_vec(mcx, &op.plan_head().targetlist)?,
        None => PgVec::new_in(mcx),
    };

    // INNER referent: SubqueryScan / CteScan / WorkTableScan / ModifyTable
    // special cases, else innerPlan(plan).
    dpns.inner_plan = match tag {
        ntag::T_SubqueryScan => match plan.as_subqueryscan() {
            Some(s) => clone_subplan(mcx, &s.subplan)?,
            None => None,
        },
        ntag::T_CteScan => match plan.as_ctescan() {
            Some(c) => {
                // list_nth(dpns->subplans, ctePlanId - 1)
                let idx = (c.ctePlanId - 1) as usize;
                match dpns.subplans.get(idx) {
                    Some(p) => Some(mcx::alloc_in(mcx, p.clone_in(mcx)?)?),
                    None => None,
                }
            }
            None => None,
        },
        ntag::T_WorkTableScan => match plan.as_worktablescan() {
            Some(w) => Some(find_recursive_union(mcx, dpns, w.wtParam)?),
            None => None,
        },
        ntag::T_ModifyTable => match plan.as_modifytable() {
            Some(m) => {
                if m.operation == types_nodes::nodes::CmdType::CMD_MERGE {
                    clone_subplan(mcx, &header.lefttree)?
                } else {
                    // dpns->inner_plan = plan
                    Some(mcx::alloc_in(mcx, plan.clone_in(mcx)?)?)
                }
            }
            None => None,
        },
        _ => clone_subplan(mcx, &header.righttree)?,
    };

    // INNER tlist: INSERT ON CONFLICT uses exclRelTlist; else inner_plan's tlist.
    dpns.inner_tlist = match tag {
        ntag::T_ModifyTable
            if plan
                .as_modifytable()
                .is_some_and(|m| m.operation == types_nodes::nodes::CmdType::CMD_INSERT) =>
        {
            match plan.as_modifytable() {
                Some(m) => tlist_as_node_vec(mcx, &m.exclRelTlist)?,
                None => PgVec::new_in(mcx),
            }
        }
        _ => match dpns.inner_plan.as_ref() {
            Some(ip) => tlist_as_node_vec(mcx, &ip.plan_head().targetlist)?,
            None => PgVec::new_in(mcx),
        },
    };

    // INDEX referent tlist: IndexOnlyScan / ForeignScan / CustomScan.
    dpns.index_tlist = if let Some(s) = plan.as_indexonlyscan() {
        tlist_as_node_vec(mcx, &s.indextlist)?
    } else if let Some(f) = plan.as_foreignscan() {
        tlist_as_node_vec(mcx, &f.fdw_scan_tlist)?
    } else if let Some(c) = plan.as_customscan() {
        tlist_as_node_vec(mcx, &c.custom_scan_tlist)?
    } else {
        PgVec::new_in(mcx)
    };

    Ok(())
}

/// `find_recursive_union(dpns, wtscan)` (`ruleutils.c` 5232-5248) — locate the
/// RecursiveUnion ancestor whose `wtParam` matches the WorkTableScan's.
pub fn find_recursive_union<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &DeparseNamespace<'mcx>,
    wt_param: i32,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    for ancestor in dpns.ancestors.iter() {
        if ancestor.node_tag() == types_nodes::nodes::ntag::T_RecursiveUnion {
            if let Some(ru) = ancestor.as_recursiveunion() {
                if ru.wtParam == wt_param {
                    return mcx::alloc_in(mcx, ancestor.clone_in(mcx)?);
                }
            }
        }
    }
    Err(elog_error(format!(
        "could not find RecursiveUnion for WorkTableScan with wtParam {wt_param}"
    )))
}

/// `push_child_plan(dpns, plan, save_dpns)` (`ruleutils.c` 5262-5274) — descend
/// into a child plan for special-Var resolution, saving the prior namespace.
pub fn push_child_plan<'mcx, 'p>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    plan: &Node<'p>,
) -> PgResult<DeparseNamespace<'mcx>> {
    // *save_dpns = *dpns;  (owned model: deep-clone the namespace to restore.)
    let save = clone_namespace(mcx, dpns)?;

    // dpns->ancestors = lcons(dpns->plan, dpns->ancestors);
    let mut new_ancestors = PgVec::new_in(mcx);
    if let Some(p) = dpns.plan.as_ref() {
        new_ancestors
            .try_reserve(dpns.ancestors.len() + 1)
            .map_err(|_| mcx.oom(0))?;
        new_ancestors.push(mcx::alloc_in(mcx, p.clone_in(mcx)?)?);
        for a in dpns.ancestors.iter() {
            new_ancestors.push(mcx::alloc_in(mcx, a.clone_in(mcx)?)?);
        }
    }
    dpns.ancestors = new_ancestors;

    // set_deparse_plan(dpns, plan);
    set_deparse_plan(mcx, dpns, plan)?;
    Ok(save)
}

/// `pop_child_plan(dpns, save_dpns)` (`ruleutils.c` 5279-5292) — restore the
/// namespace saved by `push_child_plan`.
pub fn pop_child_plan<'mcx>(
    dpns: &mut DeparseNamespace<'mcx>,
    save_dpns: DeparseNamespace<'mcx>,
) {
    // C: ancestors = list_delete_first(dpns->ancestors); *dpns = *save_dpns;
    //    dpns->ancestors = ancestors;
    // The saved namespace already carries the original ancestors list (the cell
    // push_child_plan prepended is dropped by simply restoring the saved value).
    *dpns = save_dpns;
}

/// `push_ancestor_plan(dpns, ancestor_cell, save_dpns)` (`ruleutils.c`
/// 5310-5326) — transfer deparsing attention to an ancestor plan node when
/// expanding a `Param` reference, so the ancestor's own `Param`/special-Var
/// references resolve. `ancestor_index` is the position of the target ancestor
/// in `dpns.ancestors`; the new ancestor list is the tail *after* that cell
/// (C: `list_copy_tail(dpns->ancestors, list_cell_number(...) + 1)`). The target
/// ancestor node is supplied separately (C reads it via `lfirst(ancestor_cell)`)
/// so the caller need not re-borrow `dpns.ancestors` while it is being rebuilt.
pub fn push_ancestor_plan<'mcx, 'p>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    ancestor_index: usize,
    plan: &Node<'p>,
) -> PgResult<DeparseNamespace<'mcx>> {
    // *save_dpns = *dpns;  (owned model: deep-clone the namespace to restore.)
    let save = clone_namespace(mcx, dpns)?;

    // dpns->ancestors = list_copy_tail(dpns->ancestors, cell_number + 1);
    let mut new_ancestors = PgVec::new_in(mcx);
    let tail_start = ancestor_index + 1;
    if tail_start < dpns.ancestors.len() {
        new_ancestors
            .try_reserve(dpns.ancestors.len() - tail_start)
            .map_err(|_| mcx.oom(0))?;
        for a in dpns.ancestors.iter().skip(tail_start) {
            new_ancestors.push(mcx::alloc_in(mcx, a.clone_in(mcx)?)?);
        }
    }
    dpns.ancestors = new_ancestors;

    // set_deparse_plan(dpns, plan);
    set_deparse_plan(mcx, dpns, plan)?;
    Ok(save)
}

/// `pop_ancestor_plan(dpns, save_dpns)` (`ruleutils.c` 5331-5338) — restore the
/// namespace saved by `push_ancestor_plan`.
pub fn pop_ancestor_plan<'mcx>(
    dpns: &mut DeparseNamespace<'mcx>,
    save_dpns: DeparseNamespace<'mcx>,
) {
    *dpns = save_dpns;
}

/* -------------------------------------------------------------------------- *
 * The plan-tree deparse-context entry points (ruleutils.c 3777-3868).
 * -------------------------------------------------------------------------- */

/// `deparse_context_for_plan_tree(pstmt, rtable_names)` (`ruleutils.c`
/// 3826-3868) — build the one-deep deparse-namespace stack for an entire plan
/// tree, so plan-node expressions can be deparsed. Fields that stay the same
/// across the whole plan tree (rtable / rtable_names / subplans / appendrels) are
/// set here; per-node attention is set later by [`set_deparse_context_plan`].
pub fn deparse_context_for_plan_tree<'mcx, 'p>(
    mcx: Mcx<'mcx>,
    pstmt: &types_nodes::nodeindexscan::PlannedStmt<'p>,
    rtable_names: &PgVec<'mcx, Option<PgString<'mcx>>>,
) -> PgResult<PgVec<'mcx, DeparseNamespace<'mcx>>> {
    let mut dpns = DeparseNamespace::zeroed(mcx);

    // dpns->rtable = pstmt->rtable (cloned into the deparse arena).
    dpns.rtable = match &pstmt.rtable {
        Some(rt) => {
            let mut out = PgVec::new_in(mcx);
            out.try_reserve(rt.len()).map_err(|_| mcx.oom(0))?;
            for rte in rt.iter() {
                out.push(rte.clone_in(mcx)?);
            }
            out
        }
        None => PgVec::new_in(mcx),
    };

    // dpns->rtable_names = rtable_names (cloned).
    dpns.rtable_names = {
        let mut out = PgVec::new_in(mcx);
        out.try_reserve(rtable_names.len()).map_err(|_| mcx.oom(0))?;
        for s in rtable_names.iter() {
            out.push(match s {
                Some(p) => Some(pstrdup(mcx, p.as_str())?),
                None => None,
            });
        }
        out
    };

    // dpns->subplans = pstmt->subplans (cloned).
    dpns.subplans = match &pstmt.subplans {
        Some(sps) => {
            let mut out = PgVec::new_in(mcx);
            out.try_reserve(sps.len()).map_err(|_| mcx.oom(0))?;
            for sp in sps.iter() {
                // A NULL subplan slot maps to a zeroed/empty Result-less node; the
                // owned model stores each present subplan as a cloned Node. Only
                // CteScan reaches dpns->subplans (list_nth), which is gated below.
                match sp {
                    Some(b) => out.push(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                    None => {
                        // list_nth would return NULL; keep alignment with a
                        // placeholder the CteScan path checks for None.
                        // We cannot store None in a Vec<PgBox<Node>>, so we skip
                        // the index alignment: CteScan with a NULL subplan slot is
                        // not produced by the planner for plain queries.
                    }
                }
            }
            out
        }
        None => PgVec::new_in(mcx),
    };

    // dpns->ctes = NIL; (plan trees carry no CTE list at this level — zeroed.)

    // pstmt->appendRelations: build the appendrels array indexed by child relid
    // (ruleutils.c:3700-3715). For each AppendRelInfo carrier, slot it at
    // [child_relid]; later get_variable uses it to map an Append child Var up to
    // its inheritance parent for EXPLAIN display.
    if !pstmt.appendRelations.is_empty() {
        let nrels = dpns.rtable.len();
        let mut appendrels: PgVec<Option<types_nodes::appendrel_carrier::AppendRelInfoCarrier>> =
            PgVec::new_in(mcx);
        appendrels.try_reserve(nrels + 1).map_err(|_| mcx.oom(0))?;
        for _ in 0..(nrels + 1) {
            appendrels.push(None);
        }
        for appinfo in pstmt.appendRelations.iter() {
            let cr = appinfo.child_relid as usize;
            if cr < appendrels.len() {
                appendrels[cr] = Some(appinfo.clone());
            }
        }
        dpns.appendrels = appendrels;
    }

    // set_simple_column_names(dpns): assign per-RTE column aliases (ignoring JOIN
    // RTEs — plan trees contain no join alias Vars).
    set_simple_column_names(mcx, &mut dpns)?;

    // Return a one-deep namespace stack.
    let mut stack = PgVec::new_in(mcx);
    lappend(mcx, &mut stack, dpns)?;
    Ok(stack)
}

/// `set_deparse_context_plan(dpcontext, plan, ancestors)` (`ruleutils.c`
/// 3777-3804) — point the head deparse namespace at a specific plan node and its
/// ancestor list so `Var` / `PARAM_EXEC` references resolve.
pub fn set_deparse_context_plan<'mcx, 'p>(
    mcx: Mcx<'mcx>,
    mut dpcontext: PgVec<'mcx, DeparseNamespace<'mcx>>,
    plan: &Node<'p>,
    ancestors: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
) -> PgResult<PgVec<'mcx, DeparseNamespace<'mcx>>> {
    // Assert(list_length(dpcontext) == 1); dpns = linitial(dpcontext);
    if dpcontext.is_empty() {
        return Err(elog_error(
            "set_deparse_context_plan: empty deparse context".into(),
        ));
    }
    // dpns->ancestors = ancestors;
    let mut new_ancestors = PgVec::new_in(mcx);
    new_ancestors
        .try_reserve(ancestors.len())
        .map_err(|_| mcx.oom(0))?;
    for a in ancestors.iter() {
        new_ancestors.push(mcx::alloc_in(mcx, a.clone_in(mcx)?)?);
    }
    dpcontext[0].ancestors = new_ancestors;

    // set_deparse_plan(dpns, plan);
    set_deparse_plan(mcx, &mut dpcontext[0], plan)?;

    // For ModifyTable, set aliases for OLD and NEW in RETURNING.
    if let Some(m) = plan.as_modifytable() {
        dpcontext[0].ret_old_alias = match m.returningOldAlias.as_ref() {
            Some(b) => Some(pstrdup(
                mcx,
                core::str::from_utf8(b).unwrap_or(""),
            )?),
            None => None,
        };
        dpcontext[0].ret_new_alias = match m.returningNewAlias.as_ref() {
            Some(b) => Some(pstrdup(
                mcx,
                core::str::from_utf8(b).unwrap_or(""),
            )?),
            None => None,
        };
    }

    Ok(dpcontext)
}

/// EXPLAIN's `show_expression` deparse step folded into one call: build the
/// plan-tree deparse context, point it at `plan`, and render `expr` to SQL text.
/// Installs the `deparse_expr_for_plan` seam.
pub fn deparse_expr_for_plan<'mcx, 'p>(
    mcx: Mcx<'mcx>,
    pstmt: &types_nodes::nodeindexscan::PlannedStmt<'p>,
    rtable_names: &PgVec<'mcx, Option<PgString<'mcx>>>,
    plan: &Node<'p>,
    ancestors: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    expr: &Node<'p>,
    forceprefix: bool,
    showimplicit: bool,
) -> PgResult<PgString<'mcx>> {
    // context = set_deparse_context_plan(deparse_context_for_plan_tree(pstmt,
    //                                    rtable_names), plan, ancestors);
    let base = deparse_context_for_plan_tree(mcx, pstmt, rtable_names)?;
    let context = set_deparse_context_plan(mcx, base, plan, ancestors)?;
    // exprstr = deparse_expression(node, context, useprefix, false);
    let expr_owned = expr.clone_in(mcx)?;
    deparse_expression(mcx, &expr_owned, context, forceprefix, showimplicit)
}

/// EXPLAIN's `show_window_def` frame-options step folded into one call: build
/// the plan-tree deparse context, point it at the WindowAgg `plan` node (so any
/// frame-offset expressions resolve against it), and render the frame-clause
/// text (e.g. `ROWS UNBOUNDED PRECEDING`). Mirrors C's
/// `get_window_frame_options_for_explain(frameOptions, startOffset, endOffset,
/// set_deparse_context_plan(es->deparse_cxt, (Plan *) wagg, ancestors),
/// useprefix)`. Installs the `deparse_window_frame_for_plan` seam.
pub fn deparse_window_frame_for_plan<'mcx, 'p>(
    mcx: Mcx<'mcx>,
    pstmt: &types_nodes::nodeindexscan::PlannedStmt<'p>,
    rtable_names: &PgVec<'mcx, Option<PgString<'mcx>>>,
    plan: &Node<'p>,
    ancestors: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    frame_options: i32,
    start_offset: Option<&Node<'p>>,
    end_offset: Option<&Node<'p>>,
    forceprefix: bool,
) -> PgResult<PgString<'mcx>> {
    // context = set_deparse_context_plan(deparse_context_for_plan_tree(pstmt,
    //                                    rtable_names), (Plan *) wagg, ancestors);
    let base = deparse_context_for_plan_tree(mcx, pstmt, rtable_names)?;
    let context = set_deparse_context_plan(mcx, base, plan, ancestors)?;
    let start_owned = match start_offset {
        Some(n) => Some(n.clone_in(mcx)?),
        None => None,
    };
    let end_owned = match end_offset {
        Some(n) => Some(n.clone_in(mcx)?),
        None => None,
    };
    query_deparse::get_window_frame_options_for_explain(
        mcx,
        frame_options,
        start_owned.as_ref(),
        end_owned.as_ref(),
        context,
        forceprefix,
    )
}

/* -------------------------------------------------------------------------- *
 * Local list/node-clone + bitmapset helpers.
 * -------------------------------------------------------------------------- */

/// Deep-clone a whole `DeparseNamespace` into `mcx` (F2 threads the parent
/// namespace stack by value through `get_query_def` recursion, as C does with
/// `list_copy` / shared pointers).
fn clone_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &DeparseNamespace<'mcx>,
) -> PgResult<DeparseNamespace<'mcx>> {
    let opt_str_vec = |v: &PgVec<'mcx, Option<PgString<'mcx>>>| -> PgResult<PgVec<'mcx, Option<PgString<'mcx>>>> {
        let mut out = PgVec::new_in(mcx);
        out.try_reserve(v.len()).map_err(|_| mcx.oom(0))?;
        for s in v.iter() {
            out.push(match s {
                Some(p) => Some(pstrdup(mcx, p.as_str())?),
                None => None,
            });
        }
        Ok(out)
    };
    let opt_box_node_vec = |v: &PgVec<'mcx, Option<PgBox<'mcx, Node<'mcx>>>>| -> PgResult<PgVec<'mcx, Option<PgBox<'mcx, Node<'mcx>>>>> {
        let mut out = PgVec::new_in(mcx);
        out.try_reserve(v.len()).map_err(|_| mcx.oom(0))?;
        for n in v.iter() {
            out.push(match n {
                Some(b) => Some(Node_clone(mcx, b)?),
                None => None,
            });
        }
        Ok(out)
    };
    let mut rtable = PgVec::new_in(mcx);
    rtable.try_reserve(dpns.rtable.len()).map_err(|_| mcx.oom(0))?;
    for rte in dpns.rtable.iter() {
        rtable.push(rte.clone_in(mcx)?);
    }
    let mut rtable_columns = PgVec::new_in(mcx);
    rtable_columns.try_reserve(dpns.rtable_columns.len()).map_err(|_| mcx.oom(0))?;
    for c in dpns.rtable_columns.iter() {
        rtable_columns.push(c.clone_columns(mcx)?);
    }
    Ok(DeparseNamespace {
        rtable,
        rtable_names: opt_str_vec(&dpns.rtable_names)?,
        rtable_columns,
        subplans: clone_node_vec(mcx, &dpns.subplans)?,
        ctes: clone_node_vec(mcx, &dpns.ctes)?,
        appendrels: {
            let mut v = PgVec::new_in(mcx);
            v.try_reserve(dpns.appendrels.len()).map_err(|_| mcx.oom(0))?;
            for a in dpns.appendrels.iter() {
                v.push(a.clone());
            }
            v
        },
        ret_old_alias: match dpns.ret_old_alias.as_ref() {
            Some(s) => Some(pstrdup(mcx, s.as_str())?),
            None => None,
        },
        ret_new_alias: match dpns.ret_new_alias.as_ref() {
            Some(s) => Some(pstrdup(mcx, s.as_str())?),
            None => None,
        },
        unique_using: dpns.unique_using,
        using_names: clone_str_vec(mcx, &dpns.using_names)?,
        plan: match dpns.plan.as_ref() {
            Some(b) => Some(Node_clone(mcx, b)?),
            None => None,
        },
        ancestors: clone_node_vec(mcx, &dpns.ancestors)?,
        outer_plan: match dpns.outer_plan.as_ref() {
            Some(b) => Some(Node_clone(mcx, b)?),
            None => None,
        },
        inner_plan: match dpns.inner_plan.as_ref() {
            Some(b) => Some(Node_clone(mcx, b)?),
            None => None,
        },
        outer_tlist: clone_node_vec(mcx, &dpns.outer_tlist)?,
        inner_tlist: clone_node_vec(mcx, &dpns.inner_tlist)?,
        index_tlist: clone_node_vec(mcx, &dpns.index_tlist)?,
        funcname: match dpns.funcname.as_ref() {
            Some(s) => Some(pstrdup(mcx, s.as_str())?),
            None => None,
        },
        numargs: dpns.numargs,
        argnames: opt_str_vec(&dpns.argnames)?,
    })
}

/// Clone a `PgVec<PgString>` into `mcx`.
fn clone_str_vec<'mcx>(
    mcx: Mcx<'mcx>,
    src: &PgVec<'mcx, PgString<'mcx>>,
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(src.len()).map_err(|_| mcx.oom(0))?;
    for s in src.iter() {
        out.push(pstrdup(mcx, s.as_str())?);
    }
    Ok(out)
}

/// Clone a `PgVec<NodePtr>` into `mcx` (C `list_copy` deep-ish; we deep-copy
/// because the owned tree has no shared pointers).
fn clone_node_vec<'mcx>(
    mcx: Mcx<'mcx>,
    src: &PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
) -> PgResult<PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>> {
    let mut out = PgVec::new_in(mcx);
    out.try_reserve(src.len()).map_err(|_| mcx.oom(0))?;
    for n in src.iter() {
        out.push(Node_clone(mcx, n)?);
    }
    Ok(out)
}

/// `copyObject(node)` for a boxed Node.
#[allow(non_snake_case)]
fn Node_clone<'mcx>(
    mcx: Mcx<'mcx>,
    n: &PgBox<'mcx, Node<'mcx>>,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    mcx::alloc_in(mcx, n.clone_in(mcx)?)
}

/// Clone a `FromExpr` into `mcx`.
fn clone_fromexpr<'mcx>(
    mcx: Mcx<'mcx>,
    f: &FromExpr<'mcx>,
) -> PgResult<FromExpr<'mcx>> {
    f.clone_in(mcx)
}

/// `bms_add_member(a, x)` — set membership add over our local Bitmapset image
/// (set_join_column_names' leftmerged/rightmerged are transient workspace).
fn bms_add_member<'mcx>(
    mcx: Mcx<'mcx>,
    a: &mut types_nodes::bitmapset::Bitmapset<'mcx>,
    x: i32,
) -> PgResult<()> {
    debug_assert!(x > 0);
    const BITS_PER_WORD: i32 = 64;
    let wordnum = (x / BITS_PER_WORD) as usize;
    let bitnum = (x % BITS_PER_WORD) as u32;
    if wordnum >= a.words.len() {
        a.words
            .try_reserve(wordnum + 1 - a.words.len())
            .map_err(|_| mcx.oom(0))?;
        while a.words.len() <= wordnum {
            a.words.push(0);
        }
    }
    a.words[wordnum] |= 1u64 << bitnum;
    Ok(())
}

/// `bms_is_member(x, a)` over our local Bitmapset image.
fn bms_is_member_local(a: &types_nodes::bitmapset::Bitmapset<'_>, x: i32) -> bool {
    if x < 0 {
        return false;
    }
    const BITS_PER_WORD: i32 = 64;
    let wordnum = (x / BITS_PER_WORD) as usize;
    let bitnum = (x % BITS_PER_WORD) as u32;
    match a.words.get(wordnum) {
        Some(w) => (w & (1u64 << bitnum)) != 0,
        None => false,
    }
}

/* -------------------------------------------------------------------------- *
 * SQL-callable entry points (ruleutils.c) — the thin fmgr layer lives in
 * `fmgr_builtins`; the worker bodies that the fmgr wrappers call live here.
 * -------------------------------------------------------------------------- */

mod fmgr_builtins;
pub use fmgr_builtins::register_ruleutils_builtins;

pub mod constraintdef;
pub mod functiondef;
pub mod indexdef;
pub mod partconstrdef;
pub mod partkeydef;
pub mod statisticsdef;
pub mod triggerdef;
pub mod viewdef;

/// `PRETTYFLAG_PAREN` (ruleutils.c 88).
pub(crate) const PRETTYFLAG_PAREN: i32 = 0x0001;
/// `PRETTYFLAG_INDENT` (ruleutils.c 89).
pub(crate) const PRETTYFLAG_INDENT: i32 = 0x0002;
/// `PRETTYFLAG_SCHEMA` (ruleutils.c 90).
pub(crate) const PRETTYFLAG_SCHEMA: i32 = 0x0004;
/// `WRAP_COLUMN_DEFAULT` (ruleutils.c 98) — 0 means wrap always.
pub(crate) const WRAP_COLUMN_DEFAULT: i32 = 0;

/// `GET_PRETTY_FLAGS(pretty)` (ruleutils.c 93-95).
#[inline]
pub(crate) fn get_pretty_flags(pretty: bool) -> i32 {
    if pretty {
        PRETTYFLAG_PAREN | PRETTYFLAG_INDENT | PRETTYFLAG_SCHEMA
    } else {
        PRETTYFLAG_INDENT
    }
}

/// `deparse_expression_pretty(expr, dpcontext, forceprefix, showimplicit,
/// prettyFlags, startIndent)` (ruleutils.c 3674-3700).
///
/// General utility for deparsing expressions: builds a fresh `deparse_context`
/// over the supplied namespace stack, runs [`get_rule_expr`], and returns the
/// rendered SQL text. (We charge the buffer to `mcx`, matching the C
/// `initStringInfo` in the caller's context.)
pub fn deparse_expression_pretty<'mcx>(
    mcx: Mcx<'mcx>,
    expr: &Node<'mcx>,
    dpcontext: PgVec<'mcx, DeparseNamespace<'mcx>>,
    forceprefix: bool,
    showimplicit: bool,
    pretty_flags: i32,
    start_indent: i32,
) -> PgResult<PgString<'mcx>> {
    let mut context = DeparseContext {
        buf: types_stringinfo::StringInfo::new_in(mcx),
        namespaces: dpcontext,
        resultDesc: None,
        targetList: PgVec::new_in(mcx),
        windowClause: PgVec::new_in(mcx),
        prettyFlags: pretty_flags,
        wrapColumn: WRAP_COLUMN_DEFAULT,
        indentLevel: start_indent,
        varprefix: forceprefix,
        colNamesVisible: true,
        inGroupBy: false,
        varInOrderBy: false,
        appendparents: None,
    };

    expr_deparse::get_rule_expr(expr, &mut context, showimplicit)?;

    // buf.data -> String (palloc'd in C; here charged to mcx).
    let s = core::str::from_utf8(context.buf.data.as_slice())
        .map_err(|_| elog_error("deparse produced invalid UTF-8".into()))?;
    pstrdup(mcx, s)
}

/// `deparse_expression(expr, dpcontext, forceprefix, showimplicit)`
/// (ruleutils.c 3645-3672) — the public entry, a thin wrapper over
/// [`deparse_expression_pretty`] with no pretty-printing.
pub fn deparse_expression<'mcx>(
    mcx: Mcx<'mcx>,
    expr: &Node<'mcx>,
    dpcontext: PgVec<'mcx, DeparseNamespace<'mcx>>,
    forceprefix: bool,
    showimplicit: bool,
) -> PgResult<PgString<'mcx>> {
    deparse_expression_pretty(mcx, expr, dpcontext, forceprefix, showimplicit, 0, 0)
}

/// `OidIsValid(oid)` (`c.h`).
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    oid != Oid::default()
}

/// Drill down a node tree past surrounding `List` wrappers to the first
/// non-`List` node, mirroring C's `while (tst && IsA(tst, List)) tst =
/// linitial((List *) tst);`.
fn drill_past_lists<'a, 'mcx>(node: &'a Node<'mcx>) -> Option<&'a Node<'mcx>> {
    let mut tst = node;
    loop {
        match tst.as_list() {
            Some(items) => match items.first() {
                Some(first) => tst = first.as_ref(),
                None => return None, // empty List -> linitial would be NULL
            },
            None => return Some(tst),
        }
    }
}

/// `bms_is_subset(relids, bms_make_singleton(1))` for the relids returned by
/// `pull_varnos`: true iff every member is relid 1 (or the set is empty). The
/// owned Bitmapset stores 64-bit words; a subset of `{1}` has all words zero
/// except possibly word 0, which may contain only bit 1.
fn relids_subset_of_one(relids: &types_nodes::bitmapset::Bitmapset<'_>) -> bool {
    for (i, &w) in relids.words.iter().enumerate() {
        if i == 0 {
            if w & !(1u64 << 1) != 0 {
                return false;
            }
        } else if w != 0 {
            return false;
        }
    }
    true
}

/// `bms_is_empty(relids)` for the owned Bitmapset image.
fn relids_is_empty(relids: &types_nodes::bitmapset::Bitmapset<'_>) -> bool {
    relids.words.iter().all(|&w| w == 0)
}

/// Compute `pull_varnos(NULL, node)` for a deparse input. C calls
/// `pull_varnos` on the bare `Node*`; the `pull_varnos` owner seam takes an
/// `&Expr`, so we apply it to each `Expr` reachable from `node` (a single
/// `Expr`, or every element of a `List`/nested `List`), unioning the relids.
/// Returns `None` (the empty set) when no Vars are present.
fn pull_varnos_node<'mcx>(
    mcx: Mcx<'mcx>,
    node: &Node<'mcx>,
    acc: &mut Option<PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>>,
) -> PgResult<()> {
    if let Some(items) = node.as_list() {
        for it in items.iter() {
            pull_varnos_node(mcx, it.as_ref(), acc)?;
        }
        Ok(())
    } else if let Some(expr) = node.as_expr() {
        {
            let r = backend_optimizer_util_var_seams::pull_varnos::call(mcx, expr)?;
            if let Some(more) = r {
                match acc {
                    None => *acc = Some(more),
                    Some(existing) => {
                        // bms_union (in place): OR the words together.
                        let nwords = more.words.len();
                        let cur = existing.words.len();
                        if cur < nwords {
                            existing
                                .words
                                .try_reserve(nwords - cur)
                                .map_err(|_| mcx.oom(0))?;
                            while existing.words.len() < nwords {
                                existing.words.push(0);
                            }
                        }
                        for (dst, src) in existing.words.iter_mut().zip(more.words.iter()) {
                            *dst |= *src;
                        }
                    }
                }
            }
        }
        Ok(())
    } else {
        // A non-List/non-Expr top node carries no Vars to pull (matches the C
        // walk, which only follows Var-bearing expression nodes).
        Ok(())
    }
}

/// `pg_get_expr_worker(expr, relid, prettyFlags)` (ruleutils.c 2709-2787).
///
/// The shared body of `pg_get_expr` / `pg_get_expr_ext`: parse the
/// `pg_node_tree` text into a node tree, reject querytrees and (depending on
/// `relid`) expressions referencing relations we cannot deparse, build a
/// one-relation deparse context if a relid was given, and deparse. Returns
/// `Ok(None)` when the relation has gone away (C returns NULL).
pub fn pg_get_expr_worker<'mcx>(
    mcx: Mcx<'mcx>,
    exprstr: &str,
    relid: Oid,
    pretty_flags: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    // Convert expression to node tree.
    let node = backend_nodes_read_seams::string_to_node::call(mcx, exprstr)?;

    // Throw error if the input is a querytree rather than an expression tree.
    // Drill past surrounding Lists, then check for a Query.
    if let Some(tst) = drill_past_lists(&node) {
        if tst.node_tag() == types_nodes::nodes::ntag::T_Query {
            return Err(PgError::error("input is a query, not an expression")
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE));
        }
    }

    // Throw error if the expression contains Vars we won't be able to deparse.
    let mut relids: Option<PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>> = None;
    pull_varnos_node(mcx, &node, &mut relids)?;
    if oid_is_valid(relid) {
        // !bms_is_subset(relids, bms_make_singleton(1))
        let ok = match relids.as_ref() {
            None => true,
            Some(r) => relids_subset_of_one(r),
        };
        if !ok {
            return Err(PgError::error(
                "expression contains variables of more than one relation",
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE));
        }
    } else {
        // !bms_is_empty(relids)
        let empty = match relids.as_ref() {
            None => true,
            Some(r) => relids_is_empty(r),
        };
        if !empty {
            return Err(PgError::error("expression contains variables")
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE));
        }
    }

    // Prepare deparse context if needed. With a relid, transiently open and
    // lock the rel so it won't go away underneath us.
    let context: PgVec<'mcx, DeparseNamespace<'mcx>> = if oid_is_valid(relid) {
        let rel = backend_access_common_relation_seams::try_relation_open::call(
            mcx,
            relid,
            AccessShareLock,
        )?;
        let rel = match rel {
            Some(r) => r,
            None => return Ok(None),
        };
        let ctx = deparse_context_for(mcx, rel.name(), relid)?;
        // relation_close(rel, AccessShareLock) (ruleutils.c 2784).
        rel.close(AccessShareLock)?;
        ctx
    } else {
        PgVec::new_in(mcx)
    };

    // Deparse.
    let str = deparse_expression_pretty(mcx, &node, context, false, false, pretty_flags, 0)?;

    Ok(Some(str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_nodes::nodes::Node;
    use types_nodes::parsenodes::{RangeTblEntry, RTE_JOIN, RTE_SUBQUERY};
    use types_nodes::primnodes::{Expr, Var};
    use types_nodes::rawnodes::{Alias, FromExpr, JoinExpr, RangeTblRef};
    use types_nodes::value::StringNode;

    /// `makeString(s)` as a boxed `Node`.
    fn make_string<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgBox<'mcx, Node<'mcx>> {
        mcx::alloc_in(
            mcx,
            Node::mk_string(mcx, StringNode {
                sval: PgString::from_str_in(s, mcx).unwrap(),
            }),
        )
        .unwrap()
    }

    /// An `Alias` with the given aliasname (or none) and column names.
    fn make_alias<'mcx>(
        mcx: Mcx<'mcx>,
        name: Option<&str>,
        cols: &[&str],
    ) -> PgBox<'mcx, Alias<'mcx>> {
        let mut colnames = PgVec::new_in(mcx);
        for c in cols {
            colnames.push(make_string(mcx, c));
        }
        mcx::alloc_in(
            mcx,
            Alias {
                aliasname: name.map(|n| PgString::from_str_in(n, mcx).unwrap()),
                colnames,
            },
        )
        .unwrap()
    }

    /// A subquery RTE with an alias and the given eref column names.
    fn subquery_rte<'mcx>(
        mcx: Mcx<'mcx>,
        alias: &str,
        cols: &[&str],
    ) -> RangeTblEntry<'mcx> {
        let mut rte = RangeTblEntry::new_in(mcx);
        rte.rtekind = RTE_SUBQUERY;
        rte.alias = Some(make_alias(mcx, Some(alias), cols));
        rte.eref = Some(make_alias(mcx, Some(alias), cols));
        rte.inFromCl = true;
        rte
    }

    #[test]
    fn rtable_names_dedup() {
        let ctx = MemoryContext::new("rtable_names_dedup");
        let mcx = ctx.mcx();
        // Two subquery RTEs both aliased "t" -> "t", "t_1".
        let mut dpns = DeparseNamespace::zeroed(mcx);
        dpns.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(subquery_rte(mcx, "t", &["a"]));
            v.push(subquery_rte(mcx, "t", &["b"]));
            v
        };
        set_rtable_names(mcx, &mut dpns, &[], None).unwrap();
        assert_eq!(dpns.rtable_names.len(), 2);
        assert_eq!(dpns.rtable_names[0].as_ref().unwrap().as_str(), "t");
        assert_eq!(dpns.rtable_names[1].as_ref().unwrap().as_str(), "t_1");
    }

    #[test]
    fn relation_column_names_subquery_no_change() {
        let ctx = MemoryContext::new("relation_column_names");
        let mcx = ctx.mcx();
        let mut dpns = DeparseNamespace::zeroed(mcx);
        let rte = subquery_rte(mcx, "s", &["x", "y"]);
        dpns.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(rte.clone_in(mcx).unwrap());
            v
        };
        set_rtable_names(mcx, &mut dpns, &[], None).unwrap();
        let mut colinfo = DeparseColumns::zeroed(mcx);
        let rte0 = dpns.rtable[0].clone_in(mcx).unwrap();
        set_relation_column_names(mcx, &mut dpns, &rte0, &mut colinfo).unwrap();
        // Subquery with user alias colnames "x","y" matching its own names ->
        // no change, but it has a user-written alias colname list, so a
        // non-RELATION/FUNCTION/TABLEFUNC RTE with alias->colnames prints aliases.
        assert_eq!(colinfo.num_cols, 2);
        assert_eq!(colinfo.colnames[0].as_ref().unwrap().as_str(), "x");
        assert_eq!(colinfo.colnames[1].as_ref().unwrap().as_str(), "y");
        assert!(colinfo.printaliases); // alias->colnames non-empty
    }

    #[test]
    fn relation_column_names_unique_collision() {
        let ctx = MemoryContext::new("col_collision");
        let mcx = ctx.mcx();
        let mut dpns = DeparseNamespace::zeroed(mcx);
        // Subquery whose two columns are both named "c" -> deduped to c, c_1.
        let rte = subquery_rte(mcx, "s", &["c", "c"]);
        dpns.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(rte.clone_in(mcx).unwrap());
            v
        };
        set_rtable_names(mcx, &mut dpns, &[], None).unwrap();
        let mut colinfo = DeparseColumns::zeroed(mcx);
        let rte0 = dpns.rtable[0].clone_in(mcx).unwrap();
        set_relation_column_names(mcx, &mut dpns, &rte0, &mut colinfo).unwrap();
        assert_eq!(colinfo.colnames[0].as_ref().unwrap().as_str(), "c");
        assert_eq!(colinfo.colnames[1].as_ref().unwrap().as_str(), "c_1");
        assert!(colinfo.printaliases); // changed_any
    }

    #[test]
    fn deparse_for_query_unnamed_join_using() {
        // Build: SELECT ... FROM (subq a(k,x)) JOIN (subq b(k,y)) USING (k)
        // as an unnamed join RTE with merged column k.
        let ctx = MemoryContext::new("join_using");
        let mcx = ctx.mcx();
        let mut q = types_nodes::copy_query::Query::new(mcx);
        q.commandType = types_nodes::nodes::CmdType::CMD_SELECT;

        // RTE 1: subquery a(k,x). RTE 2: subquery b(k,y). RTE 3: the join.
        let a = subquery_rte(mcx, "a", &["k", "x"]);
        let b = subquery_rte(mcx, "b", &["k", "y"]);
        // join RTE: output cols k (merged), x, y. eref colnames + joinaliasvars.
        let mut jrte = RangeTblEntry::new_in(mcx);
        jrte.rtekind = RTE_JOIN;
        jrte.jointype = types_nodes::jointype::JoinType::JOIN_INNER;
        jrte.joinmergedcols = 1;
        // joinaliasvars: k (Var to left.k), x (Var left.x), y (Var right.y).
        let mut jav = PgVec::new_in(mcx);
        for (varno, varattno) in [(1, 1), (1, 2), (2, 2)] {
            let v = Var {
                varno,
                varattno,
                ..Default::default()
            };
            jav.push(mcx::alloc_in(mcx, Node::mk_var(mcx, v)?).unwrap());
        }
        jrte.joinaliasvars = jav;
        jrte.joinleftcols = {
            let mut v = PgVec::new_in(mcx);
            v.push(1);
            v.push(2);
            v
        };
        jrte.joinrightcols = {
            let mut v = PgVec::new_in(mcx);
            v.push(1);
            v.push(2);
            v
        };
        // eref colnames k,x,y (no alias -> unnamed join).
        jrte.eref = Some(make_alias(mcx, Some("unnamed_join"), &["k", "x", "y"]));

        q.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(a);
            v.push(b);
            v.push(jrte);
            v
        };

        // jointree: FromExpr { fromlist: [ JoinExpr(larg=RTR 1, rarg=RTR 2,
        // usingClause=[k], rtindex=3) ] }
        let join = JoinExpr {
            jointype: types_nodes::jointype::JoinType::JOIN_INNER,
            isNatural: false,
            larg: Some(mcx::alloc_in(mcx, Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex: 1 })?).unwrap()),
            rarg: Some(mcx::alloc_in(mcx, Node::mk_range_tbl_ref(mcx, RangeTblRef { rtindex: 2 })?).unwrap()),
            usingClause: {
                let mut v = PgVec::new_in(mcx);
                v.push(make_string(mcx, "k"));
                v
            },
            join_using_alias: None,
            quals: None,
            alias: None, // unnamed join
            rtindex: 3,
        };
        let fromexpr = FromExpr {
            fromlist: {
                let mut v = PgVec::new_in(mcx);
                v.push(mcx::alloc_in(mcx, Node::mk_join_expr(mcx, join)?).unwrap());
                v
            },
            quals: None,
        };
        q.jointree = Some(mcx::alloc_in(mcx, fromexpr).unwrap());

        let mut dpns = DeparseNamespace::zeroed(mcx);
        set_deparse_for_query(mcx, &mut dpns, &q, &[]).unwrap();

        // rtable_names: a, b, (None for unnamed join).
        assert_eq!(dpns.rtable_names[0].as_ref().unwrap().as_str(), "a");
        assert_eq!(dpns.rtable_names[1].as_ref().unwrap().as_str(), "b");
        assert!(dpns.rtable_names[2].is_none());

        // The join colinfo: merged column "k" named (usingNames has 1 entry),
        // leftrti=1, rightrti=2.
        let jcol = &dpns.rtable_columns[2];
        assert_eq!(jcol.leftrti, 1);
        assert_eq!(jcol.rightrti, 2);
        assert_eq!(jcol.usingNames.len(), 1);
        assert_eq!(jcol.usingNames[0].as_str(), "k");
        // Unnamed join cannot print aliases.
        assert!(!jcol.printaliases);
        // Join output colnames: k (merged), x, y.
        assert_eq!(jcol.colnames[0].as_ref().unwrap().as_str(), "k");
        assert_eq!(jcol.colnames[1].as_ref().unwrap().as_str(), "x");
        assert_eq!(jcol.colnames[2].as_ref().unwrap().as_str(), "y");
    }

    #[test]
    fn rtable_name_prefers_user_alias() {
        // A relation RTE with a user alias takes the alias name without any
        // catalog lookup (get_rel_name is only used for un-aliased relations).
        let ctx = MemoryContext::new("alias_pref");
        let mcx = ctx.mcx();
        let mut rte = RangeTblEntry::new_in(mcx);
        rte.rtekind = RTE_RELATION;
        rte.relid = Oid::default();
        rte.alias = Some(make_alias(mcx, Some("myrel"), &[]));
        rte.eref = Some(make_alias(mcx, Some("myrel"), &[]));
        let mut dpns = DeparseNamespace::zeroed(mcx);
        dpns.rtable = {
            let mut v = PgVec::new_in(mcx);
            v.push(rte);
            v
        };
        set_rtable_names(mcx, &mut dpns, &[], None).unwrap();
        assert_eq!(dpns.rtable_names[0].as_ref().unwrap().as_str(), "myrel");
    }

    #[test]
    fn generate_relation_name_catalog_visible_and_qualified() {
        // Install faithful owner bodies for the four catalog reads
        // generate_relation_name_catalog drives, then check both the visible
        // (unqualified) and not-visible (schema-qualified) paths plus the
        // force_qual (CTE-conflict) override of a visible relation.
        if !backend_utils_cache_lsyscache_seams::get_rel_name::is_installed() {
            backend_utils_cache_lsyscache_seams::get_rel_name::set(|mcx, relid| {
                let name = match relid {
                    100 => "vis_rel",
                    200 => "hidden_rel",
                    _ => return Ok(None),
                };
                Ok(Some(PgString::from_str_in(name, mcx)?))
            });
        }
        if !backend_utils_cache_lsyscache_seams::get_rel_namespace::is_installed() {
            backend_utils_cache_lsyscache_seams::get_rel_namespace::set(|relid| Ok(relid + 1));
        }
        if !backend_catalog_namespace_seams::relation_is_visible::is_installed() {
            // relid 100 visible; relid 200 not visible.
            backend_catalog_namespace_seams::relation_is_visible::set(|_mcx, relid| Ok(relid == 100));
        }
        if !backend_utils_cache_lsyscache_seams::get_namespace_name_or_temp::is_installed() {
            backend_utils_cache_lsyscache_seams::get_namespace_name_or_temp::set(|mcx, nspid| {
                Ok(Some(PgString::from_str_in(
                    &alloc::format!("ns{}", nspid),
                    mcx,
                )?))
            });
        }

        let ctx = MemoryContext::new("genrelname");
        let mcx = ctx.mcx();

        // Visible relation: unqualified.
        let r = generate_relation_name_catalog(mcx, 100, false).unwrap();
        assert_eq!(r.as_str(), "vis_rel");

        // Visible relation but force_qual (CTE conflict): qualified with its
        // schema name (nspid = relid + 1 = 101 -> "ns101").
        let r = generate_relation_name_catalog(mcx, 100, true).unwrap();
        assert_eq!(r.as_str(), "ns101.vis_rel");

        // Not-visible relation: qualified (nspid = 201 -> "ns201").
        let r = generate_relation_name_catalog(mcx, 200, false).unwrap();
        assert_eq!(r.as_str(), "ns201.hidden_rel");

        // Cache miss -> elog(ERROR).
        assert!(generate_relation_name_catalog(mcx, 999, false).is_err());
    }

}
