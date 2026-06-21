//! `utils/adt/ruleutils.c` — the index-definition deparser
//! (`pg_get_indexdef` / `pg_get_indexdef_ext`, the `pg_get_indexdef_worker`
//! body, ruleutils.c 1269-1576).
//!
//! The worker reverse-lists an index (or, for exclusion constraints, the
//! `EXCLUDE USING …` body) from its catalog rows. It reads the `pg_index`
//! tuple (via the `search_pg_index_info` syscache projection plus the
//! `pg_index_exprs_text` / `pg_index_pred_text` `pg_node_tree` readers), the
//! index relation's `pg_class` row, and the access method's `pg_am` row, then
//! renders the columns / opclasses / collations / predicate through the ported
//! `deparse_expression_pretty` engine and the `generate_*` name builders.

use alloc::format;
use alloc::string::String;
use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::{
    deparse_context_for, deparse_expression_pretty, flatten_reloptions, generate_collation_name,
    generate_relation_name_catalog, get_opclass_name, get_reloptions_pub, oid_is_valid_pub,
    quote_identifier,
};

/// `RELKIND_PARTITIONED_INDEX` (`catalog/pg_class.h`).
const RELKIND_PARTITIONED_INDEX: u8 = b'I';
/// `INDOPTION_DESC` / `INDOPTION_NULLS_FIRST` (`catalog/pg_index.h`).
const INDOPTION_DESC: i16 = 0x0001;
const INDOPTION_NULLS_FIRST: i16 = 0x0002;
/// `PRETTYFLAG_SCHEMA` (`utils/ruleutils.h` line 90: `0x0004`). NB: this is a
/// distinct bit from `PRETTYFLAG_INDENT` (`0x0002`); the plain `pg_get_indexdef`
/// path passes only `PRETTYFLAG_INDENT`, so the SCHEMA bit is clear and the
/// table name is force-qualified via `generate_qualified_relation_name`.
const PRETTYFLAG_SCHEMA: i32 = 0x0004;

/// `pg_get_indexdef_worker(indexrelid, colno, excludeOps, attrsOnly, keysOnly,
/// showTblSpc, inherits, prettyFlags, missing_ok)` (ruleutils.c 1269-1576).
///
/// The two SQL-callable entries (`pg_get_indexdef`, `pg_get_indexdef_ext`) pass
/// `excludeOps = None`, `attrsOnly = keysOnly = showTblSpc = inherits = false`,
/// `colno = 0`; the exclusion-constraint caller ([`crate::constraintdef`])
/// passes the operator list. This port carries the full parameter set so all
/// callers share one body.
///
/// Returns the index definition text, or `Ok(None)` when `missing_ok` and the
/// index is gone (the fmgr callers pass `missing_ok = true`).
#[allow(clippy::too_many_arguments)]
pub fn pg_get_indexdef_worker<'mcx>(
    mcx: Mcx<'mcx>,
    indexrelid: Oid,
    colno: i32,
    exclude_ops: Option<&[Oid]>,
    attrs_only: bool,
    keys_only: bool,
    show_tbl_spc: bool,
    inherits: bool,
    pretty_flags: i32,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    // bool isConstraint = (excludeOps != NULL);
    let is_constraint = exclude_ops.is_some();

    // ht_idx = SearchSysCache1(INDEXRELID, indexrelid); + indcollation/indclass/
    // indoption deforms — all folded into search_pg_index_info.
    let idxrec =
        match backend_utils_cache_syscache_seams::search_pg_index_info::call(mcx, indexrelid)? {
            Some(i) => i,
            None => {
                if missing_ok {
                    return Ok(None);
                }
                return Err(PgError::error(format!(
                    "cache lookup failed for index {indexrelid}"
                )));
            }
        };

    let indrelid = idxrec.indrelid;
    debug_assert_eq!(indexrelid, idxrec.indexrelid);

    // ht_idxrel = SearchSysCache1(RELOID, indexrelid) -> relname/relkind/relam.
    let idxrelname = backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, indexrelid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for relation {indexrelid}")))?;
    let idxrelkind = backend_utils_cache_lsyscache_seams::get_rel_relkind::call(indexrelid)?;
    let relam = backend_utils_cache_lsyscache_seams::get_rel_relam::call(indexrelid)?;

    // ht_am = SearchSysCache1(AMOID, relam) -> amname; amroutine = GetIndexAmRoutine.
    let amname = backend_utils_cache_lsyscache_seams::get_am_name::call(mcx, relam)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for access method {relam}")))?;
    let amroutine = backend_access_index_amapi_seams::get_index_am_routine_by_amid::call(relam)?;

    // Get the index expressions (non-const-folded), if any:
    //   indexprs = (List *) stringToNode(TextDatumGetCString(indexprs));
    let indexprs_text = backend_utils_cache_syscache_seams::pg_index_exprs_text::call(indexrelid)?;
    let indexprs = match indexprs_text {
        Some(s) => Some(backend_nodes_read_seams::string_to_node::call(mcx, &s)?),
        None => None,
    };
    // Iterate the expression list head-to-tail (list_head / lnext).
    let mut indexpr_idx: usize = 0;

    // context = deparse_context_for(get_relation_name(indrelid), indrelid);
    let indrelname = backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, indrelid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for relation {indrelid}")))?;

    let mut buf = String::new();

    if !attrs_only {
        if !is_constraint {
            // "CREATE %sINDEX %s ON %s%s USING %s ("
            let on_name = if (pretty_flags & PRETTYFLAG_SCHEMA) != 0 {
                // generate_relation_name(indrelid, NIL) — no CTE namespaces here.
                generate_relation_name_catalog(mcx, indrelid, false)?
            } else {
                // generate_qualified_relation_name(indrelid) — always qualified.
                generate_relation_name_catalog(mcx, indrelid, true)?
            };
            let only = if idxrelkind == RELKIND_PARTITIONED_INDEX && !inherits {
                "ONLY "
            } else {
                ""
            };
            let unique = if idxrec.indisunique { "UNIQUE " } else { "" };
            let qidx = quote_identifier(mcx, idxrelname.as_str())?;
            let qam = quote_identifier(mcx, amname.as_str())?;
            buf.push_str("CREATE ");
            buf.push_str(unique);
            buf.push_str("INDEX ");
            buf.push_str(qidx.as_str());
            buf.push_str(" ON ");
            buf.push_str(only);
            buf.push_str(on_name.as_str());
            buf.push_str(" USING ");
            buf.push_str(qam.as_str());
            buf.push_str(" (");
        } else {
            // EXCLUDE constraint: "EXCLUDE USING %s ("
            let qam = quote_identifier(mcx, amname.as_str())?;
            buf.push_str("EXCLUDE USING ");
            buf.push_str(qam.as_str());
            buf.push_str(" (");
        }
    }

    // Report the indexed attributes.
    let mut sep = "";
    for keyno in 0..idxrec.indnatts as usize {
        let attnum = idxrec.indkey[keyno];

        // Ignore non-key attributes if keysOnly.
        if keys_only && keyno >= idxrec.indnkeyatts as usize {
            break;
        }

        // Print INCLUDE to divide key and non-key attrs.
        if colno == 0 && keyno == idxrec.indnkeyatts as usize {
            buf.push_str(") INCLUDE (");
            sep = "";
        }

        if colno == 0 {
            buf.push_str(sep);
        }
        sep = ", ";

        let keycoltype: Oid;
        let keycolcollation: Oid;

        if attnum != 0 {
            // Simple index column.
            let attname = backend_utils_cache_lsyscache_seams::get_attname::call(
                mcx, indrelid, attnum, false,
            )?
            .ok_or_else(|| {
                PgError::error(format!(
                    "cache lookup failed for attribute {attnum} of relation {indrelid}"
                ))
            })?;
            if colno == 0 || colno == keyno as i32 + 1 {
                let q = quote_identifier(mcx, attname.as_str())?;
                buf.push_str(q.as_str());
            }
            let (typid, _typmod, coll) =
                backend_utils_cache_lsyscache_seams::get_atttypetypmodcoll::call(indrelid, attnum)?;
            keycoltype = typid;
            keycolcollation = coll;
        } else {
            // Expressional index column.
            let exprs = indexprs
                .as_ref()
                .ok_or_else(|| PgError::error("too few entries in indexprs list"))?;
            let items = exprs
                .as_list()
                .ok_or_else(|| PgError::error("too few entries in indexprs list"))?;
            let indexkey = items
                .get(indexpr_idx)
                .ok_or_else(|| PgError::error("too few entries in indexprs list"))?;
            indexpr_idx += 1;
            // str = deparse_expression_pretty(indexkey, context, false, false, ...).
            let context = deparse_context_for(mcx, indrelname.as_str(), indrelid)?;
            let s = deparse_expression_pretty(
                mcx,
                indexkey.as_ref(),
                context,
                false,
                false,
                pretty_flags,
                0,
            )?;
            if colno == 0 || colno == keyno as i32 + 1 {
                // Need parens if it's not a bare function call.
                if crate::expr_deparse::looks_like_function_pub(indexkey.as_ref()) {
                    buf.push_str(s.as_str());
                } else {
                    buf.push('(');
                    buf.push_str(s.as_str());
                    buf.push(')');
                }
            }
            keycoltype = crate::expr_deparse::expr_type_of_node(indexkey.as_ref())?;
            keycolcollation = crate::expr_deparse::expr_collation_of_node(indexkey.as_ref())?;
        }

        // Print additional decoration for (selected) key columns.
        if !attrs_only
            && keyno < idxrec.indnkeyatts as usize
            && (colno == 0 || colno == keyno as i32 + 1)
        {
            let opt = idxrec.indoption[keyno];
            let indcoll = idxrec.indcollation[keyno];
            let attoptions = backend_utils_cache_lsyscache_seams::get_attoptions::call(
                mcx,
                indexrelid,
                keyno as i16 + 1,
            )?;
            let attoptions_bytes: Option<&[u8]> = match &attoptions {
                Some(Datum::ByRef(b)) => Some(b),
                _ => None,
            };
            let has_options = attoptions_bytes.is_some();

            // Add collation, if not default for column.
            if oid_is_valid_pub(indcoll) && indcoll != keycolcollation {
                let cn = generate_collation_name(mcx, indcoll)?;
                buf.push_str(" COLLATE ");
                buf.push_str(cn.as_str());
            }

            // Add the operator class name, if not default.
            get_opclass_name(
                mcx,
                &mut buf,
                idxrec.indclass[keyno],
                if has_options { Oid::default() } else { keycoltype },
            )?;

            // Add opclass options if relevant.
            if let Some(opts) = attoptions_bytes {
                buf.push_str(" (");
                get_reloptions_pub(mcx, &mut buf, opts)?;
                buf.push(')');
            }

            // Add DESC / NULLS opts if the AM supports sort ordering.
            if amroutine.amcanorder {
                if (opt & INDOPTION_DESC) != 0 {
                    buf.push_str(" DESC");
                    // NULLS FIRST is the default in this case.
                    if (opt & INDOPTION_NULLS_FIRST) == 0 {
                        buf.push_str(" NULLS LAST");
                    }
                } else if (opt & INDOPTION_NULLS_FIRST) != 0 {
                    buf.push_str(" NULLS FIRST");
                }
            }

            // Add the exclusion operator if relevant.
            if let Some(ops) = exclude_ops {
                let opn = backend_utils_adt_ruleutils_seams::generate_operator_name::call(
                    mcx,
                    ops[keyno],
                    keycoltype,
                    keycoltype,
                )?;
                buf.push_str(" WITH ");
                buf.push_str(opn.as_str());
            }
        }
    }

    if !attrs_only {
        buf.push(')');

        if idxrec.indnullsnotdistinct {
            buf.push_str(" NULLS NOT DISTINCT");
        }

        // If it has options, append "WITH (options)".
        if let Some(opts) = flatten_reloptions(mcx, indexrelid)? {
            buf.push_str(" WITH (");
            buf.push_str(opts.as_str());
            buf.push(')');
        }

        // Print tablespace, only if requested.
        if show_tbl_spc {
            let tblspc = backend_utils_cache_lsyscache_seams::get_rel_tablespace::call(indexrelid)?;
            if oid_is_valid_pub(tblspc) {
                if is_constraint {
                    buf.push_str(" USING INDEX");
                }
                let tsname =
                    backend_commands_tablespace_seams::get_tablespace_name::call(mcx, tblspc)?
                        .ok_or_else(|| {
                            PgError::error(format!("cache lookup failed for tablespace {tblspc}"))
                        })?;
                let q = quote_identifier(mcx, tsname.as_str())?;
                buf.push_str(" TABLESPACE ");
                buf.push_str(q.as_str());
            }
        }

        // If it's a partial index, decompile and append the predicate.
        let pred_text = backend_utils_cache_syscache_seams::pg_index_pred_text::call(indexrelid)?;
        if let Some(s) = pred_text {
            let node = backend_nodes_read_seams::string_to_node::call(mcx, &s)?;
            let context = deparse_context_for(mcx, indrelname.as_str(), indrelid)?;
            let predstr = deparse_expression_pretty(
                mcx,
                node.as_ref(),
                context,
                false,
                false,
                pretty_flags,
                0,
            )?;
            if is_constraint {
                buf.push_str(" WHERE (");
                buf.push_str(predstr.as_str());
                buf.push(')');
            } else {
                buf.push_str(" WHERE ");
                buf.push_str(predstr.as_str());
            }
        }
    }

    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}

/// `pg_get_indexdef_string(indexrelid)` (ruleutils.c:1225) — internal version
/// used to feed `ATPostAlterTypeParse`: includes the index tablespace and the
/// `ONLY` inheritance marker, never NULL (missing_ok = false). Equivalent to
/// `pg_get_indexdef_worker(indexrelid, 0, NULL, false, false, true, true, 0,
/// false)`.
pub fn pg_get_indexdef_string<'mcx>(mcx: Mcx<'mcx>, indexrelid: Oid) -> PgResult<PgString<'mcx>> {
    let s = pg_get_indexdef_worker(
        mcx, indexrelid, 0, None, false, false, true, true, 0, false,
    )?;
    // missing_ok = false → the worker never returns None.
    Ok(s.expect("pg_get_indexdef_string: worker returned None with missing_ok = false"))
}
