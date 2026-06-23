//! `utils/adt/ruleutils.c` — the constraint-definition deparser
//! (`pg_get_constraintdef` / `pg_get_constraintdef_ext`, the
//! `pg_get_constraintdef_worker` body, ruleutils.c 2193-2612).
//!
//! The worker reverse-lists a `pg_constraint` row: FOREIGN KEY / PRIMARY KEY /
//! UNIQUE / CHECK / NOT NULL / TRIGGER / EXCLUDE. It reads the constraint tuple
//! (via the `search_pg_constraintdef_info` syscache projection — scalar form +
//! the `conkey` / `confkey` / `conexclop` / `confdelsetcols` / `conbin`
//! by-reference columns + the backing index's `indnatts` / `indkey` /
//! `indnullsnotdistinct`), then renders names through `generate_relation_name`,
//! the CHECK expression through the ported `deparse_expression_pretty` engine,
//! and the EXCLUDE body through [`crate::indexdef::pg_get_indexdef_worker`].

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use mcx::{Mcx, PgString};
use types_catalog::pg_constraint::{ConKeyArray, FormData_pg_constraint};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_tuple::heaptuple::INT2OID;

use crate::{
    deparse_context_for, deparse_expression_pretty, generate_relation_name_catalog,
    oid_is_valid_pub, quote_identifier, quote_qualified_identifier,
};

/// `CONSTRAINT_*` `contype` chars (`catalog/pg_constraint.h`).
const CONSTRAINT_CHECK: i8 = b'c' as i8;
const CONSTRAINT_FOREIGN: i8 = b'f' as i8;
const CONSTRAINT_NOTNULL: i8 = b'n' as i8;
const CONSTRAINT_PRIMARY: i8 = b'p' as i8;
const CONSTRAINT_UNIQUE: i8 = b'u' as i8;
const CONSTRAINT_TRIGGER: i8 = b't' as i8;
const CONSTRAINT_EXCLUSION: i8 = b'x' as i8;

/// `FKCONSTR_MATCH_*` / `FKCONSTR_ACTION_*` (`nodes/parsenodes.h`).
const FKCONSTR_MATCH_FULL: i8 = b'f' as i8;
const FKCONSTR_MATCH_PARTIAL: i8 = b'p' as i8;
const FKCONSTR_MATCH_SIMPLE: i8 = b's' as i8;
const FKCONSTR_ACTION_NOACTION: i8 = b'a' as i8;
const FKCONSTR_ACTION_RESTRICT: i8 = b'r' as i8;
const FKCONSTR_ACTION_CASCADE: i8 = b'c' as i8;
const FKCONSTR_ACTION_SETNULL: i8 = b'n' as i8;
const FKCONSTR_ACTION_SETDEFAULT: i8 = b'd' as i8;

/// `pg_get_constraintdef_worker(constraintId, fullCommand, prettyFlags,
/// missing_ok)` (ruleutils.c 2193-2612). Returns the constraint definition
/// text, or `Ok(None)` when `missing_ok` and the constraint is gone (the fmgr
/// callers pass `missing_ok = true`).
pub fn pg_get_constraintdef_worker<'mcx>(
    mcx: Mcx<'mcx>,
    constraint_id: Oid,
    full_command: bool,
    pretty_flags: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    pg_get_constraintdef_worker_inner(mcx, constraint_id, full_command, pretty_flags, true)
}

/// `pg_get_constraintdef_command(constraintId)` (ruleutils.c:2184) — internal
/// version used to feed `ATPostAlterTypeParse`: returns the full
/// `ALTER TABLE ... ADD CONSTRAINT ...` command form, never NULL
/// (missing_ok = false). Equivalent to
/// `pg_get_constraintdef_worker(constraintId, true, 0, false)`.
pub fn pg_get_constraintdef_command<'mcx>(
    mcx: Mcx<'mcx>,
    constraint_id: Oid,
) -> PgResult<PgString<'mcx>> {
    let s = pg_get_constraintdef_worker_inner(mcx, constraint_id, true, 0, false)?;
    Ok(s.expect("pg_get_constraintdef_command: worker returned None with missing_ok = false"))
}

/// As above, threading `missing_ok` (the `pg_get_constraintdef_command`
/// internal caller passes `false`).
pub(crate) fn pg_get_constraintdef_worker_inner<'mcx>(
    mcx: Mcx<'mcx>,
    constraint_id: Oid,
    full_command: bool,
    pretty_flags: i32,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    // tup = systable_getnext(...); conForm = GETSTRUCT(tup). The C uses an MVCC
    // scan over ConstraintOidIndexId; the by-OID syscache projection is
    // equivalent and is folded into search_pg_constraintdef_info.
    let info = match syscache_seams::search_pg_constraintdef_info::call(
        mcx,
        constraint_id,
    )? {
        Some(i) => i,
        None => {
            if missing_ok {
                return Ok(None);
            }
            return Err(PgError::error(format!(
                "could not find tuple for constraint {constraint_id}"
            )));
        }
    };
    let con = &info.form;

    let mut buf = String::new();

    if full_command {
        if oid_is_valid_pub(con.conrelid) {
            // "ALTER TABLE %s ADD CONSTRAINT %s "
            let rel = generate_qualified_relation_name(mcx, con.conrelid)?;
            let q = quote_identifier(mcx, conname_str(con))?;
            buf.push_str("ALTER TABLE ");
            buf.push_str(rel.as_str());
            buf.push_str(" ADD CONSTRAINT ");
            buf.push_str(q.as_str());
            buf.push(' ');
        } else {
            // Domain constraint: "ALTER DOMAIN %s ADD CONSTRAINT %s "
            debug_assert!(oid_is_valid_pub(con.contypid));
            let ty = generate_qualified_type_name(mcx, con.contypid)?;
            let q = quote_identifier(mcx, conname_str(con))?;
            buf.push_str("ALTER DOMAIN ");
            buf.push_str(ty.as_str());
            buf.push_str(" ADD CONSTRAINT ");
            buf.push_str(q.as_str());
            buf.push(' ');
        }
    }

    match con.contype {
        x if x == CONSTRAINT_FOREIGN => {
            buf.push_str("FOREIGN KEY (");

            // Referencing-column list (conkey).
            let conkey = info
                .conkey
                .as_ref()
                .ok_or_else(|| PgError::error("conkey is NULL for FK constraint"))?;
            decompile_column_index_array(mcx, conkey, con.conrelid, con.conperiod, &mut buf)?;

            // ") REFERENCES %s(", generate_relation_name(confrelid, NIL)
            let frel = generate_relation_name_catalog(mcx, con.confrelid, false)?;
            buf.push_str(") REFERENCES ");
            buf.push_str(frel.as_str());
            buf.push('(');

            // Referenced-column list (confkey).
            let confkey = info
                .confkey
                .as_ref()
                .ok_or_else(|| PgError::error("confkey is NULL for FK constraint"))?;
            decompile_column_index_array(mcx, confkey, con.confrelid, con.conperiod, &mut buf)?;
            buf.push(')');

            // Match type.
            match con.confmatchtype {
                m if m == FKCONSTR_MATCH_FULL => buf.push_str(" MATCH FULL"),
                m if m == FKCONSTR_MATCH_PARTIAL => buf.push_str(" MATCH PARTIAL"),
                m if m == FKCONSTR_MATCH_SIMPLE => {}
                other => {
                    return Err(PgError::error(format!(
                        "unrecognized confmatchtype: {other}"
                    )))
                }
            }

            // ON UPDATE.
            if let Some(s) = fk_action_string(con.confupdtype, "confupdtype")? {
                buf.push_str(" ON UPDATE ");
                buf.push_str(s);
            }
            // ON DELETE.
            if let Some(s) = fk_action_string(con.confdeltype, "confdeltype")? {
                buf.push_str(" ON DELETE ");
                buf.push_str(s);
            }

            // SET NULL / SET DEFAULT columns (confdelsetcols), if provided.
            if let Some(cols) = info.confdelsetcols.as_ref() {
                buf.push_str(" (");
                decompile_column_index_array(mcx, cols, con.conrelid, false, &mut buf)?;
                buf.push(')');
            }
        }
        x if x == CONSTRAINT_PRIMARY || x == CONSTRAINT_UNIQUE => {
            if con.contype == CONSTRAINT_PRIMARY {
                buf.push_str("PRIMARY KEY ");
            } else {
                buf.push_str("UNIQUE ");
            }

            // indtup = SearchSysCache1(INDEXRELID, conindid) — folded into info.
            // UNIQUE NULLS NOT DISTINCT (only printed for UNIQUE).
            if con.contype == CONSTRAINT_UNIQUE && info.indnullsnotdistinct == Some(true) {
                buf.push_str("NULLS NOT DISTINCT ");
            }

            buf.push('(');

            // Target column list (conkey).
            let conkey = info
                .conkey
                .as_ref()
                .ok_or_else(|| PgError::error("conkey is NULL for PK/UNIQUE constraint"))?;
            let keyatts =
                decompile_column_index_array(mcx, conkey, con.conrelid, false, &mut buf)?;
            if con.conperiod {
                buf.push_str(" WITHOUT OVERLAPS");
            }
            buf.push(')');

            // INCLUDE columns (from pg_index.indkey beyond keyatts).
            let indnatts = info
                .indnatts
                .ok_or_else(|| PgError::error("cache lookup failed for backing index"))?;
            if indnatts as i32 > keyatts {
                buf.push_str(" INCLUDE (");
                let indkey = info.indkey.as_ref().ok_or_else(|| {
                    PgError::error("cache lookup failed for backing index")
                })?;
                let mut first = true;
                for j in (keyatts as usize)..indkey.len() {
                    let col_name = lsyscache_seams::get_attname::call(
                        mcx,
                        con.conrelid,
                        indkey[j],
                        false,
                    )?
                    .ok_or_else(|| {
                        PgError::error(format!(
                            "cache lookup failed for attribute {} of relation {}",
                            indkey[j], con.conrelid
                        ))
                    })?;
                    if !first {
                        buf.push_str(", ");
                    }
                    first = false;
                    let q = quote_identifier(mcx, col_name.as_str())?;
                    buf.push_str(q.as_str());
                }
                buf.push(')');
            }

            // fullCommand: WITH (options) + USING INDEX TABLESPACE.
            if full_command && oid_is_valid_pub(con.conindid) {
                if let Some(options) = crate::flatten_reloptions(mcx, con.conindid)? {
                    buf.push_str(" WITH (");
                    buf.push_str(options.as_str());
                    buf.push(')');
                }
                let tblspc =
                    lsyscache_seams::get_rel_tablespace::call(con.conindid)?;
                if oid_is_valid_pub(tblspc) {
                    let tsname =
                        tablespace_seams::get_tablespace_name::call(mcx, tblspc)?
                            .ok_or_else(|| {
                                PgError::error(format!(
                                    "cache lookup failed for tablespace {tblspc}"
                                ))
                            })?;
                    let q = quote_identifier(mcx, tsname.as_str())?;
                    buf.push_str(" USING INDEX TABLESPACE ");
                    buf.push_str(q.as_str());
                }
            }
        }
        x if x == CONSTRAINT_CHECK => {
            // expr = stringToNode(TextDatumGetCString(conbin)).
            let conbin = info
                .conbin
                .as_ref()
                .ok_or_else(|| PgError::error("conbin is NULL for CHECK constraint"))?;
            let expr = read_seams::string_to_node::call(mcx, conbin)?;

            // context = relation constraint ? deparse_context_for(...) : NIL.
            let consrc = if con.conrelid != Oid::default() {
                let relname =
                    lsyscache_seams::get_rel_name::call(mcx, con.conrelid)?
                        .ok_or_else(|| {
                            PgError::error(format!(
                                "cache lookup failed for relation {}",
                                con.conrelid
                            ))
                        })?;
                let context = deparse_context_for(mcx, relname.as_str(), con.conrelid)?;
                deparse_expression_pretty(
                    mcx,
                    expr.as_ref(),
                    context,
                    false,
                    false,
                    pretty_flags,
                    0,
                )?
            } else {
                // domain constraint --- can't have Vars
                let context = mcx::PgVec::new_in(mcx);
                deparse_expression_pretty(
                    mcx,
                    expr.as_ref(),
                    context,
                    false,
                    false,
                    pretty_flags,
                    0,
                )?
            };

            // "CHECK (%s)%s", connoinherit ? " NO INHERIT" : "".
            buf.push_str("CHECK (");
            buf.push_str(consrc.as_str());
            buf.push(')');
            if con.connoinherit {
                buf.push_str(" NO INHERIT");
            }
        }
        x if x == CONSTRAINT_NOTNULL => {
            if con.conrelid != Oid::default() {
                let attnum = extract_not_null_column(info.conkey.as_ref())?;
                let col_name = lsyscache_seams::get_attname::call(
                    mcx,
                    con.conrelid,
                    attnum,
                    false,
                )?
                .ok_or_else(|| {
                    PgError::error(format!(
                        "cache lookup failed for attribute {attnum} of relation {}",
                        con.conrelid
                    ))
                })?;
                let q = quote_identifier(mcx, col_name.as_str())?;
                buf.push_str("NOT NULL ");
                buf.push_str(q.as_str());
                if con.connoinherit {
                    buf.push_str(" NO INHERIT");
                }
            } else if oid_is_valid_pub(con.contypid) {
                // conkey is null for domain not-null constraints
                buf.push_str("NOT NULL");
            }
        }
        x if x == CONSTRAINT_TRIGGER => {
            // There is no ALTER TABLE syntax for this, but print something.
            buf.push_str("TRIGGER");
        }
        x if x == CONSTRAINT_EXCLUSION => {
            // Extract operator OIDs from conexclop, then pg_get_indexdef_worker.
            let conexclop = info
                .conexclop
                .as_ref()
                .ok_or_else(|| PgError::error("conexclop is NULL for EXCLUDE constraint"))?;
            let operators: Vec<Oid> = conexclop.data.clone();

            // suppress tablespace because pg_dump wants it that way
            let body = crate::indexdef::pg_get_indexdef_worker(
                mcx,
                con.conindid,
                0,
                Some(&operators),
                false,
                false,
                false,
                false,
                pretty_flags,
                false,
            )?
            .ok_or_else(|| {
                PgError::error(format!("cache lookup failed for index {}", con.conindid))
            })?;
            buf.push_str(body.as_str());
        }
        other => {
            return Err(PgError::error(format!(
                "invalid constraint type \"{}\"",
                other as u8 as char
            )));
        }
    }

    if con.condeferrable {
        buf.push_str(" DEFERRABLE");
    }
    if con.condeferred {
        buf.push_str(" INITIALLY DEFERRED");
    }

    // Validated status is irrelevant when the constraint is NOT ENFORCED.
    if !con.conenforced {
        buf.push_str(" NOT ENFORCED");
    } else if !con.convalidated {
        buf.push_str(" NOT VALID");
    }

    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}

/// `NameStr(conForm->conname)` as a `&str` (the fixed 64-byte NUL-padded image).
fn conname_str(con: &FormData_pg_constraint) -> &str {
    let bytes = &con.conname;
    let len = bytes.iter().position(|&c| c == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..len]).unwrap_or("")
}

/// `fk action char -> clause word` (ruleutils.c 2316-2366). `Ok(None)`
/// suppresses the default NO ACTION.
fn fk_action_string(action: i8, field: &str) -> PgResult<Option<&'static str>> {
    Ok(match action {
        a if a == FKCONSTR_ACTION_NOACTION => None,
        a if a == FKCONSTR_ACTION_RESTRICT => Some("RESTRICT"),
        a if a == FKCONSTR_ACTION_CASCADE => Some("CASCADE"),
        a if a == FKCONSTR_ACTION_SETNULL => Some("SET NULL"),
        a if a == FKCONSTR_ACTION_SETDEFAULT => Some("SET DEFAULT"),
        other => return Err(PgError::error(format!("unrecognized {field}: {other}"))),
    })
}

/// `decompile_column_index_array(column_index_array, relId, withPeriod, buf)`
/// (ruleutils.c 2620-2647) — append a comma-separated list of the relation's
/// column names for the int16[] key array. Returns the number of keys.
fn decompile_column_index_array<'mcx>(
    mcx: Mcx<'mcx>,
    keys: &ConKeyArray,
    rel_id: Oid,
    with_period: bool,
    buf: &mut String,
) -> PgResult<i32> {
    let n_keys = keys.data.len();
    for (j, &attnum) in keys.data.iter().enumerate() {
        let col_name =
            lsyscache_seams::get_attname::call(mcx, rel_id, attnum, false)?
                .ok_or_else(|| {
                    PgError::error(format!(
                        "cache lookup failed for attribute {attnum} of relation {rel_id}"
                    ))
                })?;
        let q = quote_identifier(mcx, col_name.as_str())?;
        if j == 0 {
            buf.push_str(q.as_str());
        } else {
            buf.push_str(", ");
            if with_period && j == n_keys - 1 {
                buf.push_str("PERIOD ");
            }
            buf.push_str(q.as_str());
        }
    }
    Ok(n_keys as i32)
}

/// `extractNotNullColumn(tup)` (pg_constraint.c) — the single attnum a NOT NULL
/// constraint's `conkey` carries (a 1-D smallint array of length 1).
fn extract_not_null_column(conkey: Option<&ConKeyArray>) -> PgResult<i16> {
    let arr =
        conkey.ok_or_else(|| PgError::error("conkey is NULL for NOT NULL constraint"))?;
    if arr.ndim != 1 || arr.hasnull || arr.elemtype != INT2OID || arr.dim0 != 1 {
        return Err(PgError::error("conkey is not a 1-D smallint array"));
    }
    Ok(arr.data[0])
}

/// `generate_qualified_relation_name(relid)` (ruleutils.c 13213) — the
/// always-schema-qualified, quoted relation name.
pub(crate) fn generate_qualified_relation_name<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<PgString<'mcx>> {
    generate_relation_name_catalog(mcx, relid, true)
}

/// `generate_qualified_type_name(typid)` (ruleutils.c 13510) — the
/// schema-qualified, quoted type name (built from the `pg_type` namespace/name,
/// not `format_type_be`, mirroring the C).
fn generate_qualified_type_name<'mcx>(mcx: Mcx<'mcx>, typid: Oid) -> PgResult<PgString<'mcx>> {
    let nm = syscache_seams::type_namespace_and_name::call(mcx, typid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for type {typid}")))?;
    let nspname = lsyscache_seams::get_namespace_name_or_temp::call(
        mcx,
        nm.namespace,
    )?
    .ok_or_else(|| PgError::error(format!("cache lookup failed for namespace {}", nm.namespace)))?;
    quote_qualified_identifier(mcx, Some(nspname.as_str()), nm.name.as_str())
}
