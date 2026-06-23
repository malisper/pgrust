//! `utils/adt/ruleutils.c` — the trigger-definition deparser
//! (`pg_get_triggerdef` / `pg_get_triggerdef_ext`, the
//! `pg_get_triggerdef_worker` body, ruleutils.c 870-1163).
//!
//! The worker reverse-lists a `pg_trigger` row into a `CREATE [CONSTRAINT]
//! TRIGGER … {BEFORE|AFTER|INSTEAD OF} {events} ON tbl [FROM reftbl]
//! [NOT DEFERRABLE…] [REFERENCING …] FOR EACH {ROW|STATEMENT} [WHEN (…)]
//! EXECUTE FUNCTION fn(args)` statement. It reads the trigger tuple by OID
//! (there is no `pg_trigger` syscache, so the read is the `trigger_by_oid` genam
//! projection — `systable_beginscan(TriggerOidIndexId)`), then renders names
//! through `generate_relation_name` / `generate_function_name`, the WHEN
//! qualification through the ported `get_rule_expr` engine over an `old`/`new`
//! deparse namespace, and the argument literals through `simple_quote_literal`.

use alloc::format;
use alloc::string::String;
use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};

use crate::{
    deparse_context_for_old_new, deparse_expression_pretty, generate_function_name_catalog,
    generate_relation_name_catalog, get_pretty_flags_pub, oid_is_valid_pub, quote_identifier,
    simple_quote_literal_into_pub,
};

/// `TRIGGER_TYPE_*` bits (`catalog/pg_trigger.h` / `commands/trigger.h`).
const TRIGGER_TYPE_ROW: i16 = 1 << 0;
const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;
const TRIGGER_TYPE_TRUNCATE: i16 = 1 << 5;
const TRIGGER_TYPE_INSTEAD: i16 = 1 << 6;

/// `TRIGGER_TYPE_TIMING_MASK` (`commands/trigger.h`).
const TRIGGER_TYPE_TIMING_MASK: i16 = TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSTEAD;

#[inline]
fn trigger_for_row(t: i16) -> bool {
    t & TRIGGER_TYPE_ROW != 0
}
#[inline]
fn trigger_for_before(t: i16) -> bool {
    t & TRIGGER_TYPE_TIMING_MASK == TRIGGER_TYPE_BEFORE
}
#[inline]
fn trigger_for_after(t: i16) -> bool {
    t & TRIGGER_TYPE_TIMING_MASK == 0
}
#[inline]
fn trigger_for_instead(t: i16) -> bool {
    t & TRIGGER_TYPE_TIMING_MASK == TRIGGER_TYPE_INSTEAD
}
#[inline]
fn trigger_for_insert(t: i16) -> bool {
    t & TRIGGER_TYPE_INSERT != 0
}
#[inline]
fn trigger_for_delete(t: i16) -> bool {
    t & TRIGGER_TYPE_DELETE != 0
}
#[inline]
fn trigger_for_update(t: i16) -> bool {
    t & TRIGGER_TYPE_UPDATE != 0
}
#[inline]
fn trigger_for_truncate(t: i16) -> bool {
    t & TRIGGER_TYPE_TRUNCATE != 0
}

/// `pg_get_triggerdef_worker(trigid, pretty)` (ruleutils.c 899-1163). The
/// SQL-callable `pg_get_triggerdef` passes `pretty = false`,
/// `pg_get_triggerdef_ext` threads the flag. Returns the trigger definition
/// text, or `Ok(None)` when the trigger is gone (both fmgr callers map that to
/// `PG_RETURN_NULL`).
pub fn pg_get_triggerdef_worker<'mcx>(
    mcx: Mcx<'mcx>,
    trigid: Oid,
    pretty: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    // ht_trig = systable scan TriggerOidIndexId by trigid (no pg_trigger
    // syscache). The projection folds the GETSTRUCT scalars + the four
    // variable-length columns (tgattr / tgargs / tgqual / tgoldtable /
    // tgnewtable).
    let trig = match genam_seams::trigger_by_oid::call(mcx, trigid)? {
        Some(t) => t,
        None => return Ok(None),
    };

    let mut buf = String::new();

    // "CREATE %sTRIGGER %s ", OidIsValid(tgconstraint) ? "CONSTRAINT " : "".
    // The trigger's name is never schema-qualified.
    buf.push_str("CREATE ");
    if oid_is_valid_pub(trig.tgconstraint) {
        buf.push_str("CONSTRAINT ");
    }
    buf.push_str("TRIGGER ");
    let qname = quote_identifier(mcx, trig.tgname.as_str())?;
    buf.push_str(qname.as_str());
    buf.push(' ');

    if trigger_for_before(trig.tgtype) {
        buf.push_str("BEFORE");
    } else if trigger_for_after(trig.tgtype) {
        buf.push_str("AFTER");
    } else if trigger_for_instead(trig.tgtype) {
        buf.push_str("INSTEAD OF");
    } else {
        return Err(PgError::error(format!(
            "unexpected tgtype value: {}",
            trig.tgtype
        )));
    }

    let mut findx = 0;
    if trigger_for_insert(trig.tgtype) {
        buf.push_str(" INSERT");
        findx += 1;
    }
    if trigger_for_delete(trig.tgtype) {
        if findx > 0 {
            buf.push_str(" OR DELETE");
        } else {
            buf.push_str(" DELETE");
        }
        findx += 1;
    }
    if trigger_for_update(trig.tgtype) {
        if findx > 0 {
            buf.push_str(" OR UPDATE");
        } else {
            buf.push_str(" UPDATE");
        }
        findx += 1;
        // UPDATE OF column list (tgattr).
        if !trig.tgattr.is_empty() {
            buf.push_str(" OF ");
            for (i, &attnum) in trig.tgattr.iter().enumerate() {
                if i > 0 {
                    buf.push_str(", ");
                }
                let attname = lsyscache_seams::get_attname::call(
                    mcx,
                    trig.tgrelid,
                    attnum,
                    false,
                )?
                .ok_or_else(|| {
                    PgError::error(format!(
                        "cache lookup failed for attribute {attnum} of relation {}",
                        trig.tgrelid
                    ))
                })?;
                let q = quote_identifier(mcx, attname.as_str())?;
                buf.push_str(q.as_str());
            }
        }
    }
    if trigger_for_truncate(trig.tgtype) {
        if findx > 0 {
            buf.push_str(" OR TRUNCATE");
        } else {
            buf.push_str(" TRUNCATE");
        }
        // findx += 1;  (unused after this point, as in C)
    }

    // " ON %s ": non-pretty always schema-qualifies for safety; pretty only when
    // not visible.
    let tblname = generate_relation_name_catalog(mcx, trig.tgrelid, !pretty)?;
    buf.push_str(" ON ");
    buf.push_str(tblname.as_str());
    buf.push(' ');

    if oid_is_valid_pub(trig.tgconstraint) {
        if oid_is_valid_pub(trig.tgconstrrelid) {
            // FROM <referenced relation> (search-path-qualified only if needed).
            let frel = generate_relation_name_catalog(mcx, trig.tgconstrrelid, false)?;
            buf.push_str("FROM ");
            buf.push_str(frel.as_str());
            buf.push(' ');
        }
        if !trig.tgdeferrable {
            buf.push_str("NOT ");
        }
        buf.push_str("DEFERRABLE INITIALLY ");
        if trig.tginitdeferred {
            buf.push_str("DEFERRED ");
        } else {
            buf.push_str("IMMEDIATE ");
        }
    }

    // REFERENCING OLD/NEW TABLE AS ... (transition tables).
    if trig.tgoldtable.is_some() || trig.tgnewtable.is_some() {
        buf.push_str("REFERENCING ");
        if let Some(oldt) = trig.tgoldtable.as_ref() {
            let q = quote_identifier(mcx, oldt.as_str())?;
            buf.push_str("OLD TABLE AS ");
            buf.push_str(q.as_str());
            buf.push(' ');
        }
        if let Some(newt) = trig.tgnewtable.as_ref() {
            let q = quote_identifier(mcx, newt.as_str())?;
            buf.push_str("NEW TABLE AS ");
            buf.push_str(q.as_str());
            buf.push(' ');
        }
    }

    if trigger_for_row(trig.tgtype) {
        buf.push_str("FOR EACH ROW ");
    } else {
        buf.push_str("FOR EACH STATEMENT ");
    }

    // WHEN qualification (tgqual).
    if let Some(qual_text) = trig.tgqual.as_ref() {
        buf.push_str("WHEN (");

        let qual = read_seams::string_to_node::call(mcx, qual_text)?;
        let relkind = lsyscache_seams::get_rel_relkind::call(trig.tgrelid)?;

        // Build the one-deep old/new namespace stack, then get_rule_expr with
        // varprefix = true (so Vars print as old.col / new.col).
        let context = deparse_context_for_old_new(mcx, trig.tgrelid, relkind as i8)?;
        let s = deparse_expression_pretty(
            mcx,
            qual.as_ref(),
            context,
            /* forceprefix = */ true,
            /* showimplicit = */ false,
            get_pretty_flags_pub(pretty),
            0,
        )?;
        buf.push_str(s.as_str());

        buf.push_str(") ");
    }

    // EXECUTE FUNCTION fn(args).
    let (fname, _use_variadic) = generate_function_name_catalog(
        mcx,
        trig.tgfoid,
        0,
        mcx::PgVec::new_in(mcx),
        mcx::PgVec::new_in(mcx),
        false,
        false,
        false,
    )?;
    buf.push_str("EXECUTE FUNCTION ");
    buf.push_str(fname.as_str());
    buf.push('(');

    if trig.tgnargs > 0 {
        // tgargs is already split into the tgnargs textual arguments.
        for (i, arg) in trig.tgargs.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            simple_quote_literal_into_pub(&mut buf, arg.as_str());
        }
    }

    // We deliberately do not put a semicolon at the end.
    buf.push(')');

    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}
