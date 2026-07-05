// ATExecAddConstraint FK slice (tablecmds.c): ATAddForeignKeyConstraint +
// addFkConstraint/addFkRecurseReferenced/addFkRecurseReferencing +
// createForeignKey{Action,Check}Triggers + validateForeignKeyConstraint,
// plain-table lane, all MATCH types and ON DELETE/UPDATE actions. LOUD:
// PERIOD, partitioned rels, old_conpfeqop re-add lane.

use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, INDEX_MAX_KEYS};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_INVALID_FOREIGN_KEY, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::rawnodes::{
    Constraint, FKCONSTR_ACTION_CASCADE, FKCONSTR_ACTION_NOACTION, FKCONSTR_ACTION_RESTRICT,
    FKCONSTR_ACTION_SETDEFAULT, FKCONSTR_ACTION_SETNULL,
};
use types_nodes::NodeList;
use types_core::catalog::{RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED};
use types_rel::{NoLock, Relation, ShareRowExclusiveLock, RELKIND_RELATION};
use types_trigger::{TRIGGER_TYPE_DELETE, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_ROW,
    TRIGGER_TYPE_UPDATE};

const F_RI_FKEY_CHECK_INS: Oid = 1644;
const F_RI_FKEY_CHECK_UPD: Oid = 1645;
const F_RI_FKEY_CASCADE_DEL: Oid = 1646;
const F_RI_FKEY_CASCADE_UPD: Oid = 1647;
const F_RI_FKEY_RESTRICT_DEL: Oid = 1648;
const F_RI_FKEY_RESTRICT_UPD: Oid = 1649;
const F_RI_FKEY_SETNULL_DEL: Oid = 1650;
const F_RI_FKEY_SETNULL_UPD: Oid = 1651;
const F_RI_FKEY_SETDEFAULT_DEL: Oid = 1652;
const F_RI_FKEY_SETDEFAULT_UPD: Oid = 1653;
const F_RI_FKEY_NOACTION_DEL: Oid = 1654;
const F_RI_FKEY_NOACTION_UPD: Oid = 1655;

const BTREE_AM_OID: Oid = 403;


#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: tablecmds FK {what}")
}

#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

// NewConstraint CONSTR_FOREIGN fields (tablecmds.c), for the Phase-3 pass.
pub(crate) struct FkValidateItem<'mcx> {
    pub conname: &'mcx str,
    pub refrelid: Oid,
    pub refindid: Oid,
    pub conid: Oid,
}

pub(crate) fn ATExecAddConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    constraint: &Constraint<'mcx>,
) -> PgResult<Option<FkValidateItem<'mcx>>> {
    use types_nodes::rawnodes::ConstrType;
    match constraint.contype {
        ConstrType::CONSTR_FOREIGN => {}
        other => unported(&format!("ATExecAddConstraint {other:?} (CHECK/NOT NULL ALTER lane)")),
    }

    let relname = rel.name();
    let conname_storage;
    let conname: &str = match constraint.conname {
        Some(n) => {
            if constraint_name_is_used(mcx, rel.rd_id, n)? {
                return Err(err(
                    format!("constraint \"{n}\" for relation \"{relname}\" already exists"),
                    ERRCODE_DUPLICATE_OBJECT,
                ));
            }
            n
        }
        None => {
            let addition = choose_fkey_constraint_name_addition(mcx, &constraint.fk_attrs)?;
            conname_storage = pg_constraint::ChooseConstraintName(
                mcx,
                relname,
                Some(addition.as_str()),
                "fkey",
                rel.rd_rel.relnamespace,
                &[],
            )?;
            conname_storage.as_str()
        }
    };

    at_add_foreign_key_constraint(mcx, rel, constraint, conname)
}

// ATAddForeignKeyConstraint (tablecmds.c).
fn at_add_foreign_key_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    fkconstraint: &Constraint<'mcx>,
    conname: &str,
) -> PgResult<Option<FkValidateItem<'mcx>>> {
    if !fkconstraint.old_conpfeqop.is_nil() {
        unported("old_conpfeqop revalidation skip (TryReuseForeignKey)");
    }
    if fkconstraint.fk_with_period || fkconstraint.pk_with_period {
        unported("PERIOD (temporal FK)");
    }

    // A re-added constraint targets the pktable by OID: concurrent activity
    // could have made the deparsed name resolve differently.
    let pkrel = if fkconstraint.old_pktable_oid != InvalidOid {
        table::table_open(mcx, fkconstraint.old_pktable_oid, ShareRowExclusiveLock)?
    } else {
        let pktable = fkconstraint.pktable.expect("FK constraint without pktable");
        let pkrv = rel_vocab::RangeVar {
            catalogname: pktable.catalogname,
            schemaname: pktable.schemaname,
            relname: pktable.relname.expect("RangeVar.relname"),
            inh: pktable.inh,
            relpersistence: pktable.relpersistence,
            location: pktable.location,
        };
        table::table_openrv(mcx, &pkrv, ShareRowExclusiveLock)?
    };

    if pkrel.rd_rel.relkind != RELKIND_RELATION {
        let e = err(
            format!("referenced relation \"{}\" is not a table", pkrel.name()),
            ERRCODE_WRONG_OBJECT_TYPE,
        );
        pkrel.close(NoLock)?;
        return Err(e);
    }
    if !init_small::globals::allowSystemTableMods() && catalog::IsSystemRelation(&pkrel) {
        let e = err(
            format!("permission denied: \"{}\" is a system catalog", pkrel.name()),
            types_error::ERRCODE_INSUFFICIENT_PRIVILEGE,
        );
        pkrel.close(NoLock)?;
        return Err(e);
    }

    let persistence_err = match rel.rd_rel.relpersistence {
        RELPERSISTENCE_PERMANENT => (pkrel.rd_rel.relpersistence != RELPERSISTENCE_PERMANENT)
            .then_some("constraints on permanent tables may reference only permanent tables"),
        RELPERSISTENCE_UNLOGGED => (pkrel.rd_rel.relpersistence != RELPERSISTENCE_PERMANENT
            && pkrel.rd_rel.relpersistence != RELPERSISTENCE_UNLOGGED)
            .then_some(
                "constraints on unlogged tables may reference only permanent or unlogged tables",
            ),
        RELPERSISTENCE_TEMP => {
            if pkrel.rd_rel.relpersistence != RELPERSISTENCE_TEMP {
                Some("constraints on temporary tables may reference only temporary tables")
            } else if !pkrel.rd_islocaltemp || !rel.rd_islocaltemp {
                Some("constraints on temporary tables must involve temporary tables of this session")
            } else {
                None
            }
        }
        _ => None,
    };
    if let Some(msg) = persistence_err {
        let e = err(msg.into(), ERRCODE_INVALID_TABLE_DEFINITION);
        pkrel.close(NoLock)?;
        return Err(e);
    }

    let mut fkattnum = [0i16; INDEX_MAX_KEYS as usize];
    let mut fktypoid = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut fkcolloid = [InvalidOid; INDEX_MAX_KEYS as usize];
    let numfks = transform_column_name_list(
        rel,
        &fkconstraint.fk_attrs,
        &mut fkattnum,
        Some(&mut fktypoid),
        Some(&mut fkcolloid),
    )?;

    let mut fkdelsetcols = [0i16; INDEX_MAX_KEYS as usize];
    let numfkdelsetcols = transform_column_name_list(
        rel,
        &fkconstraint.fk_del_set_cols,
        &mut fkdelsetcols,
        None,
        None,
    )?;
    let numfkdelsetcols = validate_fk_on_delete_set_columns(
        numfks,
        &fkattnum,
        numfkdelsetcols,
        &mut fkdelsetcols,
        &fkconstraint.fk_del_set_cols,
    )?;

    let mut pkattnum = [0i16; INDEX_MAX_KEYS as usize];
    let mut pktypoid = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut pkcolloid = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut opclasses = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut pk_attnames: mcx::PgVec<'mcx, &'mcx str> = mcx::PgVec::new_in(mcx);

    let (numpks, index_oid) = if fkconstraint.pk_attrs.is_nil() {
        transform_fkey_get_primary_key(
            mcx,
            &pkrel,
            &mut pk_attnames,
            &mut pkattnum,
            &mut pktypoid,
            &mut pkcolloid,
            &mut opclasses,
        )?
    } else {
        let n = transform_column_name_list(
            &pkrel,
            &fkconstraint.pk_attrs,
            &mut pkattnum,
            Some(&mut pktypoid),
            Some(&mut pkcolloid),
        )?;
        for a in fkconstraint.pk_attrs.iter() {
            pk_attnames.push(a.as_string().expect("pk_attrs String").sval);
        }
        let idx = transform_fkey_check_attrs(mcx, &pkrel, n, &pkattnum, &mut opclasses)?;
        (n, idx)
    };

    checkFkeyPermissions(&pkrel, &pkattnum[..numpks])?;

    for i in 0..numfks {
        let attgenerated = rel.rd_att.attr(fkattnum[i] as usize - 1).attgenerated;
        if attgenerated != 0 {
            // SQL-standard restrictions on UPDATE/DELETE actions.
            if fkconstraint.fk_upd_action == FKCONSTR_ACTION_SETNULL
                || fkconstraint.fk_upd_action == FKCONSTR_ACTION_SETDEFAULT
                || fkconstraint.fk_upd_action == FKCONSTR_ACTION_CASCADE
            {
                let e = err(
                    "invalid ON UPDATE action for foreign key constraint containing \
                     generated column"
                        .into(),
                    types_error::ERRCODE_SYNTAX_ERROR,
                );
                pkrel.close(NoLock)?;
                return Err(e);
            }
            if fkconstraint.fk_del_action == FKCONSTR_ACTION_SETNULL
                || fkconstraint.fk_del_action == FKCONSTR_ACTION_SETDEFAULT
            {
                let e = err(
                    "invalid ON DELETE action for foreign key constraint containing \
                     generated column"
                        .into(),
                    types_error::ERRCODE_SYNTAX_ERROR,
                );
                pkrel.close(NoLock)?;
                return Err(e);
            }
        }
        if attgenerated == b'v' as i8 {
            let e = err(
                "foreign key constraints on virtual generated columns are not supported"
                    .into(),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            );
            pkrel.close(NoLock)?;
            return Err(e);
        }
    }

    if numfks != numpks {
        let e = err(
            "number of referencing and referenced columns for foreign key disagree".into(),
            ERRCODE_INVALID_FOREIGN_KEY,
        );
        pkrel.close(NoLock)?;
        return Err(e);
    }

    let mut pfeqoperators = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut ppeqoperators = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut ffeqoperators = [InvalidOid; INDEX_MAX_KEYS as usize];
    for i in 0..numpks {
        let pktype = pktypoid[i];
        let fktype = fktypoid[i];
        let pkcoll = pkcolloid[i];
        let fkcoll = fkcolloid[i];

        let amid = lsyscache::get_opclass_method(opclasses[i])?;
        let (opfamily, opcintype) = lsyscache::get_opclass_opfamily_and_input_type(opclasses[i])?
            .unwrap_or_else(|| panic!("cache lookup failed for opclass {}", opclasses[i]));

        if amid != BTREE_AM_OID {
            unported("non-btree FK support index (IndexAmTranslateCompareType)");
        }
        let eqstrategy: i16 = 3;

        let ppeqop = lsyscache::get_opfamily_member(opfamily, opcintype, opcintype, eqstrategy)?;
        if ppeqop == InvalidOid {
            panic!(
                "missing operator {eqstrategy}({opcintype},{opcintype}) in opfamily {opfamily}"
            );
        }

        let fktyped = lsyscache::getBaseType(fktype)?;
        let mut pfeqop = lsyscache::get_opfamily_member(opfamily, opcintype, fktyped, eqstrategy)?;
        let mut ffeqop = if pfeqop != InvalidOid {
            lsyscache::get_opfamily_member(opfamily, fktyped, fktyped, eqstrategy)?
        } else {
            InvalidOid
        };
        if pfeqop == InvalidOid || ffeqop == InvalidOid {
            let input_typeids = [pktype, fktype];
            let target_typeids = [opcintype, opcintype];
            if coerce::can_coerce_type(
                &input_typeids,
                &target_typeids,
                coerce::CoercionContext::COERCION_IMPLICIT,
            )? {
                pfeqop = ppeqop;
                ffeqop = ppeqop;
            }
        }
        if pfeqop == InvalidOid || ffeqop == InvalidOid {
            let fk_attname = fkconstraint
                .fk_attrs
                .nth(i)
                .as_string()
                .expect("fk_attrs String")
                .sval;
            let e = err(
                format!(
                    "foreign key constraint \"{conname}\" cannot be implemented"
                ),
                ERRCODE_DATATYPE_MISMATCH,
            );
            let e = Box::new((*e).with_detail(format!(
                "Key columns \"{fk_attname}\" of the referencing table and \"{}\" of the \
                 referenced table are of incompatible types: {} and {}.",
                pk_attnames[i],
                format_type::format_type_be(fktype)?,
                format_type::format_type_be(pktype)?,
            )));
            pkrel.close(NoLock)?;
            return Err(e);
        }

        if (pkcoll != InvalidOid) != (fkcoll != InvalidOid) {
            panic!("key columns are not both collatable");
        }
        if pkcoll != InvalidOid && fkcoll != InvalidOid {
            let pkcolldet = lsyscache::get_collation_isdeterministic(pkcoll)?;
            let fkcolldet = lsyscache::get_collation_isdeterministic(fkcoll)?;
            if (!pkcolldet || !fkcolldet) && pkcoll != fkcoll {
                let fk_attname = fkconstraint
                    .fk_attrs
                    .nth(i)
                    .as_string()
                    .expect("fk_attrs String")
                    .sval;
                let fkcollname = lsyscache::get_collation_name(mcx, fkcoll)?
                    .unwrap_or_else(|| panic!("cache lookup failed for collation {fkcoll}"));
                let pkcollname = lsyscache::get_collation_name(mcx, pkcoll)?
                    .unwrap_or_else(|| panic!("cache lookup failed for collation {pkcoll}"));
                let e = err(
                    format!("foreign key constraint \"{conname}\" cannot be implemented"),
                    types_error::ERRCODE_COLLATION_MISMATCH,
                );
                let e = Box::new((*e).with_detail(format!(
                    "Key columns \"{fk_attname}\" of the referencing table and \"{}\" of the \
                     referenced table have incompatible collations: \"{}\" and \"{}\".  \
                     If either collation is nondeterministic, then both collations have to be \
                     the same.",
                    pk_attnames[i],
                    fkcollname.as_str(),
                    pkcollname.as_str(),
                )));
                pkrel.close(NoLock)?;
                return Err(e);
            }
        }

        pfeqoperators[i] = pfeqop;
        ppeqoperators[i] = ppeqop;
        ffeqoperators[i] = ffeqop;
    }

    let constr_oid = add_fk_constraint(
        mcx,
        conname,
        fkconstraint,
        rel,
        &pkrel,
        index_oid,
        numfks,
        &pkattnum,
        &fkattnum,
        &pfeqoperators,
        &ppeqoperators,
        &ffeqoperators,
        &fkdelsetcols[..numfkdelsetcols],
    )?;

    if fkconstraint.is_enforced {
        create_foreign_key_action_triggers(mcx, rel.rd_id, pkrel.rd_id, fkconstraint, constr_oid, index_oid)?;
        create_foreign_key_check_triggers(mcx, rel.rd_id, pkrel.rd_id, fkconstraint, constr_oid, index_oid)?;
    }

    let validate = if !fkconstraint.skip_validation && fkconstraint.is_enforced {
        debug_assert!(rel.rd_rel.relkind == RELKIND_RELATION);
        Some(FkValidateItem {
            conname: str_in(mcx, conname)?,
            refrelid: pkrel.rd_id,
            refindid: index_oid,
            conid: constr_oid,
        })
    } else {
        None
    };

    pkrel.close(NoLock)?;
    Ok(validate)
}

// validateFkOnDeleteSetColumns (tablecmds.c); dedups in place.
fn validate_fk_on_delete_set_columns(
    numfks: usize,
    fkattnums: &[i16],
    numfksetcols: usize,
    fksetcolsattnums: &mut [i16],
    fksetcols: &NodeList<'_>,
) -> PgResult<usize> {
    let mut numcolsout = 0usize;
    for i in 0..numfksetcols {
        let setcol_attnum = fksetcolsattnums[i];
        if !fkattnums[..numfks].contains(&setcol_attnum) {
            let col = fksetcols.nth(i).as_string().expect("set col String").sval;
            return Err(err(
                format!(
                    "column \"{col}\" referenced in ON DELETE SET action must be part of foreign key"
                ),
                types_error::ERRCODE_INVALID_COLUMN_REFERENCE,
            ));
        }
        if !fksetcolsattnums[..numcolsout].contains(&setcol_attnum) {
            fksetcolsattnums[numcolsout] = setcol_attnum;
            numcolsout += 1;
        }
    }
    Ok(numcolsout)
}

// validateForeignKeyConstraint (tablecmds.c): Phase-3 whole-table check.
pub(crate) fn validate_foreign_key_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    item: &FkValidateItem<'mcx>,
) -> PgResult<()> {
    let pkrel = table::table_open(mcx, item.refrelid, types_rel::RowShareLock)?;

    let trig = types_trigger::Trigger {
        tgoid: InvalidOid,
        tgname: mcx::PgString::from_str_in(item.conname, mcx)?,
        tgfoid: InvalidOid,
        tgtype: 0,
        tgenabled: types_trigger::TRIGGER_FIRES_ON_ORIGIN,
        tgisinternal: true,
        tgisclone: false,
        tgconstrrelid: pkrel.rd_id,
        tgconstrindid: item.refindid,
        tgconstraint: item.conid,
        tgdeferrable: false,
        tginitdeferred: false,
        tgnargs: 0,
        tgnattr: 0,
        tgattr: mcx::PgVec::new_in(mcx),
        tgargs: mcx::PgVec::new_in(mcx),
        tgqual: None,
        tgoldtable: None,
        tgnewtable: None,
    };

    if ri_triggers_seams::ri_initial_check::call(mcx, &trig, rel, &pkrel)? {
        return pkrel.close(types_rel::NoLock);
    }

    let snap = snapmgr::GetLatestSnapshot()?;
    let snap = snapmgr::RegisterSnapshot(Some(&snap))?.expect("registered snapshot");
    let mut scan = tableam::table_beginscan(
        mcx,
        rel,
        Some(snap.clone()),
        0,
        mcx::PgVec::new_in(mcx),
    )?;
    {
        let tableam::TableScanDesc::Heap(hscan) = &mut scan;
        while let Some(tup) = heapam::heap_getnext(
            hscan,
            types_scan::ScanDirection::ForwardScanDirection,
        )? {
            let data = ri_triggers_seams::RiTriggerData {
                tg_event: types_trigger::TRIGGER_EVENT_INSERT | types_trigger::TRIGGER_EVENT_ROW,
                tg_relation: rel,
                tg_trigtuple: tup,
                tg_newtuple: None,
                tg_trigger: &trig,
            };
            ri_triggers_seams::ri_fkey_trigger::call(mcx, F_RI_FKEY_CHECK_INS, &data)?;
        }
    }
    tableam::table_endscan(scan)?;
    snapmgr::UnregisterSnapshot(Some(&snap));
    pkrel.close(types_rel::NoLock)
}

// transformColumnNameList (tablecmds.c) over the open relation's descriptor
// (C probes the ATTNAME syscache).
fn transform_column_name_list(
    rel: &Relation<'_>,
    col_list: &NodeList<'_>,
    attnums: &mut [i16],
    mut atttypids: Option<&mut [Oid]>,
    mut attcollids: Option<&mut [Oid]>,
) -> PgResult<usize> {
    let mut attnum = 0usize;
    for l in col_list.iter() {
        let attname = l.as_string().expect("column name String").sval;
        let desc = &rel.rd_att;
        let mut found = None;
        for i in 0..desc.natts as usize {
            let att = desc.attr(i);
            if !att.attisdropped && att.attname.name_str() == attname.as_bytes() {
                found = Some(att);
                break;
            }
        }
        let Some(att) = found else {
            return Err(err(
                format!(
                    "column \"{attname}\" referenced in foreign key constraint does not exist"
                ),
                ERRCODE_UNDEFINED_COLUMN,
            ));
        };
        if attnum >= INDEX_MAX_KEYS as usize {
            return Err(err(
                format!("cannot have more than {INDEX_MAX_KEYS} keys in a foreign key"),
                ERRCODE_TOO_MANY_COLUMNS,
            ));
        }
        attnums[attnum] = att.attnum;
        if let Some(t) = atttypids.as_deref_mut() {
            t[attnum] = att.atttypid;
        }
        if let Some(c) = attcollids.as_deref_mut() {
            c[attnum] = att.attcollation;
        }
        attnum += 1;
    }
    Ok(attnum)
}

struct PgIndexFkShape {
    indnkeyatts: i16,
    indisunique: bool,
    indisprimary: bool,
    indimmediate: bool,
    indisvalid: bool,
    indkey: [i16; INDEX_MAX_KEYS as usize],
    indclass: [Oid; INDEX_MAX_KEYS as usize],
    has_exprs_or_pred: bool,
}

fn fetch_pg_index_fk_shape(indexoid: Oid) -> PgResult<PgIndexFkShape> {
    use cache_syscache::{SearchSysCache1, SysCacheGetAttr, SysCacheKey, INDEXRELID};
    use datum::Datum;
    const Anum_indnkeyatts: i32 = 4;
    const Anum_indisunique: i32 = 5;
    const Anum_indisprimary: i32 = 7;
    const Anum_indimmediate: i32 = 9;
    const Anum_indisvalid: i32 = 11;
    const Anum_indkey: i32 = 16;
    const Anum_indclass: i32 = 18;
    const Anum_indexprs: i32 = 20;
    const Anum_indpred: i32 = 21;

    let tup = SearchSysCache1(INDEXRELID, SysCacheKey::Value(Datum::from_oid(indexoid)))?
        .unwrap_or_else(|| panic!("cache lookup failed for index {indexoid}"));
    let get = |attno: i32| -> PgResult<(Datum, bool)> { SysCacheGetAttr(INDEXRELID, &tup, attno) };
    let req = |attno: i32| -> PgResult<Datum> {
        let (d, isnull) = get(attno)?;
        assert!(!isnull, "unexpected null pg_index attr {attno} for {indexoid}");
        Ok(d)
    };
    let mut shape = PgIndexFkShape {
        indnkeyatts: req(Anum_indnkeyatts)?.as_i16(),
        indisunique: req(Anum_indisunique)?.as_bool(),
        indisprimary: req(Anum_indisprimary)?.as_bool(),
        indimmediate: req(Anum_indimmediate)?.as_bool(),
        indisvalid: req(Anum_indisvalid)?.as_bool(),
        indkey: [0; INDEX_MAX_KEYS as usize],
        indclass: [InvalidOid; INDEX_MAX_KEYS as usize],
        has_exprs_or_pred: !get(Anum_indexprs)?.1 || !get(Anum_indpred)?.1,
    };
    let nkeys = shape.indnkeyatts as usize;
    // SAFETY: not-null plain-storage vector columns of the held syscache
    // tuple (relcache_build precedent).
    unsafe {
        let kd = req(Anum_indkey)?;
        let kp = kd.as_usize() as *const array::int2vector;
        shape.indkey[..nkeys]
            .copy_from_slice(core::slice::from_raw_parts(kp.add(1) as *const i16, nkeys));
        let cd = req(Anum_indclass)?;
        let cp = cd.as_usize() as *const array::oidvector;
        shape.indclass[..nkeys]
            .copy_from_slice(core::slice::from_raw_parts(cp.add(1) as *const Oid, nkeys));
    }
    Ok(shape)
}

// transformFkeyGetPrimaryKey (tablecmds.c).
fn transform_fkey_get_primary_key<'mcx>(
    mcx: Mcx<'mcx>,
    pkrel: &Relation<'mcx>,
    pk_attnames: &mut mcx::PgVec<'mcx, &'mcx str>,
    attnums: &mut [i16],
    atttypids: &mut [Oid],
    attcollids: &mut [Oid],
    opclasses: &mut [Oid],
) -> PgResult<(usize, Oid)> {
    let indexes = relcache::RelationGetIndexList(mcx, pkrel.rd_id)?;
    let mut found: Option<(Oid, PgIndexFkShape)> = None;
    for &indexoid in indexes.iter() {
        let shape = fetch_pg_index_fk_shape(indexoid)?;
        if shape.indisprimary && shape.indisvalid {
            if !shape.indimmediate {
                return Err(err(
                    format!(
                        "cannot use a deferrable primary key for referenced table \"{}\"",
                        pkrel.name()
                    ),
                    types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                ));
            }
            found = Some((indexoid, shape));
            break;
        }
    }
    let Some((indexoid, shape)) = found else {
        return Err(err(
            format!(
                "there is no primary key for referenced table \"{}\"",
                pkrel.name()
            ),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };
    let n = shape.indnkeyatts as usize;
    for i in 0..n {
        let pkattno = shape.indkey[i];
        let att = pkrel.rd_att.attr(pkattno as usize - 1);
        attnums[i] = pkattno;
        atttypids[i] = att.atttypid;
        attcollids[i] = att.attcollation;
        opclasses[i] = shape.indclass[i];
        let name = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
        pk_attnames.push(str_in(mcx, name)?);
    }
    Ok((n, indexoid))
}

// transformFkeyCheckAttrs (tablecmds.c); the 42830 no-matching-unique check.
fn transform_fkey_check_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    pkrel: &Relation<'mcx>,
    numattrs: usize,
    attnums: &[i16],
    opclasses: &mut [Oid],
) -> PgResult<Oid> {
    for i in 0..numattrs {
        for j in i + 1..numattrs {
            if attnums[i] == attnums[j] {
                return Err(err(
                    "foreign key referenced-columns list must not contain duplicates".into(),
                    ERRCODE_INVALID_FOREIGN_KEY,
                ));
            }
        }
    }
    let indexes = relcache::RelationGetIndexList(mcx, pkrel.rd_id)?;
    let mut found_deferrable = false;
    for &indexoid in indexes.iter() {
        let shape = fetch_pg_index_fk_shape(indexoid)?;
        if shape.indnkeyatts as usize == numattrs
            && shape.indisunique
            && shape.indisvalid
            && !shape.has_exprs_or_pred
        {
            let mut found = true;
            for i in 0..numattrs {
                let mut this_found = false;
                for j in 0..numattrs {
                    if attnums[i] == shape.indkey[j] {
                        opclasses[i] = shape.indclass[j];
                        this_found = true;
                        break;
                    }
                }
                if !this_found {
                    found = false;
                    break;
                }
            }
            if found && !shape.indimmediate {
                found_deferrable = true;
                found = false;
            }
            if found {
                return Ok(indexoid);
            }
        }
    }
    if found_deferrable {
        return Err(err(
            format!(
                "cannot use a deferrable unique constraint for referenced table \"{}\"",
                pkrel.name()
            ),
            types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
        ));
    }
    Err(err(
        format!(
            "there is no unique constraint matching given keys for referenced table \"{}\"",
            pkrel.name()
        ),
        ERRCODE_INVALID_FOREIGN_KEY,
    ))
}

// addFkConstraint (tablecmds.c), addFkBothSides top-level arm.
#[allow(clippy::too_many_arguments)]
fn add_fk_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    conname: &str,
    fkconstraint: &Constraint<'mcx>,
    rel: &Relation<'mcx>,
    pkrel: &Relation<'mcx>,
    index_oid: Oid,
    numfks: usize,
    pkattnum: &[i16],
    fkattnum: &[i16],
    pfeqoperators: &[Oid],
    ppeqoperators: &[Oid],
    ffeqoperators: &[Oid],
    fkdelsetcols: &[i16],
) -> PgResult<Oid> {
    let conname_storage;
    let conname = if constraint_name_is_used(mcx, rel.rd_id, conname)? {
        conname_storage =
            pg_constraint::ChooseConstraintName(mcx, conname, None, "", rel.rd_rel.relnamespace, &[])?;
        conname_storage.as_str()
    } else {
        conname
    };

    let mut entry = pg_constraint::ConstraintEntry::base(
        conname,
        rel.rd_rel.relnamespace,
        pg_constraint::CONSTRAINT_FOREIGN,
        rel.rd_id,
    );
    entry.deferrable = fkconstraint.deferrable;
    entry.deferred = fkconstraint.initdeferred;
    entry.is_enforced = fkconstraint.is_enforced;
    entry.is_validated = fkconstraint.initially_valid;
    entry.conkey = &fkattnum[..numfks];
    entry.n_keys = numfks;
    entry.index_relid = index_oid;
    entry.foreign_relid = pkrel.rd_id;
    entry.confkey = &pkattnum[..numfks];
    entry.pf_eq_op = &pfeqoperators[..numfks];
    entry.pp_eq_op = &ppeqoperators[..numfks];
    entry.ff_eq_op = &ffeqoperators[..numfks];
    entry.fk_del_set_cols = fkdelsetcols;
    entry.fk_upd_type = fkconstraint.fk_upd_action;
    entry.fk_del_type = fkconstraint.fk_del_action;
    entry.fk_match_type = fkconstraint.fk_matchtype;
    entry.is_local = true;
    entry.inhcount = 0;
    entry.is_no_inherit = true;
    let constr_oid = pg_constraint::CreateConstraintEntry(mcx, &entry)?;
    xact::CommandCounterIncrement()?;
    Ok(constr_oid)
}

// createForeignKeyActionTriggers (tablecmds.c): AFTER DELETE + AFTER UPDATE
// row triggers on the referenced rel.
fn create_foreign_key_action_triggers<'mcx>(
    mcx: Mcx<'mcx>,
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: &Constraint<'_>,
    constraint_oid: Oid,
    index_oid: Oid,
) -> PgResult<()> {
    let del_func = match fkconstraint.fk_del_action {
        FKCONSTR_ACTION_NOACTION => F_RI_FKEY_NOACTION_DEL,
        FKCONSTR_ACTION_RESTRICT => F_RI_FKEY_RESTRICT_DEL,
        FKCONSTR_ACTION_CASCADE => F_RI_FKEY_CASCADE_DEL,
        FKCONSTR_ACTION_SETNULL => F_RI_FKEY_SETNULL_DEL,
        FKCONSTR_ACTION_SETDEFAULT => F_RI_FKEY_SETDEFAULT_DEL,
        other => panic!("unrecognized FK action type: {other:?}"),
    };
    trigger::CreateTriggerInternal(
        mcx,
        &trigger::InternalTriggerArgs {
            trigname_base: "RI_ConstraintTrigger_a",
            relid: ref_rel_oid,
            constrrelid: my_rel_oid,
            constraint_oid,
            index_oid,
            funcoid: del_func,
            tgtype: TRIGGER_TYPE_ROW | TRIGGER_TYPE_DELETE,
            deferrable: fkconstraint.deferrable
                && fkconstraint.fk_del_action == FKCONSTR_ACTION_NOACTION,
            initdeferred: fkconstraint.initdeferred
                && fkconstraint.fk_del_action == FKCONSTR_ACTION_NOACTION,
        },
    )?;
    xact::CommandCounterIncrement()?;
    let upd_func = match fkconstraint.fk_upd_action {
        FKCONSTR_ACTION_NOACTION => F_RI_FKEY_NOACTION_UPD,
        FKCONSTR_ACTION_RESTRICT => F_RI_FKEY_RESTRICT_UPD,
        FKCONSTR_ACTION_CASCADE => F_RI_FKEY_CASCADE_UPD,
        FKCONSTR_ACTION_SETNULL => F_RI_FKEY_SETNULL_UPD,
        FKCONSTR_ACTION_SETDEFAULT => F_RI_FKEY_SETDEFAULT_UPD,
        other => panic!("unrecognized FK action type: {other:?}"),
    };
    trigger::CreateTriggerInternal(
        mcx,
        &trigger::InternalTriggerArgs {
            trigname_base: "RI_ConstraintTrigger_a",
            relid: ref_rel_oid,
            constrrelid: my_rel_oid,
            constraint_oid,
            index_oid,
            funcoid: upd_func,
            tgtype: TRIGGER_TYPE_ROW | TRIGGER_TYPE_UPDATE,
            deferrable: fkconstraint.deferrable
                && fkconstraint.fk_upd_action == FKCONSTR_ACTION_NOACTION,
            initdeferred: fkconstraint.initdeferred
                && fkconstraint.fk_upd_action == FKCONSTR_ACTION_NOACTION,
        },
    )?;
    Ok(())
}

// createForeignKeyCheckTriggers / CreateFKCheckTrigger (tablecmds.c): AFTER
// INSERT + AFTER UPDATE row triggers on the referencing rel.
fn create_foreign_key_check_triggers<'mcx>(
    mcx: Mcx<'mcx>,
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: &Constraint<'_>,
    constraint_oid: Oid,
    index_oid: Oid,
) -> PgResult<()> {
    trigger::CreateTriggerInternal(
        mcx,
        &trigger::InternalTriggerArgs {
            trigname_base: "RI_ConstraintTrigger_c",
            relid: my_rel_oid,
            constrrelid: ref_rel_oid,
            constraint_oid,
            index_oid,
            funcoid: F_RI_FKEY_CHECK_INS,
            tgtype: TRIGGER_TYPE_ROW | TRIGGER_TYPE_INSERT,
            deferrable: fkconstraint.deferrable,
            initdeferred: fkconstraint.initdeferred,
        },
    )?;
    xact::CommandCounterIncrement()?;
    trigger::CreateTriggerInternal(
        mcx,
        &trigger::InternalTriggerArgs {
            trigname_base: "RI_ConstraintTrigger_c",
            relid: my_rel_oid,
            constrrelid: ref_rel_oid,
            constraint_oid,
            index_oid,
            funcoid: F_RI_FKEY_CHECK_UPD,
            tgtype: TRIGGER_TYPE_ROW | TRIGGER_TYPE_UPDATE,
            deferrable: fkconstraint.deferrable,
            initdeferred: fkconstraint.initdeferred,
        },
    )?;
    xact::CommandCounterIncrement()?;
    Ok(())
}

// ConstraintNameIsUsed (pg_constraint.c), CONSTRAINT_RELATION arm.
fn constraint_name_is_used<'mcx>(mcx: Mcx<'mcx>, relid: Oid, conname: &str) -> PgResult<bool> {
    use datum::Datum;
    use types_scan::scankey::ScanKeyData;
    let con_rel = table::table_open(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        types_rel::AccessShareLock,
    )?;
    let mk_key = |attno: AttrNumber, func: types_core::RegProcedure, arg: Datum| {
        let mut key = ScanKeyData::empty();
        key.sk_attno = attno;
        key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
        key.sk_collation = types_core::C_COLLATION_OID;
        key.sk_func = fmgr_seams::fmgr_info::call(func)
            .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
        key.sk_argument = arg;
        key
    };
    let cname = name_arg(mcx, conname)?;
    let keys = [
        mk_key(
            pg_constraint::Anum_pg_constraint_conrelid,
            types_core::fmgr::F_OIDEQ,
            Datum::from_oid(relid),
        ),
        mk_key(
            pg_constraint::Anum_pg_constraint_contypid,
            types_core::fmgr::F_OIDEQ,
            Datum::from_oid(InvalidOid),
        ),
        mk_key(
            pg_constraint::Anum_pg_constraint_conname,
            types_core::fmgr::F_NAMEEQ,
            Datum::from_usize(cname.as_ptr() as usize),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        types_core::CONSTRAINT_RELID_TYPID_NAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let found = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::AccessShareLock)?;
    Ok(found)
}

// ChooseForeignKeyConstraintNameAddition (tablecmds.c).
fn choose_fkey_constraint_name_addition<'mcx>(
    mcx: Mcx<'mcx>,
    colnames: &NodeList<'_>,
) -> PgResult<mcx::PgString<'mcx>> {
    let namedatalen = types_core::NAMEDATALEN as usize;
    let mut buf = mcx::PgString::new_in(mcx);
    for lc in colnames.iter() {
        let name = lc.as_string().expect("fk_attrs String").sval;
        if !buf.is_empty() {
            buf.try_push_str("_")?;
        }
        let take = name.len().min(namedatalen - 1);
        buf.try_push_str(&name[..take])?;
        if buf.len() >= namedatalen {
            break;
        }
    }
    Ok(buf)
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let n = types_core::NAMEDATALEN as usize;
    assert!(name.len() < n, "makeObjectName truncation unported: {name:?}");
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("was UTF-8"))
}

fn checkFkeyPermissions(rel: &Relation<'_>, attnums: &[i16]) -> PgResult<()> {
    let roleid = miscinit::GetUserId();
    if aclchk::pg_class_aclcheck(rel.rd_id, roleid, adt_acl::ACL_REFERENCES)?
        == aclchk::ACLCHECK_OK
    {
        return Ok(());
    }
    for &attnum in attnums {
        let aclresult =
            aclchk::pg_attribute_aclcheck(rel.rd_id, attnum, roleid, adt_acl::ACL_REFERENCES)?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclchk::aclcheck_error(
                aclresult,
                crate::get_relkind_objtype(rel.rd_rel.relkind),
                rel.name(),
            )?;
        }
    }
    Ok(())
}
