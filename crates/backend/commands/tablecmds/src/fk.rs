// ATExecAddConstraint FK slice (tablecmds.c): ATAddForeignKeyConstraint +
// addFkConstraint/addFkRecurseReferenced/addFkRecurseReferencing +
// createForeignKey{Action,Check}Triggers + validateForeignKeyConstraint +
// CloneForeignKeyConstraints and the partition attach machinery, all MATCH
// types and ON DELETE/UPDATE actions. LOUD: PERIOD (temporal FKs), non-btree
// FK support indexes.

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
use types_rel::{NoLock, Relation, ShareRowExclusiveLock, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
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

#[track_caller]
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
    pub hasperiod: bool,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn ATExecAddConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut crate::alter::Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    constraint: &Constraint<'mcx>,
    recurse: bool,
    old_desc: &types_tuple::TupleDescData<'mcx>,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<()> {
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

    at_add_foreign_key_constraint(mcx, wqueue, rel, constraint, conname, recurse, old_desc, lockmode)
}

// ATAddForeignKeyConstraint (tablecmds.c).
#[allow(clippy::too_many_arguments)]
fn at_add_foreign_key_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut crate::alter::Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    fkconstraint: &Constraint<'mcx>,
    conname: &str,
    recurse: bool,
    old_desc: &types_tuple::TupleDescData<'mcx>,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<()> {
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

    if !recurse && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let e = err(
            format!(
                "cannot use ONLY for foreign key on partitioned table \"{}\" referencing relation \"{}\"",
                rel.name(),
                pkrel.name()
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        );
        pkrel.close(NoLock)?;
        return Err(e);
    }

    if pkrel.rd_rel.relkind != RELKIND_RELATION
        && pkrel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE
    {
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
    let with_period = fkconstraint.fk_with_period || fkconstraint.pk_with_period;
    if with_period && !fkconstraint.fk_with_period {
        let e = err(
            "foreign key uses PERIOD on the referenced table but not the referencing table"
                .into(),
            ERRCODE_INVALID_FOREIGN_KEY,
        );
        pkrel.close(NoLock)?;
        return Err(e);
    }

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

    let mut pk_has_without_overlaps = false;
    let (numpks, index_oid) = if fkconstraint.pk_attrs.is_nil() {
        let (n, idx) = transform_fkey_get_primary_key(
            mcx,
            &pkrel,
            &mut pk_attnames,
            &mut pkattnum,
            &mut pktypoid,
            &mut pkcolloid,
            &mut opclasses,
            &mut pk_has_without_overlaps,
        )?;
        // If the primary key uses WITHOUT OVERLAPS, the fk must use PERIOD.
        if pk_has_without_overlaps && !fkconstraint.fk_with_period {
            let e = err(
                "foreign key uses PERIOD on the referenced table but not the referencing table"
                    .into(),
                ERRCODE_INVALID_FOREIGN_KEY,
            );
            pkrel.close(NoLock)?;
            return Err(e);
        }
        (n, idx)
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
        // Since we got pk_attrs, one should be a period.
        if with_period && !fkconstraint.pk_with_period {
            let e = err(
                "foreign key uses PERIOD on the referencing table but not the referenced table"
                    .into(),
                ERRCODE_INVALID_FOREIGN_KEY,
            );
            pkrel.close(NoLock)?;
            return Err(e);
        }
        let idx = transform_fkey_check_attrs(
            mcx,
            &pkrel,
            n,
            &pkattnum,
            with_period,
            &mut opclasses,
            &mut pk_has_without_overlaps,
        )?;
        (n, idx)
    };

    if pk_has_without_overlaps && !with_period {
        let e = err(
            "foreign key must use PERIOD when referencing a primary key using WITHOUT OVERLAPS"
                .into(),
            ERRCODE_INVALID_FOREIGN_KEY,
        );
        pkrel.close(NoLock)?;
        return Err(e);
    }

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

    // Some actions are currently unsupported for foreign keys using PERIOD.
    if fkconstraint.fk_with_period {
        for (action, kind) in [
            (fkconstraint.fk_upd_action, "ON UPDATE"),
            (fkconstraint.fk_del_action, "ON DELETE"),
        ] {
            if matches!(
                action,
                FKCONSTR_ACTION_RESTRICT
                    | FKCONSTR_ACTION_CASCADE
                    | FKCONSTR_ACTION_SETNULL
                    | FKCONSTR_ACTION_SETDEFAULT
            ) {
                let e = err(
                    format!(
                        "unsupported {kind} action for foreign key constraint using PERIOD"
                    ),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                );
                pkrel.close(NoLock)?;
                return Err(e);
            }
        }
    }

    let mut pfeqoperators = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut ppeqoperators = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut ffeqoperators = [InvalidOid; INDEX_MAX_KEYS as usize];

    // On the strength of a previous constraint, we might avoid scanning
    // tables to validate this one.
    let mut old_check_ok = !fkconstraint.old_conpfeqop.is_nil();
    debug_assert!(!old_check_ok || numfks == fkconstraint.old_conpfeqop.len());

    for i in 0..numpks {
        let pktype = pktypoid[i];
        let fktype = fktypoid[i];
        let pkcoll = pkcolloid[i];
        let fkcoll = fkcolloid[i];

        let amid = lsyscache::get_opclass_method(opclasses[i])?;
        let (opfamily, opcintype) = lsyscache::get_opclass_opfamily_and_input_type(opclasses[i])?
            .unwrap_or_else(|| panic!("cache lookup failed for opclass {}", opclasses[i]));

        // For a period FK the translation can fail if a non-matching
        // exclusion constraint was selected earlier (C keeps the check here).
        let for_overlaps = with_period && i == numpks - 1;
        let cmptype = if for_overlaps {
            lsyscache::COMPARE_OVERLAP
        } else {
            lsyscache::COMPARE_EQ
        };
        let eqstrategy_u16 = amapi::IndexAmTranslateCompareType(cmptype, amid, opfamily, true)?;
        if eqstrategy_u16 == 0 {
            let famname = lsyscache::get_opfamily_name(mcx, opfamily, false)?
                .expect("opfamily name");
            let msg = if for_overlaps {
                "could not identify an overlaps operator for foreign key"
            } else {
                "could not identify an equality operator for foreign key"
            };
            let e = Box::new((*err(msg.into(), ERRCODE_UNDEFINED_OBJECT)).with_detail(
                format!(
                    "Could not translate compare type {cmptype} for operator family \"{}\" of access method \"{}\".",
                    famname.as_str(),
                    get_am_name_closed(amid)
                ),
            ));
            pkrel.close(NoLock)?;
            return Err(e);
        }
        let eqstrategy: i16 = eqstrategy_u16 as i16;

        let ppeqop = lsyscache::get_opfamily_member(opfamily, opcintype, opcintype, eqstrategy)?;
        if ppeqop == InvalidOid {
            panic!(
                "missing operator {eqstrategy}({opcintype},{opcintype}) in opfamily {opfamily}"
            );
        }

        let fktyped = lsyscache::getBaseType(fktype)?;
        let mut pfeqop = lsyscache::get_opfamily_member(opfamily, opcintype, fktyped, eqstrategy)?;
        let mut pfeqop_right = InvalidOid;
        let mut ffeqop = if pfeqop != InvalidOid {
            pfeqop_right = fktyped;
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
                pfeqop_right = opcintype;
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

        if old_check_ok {
            // When a pfeqop changes, revalidate the constraint; ppeqop and
            // ffeqop are not used by RI_Initial_Check.
            old_check_ok = pfeqop == fkconstraint.old_conpfeqop.nth(i);
        }
        if old_check_ok {
            let attr = old_desc.attr(fkattnum[i] as usize - 1);
            let old_fktype = attr.atttypid;
            let new_fktype = fktype;
            let (old_pathtype, old_castfunc) = find_fkey_cast(pfeqop_right, old_fktype)?;
            let (new_pathtype, new_castfunc) = find_fkey_cast(pfeqop_right, new_fktype)?;
            let old_fkcoll = attr.attcollation;
            let new_fkcoll = fkcoll;

            // A polymorphic cast destination, or a collation change between
            // non-deterministic collations, forces revalidation.
            old_check_ok = new_pathtype == old_pathtype
                && new_castfunc == old_castfunc
                && (!coerce::IsPolymorphicType(pfeqop_right) || new_fktype == old_fktype)
                && (new_fkcoll == old_fkcoll
                    || (lsyscache::get_collation_isdeterministic(old_fkcoll)?
                        && lsyscache::get_collation_isdeterministic(new_fkcoll)?));
        }

        pfeqoperators[i] = pfeqop;
        ppeqoperators[i] = ppeqop;
        ffeqoperators[i] = ffeqop;
    }

    // FKs with PERIOD look their operators up at runtime; prove the lookup
    // works now (fk.periodatt <@ range_agg(pk.periodatt)).
    if with_period {
        pg_constraint::FindFKPeriodOpers(opclasses[numpks - 1])?;
    }

    let (constr_oid, _) = add_fk_constraint(
        mcx,
        AddFkSide::BothSides,
        conname,
        fkconstraint,
        rel,
        &pkrel,
        index_oid,
        InvalidOid,
        numfks,
        &pkattnum,
        &fkattnum,
        &pfeqoperators,
        &ppeqoperators,
        &ffeqoperators,
        &fkdelsetcols[..numfkdelsetcols],
        false,
        with_period,
    )?;

    add_fk_recurse_referenced(
        mcx,
        conname,
        fkconstraint,
        rel,
        &pkrel,
        index_oid,
        constr_oid,
        numfks,
        &pkattnum,
        &fkattnum,
        &pfeqoperators,
        &ppeqoperators,
        &ffeqoperators,
        &fkdelsetcols[..numfkdelsetcols],
        old_check_ok,
        InvalidOid,
        InvalidOid,
        with_period,
    )?;

    add_fk_recurse_referencing(
        mcx,
        Some(wqueue),
        conname,
        fkconstraint,
        rel,
        &pkrel,
        index_oid,
        constr_oid,
        numfks,
        &pkattnum,
        &fkattnum,
        &pfeqoperators,
        &ppeqoperators,
        &ffeqoperators,
        &fkdelsetcols[..numfkdelsetcols],
        old_check_ok,
        lockmode,
        InvalidOid,
        InvalidOid,
        with_period,
    )?;

    pkrel.close(NoLock)
}

// addFkRecurseReferenced (tablecmds.c): action triggers on the referenced
// side, recursing to referenced-side partitions.
#[allow(clippy::too_many_arguments)]
fn add_fk_recurse_referenced<'mcx>(
    mcx: Mcx<'mcx>,
    conname: &str,
    fkconstraint: &Constraint<'mcx>,
    rel: &Relation<'mcx>,
    pkrel: &Relation<'mcx>,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: usize,
    pkattnum: &[i16],
    fkattnum: &[i16],
    pfeqoperators: &[Oid],
    ppeqoperators: &[Oid],
    ffeqoperators: &[Oid],
    fkdelsetcols: &[i16],
    old_check_ok: bool,
    parent_del_trigger: Oid,
    parent_upd_trigger: Oid,
    with_period: bool,
) -> PgResult<()> {
    let (mut delete_trigger_oid, mut update_trigger_oid) = (InvalidOid, InvalidOid);
    if fkconstraint.is_enforced {
        (delete_trigger_oid, update_trigger_oid) = create_foreign_key_action_triggers(
            mcx,
            rel.rd_id,
            pkrel.rd_id,
            fkconstraint,
            parent_constr,
            index_oid,
            parent_del_trigger,
            parent_upd_trigger,
        )?;
    }

    // A partitioned referenced table needs one pg_constraint row per
    // partition in addition to the parent-table row.
    if pkrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let pd = partdesc::RelationGetPartitionDesc(pkrel, true)?;
        for i in 0..pd.nparts {
            let part_rel = table::table_open(mcx, pd.oids[i], ShareRowExclusiveLock)?;
            let map = tupdesc::build_attrmap_by_name(mcx, &part_rel.rd_att, &pkrel.rd_att)?;
            let mut mapped_pkattnum = [0i16; INDEX_MAX_KEYS as usize];
            for j in 0..numfks {
                mapped_pkattnum[j] = map[pkattnum[j] as usize - 1];
            }
            let part_index_id = pg_inherits::index_get_partition(mcx, part_rel.rd_id, index_oid)?;
            if part_index_id == InvalidOid {
                panic!("index for {index_oid} not found in partition {}", part_rel.name());
            }
            let (child_constr, _) = add_fk_constraint(
                mcx,
                AddFkSide::ReferencedSide,
                conname,
                fkconstraint,
                rel,
                &part_rel,
                part_index_id,
                parent_constr,
                numfks,
                &mapped_pkattnum,
                fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                fkdelsetcols,
                true,
                with_period,
            )?;
            add_fk_recurse_referenced(
                mcx,
                conname,
                fkconstraint,
                rel,
                &part_rel,
                part_index_id,
                child_constr,
                numfks,
                &mapped_pkattnum,
                fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                fkdelsetcols,
                old_check_ok,
                delete_trigger_oid,
                update_trigger_oid,
                with_period,
            )?;
            part_rel.close(NoLock)?;
        }
    }
    Ok(())
}

// addFkRecurseReferencing (tablecmds.c): check triggers and Phase-3 queueing
// on the referencing side, recursing to referencing-side partitions.
#[allow(clippy::too_many_arguments)]
fn add_fk_recurse_referencing<'mcx>(
    mcx: Mcx<'mcx>,
    mut wqueue: Option<&mut crate::alter::Wqueue<'mcx>>,
    conname: &str,
    fkconstraint: &Constraint<'mcx>,
    rel: &Relation<'mcx>,
    pkrel: &Relation<'mcx>,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: usize,
    pkattnum: &[i16],
    fkattnum: &[i16],
    pfeqoperators: &[Oid],
    ppeqoperators: &[Oid],
    ffeqoperators: &[Oid],
    fkdelsetcols: &[i16],
    old_check_ok: bool,
    lockmode: types_rel::LOCKMODE,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    with_period: bool,
) -> PgResult<()> {
    debug_assert!(parent_constr != InvalidOid);
    if rel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
        return Err(err(
            "foreign key constraints are not supported on foreign tables".into(),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }

    let (mut insert_trigger_oid, mut update_trigger_oid) = (InvalidOid, InvalidOid);
    if fkconstraint.is_enforced {
        (insert_trigger_oid, update_trigger_oid) = create_foreign_key_check_triggers(
            mcx,
            rel.rd_id,
            pkrel.rd_id,
            fkconstraint,
            parent_constr,
            index_oid,
            parent_ins_trigger,
            parent_upd_trigger,
        )?;
    }
    if rel.rd_rel.relkind == RELKIND_RELATION {
        if let Some(wqueue) = wqueue {
            if !old_check_ok && !fkconstraint.skip_validation && fkconstraint.is_enforced {
                let name = lsyscache::get_constraint_name(mcx, parent_constr)?
                    .unwrap_or_else(|| panic!("cache lookup failed for constraint {parent_constr}"));
                let tabidx = crate::alter::ATGetQueueEntry(mcx, wqueue, rel);
                wqueue[tabidx].fk_checks.push(FkValidateItem {
                    conname: str_in(mcx, name.as_str())?,
                    refrelid: pkrel.rd_id,
                    refindid: index_oid,
                    conid: parent_constr,
                    hasperiod: with_period,
                });
            }
        }
    } else if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let pd = partdesc::RelationGetPartitionDesc(rel, true)?;
        for i in 0..pd.nparts {
            let partition = table::table_open(mcx, pd.oids[i], lockmode)?;
            catalog_heap::CheckTableNotInUse(&partition, "ALTER TABLE")?;

            let attmap = tupdesc::build_attrmap_by_name(mcx, &partition.rd_att, &rel.rd_att)?;
            let mut mapped_fkattnum = [0i16; INDEX_MAX_KEYS as usize];
            for j in 0..numfks {
                mapped_fkattnum[j] = attmap[fkattnum[j] as usize - 1];
            }

            let part_fks = rel_fk_constraint_list(mcx, partition.rd_id)?;
            let mut attached = false;
            for fk in part_fks.iter() {
                if try_attach_partition_foreign_key(
                    mcx,
                    wqueue.as_deref_mut(),
                    fk,
                    &partition,
                    parent_constr,
                    numfks,
                    &mapped_fkattnum,
                    pkattnum,
                    pfeqoperators,
                    insert_trigger_oid,
                    update_trigger_oid,
                )? {
                    attached = true;
                    break;
                }
            }
            if attached {
                partition.close(NoLock)?;
                continue;
            }

            let (child_constr, _) = add_fk_constraint(
                mcx,
                AddFkSide::ReferencingSide,
                conname,
                fkconstraint,
                &partition,
                pkrel,
                index_oid,
                parent_constr,
                numfks,
                pkattnum,
                &mapped_fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                fkdelsetcols,
                true,
                with_period,
            )?;
            add_fk_recurse_referencing(
                mcx,
                wqueue.as_deref_mut(),
                conname,
                fkconstraint,
                &partition,
                pkrel,
                index_oid,
                child_constr,
                numfks,
                pkattnum,
                &mapped_fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                fkdelsetcols,
                old_check_ok,
                lockmode,
                insert_trigger_oid,
                update_trigger_oid,
                with_period,
            )?;
            partition.close(NoLock)?;
        }
    }
    Ok(())
}

// findFkeyCast (tablecmds.c); a previously-relied-upon cast must still exist.
fn find_fkey_cast(target_type_id: Oid, source_type_id: Oid) -> PgResult<(coerce::CoercionPathType, Oid)> {
    if target_type_id == source_type_id {
        return Ok((coerce::COERCION_PATH_RELABELTYPE, InvalidOid));
    }
    let (ret, funcid) = coerce::find_coercion_pathway(
        target_type_id,
        source_type_id,
        coerce::CoercionContext::COERCION_IMPLICIT,
    )?;
    if ret == coerce::COERCION_PATH_NONE {
        panic!("could not find cast from {source_type_id} to {target_type_id}");
    }
    Ok((ret, funcid))
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

    // C: no LEFT JOIN shortcut for temporal FKs (no temporal left joins yet).
    if !item.hasperiod && ri_triggers_seams::ri_initial_check::call(mcx, &trig, rel, &pkrel)? {
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
        let tableam::TableScanDesc::Heap(hscan) = &mut scan else {
            panic!("FK validation scan on a non-heap AM");
        };
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
            if catalog_heap::SystemAttributeByName(attname).is_some() {
                return Err(err(
                    "system columns cannot be used in foreign keys".to_string(),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
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
    indisexclusion: bool,
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
    const Anum_indisexclusion: i32 = 8;
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
        indisexclusion: req(Anum_indisexclusion)?.as_bool(),
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

// Closed-set get_am_name (pg_am.dat) for error details.
fn get_am_name_closed(amid: Oid) -> &'static str {
    match amid {
        BTREE_AM_OID => "btree",
        405 => "hash",
        2742 => "gin",
        783 => "gist",
        4000 => "spgist",
        3580 => "brin",
        _ => "???",
    }
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
    pk_has_without_overlaps: &mut bool,
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
    *pk_has_without_overlaps = shape.indisexclusion;
    Ok((n, indexoid))
}

// transformFkeyCheckAttrs (tablecmds.c); the 42830 no-matching-unique check.
fn transform_fkey_check_attrs<'mcx>(
    mcx: Mcx<'mcx>,
    pkrel: &Relation<'mcx>,
    numattrs: usize,
    attnums: &[i16],
    with_period: bool,
    opclasses: &mut [Oid],
    pk_has_without_overlaps: &mut bool,
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
        // Temporal FKs match an exclusion (WITHOUT OVERLAPS) index instead
        // of a unique one.
        if shape.indnkeyatts as usize == numattrs
            && (if with_period { shape.indisexclusion } else { shape.indisunique })
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
            // The last attribute in the index must be the PERIOD FK part.
            if found && with_period {
                found = attnums[numattrs - 1] == shape.indkey[numattrs - 1];
            }
            if found && !shape.indimmediate {
                found_deferrable = true;
                found = false;
            }
            if found {
                *pk_has_without_overlaps = shape.indisexclusion;
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

// addFkConstraintSides (tablecmds.c).
#[derive(Clone, Copy, PartialEq, Eq)]
enum AddFkSide {
    ReferencedSide,
    ReferencingSide,
    BothSides,
}

// addFkConstraint (tablecmds.c).
#[allow(clippy::too_many_arguments)]
fn add_fk_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    fkside: AddFkSide,
    constraintname: &str,
    fkconstraint: &Constraint<'mcx>,
    rel: &Relation<'mcx>,
    pkrel: &Relation<'mcx>,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: usize,
    pkattnum: &[i16],
    fkattnum: &[i16],
    pfeqoperators: &[Oid],
    ppeqoperators: &[Oid],
    ffeqoperators: &[Oid],
    fkdelsetcols: &[i16],
    // C forwards is_internal to the object-access hooks, which do not exist
    // here.
    _is_internal: bool,
    with_period: bool,
) -> PgResult<(Oid, &'mcx str)> {
    // Redundant at the top level; needed when recursing to referenced
    // partitions.
    if pkrel.rd_rel.relkind != RELKIND_RELATION
        && pkrel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE
    {
        return Err(err(
            format!("referenced relation \"{}\" is not a table", pkrel.name()),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }

    let conname_storage;
    let conname = if constraint_name_is_used(mcx, rel.rd_id, constraintname)? {
        conname_storage = pg_constraint::ChooseConstraintName(
            mcx,
            constraintname,
            None,
            "",
            rel.rd_rel.relnamespace,
            &[],
        )?;
        conname_storage.as_str()
    } else {
        constraintname
    };

    let (conislocal, coninhcount, connoinherit) = if parent_constr != InvalidOid {
        (false, 1, false)
    } else {
        // always inherit for partitioned tables, never for legacy inheritance
        (true, 0, rel.rd_rel.relkind != RELKIND_PARTITIONED_TABLE)
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
    entry.parent_constr_id = parent_constr;
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
    entry.is_local = conislocal;
    entry.inhcount = coninhcount;
    entry.is_no_inherit = connoinherit;
    entry.con_period = with_period;
    let constr_oid = pg_constraint::CreateConstraintEntry(mcx, &entry)?;

    // Subsidiary rows in partitions hang off the parent constraint: an
    // internal dependency on the referenced side, partition-primary/secondary
    // dependencies on the referencing side.
    if parent_constr != InvalidOid {
        use pg_depend::{DependencyType, ObjectAddress};
        let address = ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, constr_oid);
        let referenced = ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, parent_constr);
        debug_assert!(fkside != AddFkSide::BothSides);
        if fkside == AddFkSide::ReferencedSide {
            pg_depend::recordDependencyOn(mcx, &address, &referenced, DependencyType::Internal)?;
        } else {
            pg_depend::recordDependencyOn(
                mcx,
                &address,
                &referenced,
                DependencyType::PartitionPri,
            )?;
            let referenced = ObjectAddress::set(types_core::RELATION_RELATION_ID, rel.rd_id);
            pg_depend::recordDependencyOn(
                mcx,
                &address,
                &referenced,
                DependencyType::PartitionSec,
            )?;
        }
    }

    xact::CommandCounterIncrement()?;
    Ok((constr_oid, str_in(mcx, conname)?))
}

// createForeignKeyActionTriggers (tablecmds.c): AFTER DELETE + AFTER UPDATE
// row triggers on the referenced rel.
#[allow(clippy::too_many_arguments)]
fn create_foreign_key_action_triggers<'mcx>(
    mcx: Mcx<'mcx>,
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: &Constraint<'_>,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_del_trigger: Oid,
    parent_upd_trigger: Oid,
) -> PgResult<(Oid, Oid)> {
    let del_func = match fkconstraint.fk_del_action {
        FKCONSTR_ACTION_NOACTION => F_RI_FKEY_NOACTION_DEL,
        FKCONSTR_ACTION_RESTRICT => F_RI_FKEY_RESTRICT_DEL,
        FKCONSTR_ACTION_CASCADE => F_RI_FKEY_CASCADE_DEL,
        FKCONSTR_ACTION_SETNULL => F_RI_FKEY_SETNULL_DEL,
        FKCONSTR_ACTION_SETDEFAULT => F_RI_FKEY_SETDEFAULT_DEL,
        other => panic!("unrecognized FK action type: {other:?}"),
    };
    let delete_trig_oid = trigger::CreateTriggerInternal(
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
            parent_trigger_oid: parent_del_trigger,
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
    let update_trig_oid = trigger::CreateTriggerInternal(
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
            parent_trigger_oid: parent_upd_trigger,
        },
    )?;
    Ok((delete_trig_oid, update_trig_oid))
}

// createForeignKeyCheckTriggers / CreateFKCheckTrigger (tablecmds.c): AFTER
// INSERT + AFTER UPDATE row triggers on the referencing rel.
#[allow(clippy::too_many_arguments)]
fn create_foreign_key_check_triggers<'mcx>(
    mcx: Mcx<'mcx>,
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: &Constraint<'_>,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
) -> PgResult<(Oid, Oid)> {
    let insert_trig_oid = trigger::CreateTriggerInternal(
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
            parent_trigger_oid: parent_ins_trigger,
        },
    )?;
    xact::CommandCounterIncrement()?;
    let update_trig_oid = trigger::CreateTriggerInternal(
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
            parent_trigger_oid: parent_upd_trigger,
        },
    )?;
    xact::CommandCounterIncrement()?;
    Ok((insert_trig_oid, update_trig_oid))
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

const TriggerRelationId: Oid = 2620;
const TriggerConstraintIndexId: Oid = 2699;
const ConstraintParentIndexId: Oid = 2579;
const Anum_pg_trigger_oid: usize = 1;
const Anum_pg_trigger_tgrelid: usize = 2;
const Anum_pg_trigger_tgfoid: usize = 5;
const Anum_pg_trigger_tgtype: usize = 6;
const Anum_pg_trigger_tgconstrrelid: usize = 9;
const Anum_pg_trigger_tgconstraint: usize = 11;

fn getattr(
    tup: &types_tuple::HeapTupleData<'_>,
    desc: &types_tuple::TupleDescData<'_>,
    attno: usize,
) -> datum::Datum {
    let mut isnull = false;
    // SAFETY: fixed NOT NULL catalog columns under the relation's descriptor.
    unsafe { types_tuple::heap_getattr(tup, attno as i32, desc, &mut isnull) }
}

// The Form_pg_constraint fields the FK partition machinery reads.
pub(crate) struct FkConstraintForm {
    oid: Oid,
    conname: [u8; 64],
    contype: u8,
    condeferrable: bool,
    condeferred: bool,
    conenforced: bool,
    convalidated: bool,
    conrelid: Oid,
    conindid: Oid,
    conparentid: Oid,
    confrelid: Oid,
    confupdtype: u8,
    confdeltype: u8,
    confmatchtype: u8,
    conperiod: bool,
}

impl FkConstraintForm {
    fn name_str(&self) -> &str {
        let len = self.conname.iter().position(|&b| b == 0).unwrap_or(64);
        core::str::from_utf8(&self.conname[..len]).expect("conname UTF-8")
    }
}

fn decode_fk_constraint_form(
    tup: &types_tuple::HeapTupleData<'_>,
    desc: &types_tuple::TupleDescData<'_>,
) -> FkConstraintForm {
    use pg_constraint::*;
    let name_datum = getattr(tup, desc, Anum_pg_constraint_conname as usize);
    let mut conname = [0u8; 64];
    // SAFETY: NameData is a 64-byte NUL-padded buffer.
    unsafe {
        conname.copy_from_slice(core::slice::from_raw_parts(
            name_datum.as_usize() as *const u8,
            64,
        ));
    }
    FkConstraintForm {
        oid: getattr(tup, desc, Anum_pg_constraint_oid as usize).as_oid(),
        conname,
        contype: getattr(tup, desc, Anum_pg_constraint_contype as usize).as_i8() as u8,
        condeferrable: getattr(tup, desc, Anum_pg_constraint_condeferrable as usize).as_bool(),
        condeferred: getattr(tup, desc, Anum_pg_constraint_condeferred as usize).as_bool(),
        conenforced: getattr(tup, desc, Anum_pg_constraint_conenforced as usize).as_bool(),
        convalidated: getattr(tup, desc, Anum_pg_constraint_convalidated as usize).as_bool(),
        conrelid: getattr(tup, desc, Anum_pg_constraint_conrelid as usize).as_oid(),
        conindid: getattr(tup, desc, Anum_pg_constraint_conindid as usize).as_oid(),
        conparentid: getattr(tup, desc, Anum_pg_constraint_conparentid as usize).as_oid(),
        confrelid: getattr(tup, desc, Anum_pg_constraint_confrelid as usize).as_oid(),
        confupdtype: getattr(tup, desc, Anum_pg_constraint_confupdtype as usize).as_i8() as u8,
        confdeltype: getattr(tup, desc, Anum_pg_constraint_confdeltype as usize).as_i8() as u8,
        confmatchtype: getattr(tup, desc, Anum_pg_constraint_confmatchtype as usize).as_i8()
            as u8,
        conperiod: getattr(tup, desc, Anum_pg_constraint_conperiod as usize).as_bool(),
    }
}

pub(crate) fn read_fk_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
) -> PgResult<(FkConstraintForm, pg_constraint::FkConstraintArrays)> {
    let con_rel = table::table_open(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        types_rel::AccessShareLock,
    )?;
    let keys = [crate::alter::oid_scankey(pg_constraint::Anum_pg_constraint_oid as usize, conoid)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        types_core::CONSTRAINT_OID_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {conoid}"));
    let desc = con_rel.descr();
    let form = decode_fk_constraint_form(tup, desc);
    let arrays = pg_constraint::DeconstructFkConstraintRow(mcx, tup, desc)?;
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::AccessShareLock)?;
    Ok((form, arrays))
}

// ForeignKeyCacheInfo (rel.h): the fields RelationGetFKeyList exposes.
struct FkCacheInfo {
    conoid: Oid,
    conrelid: Oid,
    confrelid: Oid,
    conenforced: bool,
    numfks: usize,
    conkey: [i16; INDEX_MAX_KEYS as usize],
    confkey: [i16; INDEX_MAX_KEYS as usize],
    conpfeqop: [Oid; INDEX_MAX_KEYS as usize],
}

// RelationGetFKeyList (relcache.c), uncached: the rel's FK constraints in
// (conrelid, contypid, conname) index order.
fn rel_fk_constraint_list<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<mcx::PgVec<'mcx, FkCacheInfo>> {
    let con_rel = table::table_open(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        types_rel::AccessShareLock,
    )?;
    let keys =
        [crate::alter::oid_scankey(pg_constraint::Anum_pg_constraint_conrelid as usize, relid)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        types_core::CONSTRAINT_RELID_TYPID_NAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let desc = con_rel.descr();
    let mut out: mcx::PgVec<'mcx, FkCacheInfo> = mcx::PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let contype =
            getattr(tup, desc, pg_constraint::Anum_pg_constraint_contype as usize).as_i8() as u8;
        if contype != pg_constraint::CONSTRAINT_FOREIGN {
            continue;
        }
        let arrays = pg_constraint::DeconstructFkConstraintRow(mcx, tup, desc)?;
        out.push(FkCacheInfo {
            conoid: getattr(tup, desc, pg_constraint::Anum_pg_constraint_oid as usize).as_oid(),
            conrelid: getattr(tup, desc, pg_constraint::Anum_pg_constraint_conrelid as usize)
                .as_oid(),
            confrelid: getattr(tup, desc, pg_constraint::Anum_pg_constraint_confrelid as usize)
                .as_oid(),
            conenforced: getattr(
                tup,
                desc,
                pg_constraint::Anum_pg_constraint_conenforced as usize,
            )
            .as_bool(),
            numfks: arrays.numfks,
            conkey: arrays.conkey,
            confkey: arrays.confkey,
            conpfeqop: arrays.pf_eq_oprs,
        });
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::AccessShareLock)?;
    Ok(out)
}

// RI_FKey_trigger_type (ri_triggers.c), by RI function OID.
const RI_TRIGGER_PK: u8 = 1;
const RI_TRIGGER_FK: u8 = 2;
const RI_TRIGGER_NONE: u8 = 0;

fn ri_fkey_trigger_type(tgfoid: Oid) -> u8 {
    match tgfoid {
        F_RI_FKEY_CASCADE_DEL | F_RI_FKEY_CASCADE_UPD | F_RI_FKEY_SETNULL_DEL
        | F_RI_FKEY_SETNULL_UPD | F_RI_FKEY_SETDEFAULT_DEL | F_RI_FKEY_SETDEFAULT_UPD
        | F_RI_FKEY_NOACTION_DEL | F_RI_FKEY_NOACTION_UPD | F_RI_FKEY_RESTRICT_DEL
        | F_RI_FKEY_RESTRICT_UPD => RI_TRIGGER_PK,
        F_RI_FKEY_CHECK_INS | F_RI_FKEY_CHECK_UPD => RI_TRIGGER_FK,
        _ => RI_TRIGGER_NONE,
    }
}

// GetForeignKeyActionTriggers (tablecmds.c).
fn get_foreign_key_action_triggers<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
) -> PgResult<(Oid, Oid)> {
    let (mut delete_trigger_oid, mut update_trigger_oid) = (InvalidOid, InvalidOid);
    let trig_rel = table::table_open(mcx, TriggerRelationId, types_rel::RowExclusiveLock)?;
    let keys = [crate::alter::oid_scankey(Anum_pg_trigger_tgconstraint, conoid)];
    let mut scan =
        genam::systable_beginscan(mcx, &trig_rel, TriggerConstraintIndexId, true, None, &keys)?;
    let desc = trig_rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        if getattr(tup, desc, Anum_pg_trigger_tgconstrrelid).as_oid() != conrelid {
            continue;
        }
        if getattr(tup, desc, Anum_pg_trigger_tgrelid).as_oid() != confrelid {
            continue;
        }
        let tgfoid = getattr(tup, desc, Anum_pg_trigger_tgfoid).as_oid();
        if ri_fkey_trigger_type(tgfoid) != RI_TRIGGER_PK {
            continue;
        }
        let tgtype = getattr(tup, desc, Anum_pg_trigger_tgtype).as_i16();
        let oid = getattr(tup, desc, Anum_pg_trigger_oid).as_oid();
        if tgtype & TRIGGER_TYPE_DELETE != 0 {
            debug_assert!(delete_trigger_oid == InvalidOid);
            delete_trigger_oid = oid;
        } else if tgtype & TRIGGER_TYPE_UPDATE != 0 {
            debug_assert!(update_trigger_oid == InvalidOid);
            update_trigger_oid = oid;
        }
        if cfg!(not(debug_assertions))
            && delete_trigger_oid != InvalidOid
            && update_trigger_oid != InvalidOid
        {
            break;
        }
    }
    if delete_trigger_oid == InvalidOid {
        panic!("could not find ON DELETE action trigger of foreign key constraint {conoid}");
    }
    if update_trigger_oid == InvalidOid {
        panic!("could not find ON UPDATE action trigger of foreign key constraint {conoid}");
    }
    genam::systable_endscan(mcx, scan)?;
    trig_rel.close(types_rel::RowExclusiveLock)?;
    Ok((delete_trigger_oid, update_trigger_oid))
}

// GetForeignKeyCheckTriggers (tablecmds.c).
fn get_foreign_key_check_triggers<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
) -> PgResult<(Oid, Oid)> {
    let (mut insert_trigger_oid, mut update_trigger_oid) = (InvalidOid, InvalidOid);
    let trig_rel = table::table_open(mcx, TriggerRelationId, types_rel::RowExclusiveLock)?;
    let keys = [crate::alter::oid_scankey(Anum_pg_trigger_tgconstraint, conoid)];
    let mut scan =
        genam::systable_beginscan(mcx, &trig_rel, TriggerConstraintIndexId, true, None, &keys)?;
    let desc = trig_rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        if getattr(tup, desc, Anum_pg_trigger_tgconstrrelid).as_oid() != confrelid {
            continue;
        }
        if getattr(tup, desc, Anum_pg_trigger_tgrelid).as_oid() != conrelid {
            continue;
        }
        let tgfoid = getattr(tup, desc, Anum_pg_trigger_tgfoid).as_oid();
        if ri_fkey_trigger_type(tgfoid) != RI_TRIGGER_FK {
            continue;
        }
        let tgtype = getattr(tup, desc, Anum_pg_trigger_tgtype).as_i16();
        let oid = getattr(tup, desc, Anum_pg_trigger_oid).as_oid();
        if tgtype & TRIGGER_TYPE_INSERT != 0 {
            debug_assert!(insert_trigger_oid == InvalidOid);
            insert_trigger_oid = oid;
        } else if tgtype & TRIGGER_TYPE_UPDATE != 0 {
            debug_assert!(update_trigger_oid == InvalidOid);
            update_trigger_oid = oid;
        }
        if cfg!(not(debug_assertions))
            && insert_trigger_oid != InvalidOid
            && update_trigger_oid != InvalidOid
        {
            break;
        }
    }
    if insert_trigger_oid == InvalidOid {
        panic!("could not find ON INSERT check triggers of foreign key constraint {conoid}");
    }
    if update_trigger_oid == InvalidOid {
        panic!("could not find ON UPDATE check triggers of foreign key constraint {conoid}");
    }
    genam::systable_endscan(mcx, scan)?;
    trig_rel.close(types_rel::RowExclusiveLock)?;
    Ok((insert_trigger_oid, update_trigger_oid))
}

// tryAttachPartitionForeignKey (tablecmds.c): compare an existing partition
// FK against the one being propagated; attach it if equivalent.
#[allow(clippy::too_many_arguments)]
fn try_attach_partition_foreign_key<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: Option<&mut crate::alter::Wqueue<'mcx>>,
    fk: &FkCacheInfo,
    partition: &Relation<'mcx>,
    parent_constr_oid: Oid,
    numfks: usize,
    mapped_conkey: &[i16],
    confkey: &[i16],
    conpfeqop: &[Oid],
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
) -> PgResult<bool> {
    let (parent_form, _) = read_fk_constraint(mcx, parent_constr_oid)?;

    if fk.confrelid != parent_form.confrelid || fk.numfks != numfks {
        return Ok(false);
    }
    for i in 0..numfks {
        if fk.conkey[i] != mapped_conkey[i]
            || fk.confkey[i] != confkey[i]
            || fk.conpfeqop[i] != conpfeqop[i]
        {
            return Ok(false);
        }
    }

    let (part_form, _) = read_fk_constraint(mcx, fk.conoid)?;

    // A mismatched enforceability would otherwise silently produce a
    // duplicate constraint; make the user resolve it.
    if part_form.conenforced != parent_form.conenforced {
        return Err(err(
            format!(
                "constraint \"{}\" enforceability conflicts with constraint \"{}\" on relation \"{}\"",
                parent_form.name_str(),
                part_form.name_str(),
                partition.name()
            ),
            types_error::ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }

    if part_form.conparentid != InvalidOid
        || part_form.condeferrable != parent_form.condeferrable
        || part_form.condeferred != parent_form.condeferred
        || part_form.confupdtype != parent_form.confupdtype
        || part_form.confdeltype != parent_form.confdeltype
        || part_form.confmatchtype != parent_form.confmatchtype
    {
        return Ok(false);
    }

    attach_partition_foreign_key(
        mcx,
        wqueue,
        partition,
        fk.conoid,
        parent_constr_oid,
        parent_ins_trigger,
        parent_upd_trigger,
    )?;
    Ok(true)
}

// AttachPartitionForeignKey (tablecmds.c).
fn attach_partition_foreign_key<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: Option<&mut crate::alter::Wqueue<'mcx>>,
    partition: &Relation<'mcx>,
    part_constr_oid: Oid,
    parent_constr_oid: Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
) -> PgResult<()> {
    let (parent_form, _) = read_fk_constraint(mcx, parent_constr_oid)?;
    let (part_form, _) = read_fk_constraint(mcx, part_constr_oid)?;
    let part_constr_frelid = part_form.confrelid;
    let part_constr_relid = part_form.conrelid;

    // A partitioned referenced table left extra per-partition rows and
    // action triggers on the attached constraint; remove them.
    if lsyscache::relation::get_rel_relkind(part_constr_frelid)? as u8
        == RELKIND_PARTITIONED_TABLE
    {
        remove_inherited_constraint(mcx, part_constr_oid, part_constr_relid)?;
    }

    let queue_validation = parent_form.convalidated && !part_form.convalidated;

    drop_foreign_key_constraint_triggers(mcx, part_constr_oid, part_constr_frelid, part_constr_relid)?;

    pg_constraint::ConstraintSetParentConstraint(
        mcx,
        part_constr_oid,
        parent_constr_oid,
        partition.rd_id,
    )?;

    if parent_form.conenforced {
        let (insert_trigger_oid, update_trigger_oid) = get_foreign_key_check_triggers(
            mcx,
            part_constr_oid,
            part_constr_frelid,
            part_constr_relid,
        )?;
        debug_assert!(insert_trigger_oid != InvalidOid && parent_ins_trigger != InvalidOid);
        trigger::TriggerSetParentTrigger(mcx, insert_trigger_oid, parent_ins_trigger, partition.rd_id)?;
        debug_assert!(update_trigger_oid != InvalidOid && parent_upd_trigger != InvalidOid);
        trigger::TriggerSetParentTrigger(mcx, update_trigger_oid, parent_upd_trigger, partition.rd_id)?;
    }

    xact::CommandCounterIncrement()?;

    if queue_validation {
        // C dereferences wqueue unconditionally here; a NULL wqueue caller
        // (CREATE TABLE .. PARTITION OF) cannot own pre-validated FKs.
        let wqueue = wqueue.expect("FK validation queueing without a work queue");
        let (part_form, _) = read_fk_constraint(mcx, part_constr_oid)?;
        queue_fk_constraint_validation(
            mcx,
            wqueue,
            partition,
            part_form.confrelid,
            &part_form,
            types_rel::ShareUpdateExclusiveLock,
        )?;
    }
    Ok(())
}

// QueueFKConstraintValidation (tablecmds.c): queue Phase-3 verification for
// an invalid FK constraint, recursing over child constraints, and flip
// convalidated.
pub(crate) fn queue_fk_constraint_validation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut crate::alter::Wqueue<'mcx>,
    fkrel: &Relation<'mcx>,
    pkrelid: Oid,
    con: &FkConstraintForm,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<()> {
    debug_assert!(con.contype == pg_constraint::CONSTRAINT_FOREIGN);
    debug_assert!(!con.convalidated);

    // Partitioned tables themselves need no scan; nor do the extra rows a
    // partitioned referenced table hangs on the referencing rel.
    if fkrel.rd_rel.relkind == RELKIND_RELATION && con.confrelid == pkrelid {
        let tabidx = crate::alter::ATGetQueueEntry(mcx, wqueue, fkrel);
        wqueue[tabidx].fk_checks.push(FkValidateItem {
            conname: str_in(mcx, con.name_str())?,
            refrelid: con.confrelid,
            refindid: con.conindid,
            conid: con.oid,
            hasperiod: con.conperiod,
        });
    }

    if fkrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
        || lsyscache::relation::get_rel_relkind(con.confrelid)? as u8 == RELKIND_PARTITIONED_TABLE
    {
        let con_rel = table::table_open(
            mcx,
            types_core::CONSTRAINT_RELATION_ID,
            types_rel::RowExclusiveLock,
        )?;
        let keys = [crate::alter::oid_scankey(
            pg_constraint::Anum_pg_constraint_conparentid as usize,
            con.oid,
        )];
        let mut scan =
            genam::systable_beginscan(mcx, &con_rel, ConstraintParentIndexId, true, None, &keys)?;
        let desc = con_rel.descr();
        let mut children: mcx::PgVec<'mcx, FkConstraintForm> = mcx::PgVec::new_in(mcx);
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            children.push(decode_fk_constraint_form(tup, desc));
        }
        genam::systable_endscan(mcx, scan)?;
        con_rel.close(types_rel::RowExclusiveLock)?;
        for childcon in children.iter() {
            if childcon.convalidated {
                continue;
            }
            let childrel = table::table_open(mcx, childcon.conrelid, lockmode)?;
            // pkrelid passes through as-is: it identifies the root
            // referenced table.
            queue_fk_constraint_validation(mcx, wqueue, &childrel, pkrelid, childcon, lockmode)?;
            childrel.close(NoLock)?;
        }
    }

    pg_constraint::SetConstraintValidated(mcx, con.oid)
}

// RemoveInheritedConstraint (tablecmds.c): drop the per-partition constraint
// rows (and their triggers) hanging off a referenced-side clone.
fn remove_inherited_constraint<'mcx>(mcx: Mcx<'mcx>, conoid: Oid, conrelid: Oid) -> PgResult<()> {
    let con_rel =
        table::table_open(mcx, types_core::CONSTRAINT_RELATION_ID, types_rel::RowShareLock)?;
    let keys = [crate::alter::oid_scankey(
        pg_constraint::Anum_pg_constraint_conrelid as usize,
        conrelid,
    )];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        types_core::CONSTRAINT_RELID_TYPID_NAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let desc = con_rel.descr();
    let mut objs = catalog_dependency::ObjectAddresses::new();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let form = decode_fk_constraint_form(tup, desc);
        if form.conparentid != conoid {
            continue;
        }
        objs.add_exact_object_address(pg_depend::ObjectAddress::set(
            types_core::CONSTRAINT_RELATION_ID,
            form.oid,
        ));
        let n = pg_depend::deleteDependencyRecordsForSpecific(
            mcx,
            types_core::CONSTRAINT_RELATION_ID,
            form.oid,
            pg_depend::DependencyType::Internal.as_char(),
            types_core::CONSTRAINT_RELATION_ID,
            conoid,
        )?;
        debug_assert!(n == 1);

        let trig_rel = table::table_open(mcx, TriggerRelationId, types_rel::RowExclusiveLock)?;
        let keys2 = [crate::alter::oid_scankey(Anum_pg_trigger_tgconstraint, form.oid)];
        let mut scan2 = genam::systable_beginscan(
            mcx,
            &trig_rel,
            TriggerConstraintIndexId,
            true,
            None,
            &keys2,
        )?;
        let tdesc = trig_rel.descr();
        while let Some(trigtup) = genam::systable_getnext(mcx, &mut scan2)? {
            objs.add_exact_object_address(pg_depend::ObjectAddress::set(
                TriggerRelationId,
                getattr(trigtup, tdesc, Anum_pg_trigger_oid).as_oid(),
            ));
        }
        genam::systable_endscan(mcx, scan2)?;
        trig_rel.close(types_rel::RowExclusiveLock)?;
    }
    xact::CommandCounterIncrement()?;
    catalog_dependency::performMultipleDeletions(
        mcx,
        &objs,
        catalog_dependency::DropBehavior::DROP_RESTRICT,
        catalog_dependency::PERFORM_DELETION_INTERNAL,
    )?;
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::RowShareLock)
}

// DropForeignKeyConstraintTriggers (tablecmds.c): remove a constraint's RI
// triggers, severing their dependency records first.
fn drop_foreign_key_constraint_triggers<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
) -> PgResult<()> {
    let trig_rel = table::table_open(mcx, TriggerRelationId, types_rel::RowExclusiveLock)?;
    let keys = [crate::alter::oid_scankey(Anum_pg_trigger_tgconstraint, conoid)];
    let mut scan =
        genam::systable_beginscan(mcx, &trig_rel, TriggerConstraintIndexId, true, None, &keys)?;
    let desc = trig_rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        if getattr(tup, desc, Anum_pg_trigger_tgconstrrelid).as_oid() == InvalidOid {
            continue;
        }
        if conrelid != InvalidOid
            && getattr(tup, desc, Anum_pg_trigger_tgconstrrelid).as_oid() != conrelid
        {
            continue;
        }
        if confrelid != InvalidOid
            && getattr(tup, desc, Anum_pg_trigger_tgrelid).as_oid() != confrelid
        {
            continue;
        }
        debug_assert!(
            ri_fkey_trigger_type(getattr(tup, desc, Anum_pg_trigger_tgfoid).as_oid())
                != RI_TRIGGER_NONE
        );
        let trigoid = getattr(tup, desc, Anum_pg_trigger_oid).as_oid();
        // The dependency record binding trigger to constraint must go first
        // so the trigger can drop while the constraint stays.
        pg_depend::deleteDependencyRecordsFor(mcx, TriggerRelationId, trigoid, false)?;
        xact::CommandCounterIncrement()?;
        catalog_dependency::performDeletion(
            mcx,
            &pg_depend::ObjectAddress::set(TriggerRelationId, trigoid),
            catalog_dependency::DropBehavior::DROP_RESTRICT,
            0,
        )?;
        xact::CommandCounterIncrement()?;
    }
    genam::systable_endscan(mcx, scan)?;
    trig_rel.close(types_rel::RowExclusiveLock)
}

fn attnames_string_list<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &types_tuple::TupleDescData<'mcx>,
    attnums: &[i16],
) -> PgResult<NodeList<'mcx>> {
    let mut list = NodeList::nil();
    for &attnum in attnums {
        let att = desc.attr(attnum as usize - 1);
        let name = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
        let node = types_nodes::Node::mk(mcx, types_nodes::String { sval: str_in(mcx, name)? })?;
        list.lappend(mcx, node)?;
    }
    Ok(list)
}

fn clone_constraint_node<'mcx>(
    form: &FkConstraintForm,
    conname: Option<&'mcx str>,
    skip_validation: bool,
    fk_attrs: NodeList<'mcx>,
) -> Constraint<'mcx> {
    Constraint {
        contype: types_nodes::rawnodes::ConstrType::CONSTR_FOREIGN,
        conname,
        deferrable: form.condeferrable,
        initdeferred: form.condeferred,
        location: -1,
        pktable: None,
        fk_attrs,
        pk_attrs: NodeList::nil(),
        fk_matchtype: form.confmatchtype,
        fk_upd_action: form.confupdtype,
        fk_del_action: form.confdeltype,
        fk_del_set_cols: NodeList::nil(),
        old_conpfeqop: types_nodes::list::OidList::nil(),
        old_pktable_oid: InvalidOid,
        is_enforced: form.conenforced,
        skip_validation,
        initially_valid: form.convalidated,
        ..Default::default()
    }
}

// CloneForeignKeyConstraints (tablecmds.c): clone FKs from a partitioned
// table to a newly acquired partition.
pub(crate) fn CloneForeignKeyConstraints<'mcx>(
    mcx: Mcx<'mcx>,
    mut wqueue: Option<&mut crate::alter::Wqueue<'mcx>>,
    parent_rel: &Relation<'mcx>,
    partition_rel: &Relation<'mcx>,
) -> PgResult<()> {
    debug_assert!(parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);
    clone_fk_referencing(mcx, wqueue.as_deref_mut(), parent_rel, partition_rel)?;
    clone_fk_referenced(mcx, parent_rel, partition_rel)
}

// CloneFkReferenced (tablecmds.c): clone constraints that have the parent on
// the referenced side.
fn clone_fk_referenced<'mcx>(
    mcx: Mcx<'mcx>,
    parent_rel: &Relation<'mcx>,
    partition_rel: &Relation<'mcx>,
) -> PgResult<()> {
    // Two steps so a constraint whose parent is also being cloned is skipped
    // regardless of scan order.
    let mut clone: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    {
        let con_rel = table::table_open(
            mcx,
            types_core::CONSTRAINT_RELATION_ID,
            types_rel::RowShareLock,
        )?;
        let keys = [
            crate::alter::oid_scankey(
                pg_constraint::Anum_pg_constraint_confrelid as usize,
                parent_rel.rd_id,
            ),
            char_scankey(
                pg_constraint::Anum_pg_constraint_contype as usize,
                pg_constraint::CONSTRAINT_FOREIGN,
            ),
        ];
        let mut scan = genam::systable_beginscan(mcx, &con_rel, InvalidOid, true, None, &keys)?;
        let desc = con_rel.descr();
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            clone.push(getattr(tup, desc, pg_constraint::Anum_pg_constraint_oid as usize).as_oid());
        }
        genam::systable_endscan(mcx, scan)?;
        con_rel.close(types_rel::RowShareLock)?;
    }

    let attmap = tupdesc::build_attrmap_by_name(mcx, &partition_rel.rd_att, &parent_rel.rd_att)?;
    for &constr_oid in clone.iter() {
        let (form, arrays) = read_fk_constraint(mcx, constr_oid)?;
        if form.conparentid != InvalidOid && clone.contains(&form.conparentid) {
            continue;
        }
        // Same lock level that CreateTrigger will acquire.
        let fk_rel = table::table_open(mcx, form.conrelid, ShareRowExclusiveLock)?;
        let index_oid = form.conindid;
        let numfks = arrays.numfks;
        let mut mapped_confkey = [0i16; INDEX_MAX_KEYS as usize];
        for i in 0..numfks {
            mapped_confkey[i] = attmap[arrays.confkey[i] as usize - 1];
        }

        let conname = str_in(mcx, form.name_str())?;
        let fk_attrs = attnames_string_list(mcx, &fk_rel.rd_att, &arrays.conkey[..numfks])?;
        let fkconstraint = clone_constraint_node(&form, Some(conname), false, fk_attrs);

        let part_index_id = pg_inherits::index_get_partition(mcx, partition_rel.rd_id, index_oid)?;
        if part_index_id == InvalidOid {
            panic!("index for {index_oid} not found in partition {}", partition_rel.name());
        }

        // The constraint's own action triggers parent the equivalents that
        // addFkRecurseReferenced creates on the partition.
        let (mut delete_trigger_oid, mut update_trigger_oid) = (InvalidOid, InvalidOid);
        if form.conenforced {
            (delete_trigger_oid, update_trigger_oid) =
                get_foreign_key_action_triggers(mcx, constr_oid, form.confrelid, form.conrelid)?;
        }

        let (child_constr, _) = add_fk_constraint(
            mcx,
            AddFkSide::ReferencedSide,
            conname,
            &fkconstraint,
            &fk_rel,
            partition_rel,
            part_index_id,
            constr_oid,
            numfks,
            &mapped_confkey,
            &arrays.conkey,
            &arrays.pf_eq_oprs,
            &arrays.pp_eq_oprs,
            &arrays.ff_eq_oprs,
            &arrays.fk_del_set_cols[..arrays.num_fk_del_set_cols],
            false,
            form.conperiod,
        )?;
        add_fk_recurse_referenced(
            mcx,
            conname,
            &fkconstraint,
            &fk_rel,
            partition_rel,
            part_index_id,
            child_constr,
            numfks,
            &mapped_confkey,
            &arrays.conkey,
            &arrays.pf_eq_oprs,
            &arrays.pp_eq_oprs,
            &arrays.ff_eq_oprs,
            &arrays.fk_del_set_cols[..arrays.num_fk_del_set_cols],
            true,
            delete_trigger_oid,
            update_trigger_oid,
            form.conperiod,
        )?;
        fk_rel.close(NoLock)?;
    }
    Ok(())
}

// CloneFkReferencing (tablecmds.c): clone (or reparent) each FK of the parent
// onto the partition.
fn clone_fk_referencing<'mcx>(
    mcx: Mcx<'mcx>,
    mut wqueue: Option<&mut crate::alter::Wqueue<'mcx>>,
    parent_rel: &Relation<'mcx>,
    part_rel: &Relation<'mcx>,
) -> PgResult<()> {
    let parent_fks = rel_fk_constraint_list(mcx, parent_rel.rd_id)?;
    let mut clone: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    for fk in parent_fks.iter() {
        // A table referenced by this partitioned table cannot become one of
        // its partitions; that dodges pg_constraint/pg_trigger complexities
        // on ATTACH/DETACH.
        if fk.confrelid == part_rel.rd_id {
            let name = lsyscache::get_constraint_name(mcx, fk.conoid)?
                .unwrap_or_else(|| panic!("cache lookup failed for constraint {}", fk.conoid));
            return Err(err(
                format!(
                    "cannot attach table \"{}\" as a partition because it is referenced by foreign key \"{}\"",
                    part_rel.name(),
                    name.as_str()
                ),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        clone.push(fk.conoid);
    }

    // Silently do nothing when there is nothing to do; this avoids a
    // spurious error for foreign tables.
    if clone.is_empty() {
        return Ok(());
    }
    if part_rel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
        return Err(err(
            "foreign key constraints are not supported on foreign tables".into(),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }

    let attmap = tupdesc::build_attrmap_by_name(mcx, &part_rel.rd_att, &parent_rel.rd_att)?;
    let part_fks = rel_fk_constraint_list(mcx, part_rel.rd_id)?;

    for &parent_constr_oid in clone.iter() {
        let (form, arrays) = read_fk_constraint(mcx, parent_constr_oid)?;
        if form.conparentid != InvalidOid && clone.contains(&form.conparentid) {
            continue;
        }

        // Prevent concurrent deletions; a partitioned pkrel means locking
        // every partition.
        let pkrel = table::table_open(mcx, form.confrelid, ShareRowExclusiveLock)?;
        if pkrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            pg_inherits::find_all_inheritors(mcx, pkrel.rd_id, ShareRowExclusiveLock)?;
        }

        let numfks = arrays.numfks;
        let mut mapped_conkey = [0i16; INDEX_MAX_KEYS as usize];
        for i in 0..numfks {
            mapped_conkey[i] = attmap[arrays.conkey[i] as usize - 1];
        }

        let (mut insert_trigger_oid, mut update_trigger_oid) = (InvalidOid, InvalidOid);
        if form.conenforced {
            (insert_trigger_oid, update_trigger_oid) = get_foreign_key_check_triggers(
                mcx,
                form.oid,
                form.confrelid,
                form.conrelid,
            )?;
        }

        let mut attached = false;
        for fk in part_fks.iter() {
            if try_attach_partition_foreign_key(
                mcx,
                wqueue.as_deref_mut(),
                fk,
                part_rel,
                parent_constr_oid,
                numfks,
                &mapped_conkey,
                &arrays.confkey,
                &arrays.pf_eq_oprs,
                insert_trigger_oid,
                update_trigger_oid,
            )? {
                attached = true;
                break;
            }
        }
        if attached {
            pkrel.close(NoLock)?;
            continue;
        }

        let conname = str_in(mcx, form.name_str())?;
        let fk_attrs = attnames_string_list(mcx, &part_rel.rd_att, &mapped_conkey[..numfks])?;
        let fkconstraint = clone_constraint_node(&form, None, false, fk_attrs);

        let index_oid = form.conindid;
        let (child_constr, chosen_conname) = add_fk_constraint(
            mcx,
            AddFkSide::ReferencingSide,
            conname,
            &fkconstraint,
            part_rel,
            &pkrel,
            index_oid,
            parent_constr_oid,
            numfks,
            &arrays.confkey,
            &mapped_conkey,
            &arrays.pf_eq_oprs,
            &arrays.pp_eq_oprs,
            &arrays.ff_eq_oprs,
            &arrays.fk_del_set_cols[..arrays.num_fk_del_set_cols],
            false,
            form.conperiod,
        )?;
        add_fk_recurse_referencing(
            mcx,
            wqueue.as_deref_mut(),
            chosen_conname,
            &fkconstraint,
            part_rel,
            &pkrel,
            index_oid,
            child_constr,
            numfks,
            &arrays.confkey,
            &mapped_conkey,
            &arrays.pf_eq_oprs,
            &arrays.pp_eq_oprs,
            &arrays.ff_eq_oprs,
            &arrays.fk_del_set_cols[..arrays.num_fk_del_set_cols],
            false,
            types_rel::AccessExclusiveLock,
            insert_trigger_oid,
            update_trigger_oid,
            form.conperiod,
        )?;
        pkrel.close(NoLock)?;
    }
    Ok(())
}

fn char_scankey(attno: usize, value: u8) -> types_scan::scankey::ScanKeyData {
    use types_scan::scankey::ScanKeyData;
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_CHAREQ)
        .unwrap_or_else(|e| panic!("fmgr_info(chareq) failed: {e:?}"));
    key.sk_argument = datum::Datum::from_i8(value as i8);
    key
}

// The DetachPartitionFinalize FK slice (tablecmds.c:21122-21283): inherited
// FKs on the detached partition become standalone constraints with their own
// referenced-side action triggers.
pub(crate) fn detach_partition_finalize_fks<'mcx>(
    mcx: Mcx<'mcx>,
    part_rel: &Relation<'mcx>,
) -> PgResult<()> {
    let fks = rel_fk_constraint_list(mcx, part_rel.rd_id)?;

    // A partition with an FK to a partitioned table carries one row per
    // referenced partition; only the topmost row (whose parent is not also
    // ours) detaches.
    let mut fkoids: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    for fk in fks.iter() {
        fkoids.push(fk.conoid);
    }

    for fk in fks.iter() {
        let (form, arrays) = read_fk_constraint(mcx, fk.conoid)?;
        if form.contype != pg_constraint::CONSTRAINT_FOREIGN
            || form.conparentid == InvalidOid
            || fkoids.contains(&form.conparentid)
        {
            continue;
        }

        pg_constraint::ConstraintSetParentConstraint(mcx, fk.conoid, InvalidOid, InvalidOid)?;

        if fk.conenforced {
            let (insert_trigger_oid, update_trigger_oid) =
                get_foreign_key_check_triggers(mcx, fk.conoid, fk.confrelid, fk.conrelid)?;
            debug_assert!(insert_trigger_oid != InvalidOid);
            trigger::TriggerSetParentTrigger(mcx, insert_trigger_oid, InvalidOid, part_rel.rd_id)?;
            debug_assert!(update_trigger_oid != InvalidOid);
            trigger::TriggerSetParentTrigger(mcx, update_trigger_oid, InvalidOid, part_rel.rd_id)?;
        }

        // The pg_constraint row already exists, so no addFkConstraint here;
        // only the action triggers (recursing over referenced partitions).
        let numfks = arrays.numfks;
        let conname = str_in(mcx, form.name_str())?;
        let fk_attrs = attnames_string_list(mcx, &part_rel.rd_att, &arrays.conkey[..numfks])?;
        let fkconstraint = clone_constraint_node(&form, Some(conname), true, fk_attrs);

        let refd_rel = table::table_open(mcx, fk.confrelid, ShareRowExclusiveLock)?;
        add_fk_recurse_referenced(
            mcx,
            conname,
            &fkconstraint,
            part_rel,
            &refd_rel,
            form.conindid,
            fk.conoid,
            numfks,
            &arrays.confkey,
            &arrays.conkey,
            &arrays.pf_eq_oprs,
            &arrays.pp_eq_oprs,
            &arrays.ff_eq_oprs,
            &arrays.fk_del_set_cols[..arrays.num_fk_del_set_cols],
            true,
            InvalidOid,
            InvalidOid,
            form.conperiod,
        )?;
        refd_rel.close(NoLock)?;
    }
    Ok(())
}

// The DetachPartitionFinalize referenced-side cleanup (tablecmds.c:21285-21305)
// for one parented inbound constraint: the partition leaves the constraint's
// key space, so its sub-constraint row is dropped.
pub(crate) fn detach_referenced_fk_sub_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    constr_oid: Oid,
) -> PgResult<()> {
    pg_constraint::ConstraintSetParentConstraint(mcx, constr_oid, InvalidOid, InvalidOid)?;
    pg_depend::deleteDependencyRecordsForClass(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        constr_oid,
        types_core::CONSTRAINT_RELATION_ID,
        pg_depend::DependencyType::Internal,
    )?;
    xact::CommandCounterIncrement()?;
    catalog_dependency::performDeletion(
        mcx,
        &pg_depend::ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, constr_oid),
        catalog_dependency::DropBehavior::DROP_RESTRICT,
        0,
    )
}

// The ATDetachCheckNoForeignKeyRefs loop body (tablecmds.c:21995-22040): run
// RI_PartitionRemove_Check for one inbound parented constraint.
pub(crate) fn partition_remove_check<'mcx>(
    mcx: Mcx<'mcx>,
    partition: &Relation<'mcx>,
    constr_oid: Oid,
) -> PgResult<()> {
    let (form, _) = read_fk_constraint(mcx, constr_oid)?;
    debug_assert!(form.conparentid != InvalidOid);
    debug_assert!(form.confrelid == partition.rd_id);

    // Prevent data changes into the referencing table until commit.
    let rel = table::table_open(mcx, form.conrelid, types_rel::ShareLock)?;

    let trig = types_trigger::Trigger {
        tgoid: InvalidOid,
        tgname: mcx::PgString::from_str_in(form.name_str(), mcx)?,
        tgfoid: InvalidOid,
        tgtype: 0,
        tgenabled: types_trigger::TRIGGER_FIRES_ON_ORIGIN,
        tgisinternal: true,
        tgisclone: false,
        tgconstrrelid: partition.rd_id,
        tgconstrindid: form.conindid,
        tgconstraint: form.oid,
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

    ri_triggers_seams::ri_partition_remove_check::call(mcx, &trig, &rel, partition)?;
    rel.close(NoLock)
}

// ALTER TABLE .. ALTER CONSTRAINT (tablecmds.c:12198-12920):
// ATExecAlterConstraint + the enforceability/deferrability/inheritability
// legs and their conparentid-driven recursion.

const Anum_pg_trigger_tgdeferrable: usize = 12;
const Anum_pg_trigger_tginitdeferred: usize = 13;

pub(crate) fn ATExecAlterConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut crate::alter::Wqueue<'mcx>,
    rel: &Relation<'mcx>,
    cmdcon: &types_nodes::parsenodes::ATAlterConstraint<'_>,
    recurse: bool,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<()> {
    let relname = rel.name().to_string();
    // Altering ONLY a partitioned table would desynchronize the children.
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE && !recurse {
        return Err(Box::new(
            PgError::new(ERROR, "constraint must be altered in child tables too".to_string())
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION)
                .with_hint("Do not specify the ONLY keyword.".to_string()),
        ));
    }
    let conname = cmdcon.conname.expect("ATAlterConstraint conname");
    let Some(con) = pg_constraint::findConstraintByName(mcx, rel.rd_id, conname)? else {
        return Err(err(
            format!("constraint \"{conname}\" of relation \"{relname}\" does not exist"),
            ERRCODE_UNDEFINED_OBJECT,
        ));
    };
    if cmdcon.alterDeferrability && con.contype != pg_constraint::CONSTRAINT_FOREIGN {
        return Err(err(
            format!(
                "constraint \"{conname}\" of relation \"{relname}\" is not a foreign key \
                 constraint"
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    if cmdcon.alterEnforceability && con.contype != pg_constraint::CONSTRAINT_FOREIGN {
        return Err(err(
            format!(
                "cannot alter enforceability of constraint \"{conname}\" of relation \
                 \"{relname}\""
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    if cmdcon.alterInheritability && con.contype != pg_constraint::CONSTRAINT_NOTNULL {
        return Err(err(
            format!(
                "constraint \"{conname}\" of relation \"{relname}\" is not a not-null constraint"
            ),
            ERRCODE_WRONG_OBJECT_TYPE,
        ));
    }
    // Refuse to modify inheritability of inherited constraints.
    if cmdcon.alterInheritability && cmdcon.noinherit && con.coninhcount > 0 {
        return Err(err(
            format!(
                "cannot alter inherited constraint \"{}\" on relation \"{relname}\"",
                con.name_str()
            ),
            types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
        ));
    }
    // Non-topmost constraints leave triggers untouched and confuse pg_dump;
    // tell the user to alter the topmost ancestor instead.
    if con.conparentid != InvalidOid {
        let mut parent = con.conparentid;
        let mut ancestor = Option::None;
        loop {
            let Some((grandparent, name, conrelid)) = constraint_parent_probe(mcx, parent)? else {
                break;
            };
            if grandparent == InvalidOid {
                let table = lsyscache::relation::get_rel_name(mcx, conrelid)?
                    .map(|s| s.as_str().to_string());
                ancestor = table.map(|t| (name, t));
                break;
            }
            parent = grandparent;
        }
        let mut e = PgError::new(
            ERROR,
            format!("cannot alter constraint \"{conname}\" on relation \"{relname}\""),
        )
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint("You may alter the constraint it derives from instead.".to_string());
        if let Some((aname, atable)) = ancestor {
            e = e.with_detail(format!(
                "Constraint \"{conname}\" is derived from constraint \"{aname}\" of relation \
                 \"{atable}\"."
            ));
        }
        return Err(Box::new(e));
    }

    // ATExecAlterConstraintInternal: enforceability change re-creates or
    // drops triggers (adjusting deferrability on the way); an explicit
    // deferrability change patches the existing triggers instead.
    let mut otherrelids: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    if cmdcon.alterEnforceability {
        let (form, _) = read_fk_constraint(mcx, con.oid)?;
        alter_constr_enforceability(
            mcx,
            wqueue,
            cmdcon,
            form.conrelid,
            form.confrelid,
            &form,
            lockmode,
            InvalidOid,
            InvalidOid,
            InvalidOid,
            InvalidOid,
        )?;
    } else if cmdcon.alterDeferrability {
        let (form, _) = read_fk_constraint(mcx, con.oid)?;
        if alter_constr_deferrability(
            mcx,
            wqueue,
            cmdcon,
            rel,
            &form,
            recurse,
            &mut otherrelids,
            lockmode,
        )? {
            // Relations owning affected triggers also need a relcache flush.
            for &relid in otherrelids.iter() {
                inval::invalidate::CacheInvalidateRelcacheByRelid(relid)?;
            }
        }
    }
    if cmdcon.alterInheritability {
        alter_constr_inheritability(mcx, wqueue, cmdcon, rel, &con, lockmode)?;
    }
    Ok(())
}

// The topmost-ancestor walk of ATExecAlterConstraint: (conparentid, conname,
// conrelid) for one constraint OID.
fn constraint_parent_probe<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
) -> PgResult<Option<(Oid, String, Oid)>> {
    let con_rel = table::table_open(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        types_rel::AccessShareLock,
    )?;
    let keys = [crate::alter::oid_scankey(pg_constraint::Anum_pg_constraint_oid as usize, conoid)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        types_core::CONSTRAINT_OID_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let desc = con_rel.descr();
    let found = genam::systable_getnext(mcx, &mut scan)?.map(|tup| {
        let form = decode_fk_constraint_form(tup, desc);
        (form.conparentid, form.name_str().to_string(), form.conrelid)
    });
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::AccessShareLock)?;
    Ok(found)
}

// ATExecAlterConstrEnforceability (tablecmds.c): flip conenforced and create
// or drop the constraint's RI triggers, recursing over child constraints.
#[allow(clippy::too_many_arguments)]
fn alter_constr_enforceability<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut crate::alter::Wqueue<'mcx>,
    cmdcon: &types_nodes::parsenodes::ATAlterConstraint<'_>,
    fkrelid: Oid,
    pkrelid: Oid,
    con: &FkConstraintForm,
    lockmode: types_rel::LOCKMODE,
    referenced_parent_del_trigger: Oid,
    referenced_parent_upd_trigger: Oid,
    referencing_parent_ins_trigger: Oid,
    referencing_parent_upd_trigger: Oid,
) -> PgResult<bool> {
    stack_depth::check_stack_depth()?;
    debug_assert!(cmdcon.alterEnforceability);
    debug_assert!(con.contype == pg_constraint::CONSTRAINT_FOREIGN);
    let rel = table::table_open(mcx, con.conrelid, lockmode)?;

    let mut changed = false;
    if con.conenforced != cmdcon.is_enforced {
        alter_constr_update_constraint_entry(mcx, cmdcon, con.oid, con.conrelid)?;
        changed = true;
    }
    if !cmdcon.is_enforced {
        // Children first: their triggers depend on the parent's.
        if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
            || lsyscache::relation::get_rel_relkind(con.confrelid)? as u8
                == RELKIND_PARTITIONED_TABLE
        {
            alter_constr_enforceability_recurse(
                mcx,
                wqueue,
                cmdcon,
                fkrelid,
                pkrelid,
                con,
                lockmode,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                InvalidOid,
            )?;
        }
        drop_foreign_key_constraint_triggers(mcx, con.oid, InvalidOid, InvalidOid)?;
    } else if changed {
        // Minimal Constraint node carrying what trigger creation reads.
        // 18.3 leaves deferrable/initdeferred at makeNode zero here, so a
        // re-ENFORCED constraint's triggers come back NOT DEFERRABLE.
        let fkconstraint = Constraint {
            contype: types_nodes::rawnodes::ConstrType::CONSTR_FOREIGN,
            conname: Some(str_in(mcx, con.name_str())?),
            fk_matchtype: con.confmatchtype,
            fk_upd_action: con.confupdtype,
            fk_del_action: con.confdeltype,
            location: -1,
            ..Default::default()
        };
        let mut referenced_del_trigger = InvalidOid;
        let mut referenced_upd_trigger = InvalidOid;
        let mut referencing_ins_trigger = InvalidOid;
        let mut referencing_upd_trigger = InvalidOid;
        if con.conrelid == fkrelid {
            (referenced_del_trigger, referenced_upd_trigger) =
                create_foreign_key_action_triggers(
                    mcx,
                    con.conrelid,
                    con.confrelid,
                    &fkconstraint,
                    con.oid,
                    con.conindid,
                    referenced_parent_del_trigger,
                    referenced_parent_upd_trigger,
                )?;
        }
        if con.confrelid == pkrelid {
            (referencing_ins_trigger, referencing_upd_trigger) =
                create_foreign_key_check_triggers(
                    mcx,
                    con.conrelid,
                    pkrelid,
                    &fkconstraint,
                    con.oid,
                    con.conindid,
                    referencing_parent_ins_trigger,
                    referencing_parent_upd_trigger,
                )?;
        }
        // Phase 3 must verify existing rows; leaf partitions only, and only
        // for the row that is not an action-trigger support row.
        if rel.rd_rel.relkind == RELKIND_RELATION && con.confrelid == pkrelid {
            let tabidx = crate::alter::ATGetQueueEntry(mcx, wqueue, &rel);
            wqueue[tabidx].fk_checks.push(FkValidateItem {
                conname: str_in(mcx, con.name_str())?,
                refrelid: con.confrelid,
                refindid: con.conindid,
                conid: con.oid,
                hasperiod: con.conperiod,
            });
        }
        if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
            || lsyscache::relation::get_rel_relkind(con.confrelid)? as u8
                == RELKIND_PARTITIONED_TABLE
        {
            alter_constr_enforceability_recurse(
                mcx,
                wqueue,
                cmdcon,
                fkrelid,
                pkrelid,
                con,
                lockmode,
                referenced_del_trigger,
                referenced_upd_trigger,
                referencing_ins_trigger,
                referencing_upd_trigger,
            )?;
        }
    }
    rel.close(NoLock)?;
    Ok(changed)
}

// ATExecAlterConstrDeferrability (tablecmds.c): flip condeferrable/
// condeferred and patch the constraint's triggers; recurse even when this
// level already matched so locally-altered descendants get fixed.
#[allow(clippy::too_many_arguments)]
fn alter_constr_deferrability<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut crate::alter::Wqueue<'mcx>,
    cmdcon: &types_nodes::parsenodes::ATAlterConstraint<'_>,
    rel: &Relation<'mcx>,
    con: &FkConstraintForm,
    recurse: bool,
    otherrelids: &mut mcx::PgVec<'mcx, Oid>,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<bool> {
    stack_depth::check_stack_depth()?;
    debug_assert!(cmdcon.alterDeferrability);
    debug_assert!(con.contype == pg_constraint::CONSTRAINT_FOREIGN);
    let mut changed = false;
    if con.condeferrable != cmdcon.deferrable || con.condeferred != cmdcon.initdeferred {
        alter_constr_update_constraint_entry(mcx, cmdcon, con.oid, con.conrelid)?;
        changed = true;
        alter_constr_trigger_deferrability(
            mcx,
            con.oid,
            rel,
            cmdcon.deferrable,
            cmdcon.initdeferred,
            otherrelids,
        )?;
    }
    if recurse
        && changed
        && (rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
            || lsyscache::relation::get_rel_relkind(con.confrelid)? as u8
                == RELKIND_PARTITIONED_TABLE)
    {
        alter_constr_deferrability_recurse(
            mcx, wqueue, cmdcon, con, recurse, otherrelids, lockmode,
        )?;
    }
    Ok(changed)
}

// ATExecAlterConstrInheritability (tablecmds.c): flip connoinherit on a
// not-null constraint and adjust the immediate children only.
fn alter_constr_inheritability<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut crate::alter::Wqueue<'mcx>,
    cmdcon: &types_nodes::parsenodes::ATAlterConstraint<'_>,
    rel: &Relation<'mcx>,
    con: &pg_constraint::ConShape,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<bool> {
    debug_assert!(cmdcon.alterInheritability);
    debug_assert!(con.contype == pg_constraint::CONSTRAINT_NOTNULL);
    if cmdcon.noinherit == con.connoinherit {
        return Ok(false);
    }
    alter_constr_update_constraint_entry(mcx, cmdcon, con.oid, rel.rd_id)?;
    xact::CommandCounterIncrement()?;

    let col_name = lsyscache::attribute::get_attname(mcx, rel.rd_id, con.notnull_attnum, false)?
        .expect("not-null constraint column")
        .as_str()
        .to_string();
    let children = pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?;
    for &childoid in children.iter() {
        if cmdcon.noinherit {
            let childcon = crate::alter::find_notnull_constraint_by_colname(
                mcx, childoid, &col_name,
            )?
            .unwrap_or_else(|| {
                panic!(
                    "cache lookup failed for not-null constraint on column \"{col_name}\" of \
                     relation {childoid}"
                )
            });
            debug_assert!(childcon.coninhcount > 0);
            pg_constraint::update_constraint_fields(
                mcx,
                childcon.oid,
                &[
                    (
                        pg_constraint::Anum_pg_constraint_coninhcount,
                        datum::Datum::from_i16(childcon.coninhcount - 1),
                    ),
                    (
                        pg_constraint::Anum_pg_constraint_conislocal,
                        datum::Datum::from_bool(true),
                    ),
                ],
            )?;
        } else {
            let childrel = table::table_open(mcx, childoid, NoLock)?;
            // DIVERGENCE: C only CCIs when SetNotNull reports a change; the
            // port's ATExecSetNotNull has no address result, so CCI always.
            crate::alter::ATExecSetNotNull(
                mcx,
                wqueue,
                &childrel,
                Some(con.name_str()),
                &col_name,
                true,
                true,
                lockmode,
            )?;
            xact::CommandCounterIncrement()?;
            childrel.close(NoLock)?;
        }
    }
    Ok(true)
}

// AlterConstrTriggerDeferrability (tablecmds.c): patch tgdeferrable/
// tginitdeferred on the RI_FKey_noaction_{del,upd} / RI_FKey_check_{ins,upd}
// triggers of one constraint.
fn alter_constr_trigger_deferrability<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
    rel: &Relation<'mcx>,
    deferrable: bool,
    initdeferred: bool,
    otherrelids: &mut mcx::PgVec<'mcx, Oid>,
) -> PgResult<()> {
    use mcx::PgVec;
    let trig_rel = table::table_open(mcx, TriggerRelationId, types_rel::RowExclusiveLock)?;
    let keys = [crate::alter::oid_scankey(Anum_pg_trigger_tgconstraint, conoid)];
    let mut scan =
        genam::systable_beginscan(mcx, &trig_rel, TriggerConstraintIndexId, true, None, &keys)?;
    let desc = trig_rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tgrelid = getattr(tup, desc, Anum_pg_trigger_tgrelid).as_oid();
        // Conservatively force a relcache inval on every other rel involved.
        if tgrelid != rel.rd_id && !otherrelids.contains(&tgrelid) {
            otherrelids.push(tgrelid);
        }
        // Only the deferrable trigger flavors change; see
        // createForeignKeyActionTriggers / CreateFKCheckTrigger.
        let tgfoid = getattr(tup, desc, Anum_pg_trigger_tgfoid).as_oid();
        if tgfoid != F_RI_FKEY_NOACTION_DEL
            && tgfoid != F_RI_FKEY_NOACTION_UPD
            && tgfoid != F_RI_FKEY_CHECK_INS
            && tgfoid != F_RI_FKEY_CHECK_UPD
        {
            continue;
        }
        let natts = desc.natts as usize;
        let mut values: PgVec<'_, datum::Datum> =
            mcx::vec_from_elem_in(mcx, datum::Datum::null(), natts);
        let nulls: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, false, natts);
        let mut replace: PgVec<'_, bool> = mcx::vec_from_elem_in(mcx, false, natts);
        values[Anum_pg_trigger_tgdeferrable - 1] = datum::Datum::from_bool(deferrable);
        replace[Anum_pg_trigger_tgdeferrable - 1] = true;
        values[Anum_pg_trigger_tginitdeferred - 1] = datum::Datum::from_bool(initdeferred);
        replace[Anum_pg_trigger_tginitdeferred - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
        let otid = tup.t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, &trig_rel, &otid, &mut newtup)?;
    }
    genam::systable_endscan(mcx, scan)?;
    trig_rel.close(types_rel::RowExclusiveLock)
}

// AlterConstrEnforceabilityRecurse (tablecmds.c): children via conparentid.
#[allow(clippy::too_many_arguments)]
fn alter_constr_enforceability_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut crate::alter::Wqueue<'mcx>,
    cmdcon: &types_nodes::parsenodes::ATAlterConstraint<'_>,
    fkrelid: Oid,
    pkrelid: Oid,
    con: &FkConstraintForm,
    lockmode: types_rel::LOCKMODE,
    referenced_parent_del_trigger: Oid,
    referenced_parent_upd_trigger: Oid,
    referencing_parent_ins_trigger: Oid,
    referencing_parent_upd_trigger: Oid,
) -> PgResult<()> {
    for childcon in constraint_children(mcx, con.oid)?.iter() {
        alter_constr_enforceability(
            mcx,
            wqueue,
            cmdcon,
            fkrelid,
            pkrelid,
            childcon,
            lockmode,
            referenced_parent_del_trigger,
            referenced_parent_upd_trigger,
            referencing_parent_ins_trigger,
            referencing_parent_upd_trigger,
        )?;
    }
    Ok(())
}

// AlterConstrDeferrabilityRecurse (tablecmds.c): children via conparentid.
#[allow(clippy::too_many_arguments)]
fn alter_constr_deferrability_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut crate::alter::Wqueue<'mcx>,
    cmdcon: &types_nodes::parsenodes::ATAlterConstraint<'_>,
    con: &FkConstraintForm,
    recurse: bool,
    otherrelids: &mut mcx::PgVec<'mcx, Oid>,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<()> {
    for childcon in constraint_children(mcx, con.oid)?.iter() {
        let childrel = table::table_open(mcx, childcon.conrelid, lockmode)?;
        alter_constr_deferrability(
            mcx, wqueue, cmdcon, &childrel, childcon, recurse, otherrelids, lockmode,
        )?;
        childrel.close(NoLock)?;
    }
    Ok(())
}

// The conparentid scan both Recurse helpers share; decoded before recursing
// so no scan stays open across the child work.
fn constraint_children<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
) -> PgResult<mcx::PgVec<'mcx, FkConstraintForm>> {
    let con_rel = table::table_open(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        types_rel::RowExclusiveLock,
    )?;
    let keys = [crate::alter::oid_scankey(
        pg_constraint::Anum_pg_constraint_conparentid as usize,
        conoid,
    )];
    let mut scan =
        genam::systable_beginscan(mcx, &con_rel, ConstraintParentIndexId, true, None, &keys)?;
    let desc = con_rel.descr();
    let mut children: mcx::PgVec<'mcx, FkConstraintForm> = mcx::PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        children.push(decode_fk_constraint_form(tup, desc));
    }
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::RowExclusiveLock)?;
    Ok(children)
}

// AlterConstrUpdateConstraintEntry (tablecmds.c): apply the requested flag
// changes to the pg_constraint row and flush the owning rel's relcache.
fn alter_constr_update_constraint_entry<'mcx>(
    mcx: Mcx<'mcx>,
    cmdcon: &types_nodes::parsenodes::ATAlterConstraint<'_>,
    conoid: Oid,
    conrelid: Oid,
) -> PgResult<()> {
    debug_assert!(
        cmdcon.alterEnforceability || cmdcon.alterDeferrability || cmdcon.alterInheritability
    );
    let mut fields: [(types_core::AttrNumber, datum::Datum); 5] =
        [(0, datum::Datum::null()); 5];
    let mut n = 0;
    let mut push = |anum, v| {
        fields[n] = (anum, v);
        n += 1;
    };
    if cmdcon.alterEnforceability {
        push(
            pg_constraint::Anum_pg_constraint_conenforced,
            datum::Datum::from_bool(cmdcon.is_enforced),
        );
        // convalidated tracks enforcement: NOT ENFORCED rows read as not
        // validated, re-ENFORCED rows are validated by the Phase-3 recheck.
        push(
            pg_constraint::Anum_pg_constraint_convalidated,
            datum::Datum::from_bool(cmdcon.is_enforced),
        );
    }
    if cmdcon.alterDeferrability {
        push(
            pg_constraint::Anum_pg_constraint_condeferrable,
            datum::Datum::from_bool(cmdcon.deferrable),
        );
        push(
            pg_constraint::Anum_pg_constraint_condeferred,
            datum::Datum::from_bool(cmdcon.initdeferred),
        );
    }
    if cmdcon.alterInheritability {
        push(
            pg_constraint::Anum_pg_constraint_connoinherit,
            datum::Datum::from_bool(cmdcon.noinherit),
        );
    }
    let n_final = n;
    pg_constraint::update_constraint_fields(mcx, conoid, &fields[..n_final])?;
    inval::invalidate::CacheInvalidateRelcacheByRelid(conrelid)
}
