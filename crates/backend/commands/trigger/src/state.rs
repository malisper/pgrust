// AfterTriggerSetState (trigger.c): SET CONSTRAINTS. LOUD: catalog-qualified
// constraint names (cross-database check needs get_database_name).
use datum::Datum;
use mcx::Mcx;
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use types_core::{
    AttrNumber, Oid, RegProcedure, CONSTRAINT_NAME_NSP_INDEX_ID, CONSTRAINT_RELATION_ID,
    NAMEDATALEN,
};
use types_error::{
    PgError, PgResult, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::rawnodes::ConstraintsSetStmt;
use types_rel::AccessShareLock;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use crate::catalog::TRIGGER_RELATION_ID;
use crate::queue;

const CONSTRAINT_PARENT_INDEX_ID: Oid = 2579;
const TRIGGER_CONSTRAINT_INDEX_ID: Oid = 2699;

const Anum_pg_constraint_oid: i32 = 1;
const Anum_pg_constraint_conname: AttrNumber = 2;
const Anum_pg_constraint_connamespace: AttrNumber = 3;
const Anum_pg_constraint_condeferrable: i32 = 5;
const Anum_pg_constraint_conparentid: AttrNumber = 12;
const Anum_pg_trigger_oid: i32 = 1;
const Anum_pg_trigger_tgconstraint: AttrNumber = 11;
const Anum_pg_trigger_tgdeferrable: i32 = 12;

#[track_caller]
#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

fn mk_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "constraint name overflows NAMEDATALEN: {name:?}");
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

pub fn AfterTriggerSetState<'mcx>(mcx: Mcx<'mcx>, stmt: &ConstraintsSetStmt<'_>) -> PgResult<()> {
    if stmt.constraints.is_nil() {
        queue::with_con_state(|state| {
            state.trigstates.clear();
            state.all_isset = true;
            state.all_isdeferred = stmt.deferred;
        });
    } else {
        let mut conoidlist: Vec<Oid> = Vec::new();
        let conrel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
        for lc in stmt.constraints.iter() {
            let constraint = lc.as_range_var().expect("SET CONSTRAINTS name is a RangeVar");
            let conname = constraint.relname.expect("RangeVar.relname");
            if constraint.catalogname.is_some() {
                panic!("AfterTriggerSetState (trigger.c): catalog-qualified constraint name");
            }
            let namespacelist: mcx::PgVec<'mcx, Oid> = match constraint.schemaname {
                Some(schema) => {
                    let nsp = catalog_namespace::LookupExplicitNamespace(schema, false)?;
                    let mut v: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 1)?;
                    v.push(nsp);
                    v
                }
                None => catalog_namespace::fetch_search_path(mcx, true)?,
            };
            let mut found = false;
            let cname = name_arg(mcx, conname)?;
            for &namespace_id in namespacelist.iter() {
                let keys = [
                    mk_key(
                        Anum_pg_constraint_conname,
                        F_NAMEEQ,
                        Datum::from_usize(cname.as_ptr() as usize),
                    ),
                    mk_key(
                        Anum_pg_constraint_connamespace,
                        F_OIDEQ,
                        Datum::from_oid(namespace_id),
                    ),
                ];
                let mut scan = genam::systable_beginscan(
                    mcx,
                    &conrel,
                    CONSTRAINT_NAME_NSP_INDEX_ID,
                    true,
                    None,
                    &keys,
                )?;
                let td = conrel.descr();
                while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                    let mut isnull = false;
                    // SAFETY (both): declared pg_constraint columns under its
                    // own descriptor.
                    let deferrable = unsafe {
                        types_tuple::heap_getattr(
                            tup,
                            Anum_pg_constraint_condeferrable,
                            td,
                            &mut isnull,
                        )
                    }
                    .as_bool();
                    if deferrable {
                        let oid = unsafe {
                            types_tuple::heap_getattr(tup, Anum_pg_constraint_oid, td, &mut isnull)
                        }
                        .as_oid();
                        conoidlist.push(oid);
                    } else if stmt.deferred {
                        genam::systable_endscan(mcx, scan)?;
                        conrel.close(AccessShareLock)?;
                        return Err(err(
                            format!("constraint \"{conname}\" is not deferrable"),
                            ERRCODE_WRONG_OBJECT_TYPE,
                        ));
                    }
                    found = true;
                }
                genam::systable_endscan(mcx, scan)?;
                if found {
                    break;
                }
            }
            if !found {
                conrel.close(AccessShareLock)?;
                return Err(err(
                    format!("constraint \"{conname}\" does not exist"),
                    ERRCODE_UNDEFINED_OBJECT,
                ));
            }
        }
        // Descendant constraints of partitioned parents; the list grows as
        // it is scanned.
        let mut i = 0;
        while i < conoidlist.len() {
            let parent = conoidlist[i];
            let keys =
                [mk_key(Anum_pg_constraint_conparentid, F_OIDEQ, Datum::from_oid(parent))];
            let mut scan = genam::systable_beginscan(
                mcx,
                &conrel,
                CONSTRAINT_PARENT_INDEX_ID,
                true,
                None,
                &keys,
            )?;
            let td = conrel.descr();
            while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                let mut isnull = false;
                // SAFETY: declared pg_constraint column under its descriptor.
                let oid = unsafe {
                    types_tuple::heap_getattr(tup, Anum_pg_constraint_oid, td, &mut isnull)
                }
                .as_oid();
                conoidlist.push(oid);
            }
            genam::systable_endscan(mcx, scan)?;
            i += 1;
        }
        conrel.close(AccessShareLock)?;

        let mut tgoidlist: Vec<Oid> = Vec::new();
        let tgrel = table::table_open(mcx, TRIGGER_RELATION_ID, AccessShareLock)?;
        for &conoid in &conoidlist {
            let keys = [mk_key(Anum_pg_trigger_tgconstraint, F_OIDEQ, Datum::from_oid(conoid))];
            let mut scan = genam::systable_beginscan(
                mcx,
                &tgrel,
                TRIGGER_CONSTRAINT_INDEX_ID,
                true,
                None,
                &keys,
            )?;
            let td = tgrel.descr();
            let mut found = false;
            while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
                let mut isnull = false;
                // SAFETY (both): declared pg_trigger columns under its
                // descriptor.
                let deferrable = unsafe {
                    types_tuple::heap_getattr(tup, Anum_pg_trigger_tgdeferrable, td, &mut isnull)
                }
                .as_bool();
                if deferrable {
                    let oid = unsafe {
                        types_tuple::heap_getattr(tup, Anum_pg_trigger_oid, td, &mut isnull)
                    }
                    .as_oid();
                    tgoidlist.push(oid);
                }
                found = true;
            }
            genam::systable_endscan(mcx, scan)?;
            assert!(found, "no triggers found for constraint with OID {conoid}");
        }
        tgrel.close(AccessShareLock)?;

        queue::with_con_state(|state| {
            for &tgoid in &tgoidlist {
                if let Some(slot) = state.trigstates.iter_mut().find(|(oid, _)| *oid == tgoid) {
                    slot.1 = stmt.deferred;
                } else {
                    state.trigstates.push((tgoid, stmt.deferred));
                }
            }
        });
    }

    if !stmt.deferred {
        queue::fire_now_immediate()?;
    }
    Ok(())
}
