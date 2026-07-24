// ATExecSetRelOptions (tablecmds.c): ALTER TABLE/INDEX SET/RESET (...).
#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_nodes::parsenodes::AlterTableType;
use types_nodes::NodeList;
use types_rel::{
    InplaceUpdateTupleLock, Relation, LOCKMODE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW,
    RowExclusiveLock,
};

use crate::alter::oid_scankey;

const Natts_pg_class: usize = 34;
const Anum_pg_class_reloptions: usize = 33;

pub(crate) fn ATExecSetRelOptions<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    def_list: &NodeList<'mcx>,
    operation: AlterTableType,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    if def_list.is_nil() && operation != AlterTableType::AT_ReplaceRelOptions {
        return Ok(());
    }

    let pgclass = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;

    // locktup mirrors C exactly: the main relation's row is fetched with
    // SearchSysCacheLocked1 (tablecmds.c:16666) and released with
    // UnlockTuple (:16773); the toast arm uses the plain, unlocked
    // SearchSysCache1 (:16790). Do not "tidy" that asymmetry away.
    update_one(mcx, &pgclass, rel, None, def_list, operation, true)?;

    if rel.rd_rel.reltoastrelid != InvalidOid {
        let toastid = rel.rd_rel.reltoastrelid;
        let toastrel = table::table_open(mcx, toastid, lockmode)?;
        update_one(mcx, &pgclass, &toastrel, Some("toast"), def_list, operation, false)?;
        toastrel.close(types_rel::NoLock)?;
    }

    pgclass.close(RowExclusiveLock)
}

fn update_one<'mcx>(
    mcx: Mcx<'mcx>,
    pgclass: &Relation<'mcx>,
    rel: &Relation<'mcx>,
    namspace: Option<&str>,
    def_list: &NodeList<'mcx>,
    operation: AlterTableType,
    locktup: bool,
) -> PgResult<()> {
    let relid = rel.rd_id;
    let relkind = rel.rd_rel.relkind;
    let relam = rel.rd_rel.relam;
    let key = oid_scankey(1, relid);
    let mut scan = genam::systable_beginscan(
        mcx,
        pgclass,
        catalog::ClassOidIndexId,
        true,
        None,
        &[key],
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    // LOCKTAG_TUPLE at InplaceUpdateTupleLock, before any content read that
    // feeds the replacement image: the tuple bytes alias the pinned page, so
    // reading them after the lock is what makes a concurrent inplace writer's
    // relfrozenxid/relminmxid advance visible instead of silently discarded.
    // Same ordering as C's systable arm (objectaddress.c:2846 LockTuple then
    // heap_copytuple). Released after CatalogTupleUpdate.
    let otid = tup.t_self;
    if locktup {
        lmgr::LockTuple(pgclass, &otid, InplaceUpdateTupleLock)?;
    }
    let desc = pgclass.descr();

    let old_image;
    let datum = if operation == AlterTableType::AT_ReplaceRelOptions {
        None
    } else {
        let mut isnull = false;
        // SAFETY: reloptions attr under pg_class's own descriptor.
        let d = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_class_reloptions as i32, desc, &mut isnull)
        };
        if isnull {
            None
        } else {
            old_image = reloptions::text_array_image(mcx, d)?;
            Some(old_image.as_slice())
        }
    };

    let new_options = reloptions::transformRelOptions(
        mcx,
        datum,
        def_list,
        namspace,
        reloptions::HEAP_RELOPT_NAMESPACES,
        false,
        operation == AlterTableType::AT_ResetRelOptions,
    )?;

    if namspace == Some("toast") {
        reloptions::heap_reloptions(
            mcx,
            types_rel::RELKIND_TOASTVALUE,
            new_options.as_deref(),
            true,
        )?;
    } else {
        match relkind {
            RELKIND_RELATION if reloptions::relam_is_pgrcolumnar(relam) => {
                reloptions::pgrcolumnar_reloptions(mcx, new_options.as_deref(), true)?;
            }
            RELKIND_RELATION | RELKIND_MATVIEW => {
                reloptions::heap_reloptions(mcx, relkind, new_options.as_deref(), true)?;
            }
            RELKIND_PARTITIONED_TABLE => {
                reloptions::partitioned_table_reloptions(new_options.as_deref(), true)?;
            }
            RELKIND_VIEW => {
                let opts = reloptions::view_reloptions(mcx, new_options.as_deref(), true)?;
                let check_option = opts.is_some_and(|o| {
                    o.check_option
                        != types_rel::ViewOptCheckOption::VIEW_OPTION_CHECK_OPTION_NOT_SET
                });
                if check_option {
                    let view_query = rewrite_handler::get_view_query(mcx, rel)?;
                    if let Some(view_updatable_error) =
                        rewrite_handler::view_query_is_auto_updatable(view_query, true)
                    {
                        genam::systable_endscan(mcx, scan)?;
                        return Err(Box::new(
                            types_error::PgError::new(
                                ERROR,
                                "WITH CHECK OPTION is supported only on automatically \
                                 updatable views"
                                    .to_string(),
                            )
                            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                            .with_hint(view_updatable_error),
                        ));
                    }
                }
            }
            RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => {
                reloptions::index_reloptions(mcx, relam, new_options.as_deref(), true)?;
            }
            _ => {
                genam::systable_endscan(mcx, scan)?;
                return Err(Box::new(
                    types_error::PgError::new(
                        ERROR,
                        format!("cannot set options for relation {relid}"),
                    )
                    .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
        }
    }

    let mut repl_val = [Datum::null(); Natts_pg_class];
    let mut repl_null = [false; Natts_pg_class];
    let mut repl_repl = [false; Natts_pg_class];
    match &new_options {
        Some(img) => {
            repl_val[Anum_pg_class_reloptions - 1] = Datum::from_usize(img.as_ptr() as usize)
        }
        None => repl_null[Anum_pg_class_reloptions - 1] = true,
    }
    repl_repl[Anum_pg_class_reloptions - 1] = true;

    let mut newtuple =
        heaptuple::heap_modify_tuple(mcx, tup, desc, &repl_val, &repl_null, &repl_repl)?;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, pgclass, &otid, &mut newtuple)?;
    if locktup {
        lmgr::UnlockTuple(pgclass, &otid, InplaceUpdateTupleLock)?;
    }
    Ok(())
}
