// ATExecSetRelOptions (tablecmds.c): ALTER TABLE/INDEX SET/RESET (...).
#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgResult, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_nodes::parsenodes::AlterTableType;
use types_nodes::NodeList;
use types_rel::{
    Relation, LOCKMODE, RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW, RowExclusiveLock,
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

    update_one(mcx, &pgclass, rel.rd_id, rel.rd_rel.relkind, rel.rd_rel.relam, None, def_list, operation)?;

    if rel.rd_rel.reltoastrelid != InvalidOid {
        let toastid = rel.rd_rel.reltoastrelid;
        let toastrel = table::table_open(mcx, toastid, lockmode)?;
        update_one(
            mcx,
            &pgclass,
            toastid,
            toastrel.rd_rel.relkind,
            toastrel.rd_rel.relam,
            Some("toast"),
            def_list,
            operation,
        )?;
        toastrel.close(types_rel::NoLock)?;
    }

    pgclass.close(RowExclusiveLock)
}

#[allow(clippy::too_many_arguments)]
fn update_one<'mcx>(
    mcx: Mcx<'mcx>,
    pgclass: &Relation<'mcx>,
    relid: Oid,
    relkind: u8,
    relam: Oid,
    namspace: Option<&str>,
    def_list: &NodeList<'mcx>,
    operation: AlterTableType,
) -> PgResult<()> {
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
            RELKIND_RELATION | RELKIND_MATVIEW => {
                reloptions::heap_reloptions(mcx, relkind, new_options.as_deref(), true)?;
            }
            RELKIND_PARTITIONED_TABLE => {
                reloptions::partitioned_table_reloptions(new_options.as_deref(), true)?;
            }
            RELKIND_VIEW => panic!(
                "unported: tablecmds ATExecSetRelOptions view arm \
                 (view_query_is_auto_updatable; CREATE VIEW lane)"
            ),
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
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, pgclass, &otid, &mut newtuple)?;
    Ok(())
}
