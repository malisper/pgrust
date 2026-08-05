#![allow(non_snake_case)]

use core::cell::RefCell;
use core::mem::ManuallyDrop;

use commands_publicationcmds::{pub_contains_invalid_column, pub_rf_contains_invalid_column};
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use pg_publication::{
    is_publishable_relation, GetAllTablesPublications, GetPublication, GetRelationPublications,
    GetSchemaPublications, PublicationActions,
};
use types_core::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
};
use types_nodes::nodes_enums::CmdType;
use types_rel::pg_class::{RELKIND_PARTITIONED_TABLE, REPLICA_IDENTITY_FULL};
use types_rel::Relation;

#[derive(Clone, Copy, Default)]
pub struct PublicationDesc {
    pub pubactions: PublicationActions,
    pub rf_valid_for_update: bool,
    pub rf_valid_for_delete: bool,
    pub cols_valid_for_update: bool,
    pub cols_valid_for_delete: bool,
    pub gencols_valid_for_update: bool,
    pub gencols_valid_for_delete: bool,
}

impl PublicationDesc {
    fn all_valid() -> Self {
        PublicationDesc {
            pubactions: PublicationActions::default(),
            rf_valid_for_update: true,
            rf_valid_for_delete: true,
            cols_valid_for_update: true,
            cols_valid_for_delete: true,
            gencols_valid_for_update: true,
            gencols_valid_for_delete: true,
        }
    }
}

// C rd_pubdesc (rule-5 cache): relid-keyed side table, relcache-inval cleared
// (the trimmed RelationData has no rd_pubdesc field; indexattr precedent).
struct PubDescState {
    descs: PgHashMap<'static, Oid, PublicationDesc>,
    callbacks_registered: bool,
}

thread_local! {
    static STATE: RefCell<Option<ManuallyDrop<PubDescState>>> = const { RefCell::new(None) };
}

fn with_state<R>(f: impl FnOnce(&mut PubDescState) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            let mcx = ::mcx::session_root("PubDescContext").mcx();
            ManuallyDrop::new(PubDescState {
                descs: PgHashMap::with_capacity_in(8, mcx),
                callbacks_registered: false,
            })
        });
        f(st)
    })
}

fn PubDescRelCallback(_arg: Datum, relid: Oid) {
    with_state(|st| {
        if relid != InvalidOid {
            st.descs.remove(&relid);
        } else {
            st.descs.clear();
        }
    });
}

fn concat_unique_oid<'mcx>(dst: &mut PgVec<'mcx, Oid>, src: &[Oid]) {
    for &oid in src {
        if !dst.contains(&oid) {
            dst.push(oid);
        }
    }
}

pub fn RelationBuildPublicationDesc<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<PublicationDesc> {
    if !is_publishable_relation(rel) {
        return Ok(PublicationDesc::all_valid());
    }
    if let Some(hit) = with_state(|st| st.descs.get(&rel.rd_id).copied()) {
        return Ok(hit);
    }
    if !with_state(|st| st.callbacks_registered) {
        inval::invalidate::CacheRegisterRelcacheCallback(
            PubDescRelCallback,
            Datum::from_oid(InvalidOid),
        )?;
        with_state(|st| st.callbacks_registered = true);
    }

    let relid = rel.rd_id;
    let mut desc = PublicationDesc::all_valid();

    let mut puboids = GetRelationPublications(mcx, relid)?;
    concat_unique_oid(
        &mut puboids,
        &GetSchemaPublications(mcx, rel.rd_rel.relnamespace)?,
    );
    let mut ancestors: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    if rel.rd_rel.relispartition {
        ancestors = pg_inherits::get_partition_ancestors(mcx, relid)?;
        for i in 0..ancestors.len() {
            let ancestor = ancestors[i];
            concat_unique_oid(&mut puboids, &GetRelationPublications(mcx, ancestor)?);
            let schemaid = lsyscache::get_rel_namespace(ancestor)?;
            concat_unique_oid(&mut puboids, &GetSchemaPublications(mcx, schemaid)?);
        }
    }
    concat_unique_oid(&mut puboids, &GetAllTablesPublications(mcx)?);

    for &pubid in puboids.iter() {
        let publication = GetPublication(mcx, pubid)?;
        let acts = publication.pubactions;

        desc.pubactions.pubinsert |= acts.pubinsert;
        desc.pubactions.pubupdate |= acts.pubupdate;
        desc.pubactions.pubdelete |= acts.pubdelete;
        desc.pubactions.pubtruncate |= acts.pubtruncate;

        if !publication.alltables
            && (acts.pubupdate || acts.pubdelete)
            && pub_rf_contains_invalid_column(mcx, pubid, rel, &ancestors, publication.pubviaroot)?
        {
            if acts.pubupdate {
                desc.rf_valid_for_update = false;
            }
            if acts.pubdelete {
                desc.rf_valid_for_delete = false;
            }
        }

        if acts.pubupdate || acts.pubdelete {
            let mut invalid_column_list = false;
            let mut invalid_gen_col = false;
            if pub_contains_invalid_column(
                mcx,
                pubid,
                rel,
                &ancestors,
                publication.pubviaroot,
                publication.pubgencols_type,
                &mut invalid_column_list,
                &mut invalid_gen_col,
            )? {
                if acts.pubupdate {
                    desc.cols_valid_for_update = !invalid_column_list;
                    desc.gencols_valid_for_update = !invalid_gen_col;
                }
                if acts.pubdelete {
                    desc.cols_valid_for_delete = !invalid_column_list;
                    desc.gencols_valid_for_delete = !invalid_gen_col;
                }
            }
        }

        let everything = desc.pubactions.pubinsert
            && desc.pubactions.pubupdate
            && desc.pubactions.pubdelete
            && desc.pubactions.pubtruncate;
        if everything
            && ((!desc.rf_valid_for_update && !desc.rf_valid_for_delete)
                || (!desc.cols_valid_for_update && !desc.cols_valid_for_delete)
                || (!desc.gencols_valid_for_update && !desc.gencols_valid_for_delete))
        {
            break;
        }
    }

    with_state(|st| st.descs.insert(relid, desc));
    Ok(desc)
}

fn RelationGetReplicaIndex<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<Oid> {
    if rel.rd_indexlist.borrow().is_none() {
        let _ = relcache::RelationGetIndexList(mcx, rel.rd_id)?;
    }
    Ok(rel
        .rd_indexlist
        .borrow()
        .as_ref()
        .map(|l| l.replidindex)
        .unwrap_or(InvalidOid))
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_invalid_column_reference(rel_name: &str, update: bool, detail: &str) -> Box<PgError> {
    let msg = if update {
        format!("cannot update table \"{rel_name}\"")
    } else {
        format!("cannot delete from table \"{rel_name}\"")
    };
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE)
            .with_detail(detail),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_no_replica_identity(rel_name: &str, update: bool) -> Box<PgError> {
    let (msg, hint) = if update {
        (
            format!("cannot update table \"{rel_name}\" because it does not have a replica identity and publishes updates"),
            "To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.",
        )
    } else {
        (
            format!("cannot delete from table \"{rel_name}\" because it does not have a replica identity and publishes deletes"),
            "To enable deleting from the table, set REPLICA IDENTITY using ALTER TABLE.",
        )
    };
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(hint),
    )
}

const RF_DETAIL: &str =
    "Column used in the publication WHERE expression is not part of the replica identity.";
const COLS_DETAIL: &str =
    "Column list used by the publication does not cover the replica identity.";
const GENCOLS_DETAIL: &str = "Replica identity must not contain unpublished generated columns.";

pub fn CheckCmdReplicaIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: CmdType,
) -> PgResult<()> {
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Ok(());
    }
    if cmd != CmdType::CMD_UPDATE && cmd != CmdType::CMD_DELETE {
        return Ok(());
    }

    let pubdesc = RelationBuildPublicationDesc(mcx, rel)?;
    let update = cmd == CmdType::CMD_UPDATE;
    let (rf_valid, cols_valid, gencols_valid, publishes) = if update {
        (
            pubdesc.rf_valid_for_update,
            pubdesc.cols_valid_for_update,
            pubdesc.gencols_valid_for_update,
            pubdesc.pubactions.pubupdate,
        )
    } else {
        (
            pubdesc.rf_valid_for_delete,
            pubdesc.cols_valid_for_delete,
            pubdesc.gencols_valid_for_delete,
            pubdesc.pubactions.pubdelete,
        )
    };
    if !rf_valid {
        return Err(err_invalid_column_reference(rel.name(), update, RF_DETAIL));
    }
    if !cols_valid {
        return Err(err_invalid_column_reference(rel.name(), update, COLS_DETAIL));
    }
    if !gencols_valid {
        return Err(err_invalid_column_reference(
            rel.name(),
            update,
            GENCOLS_DETAIL,
        ));
    }

    if RelationGetReplicaIndex(mcx, rel)? != InvalidOid {
        return Ok(());
    }
    if rel.rd_rel.relreplident == REPLICA_IDENTITY_FULL {
        return Ok(());
    }

    if publishes {
        return Err(err_no_replica_identity(rel.name(), update));
    }
    Ok(())
}

pub fn init_seams() {
    execreplication_seams::check_cmd_replica_identity::set(CheckCmdReplicaIdentity);
}
