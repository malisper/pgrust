use super::CreatedTempRelation;
use crate::backend::catalog::store::CreateTableResult;
use crate::backend::catalog::toasting::ToastCatalogChanges;
use crate::backend::executor::ToastRelationRef;
use crate::backend::parser::BoundIndexRelation;
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::catalog::BTREE_AM_OID;

fn toast_index_relation_from_changes(changes: &ToastCatalogChanges) -> Option<BoundIndexRelation> {
    let meta = changes.index_entry.index_meta.as_ref()?;
    Some(BoundIndexRelation {
        name: changes.index_name.clone(),
        rel: changes.index_entry.rel,
        relation_oid: changes.index_entry.relation_oid,
        desc: changes.index_entry.desc.clone(),
        index_meta: IndexRelCacheEntry {
            indexrelid: changes.index_entry.relation_oid,
            indrelid: meta.indrelid,
            indnatts: meta.indkey.len() as i16,
            indnkeyatts: meta.indkey.len() as i16,
            indisunique: meta.indisunique,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: false,
            indisclustered: false,
            indisvalid: meta.indisvalid,
            indcheckxmin: false,
            indisready: meta.indisready,
            indislive: meta.indislive,
            indisreplident: false,
            am_oid: BTREE_AM_OID,
            am_handler_oid: None,
            indkey: meta.indkey.clone(),
            indclass: meta.indclass.clone(),
            indcollation: meta.indcollation.clone(),
            indoption: meta.indoption.clone(),
            opfamily_oids: Vec::new(),
            opcintype_oids: Vec::new(),
            indexprs: meta.indexprs.clone(),
            indpred: meta.indpred.clone(),
        },
        index_exprs: Vec::new(),
    })
}

pub(super) fn toast_bindings_from_create_result(
    created: &CreateTableResult,
) -> (Option<ToastRelationRef>, Option<BoundIndexRelation>) {
    let toast = created.toast.as_ref().map(|changes| ToastRelationRef {
        rel: changes.toast_entry.rel,
        relation_oid: changes.toast_entry.relation_oid,
    });
    let toast_index = created
        .toast
        .as_ref()
        .and_then(toast_index_relation_from_changes);
    (toast, toast_index)
}

pub(super) fn toast_bindings_from_temp_relation(
    created: &CreatedTempRelation,
) -> (Option<ToastRelationRef>, Option<BoundIndexRelation>) {
    let toast = created.toast.as_ref().map(|changes| ToastRelationRef {
        rel: changes.toast_entry.rel,
        relation_oid: changes.toast_entry.relation_oid,
    });
    let toast_index = created
        .toast
        .as_ref()
        .and_then(toast_index_relation_from_changes);
    (toast, toast_index)
}
