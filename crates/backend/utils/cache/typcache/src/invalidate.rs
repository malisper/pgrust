use std::rc::Rc;

use datum::Datum;
use mcx::PgHashMap;
use types_core::{InvalidOid, Oid};

use crate::{
    compute_ready, with_state, TypCacheState, TypeCacheEntry,
    TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS, TCFLAGS_DOMAIN_BASE_IS_COMPOSITE,
    TCFLAGS_HAVE_PG_TYPE_DATA, TCFLAGS_OPERATOR_FLAGS,
};
use lsyscache::{TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN};

pub(crate) fn insert_rel_type_cache_if_needed(
    rel_map: &mut PgHashMap<'static, Oid, Oid>,
    e: &TypeCacheEntry,
) {
    if e.typtype() != TYPTYPE_COMPOSITE {
        return;
    }
    debug_assert!(e.typrelid() != InvalidOid);
    // C's third disjunct is `tupDesc != NULL`; that lane is unported.
    if e.flags_raw() & (TCFLAGS_HAVE_PG_TYPE_DATA | TCFLAGS_OPERATOR_FLAGS) != 0 {
        rel_map.insert(e.typrelid(), e.type_id);
    }
}

fn delete_rel_type_cache_if_needed(
    rel_map: &mut PgHashMap<'static, Oid, Oid>,
    e: &TypeCacheEntry,
) {
    if e.typtype() != TYPTYPE_COMPOSITE {
        return;
    }
    debug_assert!(e.typrelid() != InvalidOid);
    if e.flags_raw() & (TCFLAGS_HAVE_PG_TYPE_DATA | TCFLAGS_OPERATOR_FLAGS) == 0 {
        rel_map.remove(&e.typrelid());
    }
}

// C's InvalidateCompositeTypeCacheEntry resets the entry's tupDesc; the
// composite TUPDESC lane is unported, so there is nothing stored to reset.
fn invalidate_composite_entry(_e: &Rc<TypeCacheEntry>) {}

pub(crate) fn TypeCacheRelCallback(_arg: Datum, relid: Oid) {
    with_state(|st| {
        let TypCacheState { type_cache, rel_id_to_type_id, first_domain_type_entry, .. } =
            st;
        if relid != InvalidOid {
            if let Some(typid) = rel_id_to_type_id.get(&relid).copied() {
                if let Some(e) = type_cache.get(&typid) {
                    debug_assert_eq!(e.typtype(), TYPTYPE_COMPOSITE);
                    debug_assert_eq!(relid, e.typrelid());
                    invalidate_composite_entry(e);
                }
            }
            let mut t = *first_domain_type_entry;
            while t != InvalidOid {
                let e = type_cache.get(&t).expect("domain chain entry present");
                if e.flags_raw() & TCFLAGS_DOMAIN_BASE_IS_COMPOSITE != 0 {
                    e.clear_flags(TCFLAGS_OPERATOR_FLAGS);
                    e.set_ready(compute_ready(e));
                }
                t = e.next_domain_get();
            }
        } else {
            for e in type_cache.values() {
                if e.typtype() == TYPTYPE_COMPOSITE {
                    invalidate_composite_entry(e);
                } else if e.typtype() == TYPTYPE_DOMAIN
                    && e.flags_raw() & TCFLAGS_DOMAIN_BASE_IS_COMPOSITE != 0
                {
                    e.clear_flags(TCFLAGS_OPERATOR_FLAGS);
                    e.set_ready(compute_ready(e));
                }
            }
        }
    });
}

pub(crate) fn TypeCacheTypCallback(_arg: Datum, _cacheid: i32, hashvalue: u32) {
    with_state(|st| {
        let TypCacheState { type_cache, rel_id_to_type_id, .. } = st;
        // C scans by hashvalue bucket (hash_seq_init_with_hash_value); the
        // full-map compare is equivalent and typcache stays small (DDL-rare).
        for e in type_cache.values() {
            if hashvalue != 0 && e.type_id_hash != hashvalue {
                continue;
            }
            let had_pg_type_data = e.flags_raw() & TCFLAGS_HAVE_PG_TYPE_DATA != 0;
            e.clear_flags(TCFLAGS_HAVE_PG_TYPE_DATA | TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS);
            e.set_ready(compute_ready(e));
            if had_pg_type_data {
                delete_rel_type_cache_if_needed(rel_id_to_type_id, e);
            }
        }
    });
}

pub(crate) fn TypeCacheOpcCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    with_state(|st| {
        let TypCacheState { type_cache, rel_id_to_type_id, .. } = st;
        for e in type_cache.values() {
            let had_opclass = e.flags_raw() & TCFLAGS_OPERATOR_FLAGS != 0;
            e.clear_flags(TCFLAGS_OPERATOR_FLAGS);
            e.set_ready(compute_ready(e));
            if had_opclass {
                delete_rel_type_cache_if_needed(rel_id_to_type_id, e);
            }
        }
    });
}

pub(crate) fn TypeCacheConstrCallback(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    with_state(|st| {
        let TypCacheState { type_cache, first_domain_type_entry, .. } = st;
        let mut t = *first_domain_type_entry;
        while t != InvalidOid {
            let e = type_cache.get(&t).expect("domain chain entry present");
            e.clear_flags(TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS);
            e.set_ready(compute_ready(e));
            t = e.next_domain_get();
        }
    });
}

fn finalize_in_progress_typentries() {
    with_state(|st| {
        let TypCacheState { type_cache, rel_id_to_type_id, in_progress, .. } = st;
        for &t in in_progress.iter() {
            if let Some(e) = type_cache.get(&t) {
                insert_rel_type_cache_if_needed(rel_id_to_type_id, e);
            }
        }
        in_progress.clear();
    });
}

pub fn AtEOXact_TypeCache() {
    finalize_in_progress_typentries();
}

pub fn AtEOSubXact_TypeCache() {
    finalize_in_progress_typentries();
}
