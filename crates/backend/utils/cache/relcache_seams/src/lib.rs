use std::rc::Rc;
use types_core::Oid;
use types_error::PgResult;
use types_rel::RelationData;

// RelationIdGetRelation: a clone of the cache's Rc is C's returned pointer +
// rd_refcnt increment (rd_refcnt == strong count); dropping it is the
// RelationClose decrement. Ok(None) is the C NULL (no pg_class row); entries
// live in CacheMemoryContext, hence 'static.
seam_core::seam!(
    pub fn relation_id_get_relation(
        relation_id: Oid,
    ) -> PgResult<Option<Rc<RelationData<'static>>>>
);
