// attoptcache.c. Divergence owned here: C keeps a syscache-invalidated hash
// of parsed AttributeOpts (get_attribute_options caches per (attrelid,
// attnum)); this port re-parses from the ATTNUM syscache on every call.
use mcx::Mcx;
use reloptions::AttributeOpts;
use types_core::Oid;
use types_error::PgResult;

// get_attribute_options (attoptcache.c:130). A missing attribute reads as
// "no options specified", as does a null attoptions column.
pub fn get_attribute_options(
    mcx: Mcx<'_>,
    attrelid: Oid,
    attnum: i16,
) -> PgResult<Option<AttributeOpts>> {
    let Some(attopts) = syscache_seams::pg_attribute_attoptions::call(mcx, attrelid, attnum)?
    else {
        return Ok(None);
    };
    let Some(datum) = attopts else {
        return Ok(None);
    };
    let p = datum.as_usize() as *const u8;
    // SAFETY: not-null attoptions datum off the syscache projection — a live
    // varlena image readable through its varsize_any extent.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    reloptions::attribute_reloptions(mcx, Some(image), false)
}

#[cfg(test)]
mod tests;
