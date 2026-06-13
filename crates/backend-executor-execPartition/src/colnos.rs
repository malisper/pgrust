//! UPDATE target-column remapping family: `adjust_partition_colnos`,
//! `adjust_partition_colnos_using_map`.

use mcx::{Mcx, PgVec};
use types_core::primitive::AttrNumber;
use types_error::{PgError, PgResult};
use types_nodes::{EStateData, RriId};
use types_tuple::attmap::AttrMap;

/// `adjust_partition_colnos(colnos, leaf_part_rri)` — adjust an UPDATE target
/// column-number list for the attribute differences between the parent and the
/// partition, using the leaf's child→root map. Must not be called when no
/// adjustment is required. Fallible (`elog(ERROR)` on an unexpected attno, OOM
/// for the new list).
pub(crate) fn adjust_partition_colnos<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    colnos: &[i32],
    leaf_part_rri: RriId,
) -> PgResult<PgVec<'mcx, i32>> {
    // TupleConversionMap *map = ExecGetChildToRootMap(leaf_part_rri);
    let map = backend_executor_execUtils::ExecGetChildToRootMap(estate, leaf_part_rri)?;

    // Assert(map != NULL);
    let map = map.expect("adjust_partition_colnos: child-to-root map is NULL");

    // return adjust_partition_colnos_using_map(colnos, map->attrMap);
    adjust_partition_colnos_using_map(mcx, colnos, &map.attrMap)
}

/// `adjust_partition_colnos_using_map(colnos, attrMap)` — like
/// `adjust_partition_colnos`, but with a caller-supplied attribute map. Must not
/// be called when no adjustment is required. Fallible (`elog(ERROR)` on an
/// unexpected attno, OOM for the new list).
pub(crate) fn adjust_partition_colnos_using_map<'mcx>(
    mcx: Mcx<'mcx>,
    colnos: &[i32],
    attr_map: &AttrMap<'mcx>,
) -> PgResult<PgVec<'mcx, i32>> {
    // Assert(attrMap != NULL); — the parameter is a reference, so non-NULL by
    // construction.
    let attnums: &[AttrNumber] = &attr_map.attnums;
    let maplen = attnums.len() as i32;

    // List *new_colnos = NIL; built up one entry per input colno (lappend_int).
    let mut new_colnos: PgVec<'mcx, i32> = mcx::vec_with_capacity_in(mcx, colnos.len())?;

    for &parentattrno in colnos {
        if parentattrno <= 0
            || parentattrno > maplen
            || attnums[(parentattrno - 1) as usize] == 0
        {
            return Err(PgError::error(format!(
                "unexpected attno {} in target column list",
                parentattrno
            )));
        }
        // new_colnos = lappend_int(new_colnos, attrMap->attnums[parentattrno - 1]);
        new_colnos.push(attnums[(parentattrno - 1) as usize] as i32);
    }

    Ok(new_colnos)
}
