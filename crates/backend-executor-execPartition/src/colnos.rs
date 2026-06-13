//! UPDATE target-column remapping family: `adjust_partition_colnos`,
//! `adjust_partition_colnos_using_map`.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
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
    let _ = (mcx, estate, colnos, leaf_part_rri);
    todo!("decomp")
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
    let _ = (mcx, colnos, attr_map);
    todo!("decomp")
}
