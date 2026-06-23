//! Seam declarations for the `backend-access-common-tupconvert` unit
//! (`access/common/tupconvert.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `execute_attr_map_slot(attrMap, in_slot, out_slot)` (tupconvert.c):
    /// remap the attributes of the tuple in `in_slot` through `attr_map` into
    /// `out_slot` (a virtual tuple), returning the id of `out_slot`. The slots
    /// are addressed by id into the EState slot pool; the map is borrowed.
    /// Deforming the input slot can detoast/allocate, so the call is fallible.
    pub fn execute_attr_map_slot<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        attr_map: &types_tuple::attmap::AttrMap<'_>,
        in_slot: nodes::SlotId,
        out_slot: nodes::SlotId,
    ) -> types_error::PgResult<nodes::SlotId>
);
