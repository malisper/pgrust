//! `opfamily-operator` family — `lsyscache.c` lookups keyed on
//! `pg_operator` / `pg_amop` (operator metadata and opfamily membership).
//!
//! SCAFFOLD STAGE: signatures mirror the `backend-utils-cache-lsyscache-seams`
//! declarations exactly; bodies are `todo!()` until the SearchSysCache logic
//! (catcache seam) lands.
//!
//! C entry points covered here: `get_op_opfamily_properties`,
//! `get_opfamily_member`, `get_ordering_op_properties`, `get_op_hash_functions`,
//! `op_input_types`, `op_strict`, `get_opcode`, `get_commutator`.

use types_core::Oid;
use types_error::PgResult;

/// `get_commutator(opno)` (lsyscache.c).
pub fn get_commutator(_opno: Oid) -> PgResult<Oid> {
    todo!("get_commutator: SearchSysCache(OPEROID) -> oprcom")
}

/// `op_input_types(opno, &lefttype, &righttype)` (lsyscache.c).
pub fn op_input_types(_opno: Oid) -> PgResult<(Oid, Oid)> {
    todo!("op_input_types: SearchSysCache(OPEROID) -> (oprleft, oprright)")
}

/// `op_strict(opno)` (lsyscache.c).
pub fn op_strict(_opno: Oid) -> PgResult<bool> {
    todo!("op_strict: func_strict(get_opcode(opno))")
}

/// `get_opcode(opno)` (lsyscache.c).
pub fn get_opcode(_opno: Oid) -> PgResult<Oid> {
    todo!("get_opcode: SearchSysCache(OPEROID) -> oprcode")
}

/// `get_op_opfamily_properties(opno, opfamily, missing_ok, &strategy,
/// &lefttype, &righttype)` (lsyscache.c).
pub fn get_op_opfamily_properties(
    _opno: Oid,
    _opfamily: Oid,
    _missing_ok: bool,
) -> PgResult<Option<(i32, Oid, Oid)>> {
    todo!("get_op_opfamily_properties: SearchSysCache3(AMOPOPID) -> amop fields")
}

/// `get_ordering_op_properties(opno, &opfamily, &opcintype, &cmptype)`
/// (lsyscache.c).
pub fn get_ordering_op_properties(_opno: Oid) -> PgResult<Option<(Oid, Oid, i32)>> {
    todo!("get_ordering_op_properties: scan AMOPOPID for btree ordering opfamily")
}

/// `get_op_hash_functions(opno, &lhs_procno, &rhs_procno)` (lsyscache.c).
pub fn get_op_hash_functions(_opno: Oid) -> PgResult<Option<(Oid, Oid)>> {
    todo!("get_op_hash_functions: scan AMOPOPID hash family -> AMPROCNUM HASHEXTENDED/STANDARD")
}

/// `get_opfamily_member(opfamily, lefttype, righttype, strategy)` (lsyscache.c).
pub fn get_opfamily_member(
    _opfamily: Oid,
    _lefttype: Oid,
    _righttype: Oid,
    _strategy: i16,
) -> PgResult<Oid> {
    todo!("get_opfamily_member: SearchSysCache4(AMOPSTRATEGY) -> amopopr")
}
