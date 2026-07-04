//! hashvalidate.c — hashadjustmembers only; hashvalidate stays loud in amapi.
#![allow(non_snake_case)]

use index_amvalidate::opclass_for_family_datatype;
use types_core::{InvalidOid, Oid, HASH_AM_OID};
use types_error::PgResult;
use types_hash::hashpage::HASHSTANDARD_PROC;
use types_relscan::OpFamilyMember;

pub fn hashadjustmembers(
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: &mut [OpFamilyMember],
    functions: &mut [OpFamilyMember],
) -> PgResult<()> {
    let mut opclassoid = opclassoid;
    let mut opcintype = if opclassoid != InvalidOid {
        // During CREATE OPERATOR CLASS, CCI to see the pg_opclass row.
        xact::CommandCounterIncrement()?;
        lsyscache::get_opclass_input_type(opclassoid)?
    } else {
        InvalidOid
    };

    for op in operators.iter_mut().chain(functions.iter_mut()) {
        if op.is_func && op.number as u16 != HASHSTANDARD_PROC {
            op.ref_is_hard = false;
            op.ref_is_family = true;
            op.refobjid = opfamilyoid;
        } else if op.lefttype != op.righttype {
            op.ref_is_hard = false;
            op.ref_is_family = true;
            op.refobjid = opfamilyoid;
        } else {
            if op.lefttype != opcintype {
                opcintype = op.lefttype;
                opclassoid =
                    opclass_for_family_datatype(HASH_AM_OID, opfamilyoid, opcintype)?;
            }
            if opclassoid != InvalidOid {
                op.ref_is_hard = true;
                op.ref_is_family = false;
                op.refobjid = opclassoid;
            } else {
                op.ref_is_hard = false;
                op.ref_is_family = true;
                op.refobjid = opfamilyoid;
            }
        }
    }
    Ok(())
}
