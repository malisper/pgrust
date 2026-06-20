//! `aclexplode(aclitem[])` (OID 1689) registered as an executor-frame
//! materialize-mode set-returning function.
//!
//! `acl.c`'s `aclexplode` is a value-per-call SRF emitting one
//! `(grantor oid, grantee oid, privilege_type text, is_grantable bool)` row per
//! privilege bit set in each `AclItem` of the input `aclitem[]` array (it scans
//! the `N_ACL_RIGHTS` bits of every item's lower-32 privilege word, mapping the
//! bit to its keyword via `convert_aclright_to_string` and the matching
//! grant-option bit to `is_grantable`). The pure-data expansion core (the
//! `ACL_DAT`/`ACL_NUM` item walk + the per-bit `ACLITEM_GET_PRIVS` /
//! `ACLITEM_GET_GOPTIONS` test) is ported in
//! [`backend_utils_adt_acl::acl_ops::aclexplode`], which hands back a
//! `Vec<AclExplodeRow>`.
//!
//! Here that core is driven over the executor frame in materialize mode (the row
//! set is fully determined by the input array, so the whole tuplestore is filled
//! once, emitting the identical rows the C per-call series would).
//! `InitMaterializedSRF` with `MAT_SRF_USE_EXPECTED_DESC` takes the executor's
//! already-resolved `(oid, oid, text, bool)` descriptor (skipping the catalog
//! `get_call_result_type`), the rows are appended via
//! `materialized_srf_putvalues`, and the entry point returns SQL NULL.
//! Registered from [`register_aclexplode`] (called by `init_seams`); it bypasses
//! the by-OID builtin registry whose tag-only `resultinfo` cannot carry the live
//! `ReturnSetInfo` (the WONTFIX dual-home).
//!
//! The `aclitem[]` argument arrives header-ful on the by-reference side channel
//! (a varlena array image; `PG_GETARG_ACL_P`). It is deconstructed once via
//! `array_unnest` into per-element 16-byte `AclItem` windows
//! (`ACL_DAT(acl)`), each decoded with `aclitem_from_image`.

extern crate alloc;

use mcx::Mcx;
use types_core::Oid;
use types_acl::AclItem;
use types_nodes::fmgr::{FmgrArgRef, FunctionCallInfoBaseData};
use types_nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_adt_arrayfuncs::sql::array_unnest;
use backend_utils_fmgr_funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};
use types_array::ArrayElementDatum;

use crate::register_srf;

/// `aclexplode(aclitem[])` (OID 1689).
const ACLEXPLODE: Oid = 1689;

/// Register `aclexplode` in the executor-frame SRF table.
pub(crate) fn register_aclexplode() {
    register_srf(ACLEXPLODE, aclexplode);
}

/// `aclexplode(PG_FUNCTION_ARGS)` (acl.c) over the executor frame.
fn aclexplode<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("aclexplode: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: acl = PG_GETARG_ACL_P(0). The `aclitem[]` array arrives header-ful on
    // the by-reference side channel (a varlena array image). Deconstruct it once
    // into the per-element 16-byte AclItem windows (C: ACL_DAT(acl) /
    // ACL_NUM(acl)) and decode each into an owned AclItem, so the items outlive
    // the immutable `fcinfo` borrow (which must end before the mutable
    // InitMaterializedSRF / putvalues calls below). aclitem is a fixed-length
    // by-reference type, so every non-NULL element is a 16-byte window; a
    // no-nulls Acl array never yields NULL elements (check_acl property).
    let items: alloc::vec::Vec<AclItem> = {
        let image: &[u8] = match fcinfo.ref_arg(0) {
            Some(FmgrArgRef::Varlena(b)) => b.as_slice(),
            _ => panic!("aclexplode: aclitem[] argument missing from the by-ref lane"),
        };
        let elems = array_unnest(mcx, image).unwrap_or_else(|e| std::panic::panic_any(e));
        let mut items: alloc::vec::Vec<AclItem> = alloc::vec::Vec::with_capacity(elems.len());
        for (elem, isnull) in elems.iter() {
            if *isnull {
                // A NULL aclitem can't occur in a well-formed Acl (check_acl);
                // mirror C, which never sees one.
                continue;
            }
            match elem {
                ArrayElementDatum::ByRef(window) => {
                    items.push(backend_utils_adt_acl::acl_ops::aclitem_from_image(window));
                }
                ArrayElementDatum::ByValue(_) => {
                    panic!("aclexplode: aclitem element must be by-reference (16-byte image)")
                }
            }
        }
        items
    };

    // The pure-data expansion core (the item walk + per-bit priv/goption test).
    let rows = backend_utils_adt_acl::acl_ops::aclexplode(&items);

    // C: tupdesc = CreateTemplateTupleDesc(4) (grantor oid, grantee oid,
    // privilege_type text, is_grantable bool). Take the executor's
    // already-resolved descriptor via MAT_SRF_USE_EXPECTED_DESC.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)
        .unwrap_or_else(|e| std::panic::panic_any(e));

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("aclexplode: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        // values[0] = ObjectIdGetDatum(ai_grantor); values[1] =
        // ObjectIdGetDatum(ai_grantee); values[2] =
        // CStringGetTextDatum(convert_aclright_to_string(priv_bit)); values[3] =
        // BoolGetDatum(is_grantable). All non-NULL (nulls[4] = {0}).
        let priv_text = backend_utils_adt_varlena_seams::cstring_to_text_v::call(
            mcx,
            row.privilege_type,
        )
        .unwrap_or_else(|e| std::panic::panic_any(e));
        let values = [
            Datum::from_oid(row.grantor),
            Datum::from_oid(row.grantee),
            priv_text,
            Datum::from_bool(row.is_grantable),
        ];
        let nulls = [false, false, false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)
            .unwrap_or_else(|e| std::panic::panic_any(e));
    }

    // C: SRF_RETURN_DONE — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Datum::null()
}
