use mcx::{vec_append_bytes, Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DATA_EXCEPTION,
    ERRCODE_NULL_VALUE_NOT_ALLOWED,
};

use crate::{AclItem, ACLITEMOID};

pub const ACL_ITEM_SIZE: usize = 16;
const ARR_HDR: usize = 20;

// check_acl (acl.c) over the payload of a detoasted/packed aclitem[] varlena
// (the bytes after the 1B or 4B header; identical in both forms).
pub fn check_acl_payload(payload: &[u8]) -> PgResult<usize> {
    let rd = |off: usize| -> i32 {
        let mut b = [0u8; 4];
        b.copy_from_slice(&payload[off..off + 4]);
        i32::from_le_bytes(b)
    };
    if payload.len() < ARR_HDR {
        return Err(acl_shape_error(
            "ACL arrays must be one-dimensional",
            ERRCODE_DATA_EXCEPTION,
        ));
    }
    let ndim = rd(0);
    let dataoffset = rd(4);
    let elemtype = rd(8) as u32;
    if elemtype != ACLITEMOID {
        return Err(acl_shape_error(
            "ACL array contains wrong data type",
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    if ndim != 1 {
        return Err(acl_shape_error(
            "ACL arrays must be one-dimensional",
            ERRCODE_DATA_EXCEPTION,
        ));
    }
    if dataoffset != 0 {
        return Err(acl_shape_error(
            "ACL arrays must not contain null values",
            ERRCODE_NULL_VALUE_NOT_ALLOWED,
        ));
    }
    let n = rd(12);
    if n < 0 || ARR_HDR + (n as usize) * ACL_ITEM_SIZE > payload.len() {
        return Err(acl_shape_error(
            "ACL array size mismatch",
            ERRCODE_DATA_EXCEPTION,
        ));
    }
    Ok(n as usize)
}

#[track_caller]
#[cold]
#[inline(never)]
fn acl_shape_error(msg: &str, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(sqlstate))
}

#[inline]
pub fn read_acl_item(payload: &[u8], i: usize) -> AclItem {
    let off = ARR_HDR + i * ACL_ITEM_SIZE;
    let mut g = [0u8; 4];
    let mut r = [0u8; 4];
    let mut p = [0u8; 8];
    g.copy_from_slice(&payload[off..off + 4]);
    r.copy_from_slice(&payload[off + 4..off + 8]);
    p.copy_from_slice(&payload[off + 8..off + 16]);
    AclItem {
        ai_grantee: u32::from_le_bytes(g),
        ai_grantor: u32::from_le_bytes(r),
        ai_privs: u64::from_le_bytes(p),
    }
}

pub fn decode_acl_payload<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<PgVec<'mcx, AclItem>> {
    let n = check_acl_payload(payload)?;
    let mut items: PgVec<'mcx, AclItem> = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        items.push(read_acl_item(payload, i));
    }
    Ok(items)
}

fn item_bytes(items: &[AclItem]) -> &[u8] {
    const {
        assert!(core::mem::size_of::<AclItem>() == ACL_ITEM_SIZE);
        assert!(cfg!(target_endian = "little"));
    }
    // SAFETY: AclItem is repr(C) {u32,u32,u64}, padding-free, LE == on-disk form.
    unsafe {
        core::slice::from_raw_parts(items.as_ptr().cast::<u8>(), items.len() * ACL_ITEM_SIZE)
    }
}

// allocacl's image (acl.c): 4B-header 1-D no-nulls aclitem[] varlena.
pub fn acl_image<'mcx>(mcx: Mcx<'mcx>, items: &[AclItem]) -> PgResult<PgVec<'mcx, u8>> {
    let size = 4 + ARR_HDR + items.len() * ACL_ITEM_SIZE;
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, size)?;
    let mut hdr = [0u8; 4 + ARR_HDR];
    hdr[0..4].copy_from_slice(&((size as u32) << 2).to_le_bytes());
    hdr[4..8].copy_from_slice(&1i32.to_le_bytes());
    hdr[12..16].copy_from_slice(&ACLITEMOID.to_le_bytes());
    hdr[16..20].copy_from_slice(&(items.len() as i32).to_le_bytes());
    hdr[20..24].copy_from_slice(&1i32.to_le_bytes());
    vec_append_bytes(&mut out, &hdr)?;
    vec_append_bytes(&mut out, item_bytes(items))?;
    Ok(out)
}
