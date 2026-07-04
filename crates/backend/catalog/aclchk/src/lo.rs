// aclchk.c large-object arms: pg_largeobject_aclmask_snapshot /
// pg_largeobject_aclcheck_snapshot / object_ownercheck.

use adt_acl::{acldefault, aclmask, has_privs_of_role, AclMaskHow, AclObjectType, AclItem};
use mcx::Mcx;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT, ERROR};

use crate::{ACLCHECK_NO_PRIV, ACLCHECK_OK};

const VARHDRSZ: usize = 4;

fn pg_largeobject_aclmask_snapshot<'mcx>(
    mcx: Mcx<'mcx>,
    lobj_oid: Oid,
    roleid: Oid,
    mask: u64,
    how: AclMaskHow,
    snapshot: Option<pg_largeobject::Snapshot>,
) -> PgResult<u64> {
    // Superusers bypass all permission checking.
    if superuser::superuser_arg(roleid)? {
        return Ok(mask);
    }

    let Some((ownerId, acl)) = pg_largeobject::largeobject_owner_acl(mcx, lobj_oid, snapshot)?
    else {
        return Err(Box::new(
            PgError::new(ERROR, format!("large object {lobj_oid} does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    };

    match acl {
        None => aclmask(
            acldefault(AclObjectType::LargeObject, ownerId).as_slice(),
            roleid,
            ownerId,
            mask,
            how,
        ),
        Some(image) => {
            let payload = &image[VARHDRSZ..];
            let n = adt_acl::varlena::check_acl_payload(payload)?;
            let mut items: Vec<AclItem> = Vec::with_capacity(n);
            for i in 0..n {
                items.push(adt_acl::varlena::read_acl_item(payload, i));
            }
            aclmask(&items, roleid, ownerId, mask, how)
        }
    }
}

pub fn pg_largeobject_aclcheck_snapshot<'mcx>(
    mcx: Mcx<'mcx>,
    lobj_oid: Oid,
    roleid: Oid,
    mode: u64,
    snapshot: Option<pg_largeobject::Snapshot>,
) -> PgResult<i32> {
    if pg_largeobject_aclmask_snapshot(mcx, lobj_oid, roleid, mode, AclMaskHow::AclmaskAny, snapshot)?
        != 0
    {
        Ok(ACLCHECK_OK)
    } else {
        Ok(ACLCHECK_NO_PRIV)
    }
}

pub fn object_ownercheck<'mcx>(
    mcx: Mcx<'mcx>,
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
) -> PgResult<bool> {
    // Superusers bypass all permission checking.
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }

    if classid == pg_largeobject::LargeObjectRelationId {
        let Some((ownerId, _)) = pg_largeobject::largeobject_owner_acl(mcx, objectid, None)?
        else {
            return Err(Box::new(PgError::new(
                ERROR,
                format!("could not find tuple for large object {objectid}"),
            )));
        };
        return has_privs_of_role(roleid, ownerId);
    }

    panic!("object_ownercheck (aclchk.c): classid {classid} unported for non-superusers");
}
