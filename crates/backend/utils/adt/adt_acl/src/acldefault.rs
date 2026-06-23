//! Built-in default ACLs (`utils/adt/acl.c`).
//!
//! `acldefault` builds the hardwired default ACL for a given object type and
//! owner; `acldefault_sql` is its SQL-callable wrapper.

use crate::acl_ops::allocacl;
use ::mcx::Mcx;
use types_acl::{
    AclItem, AclMode, ACL_ALTER_SYSTEM, ACL_CONNECT, ACL_CREATE, ACL_CREATE_TEMP, ACL_DELETE,
    ACL_EXECUTE, ACL_ID_PUBLIC, ACL_INSERT, ACL_MAINTAIN, ACL_NO_RIGHTS, ACL_REFERENCES,
    ACL_SELECT, ACL_SET, ACL_TRIGGER, ACL_TRUNCATE, ACL_UPDATE, ACL_USAGE,
};
use ::types_core::Oid;
use types_error::{PgError, PgResult};
use ::nodes::parsenodes::ObjectType;

// `ACL_ALL_RIGHTS_*` (utils/acl.h) — the full set of grantable privileges for
// each object class.
const ACL_ALL_RIGHTS_RELATION: AclMode = ACL_INSERT
    | ACL_SELECT
    | ACL_UPDATE
    | ACL_DELETE
    | ACL_TRUNCATE
    | ACL_REFERENCES
    | ACL_TRIGGER
    | ACL_MAINTAIN;
const ACL_ALL_RIGHTS_SEQUENCE: AclMode = ACL_USAGE | ACL_SELECT | ACL_UPDATE;
const ACL_ALL_RIGHTS_DATABASE: AclMode = ACL_CREATE | ACL_CREATE_TEMP | ACL_CONNECT;
const ACL_ALL_RIGHTS_FDW: AclMode = ACL_USAGE;
const ACL_ALL_RIGHTS_FOREIGN_SERVER: AclMode = ACL_USAGE;
const ACL_ALL_RIGHTS_FUNCTION: AclMode = ACL_EXECUTE;
const ACL_ALL_RIGHTS_LANGUAGE: AclMode = ACL_USAGE;
const ACL_ALL_RIGHTS_LARGEOBJECT: AclMode = ACL_SELECT | ACL_UPDATE;
const ACL_ALL_RIGHTS_PARAMETER_ACL: AclMode = ACL_SET | ACL_ALTER_SYSTEM;
const ACL_ALL_RIGHTS_SCHEMA: AclMode = ACL_USAGE | ACL_CREATE;
const ACL_ALL_RIGHTS_TABLESPACE: AclMode = ACL_CREATE;
const ACL_ALL_RIGHTS_TYPE: AclMode = ACL_USAGE;

/// `ACLITEM_SET_PRIVS_GOPTIONS(item, privs, goptions)` (utils/acl.h).
#[inline]
fn aclitem_set_privs_goptions(item: &mut AclItem, privs: AclMode, goptions: AclMode) {
    item.ai_privs = (privs & 0xFFFF_FFFF) | ((goptions & 0xFFFF_FFFF) << 32);
}

/// `acldefault` (acl.c) — the default ACL for `objtype` owned by `owner_id`.
///
/// Allocates the result `Acl` array in `mcx`.
pub fn acldefault<'mcx>(
    mcx: Mcx<'mcx>,
    objtype: ObjectType,
    owner_id: Oid,
) -> PgResult<&'mcx mut [AclItem]> {
    let world_default: AclMode;
    let owner_default: AclMode;

    match objtype {
        ObjectType::Column => {
            // by default, columns have no extra privileges
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_NO_RIGHTS;
        }
        ObjectType::Table => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_RELATION;
        }
        ObjectType::Sequence => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_SEQUENCE;
        }
        ObjectType::Database => {
            // for backwards compatibility, grant some rights by default
            world_default = ACL_CREATE_TEMP | ACL_CONNECT;
            owner_default = ACL_ALL_RIGHTS_DATABASE;
        }
        ObjectType::Function => {
            // Grant EXECUTE by default, for now
            world_default = ACL_EXECUTE;
            owner_default = ACL_ALL_RIGHTS_FUNCTION;
        }
        ObjectType::Language => {
            // Grant USAGE by default, for now
            world_default = ACL_USAGE;
            owner_default = ACL_ALL_RIGHTS_LANGUAGE;
        }
        ObjectType::Largeobject => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_LARGEOBJECT;
        }
        ObjectType::Schema => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_SCHEMA;
        }
        ObjectType::Tablespace => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_TABLESPACE;
        }
        ObjectType::Fdw => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_FDW;
        }
        ObjectType::ForeignServer => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_FOREIGN_SERVER;
        }
        ObjectType::Domain | ObjectType::Type => {
            world_default = ACL_USAGE;
            owner_default = ACL_ALL_RIGHTS_TYPE;
        }
        ObjectType::ParameterAcl => {
            world_default = ACL_NO_RIGHTS;
            owner_default = ACL_ALL_RIGHTS_PARAMETER_ACL;
        }
        _ => {
            // elog(ERROR, "unrecognized object type: %d", (int) objtype);
            return Err(PgError::error(format!(
                "unrecognized object type: {}",
                objtype as u32
            )));
        }
    }

    let mut nacl = 0;
    if world_default != ACL_NO_RIGHTS {
        nacl += 1;
    }
    if owner_default != ACL_NO_RIGHTS {
        nacl += 1;
    }

    let acl = allocacl(mcx, nacl)?;
    // aip = ACL_DAT(acl); — the slice itself is the AclItem array.
    let mut idx = 0;

    if world_default != ACL_NO_RIGHTS {
        let aip = &mut acl[idx];
        aip.ai_grantee = ACL_ID_PUBLIC;
        aip.ai_grantor = owner_id;
        aclitem_set_privs_goptions(aip, world_default, ACL_NO_RIGHTS);
        idx += 1;
    }

    // Note that the owner's entry shows all ordinary privileges but no grant
    // options.  This is because his grant options come "from the system" and
    // not from his own efforts.  (The SQL spec says that the owner's rights
    // come from a "_SYSTEM" authid.)  However, we do consider that the owner's
    // ordinary privileges are self-granted; this lets him revoke them.  We
    // implement the owner's grant options without any explicit "_SYSTEM"-like
    // ACL entry, by internally special-casing the owner wherever we are
    // testing grant options.
    if owner_default != ACL_NO_RIGHTS {
        let aip = &mut acl[idx];
        aip.ai_grantee = owner_id;
        aip.ai_grantor = owner_id;
        aclitem_set_privs_goptions(aip, owner_default, ACL_NO_RIGHTS);
    }

    Ok(acl)
}

/// `acldefault_sql` (acl.c) — SQL wrapper over `acldefault`. Hackish mapping
/// from the `"char"` type to `OBJECT_*` values.
///
/// The fmgr `Datum`/`PG_FUNCTION_ARGS` marshaling belongs to the fmgr layer;
/// this exposes the typed scalar arguments `objtypec` (`PG_GETARG_CHAR(0)`)
/// and `owner` (`PG_GETARG_OID(1)`) and returns the freshly built `Acl`.
pub fn acldefault_sql<'mcx>(
    mcx: Mcx<'mcx>,
    objtypec: i8,
    owner: Oid,
) -> PgResult<&'mcx mut [AclItem]> {
    let objtype = match objtypec as u8 {
        b'c' => ObjectType::Column,
        b'r' => ObjectType::Table,
        b's' => ObjectType::Sequence,
        b'd' => ObjectType::Database,
        b'f' => ObjectType::Function,
        b'l' => ObjectType::Language,
        b'L' => ObjectType::Largeobject,
        b'n' => ObjectType::Schema,
        b'p' => ObjectType::ParameterAcl,
        b't' => ObjectType::Tablespace,
        b'F' => ObjectType::Fdw,
        b'S' => ObjectType::ForeignServer,
        b'T' => ObjectType::Type,
        _ => {
            // elog(ERROR, "unrecognized object type abbreviation: %c", objtypec);
            return Err(PgError::error(format!(
                "unrecognized object type abbreviation: {}",
                objtypec as u8 as char
            )));
        }
    };

    acldefault(mcx, objtype, owner)
}
