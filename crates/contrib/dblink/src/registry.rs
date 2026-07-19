// Per-backend connection state: C's `pconn` (unnamed) + `remoteConnHash`
// (named), which are process globals in C and thread-locals here (one backend
// = one thread). Plus the security policy (connstr password checks) and
// foreign-server connstr assembly.
use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use rustc_hash::FxBuildHasher;

use pgclient::{PgConn, WaitEvents};
use types_core::{Oid, NAMEDATALEN};
use types_error::{
    PgError, PgResult, ERRCODE_CONNECTION_DOES_NOT_EXIST, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED, ERRCODE_UNDEFINED_OBJECT,
};

pub struct RemoteConn {
    pub conn: PgConn,
    pub open_cursor_count: i32,
    pub new_xact_for_cursor: bool,
}

impl RemoteConn {
    fn new(conn: PgConn) -> RemoteConn {
        RemoteConn { conn, open_cursor_count: 0, new_xact_for_cursor: false }
    }
}

thread_local! {
    static PCONN: RefCell<Option<RemoteConn>> = const { RefCell::new(None) };
    static NAMED: RefCell<HashMap<String, RemoteConn, FxBuildHasher>> =
        RefCell::new(HashMap::with_hasher(FxBuildHasher));
    static WE_CONNECT: Cell<u32> = const { Cell::new(0) };
    static WE_GET_CONN: Cell<u32> = const { Cell::new(0) };
    static WE_GET_RESULT: Cell<u32> = const { Cell::new(0) };
}

fn we_lazy(cell: &'static std::thread::LocalKey<Cell<u32>>, name: &str) -> PgResult<u32> {
    let v = cell.with(Cell::get);
    if v != 0 {
        return Ok(v);
    }
    let id = waitevent::custom::WaitEventExtensionNew(name)?;
    cell.with(|c| c.set(id));
    Ok(id)
}

// The wait events dblink threads into the client's blocking loops. connect and
// get_conn share the client's `connect` slot; get_result is `receive`.
pub fn we_connect() -> PgResult<WaitEvents> {
    Ok(WaitEvents {
        connect: we_lazy(&WE_CONNECT, "DblinkConnect")?,
        receive: we_get_result()?,
    })
}

pub fn we_get_conn() -> PgResult<WaitEvents> {
    Ok(WaitEvents {
        connect: we_lazy(&WE_GET_CONN, "DblinkGetConnect")?,
        receive: we_get_result()?,
    })
}

pub fn we_get_result() -> PgResult<u32> {
    we_lazy(&WE_GET_RESULT, "DblinkGetResult")
}

// truncate_identifier to NAMEDATALEN (C keys the hash by the truncated name;
// create/lookup/delete must agree). `warn` mirrors C's create-time NOTICE.
fn conn_key(name: &str, warn: bool) -> PgResult<String> {
    if name.len() < NAMEDATALEN as usize {
        return Ok(name.to_string());
    }
    let scratch = mcx::MemoryContext::new("dblink conn key");
    let mut buf: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(scratch.mcx(), name.len())?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    parser_small1::truncate_identifier(&mut buf, warn, mbutils::GetDatabaseEncoding())?;
    Ok(name[..buf.len()].to_string())
}

// --- unnamed connection (pconn) ---

pub fn with_unnamed<R>(f: impl FnOnce(Option<&mut RemoteConn>) -> R) -> R {
    PCONN.with(|c| f(c.borrow_mut().as_mut()))
}

pub fn set_unnamed(conn: PgConn) {
    PCONN.with(|c| {
        let mut b = c.borrow_mut();
        if let Some(old) = b.take() {
            let mut old = old;
            old.conn.terminate();
        }
        *b = Some(RemoteConn::new(conn));
    });
}

pub fn take_unnamed() -> Option<RemoteConn> {
    PCONN.with(|c| c.borrow_mut().take())
}

pub fn unnamed_present() -> bool {
    PCONN.with(|c| c.borrow().is_some())
}

// --- named connections ---

pub fn named_present(name: &str) -> PgResult<bool> {
    let key = conn_key(name, false)?;
    Ok(NAMED.with(|m| m.borrow().contains_key(&key)))
}

pub fn with_named<R>(name: &str, f: impl FnOnce(Option<&mut RemoteConn>) -> R) -> PgResult<R> {
    let key = conn_key(name, false)?;
    Ok(NAMED.with(|m| f(m.borrow_mut().get_mut(&key))))
}

pub fn create_named(name: &str, conn: PgConn) -> PgResult<()> {
    let key = conn_key(name, true)?;
    NAMED.with(|m| {
        let mut m = m.borrow_mut();
        if m.contains_key(&key) {
            return Err(Box::new(
                PgError::error("duplicate connection name").with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
            ));
        }
        m.insert(key, RemoteConn::new(conn));
        Ok(())
    })
}

pub fn delete_named(name: &str) -> PgResult<()> {
    let key = conn_key(name, false)?;
    NAMED.with(|m| {
        if m.borrow_mut().remove(&key).is_some() {
            Ok(())
        } else {
            Err(Box::new(
                PgError::error("undefined connection name").with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ))
        }
    })
}

pub fn all_named_names() -> Vec<String> {
    NAMED.with(|m| m.borrow().keys().cloned().collect())
}

#[cold]
pub fn conn_not_avail(conname: Option<&str>) -> Box<PgError> {
    let msg = match conname {
        Some(n) => format!("connection \"{n}\" not available"),
        None => "connection not available".to_string(),
    };
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_CONNECTION_DOES_NOT_EXIST))
}

// --- security ---

// dblink_connstr_has_pw: the connstr carries a non-empty password.
pub fn connstr_has_pw(connstr: &str) -> bool {
    match pgclient::parse_conninfo(connstr) {
        Ok(opts) => pgclient::opt(&opts, "password").is_some_and(|p| !p.is_empty()),
        Err(_) => false,
    }
}

// dblink_connstr_check: pre-connect, non-superuser must supply a password in
// the connstr (SCRAM pass-through and GSS delegation are unported — the server
// exposes no has_scram_keys / delegated creds, so those branches are dormant).
pub fn connstr_check(connstr: &str) -> PgResult<()> {
    if superuser::superuser()? {
        return Ok(());
    }
    if connstr_has_pw(connstr) {
        return Ok(());
    }
    Err(Box::new(
        PgError::error("password or GSSAPI delegated credentials required")
            .with_sqlstate(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED)
            .with_detail(
                "Non-superusers must provide a password in the connection string or send delegated GSSAPI credentials.",
            ),
    ))
}

// dblink_security_check: post-connect, the password must actually have been
// used (PQconnectionUsedPassword). Runs after connstr_check, so a
// non-superuser reaching here supplied a password; verify the server demanded
// it. On failure the caller closes the conn / deletes the hash entry.
pub fn security_check(conn: &PgConn, connstr: &str) -> PgResult<()> {
    if superuser::superuser()? {
        return Ok(());
    }
    if conn.used_password() && connstr_has_pw(connstr) {
        return Ok(());
    }
    Err(Box::new(
        PgError::error("password or GSSAPI delegated credentials required")
            .with_sqlstate(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED)
            .with_detail(
                "Non-superusers may only connect using credentials they provide, eg: password in connection string or delegated GSSAPI credentials",
            )
            .with_hint("Ensure provided credentials match target server's authentication method."),
    ))
}

// get_connect_string: assemble a connstr from a foreign server's FDW + server
// + user-mapping options, filtered by is_valid_dblink_option. None when the
// name is not a foreign server (caller then treats the string as a connstr).
pub fn get_connect_string(mcx: mcx::Mcx<'_>, servername: &str) -> PgResult<Option<String>> {
    let key = conn_key(servername, false)?;
    let Some(server) = foreigncmds::foreign::GetForeignServerByName(mcx, &key, true)? else {
        return Ok(None);
    };
    let userid = miscinit::GetUserId();
    let mapping = foreigncmds::foreign::GetUserMapping(mcx, userid, server.serverid)?;
    let fdw = foreigncmds::foreign::GetForeignDataWrapper(mcx, server.fdwid)?;

    let aclresult = aclchk::object_aclcheck(
        types_core::FOREIGN_SERVER_RELATION_ID,
        server.serverid,
        userid,
        adt_acl::ACL_USAGE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NO_PRIV,
            types_nodes::parsenodes::ObjectType::OBJECT_FOREIGN_SERVER,
            server.servername,
        )?;
    }

    let mut buf = String::new();
    // C get_connect_string reads strVal(def->arg) unconditionally; catalog
    // options always carry values (grammar-enforced), so a NULL here is the
    // hand-built-text[] path C would crash on — error loudly instead.
    for opt in fdw.options.iter() {
        append_opt(&mut buf, opt.name, opt.require_value()?, crate::fdw::FDW_CONTEXT);
    }
    for opt in server.options.iter() {
        append_opt(&mut buf, opt.name, opt.require_value()?, crate::fdw::SERVER_CONTEXT);
    }
    for opt in mapping.options.iter() {
        append_opt(&mut buf, opt.name, opt.require_value()?, crate::fdw::USER_MAPPING_CONTEXT);
    }
    Ok(Some(buf))
}

fn append_opt(buf: &mut String, name: &str, value: &str, context: Oid) {
    if crate::fdw::is_valid_dblink_option(name, context) {
        buf.push_str(name);
        buf.push_str("='");
        buf.push_str(&escape_param_str(value));
        buf.push_str("' ");
    }
}

// escape_param_str: backslash-escape ' and \ .
pub fn escape_param_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connstr_pw_detection() {
        assert!(connstr_has_pw("dbname=x password=secret"));
        assert!(connstr_has_pw("password='s e c'"));
        assert!(!connstr_has_pw("dbname=x port=5432"));
        assert!(!connstr_has_pw("password="));
        assert!(!connstr_has_pw("password=''"));
    }

    #[test]
    fn escape_param() {
        assert_eq!(escape_param_str("plain"), "plain");
        assert_eq!(escape_param_str("a'b"), "a\\'b");
        assert_eq!(escape_param_str("a\\b"), "a\\\\b");
        assert_eq!(escape_param_str("both'\\"), "both\\'\\\\");
    }
}
