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
    static NAMED: RefCell<HashMap<Vec<u8>, Option<RemoteConn>, FxBuildHasher>> =
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

// truncate_identifier to NAMEDATALEN (C keys the hash by the truncated name's
// bytes; create/lookup/delete must agree). `warn` mirrors C's create-time
// NOTICE. The key stays raw bytes: SQL_ASCII clips per byte and can leave a
// partial multibyte tail, and distinct tails must stay distinct keys.
fn conn_key(name: &str, warn: bool) -> PgResult<Vec<u8>> {
    if name.len() < NAMEDATALEN as usize {
        return Ok(name.as_bytes().to_vec());
    }
    let scratch = mcx::MemoryContext::new("dblink conn key");
    let mut buf: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(scratch.mcx(), name.len())?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    parser_small1::truncate_identifier(&mut buf, warn, mbutils::GetDatabaseEncoding())?;
    Ok(buf.to_vec())
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

// C keeps the hash readable while a connection is in use: lift the entry out.
pub fn with_named<R>(name: &str, f: impl FnOnce(Option<&mut RemoteConn>) -> R) -> PgResult<R> {
    let key = conn_key(name, false)?;
    let taken = NAMED.with(|m| m.borrow_mut().get_mut(&key).and_then(Option::take));
    match taken {
        Some(mut rc) => {
            let r = f(Some(&mut rc));
            NAMED.with(|m| {
                if let Some(slot) = m.borrow_mut().get_mut(&key) {
                    *slot = Some(rc);
                }
            });
            Ok(r)
        }
        None => Ok(f(None)),
    }
}

pub fn with_named_present<R>(name: &str, f: impl FnOnce(&mut RemoteConn) -> R) -> PgResult<R> {
    match with_named(name, |rc| rc.map(f))? {
        Some(r) => Ok(r),
        None => Err(conn_not_avail(Some(name))),
    }
}

// createNewConnection (dblink.c:2579): the hash entry is made BEFORE the
// connect attempt ("if we need a hashtable entry, make that first, since it
// might fail", dblink_connect:321) — the truncation NOTICE and the
// "duplicate connection name" 42710 must precede any network traffic. The
// entry itself only becomes visible via `store_named` once the connection
// is up (C's rconn->conn stays NULL until then; a failed connect
// deleteConnection()s it), so nothing is reserved in the map here.
pub fn reserve_named(name: &str) -> PgResult<()> {
    let key = conn_key(name, true)?;
    NAMED.with(|m| {
        if m.borrow().contains_key(&key) {
            return Err(Box::new(
                PgError::error("duplicate connection name").with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
            ));
        }
        Ok(())
    })
}

// dblink_connect's "all OK, save away the conn" (rconn->conn = conn) after a
// `reserve_named` on the same name; the NOTICE was already emitted there.
pub fn store_named(name: &str, conn: PgConn) -> PgResult<()> {
    let key = conn_key(name, false)?;
    NAMED.with(|m| {
        let prev = m.borrow_mut().insert(key, Some(RemoteConn::new(conn)));
        debug_assert!(prev.is_none(), "store_named without reserve_named");
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

pub fn all_named_names() -> Vec<Vec<u8>> {
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

// dblink_connstr_has_pw: the connstr carries a non-empty password. A connstr
// PQconninfoParse rejects (including one with an unknown keyword) has none.
pub fn connstr_has_pw(connstr: &str) -> bool {
    match pgclient::parse_conninfo(connstr) {
        Ok(opts) => {
            opts.iter().all(|(k, _)| pgclient::conninfo::lookup_option(k).is_some())
                && pgclient::opt(&opts, "password").is_some_and(|p| !p.is_empty())
        }
        Err(_) => false,
    }
}

// `MyProcPort != NULL && MyProcPort->has_scram_keys`: the session's own
// login was a completed SCRAM exchange, so its ClientKey/ServerKey are held
// in the Port (auth-scram.c, exchange.rs) for pass-through.
fn port_has_scram_keys() -> bool {
    init_small::globals::HaveMyProcPort() && init_small::globals::WithMyProcPort(|p| p.has_scram_keys)
}

// dblink_connstr_has_required_scram_options (dblink.c:2626): the connstr
// must still carry require_auth=scram-sha-256 and non-empty
// scram_client_key / scram_server_key — every option is walked so a later
// redeclaration overriding the ones appendSCRAMKeysInfo prepended is
// caught — and the session must hold pass-through keys. A connstr
// PQconninfoParse rejects has no options at all.
pub fn connstr_has_required_scram_options(connstr: &str) -> bool {
    let (has_scram_client_key, has_scram_server_key, has_require_auth) =
        connstr_scram_options(connstr);
    let has_scram_keys = has_scram_client_key && has_scram_server_key && port_has_scram_keys();
    has_scram_keys && has_require_auth
}

// The option walk of dblink_connstr_has_required_scram_options:
// (scram_client_key non-empty, scram_server_key non-empty,
// require_auth == scram-sha-256), each reflecting the LAST declaration.
fn connstr_scram_options(connstr: &str) -> (bool, bool, bool) {
    let mut has_scram_server_key = false;
    let mut has_scram_client_key = false;
    let mut has_require_auth = false;
    if let Ok(opts) = pgclient::parse_conninfo(connstr) {
        for (keyword, val) in &opts {
            if keyword == "require_auth" {
                has_require_auth = val == "scram-sha-256";
            }
            if keyword == "scram_client_key" {
                has_scram_client_key = !val.is_empty();
            }
            if keyword == "scram_server_key" {
                has_scram_server_key = !val.is_empty();
            }
        }
    }
    (has_scram_client_key, has_scram_server_key, has_require_auth)
}

// dblink_connstr_check (dblink.c:2769): pre-connect, a non-superuser must
// supply a password in the connstr, or be using SCRAM pass-through with the
// required options intact. This keeps a password from being picked up from
// .pgpass, a service file, the environment, etc. (GSS delegation is not
// compiled in: no ENABLE_GSS arm.)
pub fn connstr_check(connstr: &str) -> PgResult<()> {
    if superuser::superuser()? {
        return Ok(());
    }
    if connstr_has_pw(connstr) {
        return Ok(());
    }
    if port_has_scram_keys() && connstr_has_required_scram_options(connstr) {
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

// dblink_security_check (dblink.c:2685): post-connect, the credentials the
// server demanded must be ones the user provided — a password from the
// connstr (PQconnectionUsedPassword), or the session's SCRAM pass-through
// keys with the required options still in place (if they are, dblink itself
// added them: users cannot set the 'D' options on a server or mapping). On
// failure the caller closes the conn / deletes the hash entry.
pub fn security_check(conn: &PgConn, connstr: &str) -> PgResult<()> {
    if superuser::superuser()? {
        return Ok(());
    }
    if conn.used_password() && connstr_has_pw(connstr) {
        return Ok(());
    }
    if port_has_scram_keys() && connstr_has_required_scram_options(connstr) {
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
    let key = String::from_utf8_lossy(&key);
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
    // First append the hardcoded options needed for SCRAM pass-through, so
    // if the user overwrites them dblink_connstr_check /
    // dblink_security_check can ereport (dblink.c:2937).
    if port_has_scram_keys() && use_scram_passthrough(mcx, &server, &mapping)? {
        append_scram_keys_info(&mut buf)?;
    }
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

// appendSCRAMKeysInfo (dblink.c:3227): the session's SCRAM ClientKey and
// ServerKey (the whole SCRAM_MAX_KEY_LEN arrays, as C sizeof's them) base64
// encoded, plus require_auth='scram-sha-256' so the remote must run SCRAM.
fn append_scram_keys_info(buf: &mut String) -> PgResult<()> {
    let (client, server) =
        init_small::globals::WithMyProcPort(|p| (p.scram_client_key, p.scram_server_key));
    let client_key = b64_key(&client).ok_or_else(|| {
        Box::new(PgError::error("could not encode SCRAM client key"))
    })?;
    let server_key = b64_key(&server).ok_or_else(|| {
        Box::new(PgError::error("could not encode SCRAM server key"))
    })?;
    buf.push_str(&format!("scram_client_key='{client_key}' "));
    buf.push_str(&format!("scram_server_key='{server_key}' "));
    buf.push_str("require_auth='scram-sha-256' ");
    Ok(())
}

fn b64_key(key: &[u8]) -> Option<String> {
    let len = pg_b64::pg_b64_enc_len(key.len() as i32);
    let mut dst = vec![0u8; len as usize];
    let n = pg_b64::pg_b64_encode(key, key.len() as i32, &mut dst, len);
    if n < 0 {
        return None;
    }
    dst.truncate(n as usize);
    Some(String::from_utf8_lossy(&dst).into_owned())
}

// UseScramPassthrough (dblink.c:3268): the user mapping's
// use_scram_passthrough wins over the foreign server's; absent on both it is
// off. defGetBoolean, as C (the validator already vetted the value).
fn use_scram_passthrough<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    server: &foreigncmds::foreign::ForeignServer<'mcx>,
    mapping: &foreigncmds::foreign::UserMapping<'mcx>,
) -> PgResult<bool> {
    for opt in mapping.options.iter().chain(server.options.iter()) {
        if opt.name == "use_scram_passthrough" {
            return commands_define::defGetBoolean(&crate::fdw::mk_def_elem(mcx, opt.name, opt.value)?);
        }
    }
    Ok(false)
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

    // dblink_connstr_has_required_scram_options' option walk: all three
    // pass-through options present, the last declaration of each wins, and
    // a keyword=value / URI connstr both count.
    #[test]
    fn scram_option_walk() {
        let key = "A".repeat(43) + "=";
        let full = format!(
            "scram_client_key='{key}' scram_server_key='{key}' require_auth='scram-sha-256' host=h"
        );
        assert_eq!(connstr_scram_options(&full), (true, true, true));
        // A later redeclaration (user-supplied option after dblink's) must be
        // seen: empty key or a different require_auth clears the flag.
        assert_eq!(
            connstr_scram_options(&format!("{full} scram_client_key=''")),
            (false, true, true)
        );
        assert_eq!(
            connstr_scram_options(&format!("{full} require_auth=none")),
            (true, true, false)
        );
        assert_eq!(
            connstr_scram_options(&format!("{full} scram_server_key=")),
            (true, false, true)
        );
        assert_eq!(connstr_scram_options("host=h user=u"), (false, false, false));
        // URI form: the base64 '=' padding must be percent-encoded in a
        // query parameter (a bare '=' is "extra key/value separator").
        let ukey = key.replace('=', "%3D");
        assert_eq!(
            connstr_scram_options(&format!(
                "postgresql://h/d?scram_client_key={ukey}&scram_server_key={ukey}&require_auth=scram-sha-256"
            )),
            (true, true, true)
        );
        assert_eq!(
            connstr_scram_options(&format!("postgresql://h/d?scram_client_key={key}")),
            (false, false, false)
        );
        // Unparseable connstr: PQconninfoParse returns NULL -> nothing found.
        assert_eq!(connstr_scram_options("host"), (false, false, false));
        // Without a MyProcPort holding keys the full predicate is false.
        assert!(!connstr_has_required_scram_options(&full));
    }

    // dblink_connstr_has_pw sees a URI password (row 2 of w2-037).
    #[test]
    fn connstr_pw_detection_uri() {
        assert!(connstr_has_pw("postgresql://u:secret@localhost:1/postgres"));
        assert!(!connstr_has_pw("postgresql://u@localhost:1/postgres"));
        assert!(!connstr_has_pw("postgresql://u:@localhost:1/postgres"));
        assert!(connstr_has_pw("postgresql:///db?password=x"));
    }

    #[test]
    fn escape_param() {
        assert_eq!(escape_param_str("plain"), "plain");
        assert_eq!(escape_param_str("a'b"), "a\\'b");
        assert_eq!(escape_param_str("a\\b"), "a\\\\b");
        assert_eq!(escape_param_str("both'\\"), "both\\'\\\\");
    }

    // createNewConnection keys the hash by truncate_identifier's bytes: in
    // SQL_ASCII the clip lands mid-character and the two tails stay distinct.
    #[test]
    fn sql_ascii_truncated_names_keep_distinct_byte_keys() {
        mbutils::SetDatabaseEncoding(mbutils::pg_char_to_encoding("SQL_ASCII")).unwrap();
        let ka = conn_key(&format!("{}é", "a".repeat(62)), false).unwrap();
        let kb = conn_key(&format!("{}Ā", "a".repeat(62)), false).unwrap();
        assert_eq!(ka.len(), 63);
        assert_eq!(kb.len(), 63);
        assert_eq!(ka[62], 0xC3);
        assert_eq!(kb[62], 0xC4);
        assert_ne!(ka, kb);
    }

    // PQconninfoParse fails on an unknown keyword, so the password it carries
    // does not count (dblink_connstr_check then raises 2F003, not 08001).
    #[test]
    fn unknown_option_hides_the_password() {
        assert!(!connstr_has_pw("password=x nonexistent_dblink_option=y"));
        assert!(!connstr_has_pw("nonexistent_dblink_option=y"));
    }
}
