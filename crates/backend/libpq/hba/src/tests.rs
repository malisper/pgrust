use std::sync::{Mutex, Once};

use types_core::init::{
    uaCert, uaImplicitReject, uaMD5, uaPassword, uaPeer, uaReject, uaSCRAM, uaTrust,
};
use types_error::LOG;
use types_startup::{
    clientCertCA, clientCertFull, clientCertOff, ctHost, ctHostSSL, ctLocal, ipCmpAll, ipCmpMask,
    ClientSocket, Port,
};

use crate::token::FileHandle;
use crate::*;

static GUC_LOCK: Mutex<()> = Mutex::new(());

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        crate::init_seams();
        regex_core::init_seams();
        mbutils::init_seams();
        acl_seams::get_role_oid::set(|name, missing_ok| {
            assert!(missing_ok);
            Ok(match name {
                "alice" => 101,
                "bob" => 102,
                _ => 0,
            })
        });
        conffiles_seams::absolute_config_location::set(|location, calling_file| {
            let p = std::path::Path::new(&location);
            if p.is_absolute() {
                p.to_path_buf()
            } else if let Some(calling) = calling_file {
                calling
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .join(p)
            } else {
                p.to_path_buf()
            }
        });
    });
}

fn tokenize(content: &str) -> Vec<TokenizedAuthLine> {
    setup();
    let file = FileHandle {
        content: content.as_bytes().to_vec(),
        depth: 0,
    };
    let mut lines = Vec::new();
    tokenize_auth_file("test_file", &file, &mut lines, LOG, 0).unwrap();
    lines
}

fn parse_one(line: &str) -> Result<types_startup::HbaLine, String> {
    let mut toks = tokenize(line);
    assert_eq!(toks.len(), 1, "{line:?} should tokenize to one line");
    if let Some(e) = &toks[0].err_msg {
        return Err(e.clone());
    }
    match parse_hba_line(&mut toks[0], LOG).unwrap() {
        Some(h) => Ok(h),
        None => Err(toks[0].err_msg.clone().unwrap_or_default()),
    }
}

fn write_temp(name: &str, content: &str) -> String {
    let dir = std::env::temp_dir().join(format!("pgrust_hba_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path.to_string_lossy().into_owned()
}

fn load_hba_content(name: &str, content: &str) -> bool {
    setup();
    let _g = GUC_LOCK.lock().unwrap();
    let path = write_temp(name, content);
    guc_tables::vars::HbaFileName.write(Some(path));
    load_hba().unwrap()
}

fn unix_port(user: &str, db: &str) -> Port {
    let mut raddr = ip::SockAddr::zeroed();
    // SAFETY: writing an aligned sockaddr_un prefix into the storage buffer.
    unsafe {
        let mut sun: libc::sockaddr_un = core::mem::MaybeUninit::zeroed().assume_init();
        sun.sun_family = libc::AF_UNIX as libc::sa_family_t;
        core::ptr::copy_nonoverlapping(
            core::ptr::from_ref(&sun).cast::<u8>(),
            raddr.addr.as_mut_ptr(),
            core::mem::size_of::<libc::sockaddr_un>(),
        );
    }
    raddr.salen = core::mem::size_of::<libc::sockaddr_un>() as u32;
    let mut port = Port::new(&ClientSocket { sock: -1, raddr });
    port.user_name = Some(user.to_string());
    port.database_name = Some(db.to_string());
    port
}

fn inet_port(addr: &str, user: &str, db: &str) -> Port {
    let hint = ip::AddrInfoHint {
        flags: libc::AI_NUMERICHOST,
        family: libc::AF_UNSPEC,
        socktype: 0,
    };
    let mut out = Vec::new();
    assert_eq!(ip::pg_getaddrinfo_all(Some(addr), None, &hint, &mut out), 0);
    let mut port = Port::new(&ClientSocket {
        sock: -1,
        raddr: out[0].addr,
    });
    port.user_name = Some(user.to_string());
    port.database_name = Some(db.to_string());
    port
}

#[test]
fn tokenizer_quotes_comments_commas() {
    let lines = tokenize("local all all trust # comment\n");
    assert_eq!(lines.len(), 1);
    let f: Vec<&str> = lines[0]
        .fields
        .iter()
        .map(|t| t[0].string.as_str())
        .collect();
    assert_eq!(f, ["local", "all", "all", "trust"]);
    assert_eq!(lines[0].line_num, 1);
    assert_eq!(lines[0].raw_line, "local all all trust # comment");

    let lines = tokenize("# full comment line\n\nlocal \"all\" db1,db2 trust\n");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].line_num, 3);
    assert!(lines[0].fields[1][0].quoted);
    assert_eq!(lines[0].fields[1][0].string, "all");
    assert_eq!(lines[0].fields[2].len(), 2);
    assert_eq!(lines[0].fields[2][0].string, "db1");
    assert_eq!(lines[0].fields[2][1].string, "db2");

    let lines = tokenize("local all \"double\"\"quote\" trust\n");
    assert_eq!(lines[0].fields[2][0].string, "double\"quote");
}

#[test]
fn tokenizer_backslash_continuation() {
    let lines = tokenize("local all \\\n  all trust\nhost all all all trust\n");
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].fields.len(), 4);
    assert_eq!(lines[1].line_num, 3);
}

#[test]
fn tokenizer_at_file_inclusion() {
    setup();
    let dbs = write_temp("dbs.txt", "db1 db2\ndb3\n");
    let lines = tokenize(&format!("local @{dbs} all trust\n"));
    let dbs_field: Vec<&str> = lines[0].fields[1]
        .iter()
        .map(|t| t.string.as_str())
        .collect();
    assert_eq!(dbs_field, ["db1", "db2", "db3"]);
}

#[test]
fn parse_local_trust() {
    let h = parse_one("local all all trust").unwrap();
    assert_eq!(h.conntype, ctLocal);
    assert_eq!(h.auth_method, uaTrust);
    assert_eq!(h.linenumber, 1);
    assert_eq!(h.sourcefile, "test_file");
    assert_eq!(h.rawline, "local all all trust");
    assert!(token_is_keyword(&h.databases[0], "all"));
    assert!(token_is_keyword(&h.roles[0], "all"));
}

#[test]
fn parse_methods() {
    assert_eq!(parse_one("local all all reject").unwrap().auth_method, uaReject);
    assert_eq!(parse_one("local all all password").unwrap().auth_method, uaPassword);
    assert_eq!(parse_one("local all all md5").unwrap().auth_method, uaMD5);
    assert_eq!(
        parse_one("local all all scram-sha-256").unwrap().auth_method,
        uaSCRAM
    );
    assert_eq!(parse_one("local all all peer").unwrap().auth_method, uaPeer);
    // ident on local is changed to peer.
    assert_eq!(parse_one("local all all ident").unwrap().auth_method, uaPeer);
    assert_eq!(
        parse_one("local all all frobnicate").unwrap_err(),
        "invalid authentication method \"frobnicate\""
    );
    assert_eq!(
        parse_one("local all all ldap").unwrap_err(),
        "invalid authentication method \"ldap\": not supported by this build"
    );
    assert_eq!(
        parse_one("host all all all peer").unwrap_err(),
        "peer authentication is only supported on local sockets"
    );
}

#[test]
fn parse_host_cidr_and_netmask() {
    let h = parse_one("host all all 127.0.0.1/32 trust").unwrap();
    assert_eq!(h.conntype, ctHost);
    assert_eq!(h.ip_cmp_method, ipCmpMask);
    assert!(h.hostname.is_none());
    assert!(h.addr.salen > 0);
    assert_eq!(h.mask.salen, h.addr.salen);

    let h6 = parse_one("host all all ::1/128 scram-sha-256").unwrap();
    assert_eq!(h6.auth_method, uaSCRAM);
    assert!(h6.addr.salen > 0);

    let hm = parse_one("host all all 10.0.0.0 255.0.0.0 md5").unwrap();
    assert_eq!(hm.auth_method, uaMD5);
    assert!(hm.mask.salen > 0);

    let ha = parse_one("host all all all trust").unwrap();
    assert_eq!(ha.ip_cmp_method, ipCmpAll);

    let hh = parse_one("host all all example.com md5").unwrap();
    assert_eq!(hh.hostname.as_deref(), Some("example.com"));

    assert_eq!(
        parse_one("host all all 127.0.0.1/33 trust").unwrap_err(),
        "invalid CIDR mask in address \"127.0.0.1/33\""
    );
    assert!(parse_one("host all all 127.0.0.1 trust")
        .unwrap_err()
        .starts_with("invalid IP mask \"trust\":"));
    assert_eq!(
        parse_one("host all all 127.0.0.1 ::1 trust").unwrap_err(),
        "IP address and mask do not match"
    );
    assert_eq!(
        parse_one("host all all example.com/24 trust").unwrap_err(),
        "specifying both host name and CIDR mask is invalid: \"example.com/24\""
    );
}

#[test]
fn parse_field_errors() {
    assert_eq!(
        parse_one("local").unwrap_err(),
        "end-of-line before database specification"
    );
    assert_eq!(
        parse_one("local all").unwrap_err(),
        "end-of-line before role specification"
    );
    assert_eq!(
        parse_one("local all all").unwrap_err(),
        "end-of-line before authentication method"
    );
    assert_eq!(
        parse_one("host all all").unwrap_err(),
        "end-of-line before IP address specification"
    );
    assert_eq!(
        parse_one("badtype all all trust").unwrap_err(),
        "invalid connection type \"badtype\""
    );
    assert_eq!(
        parse_one("local all all trust foo").unwrap_err(),
        "authentication option not in name=value format: foo"
    );
    assert_eq!(
        parse_one("local all all trust zop=1").unwrap_err(),
        "unrecognized authentication option name: \"zop\""
    );
    assert_eq!(
        parse_one("local all all trust pamservice=x").unwrap_err(),
        "authentication option \"pamservice\" is only valid for authentication methods pam"
    );
}

#[test]
fn parse_hostssl_warns_when_ssl_disabled_but_loads() {
    let mut toks = tokenize("hostssl all all 127.0.0.1/32 md5\n");
    let parsed = parse_hba_line(&mut toks[0], LOG).unwrap();
    let h = parsed.expect("line still loads");
    assert_eq!(h.conntype, ctHostSSL);
    assert_eq!(
        toks[0].err_msg.as_deref(),
        Some("hostssl record cannot match because SSL is disabled")
    );
}

#[test]
fn parse_map_option() {
    let h = parse_one("local all all peer map=mymap").unwrap();
    assert_eq!(h.usermap.as_deref(), Some("mymap"));
    assert_eq!(
        parse_one("local all all trust map=mymap").unwrap_err(),
        "authentication option \"map\" is only valid for authentication methods ident, peer, gssapi, sspi, cert, and oauth"
    );
}

#[test]
fn parse_cert_method() {
    // cert parses on hostssl and forces clientcert=verify-full (C hba.c:2043).
    let h = parse_one("hostssl all all all cert").unwrap();
    assert_eq!(h.auth_method, uaCert);
    assert_eq!(h.clientcert, clientCertFull);

    // map= is a valid option for cert.
    let h = parse_one("hostssl all all all cert map=certmap").unwrap();
    assert_eq!(h.usermap.as_deref(), Some("certmap"));

    // Explicit clientcert=verify-full is accepted (and implied anyway).
    let h = parse_one("hostssl all all all cert clientcert=verify-full").unwrap();
    assert_eq!(h.clientcert, clientCertFull);

    // cert is hostssl-only: host and local rows fail with C's wording.
    assert_eq!(
        parse_one("host all all all cert").unwrap_err(),
        "cert authentication is only supported on hostssl connections"
    );
    assert_eq!(
        parse_one("local all all cert").unwrap_err(),
        "cert authentication is only supported on hostssl connections"
    );

    // clientcert=verify-ca conflicts with the cert method; the recorded
    // err_msg is C's *err_msg wording (differs from its ereport wording).
    assert_eq!(
        parse_one("hostssl all all all cert clientcert=verify-ca").unwrap_err(),
        "clientcert can only be set to \"verify-full\" when using \"cert\" authentication"
    );

    // Non-cert methods still take verify-ca; default stays off.
    let h = parse_one("hostssl all all all trust clientcert=verify-ca").unwrap();
    assert_eq!(h.clientcert, clientCertCA);
    let h = parse_one("hostssl all all all trust").unwrap();
    assert_eq!(h.clientcert, clientCertOff);
}

#[test]
fn load_and_check_hba_local_trust() {
    assert!(load_hba_content(
        "trust.conf",
        "local all all trust\nhost all all 127.0.0.1/32 trust\n"
    ));

    let mut port = unix_port("alice", "postgres");
    check_hba(&mut port).unwrap();
    let hba = port.hba.as_ref().unwrap();
    assert_eq!(hba.auth_method, uaTrust);
    assert_eq!(hba.conntype, ctLocal);
    assert_eq!(hba.linenumber, 1);

    // Unknown role still matches "all" (get_role_oid missing_ok fold).
    let mut port = unix_port("nobody", "postgres");
    check_hba(&mut port).unwrap();
    assert_eq!(port.hba.as_ref().unwrap().auth_method, uaTrust);

    let mut port = inet_port("127.0.0.1", "alice", "postgres");
    check_hba(&mut port).unwrap();
    let hba = port.hba.as_ref().unwrap();
    assert_eq!(hba.auth_method, uaTrust);
    assert_eq!(hba.conntype, ctHost);
    assert_eq!(hba.linenumber, 2);

    // 10.0.0.1 matches neither line: implicit reject.
    let mut port = inet_port("10.0.0.1", "alice", "postgres");
    check_hba(&mut port).unwrap();
    assert_eq!(port.hba.as_ref().unwrap().auth_method, uaImplicitReject);
}

#[test]
fn check_hba_db_user_matching() {
    assert!(load_hba_content(
        "matching.conf",
        concat!(
            "local sameuser all trust\n",
            "local db1 alice,bob scram-sha-256\n",
            "local \"all\" all md5\n",
            "local all all reject\n"
        )
    ));

    // dbname == username matches sameuser.
    let mut port = unix_port("alice", "alice");
    check_hba(&mut port).unwrap();
    assert_eq!(port.hba.as_ref().unwrap().linenumber, 1);

    let mut port = unix_port("bob", "db1");
    check_hba(&mut port).unwrap();
    assert_eq!(port.hba.as_ref().unwrap().auth_method, uaSCRAM);

    // Quoted "all" is a literal database name, not the keyword.
    let mut port = unix_port("carol", "all");
    check_hba(&mut port).unwrap();
    assert_eq!(port.hba.as_ref().unwrap().auth_method, uaMD5);

    let mut port = unix_port("carol", "db2");
    check_hba(&mut port).unwrap();
    assert_eq!(port.hba.as_ref().unwrap().auth_method, uaReject);
}

#[test]
fn check_hba_ipv6_and_ssl_skip() {
    assert!(load_hba_content(
        "v6.conf",
        "hostssl all all 127.0.0.1/32 md5\nhost all all ::1/128 trust\n"
    ));

    // hostssl never matches without SSL; ::1 falls to line 2.
    let mut port = inet_port("::1", "alice", "postgres");
    check_hba(&mut port).unwrap();
    assert_eq!(port.hba.as_ref().unwrap().linenumber, 2);

    let mut port = inet_port("127.0.0.1", "alice", "postgres");
    check_hba(&mut port).unwrap();
    assert_eq!(port.hba.as_ref().unwrap().auth_method, uaImplicitReject);
}

// C accepts radius/oauth in every build (hba.c:1750,1752); their handlers are
// unported here, so the parse must produce C's clean per-line config error
// (hba.c:1771 "not supported by this build") — never a panic. load_hba then
// returns false, which the postmaster maps to reload-keeps-old-config
// (process_pm_reload_request) or a clean startup FATAL (PostmasterMain).
#[test]
fn radius_oauth_unported_reject_cleanly() {
    for method in ["radius", "oauth"] {
        let err = parse_one(&format!("host all all 127.0.0.1/32 {method}")).unwrap_err();
        assert_eq!(
            err,
            format!("invalid authentication method \"{method}\": not supported by this build")
        );
        assert!(!load_hba_content(
            &format!("{method}.conf"),
            &format!("local all all trust\nhost all all 127.0.0.1/32 {method}\n")
        ));
    }
}

#[test]
fn load_hba_failures() {
    // A file with no entries fails.
    assert!(!load_hba_content("empty.conf", "# nothing here\n"));
    // A parse error anywhere fails the load.
    assert!(!load_hba_content(
        "bad.conf",
        "local all all trust\nlocal all all frobnicate\n"
    ));
    // Missing file fails.
    setup();
    {
        let _g = GUC_LOCK.lock().unwrap();
        guc_tables::vars::HbaFileName.write(Some("/nonexistent/pg_hba.conf".to_string()));
        assert!(!load_hba().unwrap());
    }
}

// The M1 fixture: byte-identical to the entries C initdb writes.
#[test]
fn load_hba_initdb_default() {
    assert!(load_hba_content(
        "initdb_default.conf",
        concat!(
            "# TYPE  DATABASE        USER            ADDRESS                 METHOD\n",
            "\n",
            "# \"local\" is for Unix domain socket connections only\n",
            "local   all             all                                     trust\n",
            "# IPv4 local connections:\n",
            "host    all             all             127.0.0.1/32            trust\n",
            "# IPv6 local connections:\n",
            "host    all             all             ::1/128                 trust\n",
            "# Allow replication connections from localhost, by a user with the\n",
            "# replication privilege.\n",
            "local   replication     all                                     trust\n",
            "host    replication     all             127.0.0.1/32            trust\n",
            "host    replication     all             ::1/128                 trust\n"
        )
    ));
    let mut port = unix_port("malisper", "postgres");
    check_hba(&mut port).unwrap();
    let hba = port.hba.as_ref().unwrap();
    assert_eq!(hba.auth_method, uaTrust);
    assert_eq!(hba.conntype, ctLocal);
    assert_eq!(hba.linenumber, 4);
}

// Real C-initdb'd datadir: PGRUST_TEST_C_DATADIR=/path/to/datadir.
#[test]
fn load_hba_from_c_initdb_datadir() {
    let Ok(datadir) = std::env::var("PGRUST_TEST_C_DATADIR") else {
        return;
    };
    setup();
    let g = GUC_LOCK.lock().unwrap();
    guc_tables::vars::HbaFileName.write(Some(format!("{datadir}/pg_hba.conf")));
    assert!(load_hba().unwrap());
    drop(g);

    let mut port = unix_port("malisper", "postgres");
    check_hba(&mut port).unwrap();
    let hba = port.hba.as_ref().unwrap();
    assert_eq!(hba.auth_method, uaTrust);
    assert_eq!(hba.conntype, ctLocal);
}

#[test]
fn check_usermap_null_map() {
    setup();
    assert_eq!(
        check_usermap(None, "alice", "alice", false).unwrap(),
        STATUS_OK
    );
    assert_eq!(
        check_usermap(None, "alice", "bob", false).unwrap(),
        STATUS_ERROR
    );
    assert_eq!(
        check_usermap(None, "Alice", "alice", true).unwrap(),
        STATUS_OK
    );
    assert_eq!(
        check_usermap(None, "Alice", "alice", false).unwrap(),
        STATUS_ERROR
    );
}

#[test]
fn load_ident_and_usermap() {
    setup();
    let g = GUC_LOCK.lock().unwrap();
    let path = write_temp(
        "ident.conf",
        "# map system pg\nmymap   osuser   alice\nmymap   root     bob\n",
    );
    guc_tables::vars::IdentFileName.write(Some(path));
    assert!(load_ident().unwrap());
    drop(g);

    assert_eq!(
        check_usermap(Some("mymap"), "alice", "osuser", false).unwrap(),
        STATUS_OK
    );
    assert_eq!(
        check_usermap(Some("mymap"), "bob", "osuser", false).unwrap(),
        STATUS_ERROR
    );
    assert_eq!(
        check_usermap(Some("othermap"), "alice", "osuser", false).unwrap(),
        STATUS_ERROR
    );
}

#[test]
fn authname_table() {
    setup();
    assert_eq!(hba_authname(uaTrust), "trust");
    assert_eq!(hba_authname(uaReject), "reject");
    assert_eq!(hba_authname(uaImplicitReject), "implicit reject");
    assert_eq!(hba_authname(uaSCRAM), "scram-sha-256");
    assert_eq!(hba_authname(uaPeer), "peer");
    assert_eq!(hba_authname(types_core::init::uaOAuth), "oauth");
    assert_eq!(hba_seams::hba_authname::call(uaMD5), "md5");
}

#[test]
fn regex_tokens_compile_and_match() {
    setup();
    let line = parse_one("local all /^ali.*$ trust").expect("parses");
    assert!(line.roles[0].regex, "regex marker set by regcomp_auth_token");
    assert!(crate::check::check_role("alice", 101, &line.roles, false).unwrap());
    assert!(!crate::check::check_role("bob", 102, &line.roles, false).unwrap());
}

#[test]
fn samehost_matches_loopback_samenet_matches_subnet() {
    setup();
    use crate::check::{check_same_host_or_net, ipaddr_to_sockaddr};
    use std::net::{IpAddr, Ipv4Addr};
    let lo = ipaddr_to_sockaddr(&IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert!(check_same_host_or_net(&lo, types_startup::ipCmpSameHost).unwrap());
    assert!(check_same_host_or_net(&lo, types_startup::ipCmpSameNet).unwrap());
    // 127.0.0.2 is not an interface address but shares loopback's /8 net.
    let near = ipaddr_to_sockaddr(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
    assert!(!check_same_host_or_net(&near, types_startup::ipCmpSameHost).unwrap());
    assert!(check_same_host_or_net(&near, types_startup::ipCmpSameNet).unwrap());
    let far = ipaddr_to_sockaddr(&IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)));
    assert!(!check_same_host_or_net(&far, types_startup::ipCmpSameHost).unwrap());
    assert!(!check_same_host_or_net(&far, types_startup::ipCmpSameNet).unwrap());
}

// ========================================================================
// authfuzz robustness harness (campaign sub-target 1, docs/authfuzz/charter.md)
//
// Feeds adversarial pg_hba.conf / pg_ident.conf bytes to the tokenizer +
// line parsers and asserts the pgrust ST1 surface (next_token /
// next_field_expand / tokenize_auth_file / parse_hba_line / parse_ident_line)
// never panics and preserves its parse/err_msg invariants. This is the
// pgrust half of the ST1 differential; the C-oracle ASan half runs on the
// CI cluster against the verbatim hba.c tokenizer (see the charter). A panic here
// is a pgrust robustness finding for docs/authfuzz/findings-hba.md.
//
// DOMAIN CARVE (hermetic, matches the guc_file harness precedent): inputs
// containing case-insensitive "include" or a '@'-file reference are skipped
// before the driver runs, so no include/@ expansion reaches disk IO or the
// unset get_conf_files_in_dir seam. The whole next_token quote/NUL/comma/
// continuation lexer plus both line parsers stay fully exercised.
//
// The driver is FAITHFUL to load_hba()/load_ident(): a tokenized line whose
// err_msg is set is never handed to a line parser (mirrors the postmaster
// loop; parse_*_line's debug_assert on non-empty fields only holds there).

fn af_carved(b: &[u8]) -> bool {
    b.windows(7).any(|w| w.eq_ignore_ascii_case(b"include")) || b.contains(&b'@')
}

/// Drive one input through the full ST1 pgrust surface at LOG elevel (errors
/// are recorded into err_msg, never thrown, so every call returns Ok). Any
/// panic unwinds into the test harness = a finding.
fn af_drive(bytes: &[u8]) {
    if af_carved(bytes) {
        return;
    }
    let file = FileHandle { content: bytes.to_vec(), depth: 0 };
    let mut lines = Vec::new();
    tokenize_auth_file("authfuzz", &file, &mut lines, LOG, 0)
        .expect("tokenize_auth_file must not throw at LOG");
    for tl in lines.iter() {
        // Faithful to load_hba/load_ident: skip lines the tokenizer flagged.
        if tl.err_msg.is_some() {
            continue;
        }
        let mut hb = tl.clone();
        let _ = parse_hba_line(&mut hb, LOG).expect("parse_hba_line must not throw at LOG");
        let mut id = tl.clone();
        let _ = parse_ident_line(&mut id, LOG).expect("parse_ident_line must not throw at LOG");
    }
}

// Adversarial seed corpus: valid + hostile pg_hba.conf / pg_ident.conf lines.
// Addresses are kept numeric / keyword to avoid getaddrinfo DNS on hostnames
// during the bounded local run (see charter "harness carves").
const AF_SEEDS: &[&str] = &[
    // --- valid, every reachable auth method ---
    "local all all trust\n",
    "local all all reject\n",
    "host all all 0.0.0.0/0 password\n",
    "host all all 127.0.0.1/32 md5\n",
    "host all all ::1/128 scram-sha-256\n",
    "hostssl all all 0.0.0.0/0 cert clientcert=verify-full\n",
    "host all all 10.0.0.0/8 ident map=m1\n",
    "local all all peer\n",
    "host all all 192.168.0.0 255.255.0.0 trust\n",
    "host all all samehost trust\n",
    "host all all samenet scram-sha-256\n",
    // --- build-rejected + deferred methods (must reject, not panic) ---
    "host all all 0.0.0.0/0 gss\n",
    "host all all 0.0.0.0/0 sspi\n",
    "host all all 0.0.0.0/0 pam\n",
    "host all all 0.0.0.0/0 bsd\n",
    "host all all 0.0.0.0/0 ldap\n",
    "host all all 0.0.0.0/0 radius\n",
    "host all all 0.0.0.0/0 oauth\n",
    "host all all 0.0.0.0/0 bogusmethod\n",
    // --- option key/value coverage ---
    "host all all 0.0.0.0/0 md5 map=m clientcert=verify-ca\n",
    "host all all 0.0.0.0/0 cert clientname=DN\n",
    "host all all 0.0.0.0/0 scram-sha-256 badopt=x\n",
    "host all all 0.0.0.0/0 md5 optionwithnoequalssign\n",
    // --- quoting / tokenizer adversarial ---
    "local \"all\" \"all\" trust\n",
    "local \"a\"\"b\" all trust\n",           // doubled-quote literal
    "local \"unterminated all trust\n",        // unbalanced quote
    "local all,+group,\"q\" all trust\n",      // comma lists + member + quote
    "local \"\" \"\" trust\n",                 // empty quoted tokens
    "host \"db,with,commas\" all 0.0.0.0/0 trust\n",
    "local all all trust # trailing comment\n",
    "# whole line comment\n",
    "\n\n   \n\t\n",                            // blank / whitespace lines
    "local all all \\\n trust\n",              // backslash continuation
    "local all all trust\\",                    // trailing backslash at EOF
    // --- regex auth tokens (ident + hba role/db) ---
    "local all /^ali.*$ trust\n",
    "host \"/^db[0-9]+$\" all 0.0.0.0/0 trust\n",
    "local all /[ trust\n",                     // invalid regex
    // --- pg_ident.conf shapes ---
    "map1 /^(.*)@example\\.com$ \\1\n",
    "map1 systemuser pguser\n",
    "map1 /^(.*)$ \\1extra\n",
    "map1 sys\n",                               // missing entry at end of line
    "map1 a b c d\n",                           // multiple values
    // --- CIDR / netmask edge cases ---
    "host all all 0.0.0.0/33 trust\n",          // out-of-range prefix
    "host all all 1.2.3.4/-1 trust\n",
    "host all all not_an_ip/24 trust\n",
    "host all all ::/0 trust\n",
    "host all all fe80::/10 trust\n",
    // --- structural noise ---
    "onlyonefield\n",
    "local\n",
    "local all\n",
    "local all all\n",
    "   local    all    all    trust   \n",
];

#[test]
fn authfuzz_corpus_no_panic() {
    setup();
    let _g = GUC_LOCK.lock().unwrap();
    for seed in AF_SEEDS {
        af_drive(seed.as_bytes());
    }
    // NUL-embedded variants of every seed (bug-104-class quote/NUL surface).
    for seed in AF_SEEDS {
        let b = seed.as_bytes();
        for cut in [0usize, 1, b.len() / 2, b.len().saturating_sub(1)] {
            let mut v = b.to_vec();
            v.insert(cut.min(v.len()), 0u8);
            af_drive(&v);
        }
    }
    // Overlong tokens around the MAX_TOKEN=256 boundary.
    for n in [1usize, 255, 256, 257, 1024, 8192] {
        af_drive(format!("local {} all trust\n", "a".repeat(n)).as_bytes());
        af_drive(format!("local all all \"{}\"\n", "x".repeat(n)).as_bytes());
    }
}

/// splitmix64: deterministic, dependency-free PRNG for reproducible mutation.
struct Sm64(u64);
impl Sm64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

#[test]
fn authfuzz_mutation_no_panic() {
    setup();
    let _g = GUC_LOCK.lock().unwrap();
    // Bounded by default for `cargo test`; PGRUST_AUTHFUZZ_ITERS scales the
    // local soak (the true 5-20M-exec C-side ASan campaign is a CI cluster job).
    let iters: u64 = std::env::var("PGRUST_AUTHFUZZ_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);
    let mut rng = Sm64(0xA1B2_C3D4_E5F6_0917);
    // Interesting bytes the structural grammar keys on.
    const INTERESTING: &[u8] = b" \t\r\n\",#@/+=\\.:0123456789abcdefhilmostu\x00\xff";
    for _ in 0..iters {
        let seed = AF_SEEDS[(rng.next() as usize) % AF_SEEDS.len()].as_bytes();
        let mut buf = seed.to_vec();
        let nmut = 1 + (rng.next() % 8) as usize;
        for _ in 0..nmut {
            if buf.is_empty() {
                buf.push(b'a');
            }
            match rng.next() % 5 {
                0 => {
                    // flip a byte to an interesting value
                    let i = (rng.next() as usize) % buf.len();
                    buf[i] = INTERESTING[(rng.next() as usize) % INTERESTING.len()];
                }
                1 => {
                    // insert an interesting byte
                    let i = (rng.next() as usize) % (buf.len() + 1);
                    buf.insert(i, INTERESTING[(rng.next() as usize) % INTERESTING.len()]);
                }
                2 => {
                    // delete a byte
                    let i = (rng.next() as usize) % buf.len();
                    buf.remove(i);
                }
                3 => {
                    // duplicate a span (token amplification)
                    let i = (rng.next() as usize) % buf.len();
                    let n = 1 + (rng.next() as usize) % 32;
                    let span: Vec<u8> = buf[i..].iter().take(n).copied().collect();
                    buf.splice(i..i, span);
                }
                _ => {
                    // truncate
                    let i = (rng.next() as usize) % buf.len();
                    buf.truncate(i);
                }
            }
            if buf.len() > 64 * 1024 {
                buf.truncate(32 * 1024);
            }
        }
        af_drive(&buf);
    }
}
