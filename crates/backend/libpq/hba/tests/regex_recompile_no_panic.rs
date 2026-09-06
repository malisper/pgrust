//! audit-18.6 remediation b205 — hba.c:348 regexec_auth_token.
//!
//! C compiles a '/'-prefixed pg_hba.conf token once (regcomp_auth_token)
//! and only executes it at connection time. The port validates at parse
//! time and recompiles the pattern in the connecting backend; a recompile
//! failure must be handled like any other regexec failure (no match for
//! check_role/check_db, C's "regular expression match ... failed" LOG for
//! the ident map), never a backend panic.
//!
//! Own test binary: the regex seams are set-once, and this one installs a
//! pg_regcomp that accepts the parse-time compile and refuses every later
//! one, standing in for an engine-state divergence between the parse-time
//! and execution-time compiles.

use std::sync::atomic::{AtomicUsize, Ordering};

use regex::{RegMatch, RegcompResult, RegexCompiled, RegexFailure, RegexecResult};
use types_core::init::uaImplicitReject;
use types_core::{Oid, PgWChar};
use types_error::PgResult;
use types_startup::{ClientSocket, Port};

static REGCOMP_CALLS: AtomicUsize = AtomicUsize::new(0);

fn regcomp_first_only(_pattern: &[PgWChar], _cflags: i32, _collation: Oid) -> PgResult<RegcompResult> {
    let n = REGCOMP_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(if n == 0 {
        RegcompResult::Compiled(RegexCompiled { engine: std::rc::Rc::new(()), re_nsub: 0 })
    } else {
        RegcompResult::Failed(RegexFailure { message: "out of memory".to_string() })
    })
}

fn regexec_always_matches(
    _re: &RegexCompiled,
    _data: &[PgWChar],
    _search_start: i32,
    _pmatch: &mut [RegMatch],
) -> PgResult<RegexecResult> {
    Ok(RegexecResult::Matched)
}

fn regfree_noop(_re: RegexCompiled) {}

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

#[test]
fn regex_role_token_recompile_failure_does_not_panic() {
    guc_tables::init_seams();
    elog::init_seams();
    hba::init_seams();
    mbutils::init_seams();
    regex_core_seams::pg_regcomp::set(regcomp_first_only);
    regex_core_seams::pg_regexec::set(regexec_always_matches);
    regex_core_seams::pg_regfree::set(regfree_noop);
    acl_seams::get_role_oid::set(|_name, _missing_ok| Ok(0));

    let dir = std::env::temp_dir().join(format!("pgrust_hba_recompile_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("pg_hba.conf");
    std::fs::write(&path, "local all /^ali.*$ trust\n").unwrap();
    guc_tables::vars::HbaFileName.write(Some(path.to_string_lossy().into_owned()));

    // Parse-time compile (the one accepted call).
    assert!(hba::load_hba().unwrap(), "regex role line loads");
    assert_eq!(REGCOMP_CALLS.load(Ordering::SeqCst), 1);

    // Connection-time match: the recompile fails. C cannot reach this
    // state; the port must degrade to "no match" (implicit reject), not
    // panic the backend.
    let mut port = unix_port("alice", "postgres");
    hba::check_hba(&mut port).unwrap();
    assert!(REGCOMP_CALLS.load(Ordering::SeqCst) >= 2, "execution-time compile attempted");
    assert_eq!(port.hba.as_ref().map(|h| h.auth_method), Some(uaImplicitReject));
}
