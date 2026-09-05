//! audit-18.6 b095 (a186-candidate-fp-smgr-md-a911a046adea495a28ab-1):
//! md.c:1230 mdnblocks ALWAYS opens segment 0 with EXTENSION_FAIL before
//! answering, so a backend that has not yet opened the fork errors with
//! "could not open file ...: No such file or directory" when the file is gone
//! from disk (a fresh session's `SELECT count(*) FROM t` / `EXPLAIN` after the
//! relation file was unlinked out from under the server). The Rust size cache
//! answered from memory without touching the file.

mod common;

use types_core::primitive::{ForkNumber, INVALID_PROC_NUMBER};
use types_core::BLCKSZ;
use types_storage::{RelFileLocator, RelFileLocatorBackend};

#[test]
fn nblocks_on_fresh_open_errors_when_segment0_is_gone() {
    let dir = common::setup("nblocks_unlinked");
    let key = RelFileLocatorBackend {
        locator: RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: 16385 },
        backend: INVALID_PROC_NUMBER,
    };
    let fork = ForkNumber::MAIN_FORKNUM;

    smgr::smgropen(key.locator, key.backend).unwrap();
    smgr::smgrcreate(key, fork, false).unwrap();
    let block = [0x11u8; BLCKSZ];
    smgr::smgrextend(key, fork, 0, &block, false).unwrap();
    smgr::smgrextend(key, fork, 1, &block, false).unwrap();
    assert_eq!(smgr::smgrnblocks(key, fork).unwrap(), 2);

    // End of transaction: the SMgrRelation is destroyed and its fds closed
    // (the next smgropen is a fresh backend's first touch of the fork).
    smgr::AtEOXact_SMgr().unwrap();
    std::fs::remove_file(dir.join("base/5/16385")).unwrap();

    // smgropen (mdopen) touches no file; the first mdnblocks is the open.
    smgr::smgropen(key.locator, key.backend).unwrap();
    let r = smgr::smgrnblocks(key, fork);
    match r {
        Err(e) => assert!(
            e.message.starts_with("could not open file \"base/5/16385\""),
            "unexpected error: {}",
            e.message
        ),
        Ok(n) => panic!("mdnblocks answered {n} for an unlinked segment 0 (C: could not open file)"),
    }

    let _ = std::fs::remove_dir_all(dir);
}
