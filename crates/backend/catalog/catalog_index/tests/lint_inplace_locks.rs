//! Unit-shaped wiring for crates/backend/catalog/catalog_index/tests/lint-inplace-locks.sh — the standing guard
//! for GL-INPLACE-1 defect (A): a TRANSACTIONAL pg_class updater that does not
//! hold LOCKTAG_TUPLE at InplaceUpdateTupleLock silently discards a concurrent
//! inplace writer's relfrozenxid/relminmxid advance, which is a durable
//! wraparound-safety regression rather than a transient wrong answer. Nothing
//! else catches it: there is no error, no assertion (C's heapam.c:4241
//! LockHeldByMe assert has no counterpart here), and no output difference —
//! only a catalog that drifts. See notes/GL-INPLACE-1-letter.md.
//!
//! Lives in catalog_index because it hosts RelationSetNewRelfilenumber, the
//! one of the seven sites that writes BOTH relfrozenxid and relminmxid:
//! `cargo test -p catalog_index --test lint_inplace_locks`.

use std::path::Path;
use std::process::Command;

#[test]
fn inplace_lock_lint_passes() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../..")
        .canonicalize()
        .expect("repo root");
    let script = repo.join("crates/backend/catalog/catalog_index/tests/lint-inplace-locks.sh");
    assert!(script.is_file(), "missing {}", script.display());

    let out = Command::new("bash")
        .arg(&script)
        .current_dir(&repo)
        .output()
        .expect("run lint-inplace-locks.sh");

    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "lint-inplace-locks.sh failed — either a transactional pg_class \
         updater lost its InplaceUpdateTupleLock, a C-exact-unlocked site \
         grew one, or a new/moved pg_class writer needs classifying against \
         the C original:\n{report}"
    );
}
