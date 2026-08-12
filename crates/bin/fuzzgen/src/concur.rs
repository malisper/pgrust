//! CONCUR deck — two-session interleavings for the concurrency arms in
//! docs/fuzzing/antithesis-faultonly-inventory.md that are SQL-reachable
//! with interleaved sessions (NOT true fault injection):
//!
//!   - EvalPlanQual (EPQ) recheck arms in nodeModifyTable.c /
//!     nodeLockRows.c / execMain.c (LD9's CONCURRENCY rows: TM_Updated /
//!     TM_Deleted re-check, requal pass AND fail, update-chain follow,
//!     cross-partition move);
//!   - MERGE matched/not-matched re-check under concurrent UPDATE/DELETE
//!     (ExecMergeMatched / ExecMergeNotMatched);
//!   - ON CONFLICT with a concurrent inserter/updater (speculative
//!     insertion wait, arbiter re-check, ExecOnConflictUpdate TM_Updated,
//!     ExecCheckTupleVisible / ExecCheckTIDVisible);
//!   - row-lock conflict matrix (FOR UPDATE / NO KEY UPDATE / SHARE /
//!     KEY SHARE x NOWAIT / SKIP LOCKED) and tuple-lock upgrade through
//!     a multixact;
//!   - CREATE INDEX CONCURRENTLY / REINDEX CONCURRENTLY lock-wait phases;
//!   - DETACH PARTITION CONCURRENTLY cancelled between its two
//!     transactions (the pending-detach state) + FINALIZE / re-DETACH
//!     completion (SQLcov-A's inventory entry).
//!
//! Model reuse: these are plain `ssi::Scenario`s appended to the ssidiff
//! deck — the ordered-schedule + blocked/parked/reap engine already
//! provides the deterministic lock-step barrier (a step that lock-waits
//! is parked; the next step is the barrier release). Outcomes compare
//! differentially A-vs-B like every other scenario: which statement
//! blocks, the final row counts/tags/SQLSTATEs after the EPQ recheck,
//! and the final table state probes. A divergence here is an
//! isolation-semantics bug (HIGH severity per the lane charter).
//!
//! Determinism notes:
//!   - every scenario runs READ COMMITTED (EPQ fires only there) unless
//!     stated; every wait-producing step has an unambiguous single lock
//!     holder ordered before it;
//!   - the DETACH cancel targets its victim through pg_stat_activity by
//!     query shape (never by pid literal); the canceler excludes itself
//!     by pid <> pg_backend_pid() and state = 'active';
//!   - statement_timeout bounds every wait (57014 from a TIMEOUT is a
//!     rig bug — the deliberate pg_cancel_backend 57014 in the detach
//!     scenarios is the EXPECTED outcome and compares strictly A vs B);
//!   - CIC/REINDEX completion may land inside the reap grace window on
//!     one engine and at the end-of-scenario drain on the other (build
//!     time is wall-clock); the compare's shape-mismatch fallback
//!     digests ungrouped steps as (ever-blocked, final outcome), so
//!     that timing skew is masked by construction while a real outcome
//!     or blocked-classification skew still reports.

use crate::ssi::{Scenario, Step};

fn step(name: &'static str, session: usize, sql: &str) -> Step {
    Step { name, session, sql: sql.to_string(), group: None }
}

const T_C: &str = "DROP TABLE IF EXISTS concur_t; \
     CREATE TABLE concur_t (id int PRIMARY KEY, class int, v int); \
     INSERT INTO concur_t SELECT g, g % 2 + 1, 10 FROM generate_series(1, 100) g";

fn probe_t() -> (&'static str, String) {
    ("concur_t", "SELECT id, class, v FROM concur_t ORDER BY id".to_string())
}

fn to_20s() -> Vec<String> {
    vec!["SET statement_timeout = '20s'".into()]
}

/// The CONCUR scenario deck (appended to ssi::deck()).
pub fn deck() -> Vec<Scenario> {
    let mut v = Vec::new();

    // ---------------------------------------------------- EPQ: UPDATE --
    // EPQ requal PASSES: s2's qual references only the key, so after the
    // recheck against s1's committed version the UPDATE still applies —
    // and must apply on top of the NEW version (v = 200 + 1).
    v.push(Scenario {
        name: "epq-update-qual-pass",
        description: "RC UPDATE blocks on concurrent update; EPQ requal passes; applies on new version",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = 200 WHERE id = 5"),
            step("s2-upd", 1, "UPDATE concur_t SET v = v + 1 WHERE id = 5"), // blocks; EPQ
            step("s1-commit", 0, "COMMIT"), // s2 unblocks -> UPDATE 1, v = 201
        ],
        probes: vec![probe_t()],
    });

    // EPQ requal FAILS: s2's qual pins the OLD value; the recheck against
    // s1's committed version fails and the update silently drops the row
    // (UPDATE 0) — the TM_Updated -> EvalPlanQual -> no-tuple arm.
    v.push(Scenario {
        name: "epq-update-qual-fail",
        description: "RC UPDATE with value qual; concurrent update falsifies it; EPQ recheck -> UPDATE 0",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = 200 WHERE id = 6"),
            step("s2-upd", 1, "UPDATE concur_t SET v = v + 1 WHERE id = 6 AND v = 10"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 unblocks -> UPDATE 0
        ],
        probes: vec![probe_t()],
    });

    // Update-CHAIN follow: the blocked update must chase a two-version
    // ctid chain committed by s1 (heap EPQ fetch follows t_ctid hops).
    v.push(Scenario {
        name: "epq-update-chain",
        description: "RC UPDATE follows a two-hop committed update chain through EPQ",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd1", 0, "UPDATE concur_t SET v = 100 WHERE id = 7"),
            step("s1-upd2", 0, "UPDATE concur_t SET v = 101 WHERE id = 7"),
            step("s2-upd", 1, "UPDATE concur_t SET v = v + 1 WHERE id = 7"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 -> UPDATE 1, v = 102
        ],
        probes: vec![probe_t()],
    });

    // ---------------------------------------------------- EPQ: DELETE --
    // DELETE vs concurrent UPDATE (requal fail) and UPDATE vs concurrent
    // DELETE (TM_Deleted -> row gone -> UPDATE 0) in two phases.
    v.push(Scenario {
        name: "epq-delete",
        description: "DELETE qual falsified by concurrent update -> DELETE 0; UPDATE on concurrently-deleted row -> UPDATE 0",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = 200 WHERE id = 8"),
            step("s2-del", 1, "DELETE FROM concur_t WHERE id = 8 AND v = 10"), // blocks; EPQ fail
            step("s1-commit", 0, "COMMIT"), // s2 -> DELETE 0
            step("s1-begin2", 0, "BEGIN"),
            step("s1-del", 0, "DELETE FROM concur_t WHERE id = 9"),
            step("s2-upd", 1, "UPDATE concur_t SET v = v + 1 WHERE id = 9"), // blocks; TM_Deleted
            step("s1-commit2", 0, "COMMIT"), // s2 -> UPDATE 0
            step("s1-begin3", 0, "BEGIN"),
            step("s1-del2", 0, "DELETE FROM concur_t WHERE id = 11"),
            step("s2-del2", 1, "DELETE FROM concur_t WHERE id = 11"), // blocks; TM_Deleted
            step("s1-commit3", 0, "COMMIT"), // s2 -> DELETE 0
        ],
        probes: vec![probe_t()],
    });

    // ---------------------------------------------- EPQ: SELECT FOR ... --
    // ExecLockRows EPQ re-fetch: requal fail returns zero rows; requal
    // pass returns the committed NEW row values.
    v.push(Scenario {
        name: "epq-forupdate-requal",
        description: "SELECT FOR UPDATE EPQ: value qual falsified -> 0 rows; key-only qual -> new version",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = 999 WHERE id = 12"),
            step("s2-lock-fail", 1, "SELECT id, v FROM concur_t WHERE id = 12 AND v = 10 FOR UPDATE"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 -> 0 rows
            step("s1-begin2", 0, "BEGIN"),
            step("s1-upd2", 0, "UPDATE concur_t SET v = 42 WHERE id = 13"),
            step("s2-lock-pass", 1, "SELECT id, v FROM concur_t WHERE id = 13 FOR UPDATE"), // blocks
            step("s1-commit2", 0, "COMMIT"), // s2 -> (13, 42)
        ],
        probes: vec![probe_t()],
    });

    // EPQ with a JOIN under FOR UPDATE OF one side: the recheck must
    // re-evaluate the join with the refetched locked row plus the other
    // relation's non-locked rowmark.
    v.push(Scenario {
        name: "epq-lockrows-join",
        description: "FOR UPDATE OF a in a join; concurrent update forces EPQ re-eval across both rels",
        setup: vec![
            T_C.into(),
            "DROP TABLE IF EXISTS concur_b; \
             CREATE TABLE concur_b (id int PRIMARY KEY, w int); \
             INSERT INTO concur_b SELECT g, g * 100 FROM generate_series(1, 100) g"
                .into(),
        ],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = 77 WHERE id = 14"),
            step(
                "s2-lockjoin",
                1,
                "SELECT a.id, a.v, b.w FROM concur_t a JOIN concur_b b ON a.id = b.id \
                 WHERE a.id = 14 FOR UPDATE OF a",
            ), // blocks; EPQ re-eval of the join
            step("s1-commit", 0, "COMMIT"), // s2 -> (14, 77, 1400)
            // Requal-fail flavor through the join qual.
            step("s1-begin2", 0, "BEGIN"),
            step("s1-upd2", 0, "UPDATE concur_t SET v = 500 WHERE id = 15"),
            step(
                "s2-lockjoin-fail",
                1,
                "SELECT a.id, a.v, b.w FROM concur_t a JOIN concur_b b ON a.id = b.id \
                 WHERE a.id = 15 AND a.v < 100 FOR UPDATE OF a",
            ), // blocks; recheck fails
            step("s1-commit2", 0, "COMMIT"), // s2 -> 0 rows
        ],
        probes: vec![probe_t()],
    });

    // ------------------------------------------- EPQ: cross-partition --
    // s1 moves the row to another partition (cross-partition update =
    // delete + insert); the blocked s2 statement finds the tuple moved:
    // C raises "tuple to be locked was already moved..." — whatever the
    // exact class, A and B must agree.
    v.push(Scenario {
        name: "epq-cross-partition",
        description: "UPDATE/SELECT FOR UPDATE blocked on a row concurrently moved to another partition",
        setup: vec![
            "DROP TABLE IF EXISTS concur_p; \
             CREATE TABLE concur_p (id int, v int) PARTITION BY RANGE (id); \
             CREATE TABLE concur_p_a PARTITION OF concur_p FOR VALUES FROM (0) TO (100); \
             CREATE TABLE concur_p_b PARTITION OF concur_p FOR VALUES FROM (100) TO (200); \
             INSERT INTO concur_p SELECT g, 10 FROM generate_series(1, 99) g"
                .into(),
        ],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-move", 0, "UPDATE concur_p SET id = 150 WHERE id = 50"), // row moves a -> b
            step("s2-upd", 1, "UPDATE concur_p SET v = v + 1 WHERE id = 50"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2: moved-row outcome (C = spec)
            step("s1-begin2", 0, "BEGIN"),
            step("s1-move2", 0, "UPDATE concur_p SET id = 151 WHERE id = 51"),
            step("s2-lock", 1, "SELECT id, v FROM concur_p WHERE id = 51 FOR UPDATE"), // blocks
            step("s1-commit2", 0, "COMMIT"),
        ],
        probes: vec![(
            "concur_p",
            "SELECT id, v FROM concur_p ORDER BY id".into(),
        )],
    });

    // -------------------------------------------------- MERGE re-check --
    // ExecMergeMatched TM_Updated: WHEN MATCHED UPDATE re-applies on the
    // concurrently-committed new version.
    v.push(Scenario {
        name: "merge-concurrent-update",
        description: "MERGE WHEN MATCHED UPDATE re-checks after concurrent update (TM_Updated arm)",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = 500 WHERE id = 20"),
            step(
                "s2-merge",
                1,
                "MERGE INTO concur_t t USING (VALUES (20, 1)) AS s(id, dv) ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = t.v + s.dv \
                 WHEN NOT MATCHED THEN INSERT (id, class, v) VALUES (s.id, 9, s.dv)",
            ), // blocks; TM_Updated re-check
            step("s1-commit", 0, "COMMIT"), // s2 -> MERGE 1, v = 501
        ],
        probes: vec![probe_t()],
    });

    // ExecMergeMatched TM_Deleted -> ExecMergeNotMatched: the matched row
    // vanishes; the merge must fall through to the NOT MATCHED insert.
    v.push(Scenario {
        name: "merge-concurrent-delete",
        description: "MERGE matched row concurrently deleted -> falls to NOT MATCHED insert (TM_Deleted arm)",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-del", 0, "DELETE FROM concur_t WHERE id = 21"),
            step(
                "s2-merge",
                1,
                "MERGE INTO concur_t t USING (VALUES (21, 777)) AS s(id, nv) ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = s.nv \
                 WHEN NOT MATCHED THEN INSERT (id, class, v) VALUES (s.id, 9, s.nv)",
            ), // blocks; TM_Deleted -> NOT MATCHED
            step("s1-commit", 0, "COMMIT"), // s2 -> MERGE 1 (insert)
        ],
        probes: vec![probe_t()],
    });

    // MERGE WHEN MATCHED THEN DELETE racing a concurrent update: the
    // delete's re-check runs against the new version.
    v.push(Scenario {
        name: "merge-delete-concurrent-update",
        description: "MERGE WHEN MATCHED DELETE re-checks after concurrent update",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = 600 WHERE id = 22"),
            step(
                "s2-merge",
                1,
                "MERGE INTO concur_t t USING (VALUES (22)) AS s(id) ON t.id = s.id \
                 WHEN MATCHED THEN DELETE",
            ), // blocks; re-check then delete
            step("s1-commit", 0, "COMMIT"), // s2 -> MERGE 1 (delete)
        ],
        probes: vec![probe_t()],
    });

    // MERGE qualified WHEN MATCHED AND <value qual>: the concurrent
    // update falsifies the qual so the action must NOT fire (and with no
    // other WHEN clause the row is skipped: MERGE 0).
    v.push(Scenario {
        name: "merge-qual-fail",
        description: "MERGE WHEN MATCHED AND qual falsified by concurrent update -> action skipped",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = 700 WHERE id = 23"),
            step(
                "s2-merge",
                1,
                "MERGE INTO concur_t t USING (VALUES (23)) AS s(id) ON t.id = s.id \
                 WHEN MATCHED AND t.v = 10 THEN DELETE",
            ), // blocks; requal fails -> no action
            step("s1-commit", 0, "COMMIT"), // s2 -> MERGE 0
        ],
        probes: vec![probe_t()],
    });

    // -------------------------------------------- ON CONFLICT races -----
    // Speculative-insertion wait: the second inserter waits on the first
    // inserter's xact, then takes the DO NOTHING / DO UPDATE arm on
    // commit and the plain-insert arm on rollback (ExecInsert arbiter
    // re-check + ExecCheckTupleVisible/ExecCheckTIDVisible).
    v.push(Scenario {
        name: "onconflict-insert-race",
        description: "INSERT ON CONFLICT DO NOTHING vs concurrent inserter: commit -> skip, rollback -> insert",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-ins", 0, "INSERT INTO concur_t VALUES (301, 1, 1)"),
            step("s2-ins", 1, "INSERT INTO concur_t VALUES (301, 2, 2) ON CONFLICT (id) DO NOTHING"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 -> INSERT 0 0
            step("s1-begin2", 0, "BEGIN"),
            step("s1-ins2", 0, "INSERT INTO concur_t VALUES (302, 1, 1)"),
            step("s2-ins2", 1, "INSERT INTO concur_t VALUES (302, 2, 2) ON CONFLICT (id) DO NOTHING"), // blocks
            step("s1-rollback", 0, "ROLLBACK"), // s2 -> INSERT 0 1
        ],
        probes: vec![probe_t()],
    });

    v.push(Scenario {
        name: "onconflict-doupdate-race",
        description: "INSERT ON CONFLICT DO UPDATE vs concurrent inserter: takes the update arm on commit",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-ins", 0, "INSERT INTO concur_t VALUES (303, 1, 5)"),
            step(
                "s2-ins",
                1,
                "INSERT INTO concur_t VALUES (303, 2, 7) ON CONFLICT (id) \
                 DO UPDATE SET v = concur_t.v + excluded.v",
            ), // blocks; conflict-update on commit
            step("s1-commit", 0, "COMMIT"), // s2 -> INSERT 0 1, v = 12
        ],
        probes: vec![probe_t()],
    });

    // ExecOnConflictUpdate TM_Updated: the conflict tuple is being
    // updated by another session; the lock attempt re-fetches the new
    // version and applies the conflict update on top of it.
    v.push(Scenario {
        name: "onconflict-tuple-updated",
        description: "ON CONFLICT DO UPDATE where the conflict tuple is concurrently updated (lock retry arm)",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = v + 100 WHERE id = 40"),
            step(
                "s2-ins",
                1,
                "INSERT INTO concur_t VALUES (40, 2, 3) ON CONFLICT (id) \
                 DO UPDATE SET v = concur_t.v + 1",
            ), // blocks on the in-progress update
            step("s1-commit", 0, "COMMIT"), // s2 re-fetches -> v = 112
        ],
        probes: vec![probe_t()],
    });

    // -------------------------------------- row-lock conflict matrix ----
    // NOWAIT / SKIP LOCKED against a FOR NO KEY UPDATE holder, plus the
    // KEY SHARE compatibility hole (completes without waiting).
    v.push(Scenario {
        name: "lockmode-matrix",
        description: "FOR NO KEY UPDATE holder: KEY SHARE passes; SHARE/UPDATE NOWAIT 55P03; SKIP LOCKED skips",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-lock", 0, "SELECT id FROM concur_t WHERE id = 45 FOR NO KEY UPDATE"),
            step("s2-keyshare", 1, "SELECT id FROM concur_t WHERE id = 45 FOR KEY SHARE"), // compatible
            step("s2-share-nowait", 1, "SELECT id FROM concur_t WHERE id = 45 FOR SHARE NOWAIT"), // 55P03
            step("s2-upd-nowait", 1, "SELECT id FROM concur_t WHERE id = 45 FOR UPDATE NOWAIT"), // 55P03
            step(
                "s2-skip",
                1,
                "SELECT id FROM concur_t WHERE id BETWEEN 44 AND 46 ORDER BY id FOR NO KEY UPDATE SKIP LOCKED",
            ), // 44, 46
            step(
                "s2-keyshare-skip",
                1,
                "SELECT id FROM concur_t WHERE id BETWEEN 44 AND 46 ORDER BY id FOR KEY SHARE SKIP LOCKED",
            ), // all three (compatible)
            step("s1-commit", 0, "COMMIT"),
        ],
        probes: vec![probe_t()],
    });

    // Tuple-lock UPGRADE through a multixact: two FOR SHARE holders; one
    // upgrades to a real UPDATE and must wait for the co-holder, then
    // reprocess the (now single-member) lock state.
    v.push(Scenario {
        name: "lock-upgrade-update",
        description: "FOR SHARE co-holders; holder upgrades to UPDATE, waits out the multixact, applies",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s2-begin", 1, "BEGIN"),
            step("s1-share", 0, "SELECT id FROM concur_t WHERE id = 46 FOR SHARE"),
            step("s2-share", 1, "SELECT id FROM concur_t WHERE id = 46 FOR SHARE"), // multixact
            step("s1-upgrade", 0, "UPDATE concur_t SET v = 900 WHERE id = 46"), // blocks on s2
            step("s2-commit", 1, "COMMIT"), // s1 unblocks
            step("s1-commit", 0, "COMMIT"),
        ],
        probes: vec![probe_t()],
    });

    // ------------------------------------ concurrent index DDL waits ----
    // CREATE INDEX CONCURRENTLY: the builder waits out s1's open write
    // transaction (WaitForLockers phase), then completes; the index must
    // be valid and identical on both engines.
    v.push(Scenario {
        name: "cic-wait",
        description: "CREATE INDEX CONCURRENTLY waits for an open write xact, then builds a valid index",
        setup: vec![
            "DROP TABLE IF EXISTS concur_cic; \
             CREATE TABLE concur_cic (id int PRIMARY KEY, v int); \
             INSERT INTO concur_cic SELECT g, g % 7 FROM generate_series(1, 50) g"
                .into(),
        ],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-write", 0, "INSERT INTO concur_cic VALUES (1001, 42)"),
            step("s2-cic", 1, "CREATE INDEX CONCURRENTLY concur_cic_v ON concur_cic (v)"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 proceeds through remaining phases
        ],
        probes: vec![
            (
                "cic-valid",
                "SELECT c.relname, i.indisvalid, i.indisready FROM pg_index i \
                 JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'concur_cic_v'"
                    .into(),
            ),
            (
                "cic-rows",
                "SELECT count(*), sum(v) FROM concur_cic".into(),
            ),
        ],
    });

    // REINDEX INDEX CONCURRENTLY with a concurrent lock holder.
    v.push(Scenario {
        name: "reindex-conc-wait",
        description: "REINDEX INDEX CONCURRENTLY waits for a row-lock-holding xact, then swaps validly",
        setup: vec![
            "DROP TABLE IF EXISTS concur_ric; \
             CREATE TABLE concur_ric (id int PRIMARY KEY, v int); \
             INSERT INTO concur_ric SELECT g, g % 5 FROM generate_series(1, 50) g; \
             CREATE INDEX concur_ric_v ON concur_ric (v)"
                .into(),
        ],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-lock", 0, "SELECT id FROM concur_ric WHERE id = 3 FOR UPDATE"),
            step("s2-reindex", 1, "REINDEX INDEX CONCURRENTLY concur_ric_v"), // blocks
            step("s1-commit", 0, "COMMIT"),
        ],
        probes: vec![
            (
                "ric-valid",
                "SELECT c.relname, i.indisvalid, i.indisready FROM pg_index i \
                 JOIN pg_class c ON c.oid = i.indexrelid \
                 JOIN pg_class t ON t.oid = i.indrelid \
                 WHERE t.relname = 'concur_ric' ORDER BY c.relname"
                    .into(),
            ),
            ("ric-rows", "SELECT count(*), sum(v) FROM concur_ric".into()),
        ],
    });

    // ------------------------- DETACH PARTITION CONCURRENTLY cancel -----
    // The SQLcov-A inventory entry: DETACH CONCURRENTLY commits its first
    // transaction, then waits for lockers; a cancel BETWEEN the two
    // transactions leaves the partition pending-detach
    // (pg_inherits.inhdetachpending), which FINALIZE then completes.
    v.push(Scenario {
        name: "detach-cancel-finalize",
        description: "DETACH PARTITION CONCURRENTLY cancelled between transactions; FINALIZE completes the detach",
        setup: vec![
            "DROP TABLE IF EXISTS concur_dp CASCADE; DROP TABLE IF EXISTS concur_dp_a; \
             CREATE TABLE concur_dp (id int, v int) PARTITION BY RANGE (id); \
             CREATE TABLE concur_dp_a PARTITION OF concur_dp FOR VALUES FROM (0) TO (100); \
             CREATE TABLE concur_dp_b PARTITION OF concur_dp FOR VALUES FROM (100) TO (200); \
             INSERT INTO concur_dp VALUES (10, 1), (50, 2), (110, 3), (150, 4)"
                .into(),
        ],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            // Holds AccessShare on parent + partitions across the detach's
            // first commit, so the second transaction must wait.
            step("s1-touch", 0, "SELECT count(*) FROM concur_dp"),
            step(
                "s2-detach",
                1,
                "ALTER TABLE concur_dp DETACH PARTITION concur_dp_a CONCURRENTLY",
            ), // first txn commits; blocks waiting for s1
            step(
                "s1-cancel",
                0,
                "SELECT pg_cancel_backend(pid) FROM pg_stat_activity \
                 WHERE state = 'active' AND pid <> pg_backend_pid() \
                 AND query LIKE 'ALTER TABLE%DETACH PARTITION%CONCURRENTLY'",
            ), // s2-detach reaps with 57014; partition left pending-detach
            step(
                "s1-pending",
                0,
                "SELECT count(*) FROM pg_inherits WHERE inhdetachpending",
            ),
            step("s1-commit", 0, "COMMIT"),
            // Pending-detach partition visibility for a fresh scan (the
            // find_inheritance_children omit-detached arms).
            step("s2-scan", 1, "SELECT count(*) FROM concur_dp"),
            step(
                "s2-finalize",
                1,
                "ALTER TABLE concur_dp DETACH PARTITION concur_dp_a FINALIZE",
            ),
        ],
        probes: vec![
            (
                "dp-inherits",
                "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhparent \
                 WHERE c.relname = 'concur_dp'"
                    .into(),
            ),
            ("dp-parent", "SELECT id, v FROM concur_dp ORDER BY id".into()),
            ("dp-detached", "SELECT id, v FROM concur_dp_a ORDER BY id".into()),
        ],
    });

    // Same pending state, completed by RE-issuing DETACH CONCURRENTLY
    // (the resume-pending-detach path) instead of FINALIZE.
    v.push(Scenario {
        name: "detach-cancel-redetach",
        description: "cancelled DETACH CONCURRENTLY resumed by a second DETACH CONCURRENTLY",
        setup: vec![
            "DROP TABLE IF EXISTS concur_dq CASCADE; DROP TABLE IF EXISTS concur_dq_a; \
             CREATE TABLE concur_dq (id int, v int) PARTITION BY RANGE (id); \
             CREATE TABLE concur_dq_a PARTITION OF concur_dq FOR VALUES FROM (0) TO (100); \
             CREATE TABLE concur_dq_b PARTITION OF concur_dq FOR VALUES FROM (100) TO (200); \
             INSERT INTO concur_dq VALUES (20, 1), (120, 2)"
                .into(),
        ],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-touch", 0, "SELECT count(*) FROM concur_dq"),
            step(
                "s2-detach",
                1,
                "ALTER TABLE concur_dq DETACH PARTITION concur_dq_a CONCURRENTLY",
            ), // blocks
            step(
                "s1-cancel",
                0,
                "SELECT pg_cancel_backend(pid) FROM pg_stat_activity \
                 WHERE state = 'active' AND pid <> pg_backend_pid() \
                 AND query LIKE 'ALTER TABLE%DETACH PARTITION%CONCURRENTLY'",
            ),
            step("s1-commit", 0, "COMMIT"),
            step(
                "s2-redetach",
                1,
                "ALTER TABLE concur_dq DETACH PARTITION concur_dq_a CONCURRENTLY",
            ), // resumes + completes the pending detach
        ],
        probes: vec![
            (
                "dq-inherits",
                "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhparent \
                 WHERE c.relname = 'concur_dq'"
                    .into(),
            ),
            ("dq-parent", "SELECT id, v FROM concur_dq ORDER BY id".into()),
            ("dq-detached", "SELECT id, v FROM concur_dq_a ORDER BY id".into()),
        ],
    });

    // -------------------------------- REPEATABLE READ 40001 sweep ------
    // Under an xact snapshot the EPQ machinery is replaced by
    // serialization-failure ereports: TM_Updated in ExecUpdate/ExecDelete/
    // ExecLockRows/ExecMergeMatched, and the ExecCheckTupleVisible /
    // ExecCheckTIDVisible arms for ON CONFLICT against a tuple committed
    // after the snapshot. No blocking needed: s1 commits between s2's
    // snapshot and s2's statement.
    v.push(Scenario {
        name: "rr-serialization-sweep",
        description: "REPEATABLE READ: UPDATE/DELETE/FOR UPDATE/MERGE/ON CONFLICT vs post-snapshot commits -> 40001",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s2-begin1", 1, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            step("s2-snap1", 1, "SELECT count(*) FROM concur_t"),
            step("s1-upd1", 0, "UPDATE concur_t SET v = 800 WHERE id = 50"),
            step("s2-upd", 1, "UPDATE concur_t SET v = v + 1 WHERE id = 50"), // 40001
            step("s2-rb1", 1, "ROLLBACK"),
            step("s2-begin2", 1, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            step("s2-snap2", 1, "SELECT count(*) FROM concur_t"),
            step("s1-upd2", 0, "UPDATE concur_t SET v = 801 WHERE id = 52"),
            step("s2-del", 1, "DELETE FROM concur_t WHERE id = 52"), // 40001
            step("s2-rb2", 1, "ROLLBACK"),
            step("s2-begin3", 1, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            step("s2-snap3", 1, "SELECT count(*) FROM concur_t"),
            step("s1-upd3", 0, "UPDATE concur_t SET v = 802 WHERE id = 53"),
            step("s2-lock", 1, "SELECT id, v FROM concur_t WHERE id = 53 FOR UPDATE"), // 40001
            step("s2-rb3", 1, "ROLLBACK"),
            step("s2-begin4", 1, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            step("s2-snap4", 1, "SELECT count(*) FROM concur_t"),
            step("s1-upd4", 0, "UPDATE concur_t SET v = 803 WHERE id = 54"),
            step(
                "s2-merge",
                1,
                "MERGE INTO concur_t t USING (VALUES (54)) AS s(id) ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = t.v + 1",
            ), // 40001
            step("s2-rb4", 1, "ROLLBACK"),
            step("s2-begin5", 1, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            step("s2-snap5", 1, "SELECT count(*) FROM concur_t"),
            step("s1-ins5", 0, "INSERT INTO concur_t VALUES (320, 1, 1)"),
            step("s2-onc-nothing", 1, "INSERT INTO concur_t VALUES (320, 2, 2) ON CONFLICT (id) DO NOTHING"), // 40001 (TID visible check)
            step("s2-rb5", 1, "ROLLBACK"),
            step("s2-begin6", 1, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            step("s2-snap6", 1, "SELECT count(*) FROM concur_t"),
            step("s1-ins6", 0, "INSERT INTO concur_t VALUES (321, 1, 1)"),
            step(
                "s2-onc-update",
                1,
                "INSERT INTO concur_t VALUES (321, 2, 2) ON CONFLICT (id) \
                 DO UPDATE SET v = concur_t.v + 1",
            ), // 40001 (tuple visible check)
            step("s2-rb6", 1, "ROLLBACK"),
        ],
        probes: vec![probe_t()],
    });

    // ------------------------- MERGE RETURNING pending-not-matched ------
    // A concurrent delete flips the matched row to NOT MATCHED while the
    // MERGE carries RETURNING: exercises the mt_merge_pending_not_matched
    // deferral in ExecModifyTable + the ExecMergeNotMatched resume path.
    v.push(Scenario {
        name: "merge-returning-pending",
        description: "MERGE RETURNING races a concurrent delete: pending NOT MATCHED action resume path",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-del", 0, "DELETE FROM concur_t WHERE id = 24"),
            step(
                "s2-merge",
                1,
                "MERGE INTO concur_t t USING (VALUES (24, 888)) AS s(id, nv) ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = s.nv \
                 WHEN NOT MATCHED THEN INSERT (id, class, v) VALUES (s.id, 9, s.nv) \
                 RETURNING merge_action(), t.id, t.class, t.v",
            ), // blocks; TM_Deleted -> pending NOT MATCHED with RETURNING
            step("s1-commit", 0, "COMMIT"), // s2 -> INSERT action row
        ],
        probes: vec![probe_t()],
    });

    // ---------------------------------- MERGE cross-partition move ------
    // WHEN MATCHED UPDATE moves the row to another partition (the
    // crossPartUpdate arm of ExecMergeMatched), once clean and once after
    // a concurrent-update re-check.
    v.push(Scenario {
        name: "merge-partition-move",
        description: "MERGE WHEN MATCHED UPDATE moves row across partitions; clean + after concurrent update",
        setup: vec![
            "DROP TABLE IF EXISTS concur_mp; \
             CREATE TABLE concur_mp (id int, v int) PARTITION BY RANGE (id); \
             CREATE TABLE concur_mp_a PARTITION OF concur_mp FOR VALUES FROM (0) TO (100); \
             CREATE TABLE concur_mp_b PARTITION OF concur_mp FOR VALUES FROM (100) TO (200); \
             INSERT INTO concur_mp VALUES (70, 1), (71, 2), (110, 3)"
                .into(),
        ],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            // Clean cross-partition MERGE move.
            step(
                "s2-move-clean",
                1,
                "MERGE INTO concur_mp t USING (VALUES (71)) AS s(id) ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET id = t.id + 100",
            ),
            // Re-check flavor: blocked on s1's update, then moves.
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_mp SET v = v + 10 WHERE id = 70"),
            step(
                "s2-move-blocked",
                1,
                "MERGE INTO concur_mp t USING (VALUES (70)) AS s(id) ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET id = t.id + 100",
            ), // blocks; re-check then cross-partition move
            step("s1-commit", 0, "COMMIT"),
        ],
        probes: vec![(
            "concur_mp",
            "SELECT id, v FROM concur_mp ORDER BY id, v".into(),
        )],
    });

    // ------------------------------- MERGE NOT MATCHED BY SOURCE --------
    // The by-source join-condition path in ExecMergeMatched, with the
    // by-source row concurrently updated (prologue lock re-check).
    v.push(Scenario {
        name: "merge-by-source-concurrent",
        description: "MERGE WHEN NOT MATCHED BY SOURCE races a concurrent update on the source-less row",
        setup: vec![
            "DROP TABLE IF EXISTS concur_ms; \
             CREATE TABLE concur_ms (id int PRIMARY KEY, v int); \
             INSERT INTO concur_ms VALUES (25, 10), (26, 10)"
                .into(),
        ],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_ms SET v = 300 WHERE id = 26"),
            step(
                "s2-merge",
                1,
                "MERGE INTO concur_ms t USING (VALUES (25)) AS s(id) ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET v = t.v + 1 \
                 WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = t.v + 1000",
            ), // row 26 not matched by source; blocks on s1; re-check applies
            step("s1-commit", 0, "COMMIT"), // s2 -> MERGE 2; 26 -> 1300
        ],
        probes: vec![(
            "concur_ms",
            "SELECT id, v FROM concur_ms ORDER BY id".into(),
        )],
    });

    // ----------------------------------- DML RETURNING under EPQ --------
    // UPDATE/DELETE ... RETURNING whose row is concurrently updated: the
    // EPQ recheck plus the RETURNING old/new tuple fetch arms.
    v.push(Scenario {
        name: "epq-returning",
        description: "UPDATE/DELETE RETURNING blocked on concurrent update; EPQ then RETURNING fetch",
        setup: vec![T_C.into()],
        sessions: 2,
        session_setup: to_20s(),
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-upd", 0, "UPDATE concur_t SET v = 850 WHERE id = 55"),
            step("s2-del-ret", 1, "DELETE FROM concur_t WHERE id = 55 RETURNING id, class, v"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 -> row with v = 850
            step("s1-begin2", 0, "BEGIN"),
            step("s1-upd2", 0, "UPDATE concur_t SET v = 851 WHERE id = 56"),
            step(
                "s2-upd-ret",
                1,
                "UPDATE concur_t SET v = v + 1 WHERE id = 56 RETURNING id, class, v, \
                 old.v AS old_v, new.v AS new_v",
            ), // blocks; EPQ + OLD/NEW returning
            step("s1-commit2", 0, "COMMIT"), // s2 -> v = 852, old_v = 851
        ],
        probes: vec![probe_t()],
    });

    v
}
