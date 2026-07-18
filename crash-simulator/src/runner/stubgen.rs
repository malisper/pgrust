//! SCAFFOLD generator — WS-RUNNER's stand-in until WS-GEN's `src/gen/`
//! merges (they were chartered in parallel; contract §5 sequencing has
//! WS-GEN inc-1 land first on the integration branch, at which point the
//! runner switches to the real generator and THIS FILE IS DELETED).
//!
//! It exists so the run loop, bugbase, shrinker, replay and verdict paths
//! are exercisable end-to-end pre-integration. It obeys the same laws the
//! real generator must (contract §1.2 / §0 A3):
//!   - one ChaCha8 stream seeded from the single u64 seed; every draw from
//!     that stream in generation order; no wall-clock, no ambient entropy;
//!   - integer cumulative-weight choice (no float accumulation);
//!   - ordered containers only in plan-influencing paths;
//!   - first interaction is always CREATE TABLE;
//!   - R2 posture: LIMIT only under ORDER BY over the unique key; ORDER-less
//!     SELECTs are marked order-underdetermined;
//!   - marks: READ/MUTATION/PASSTHROUGH, ambiguous ⇒ MUTATION.

use super::planface::*;
use super::profile::Profile;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

/// Integer cumulative-weight choice over (item, weight) pairs. Panics if all
/// weights are zero (profile validator forbids that).
fn weighted<'a, T>(rng: &mut ChaCha8Rng, items: &'a [(T, u32)]) -> &'a T {
    let total: u64 = items.iter().map(|(_, w)| *w as u64).sum();
    assert!(total > 0, "weighted: zero total weight");
    let mut x = rng.gen_range(0..total);
    for (item, w) in items {
        let w = *w as u64;
        if x < w {
            return item;
        }
        x -= w;
    }
    &items[items.len() - 1].0
}

struct GenState {
    tables: Vec<String>,
    in_tx: bool,
    savepoints: u32,
    next_prop_seq: u32,
}

/// All generated SQL references harness tables SCHEMA-QUALIFIED. Law learned
/// from seed 1030 (worklog §find-1): an `ARM reset-all` inside an open tx
/// resets `search_path`; the next statement 42P01-fails, the recovery
/// ROLLBACK rolls the GUC back, and the follow-up read "sees" 0 rows — a
/// pure harness FP. Qualification makes generated SQL independent of every
/// RESET-able GUC. (WS-GEN must keep this invariant in the real generator.)
fn qual(t: &str) -> String {
    format!("simharness.{}", t)
}

pub fn generate(seed: u64, profile: &Profile, profile_sha256: &str, generator: &str) -> Plan {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut steps: Vec<Step> = Vec::new();
    let mut st = GenState { tables: Vec::new(), in_tx: false, savepoints: 0, next_prop_seq: 1 };

    let n_steps = rng.gen_range(profile.steps_min..=profile.steps_max);
    let n_tables =
        rng.gen_range(profile.table_shape.tables_min..=profile.table_shape.tables_max);

    // First interaction is always CREATE TABLE (spec §1.1).
    for i in 0..n_tables {
        let t = format!("st_{}", i + 1);
        steps.push(Step::Ddl(Sql {
            text: format!("CREATE TABLE {} (k int PRIMARY KEY, v int, s text)", qual(&t)),
            mark: Mark::Mutation,
            meta: SqlMeta::default(),
        }));
        st.tables.push(t);
    }

    let kinds: Vec<(&str, u32)> = profile
        .statement_weights
        .iter()
        .map(|(k, w)| (k.as_str(), *w))
        .collect();

    while (steps.len() as u32) < n_steps {
        let kind = *weighted(&mut rng, &kinds);
        match kind {
            "ddl" => gen_ddl(&mut rng, &mut st, &mut steps),
            "dml" => gen_dml(&mut rng, profile, &mut st, &mut steps),
            "query" => gen_query(&mut rng, &mut st, &mut steps),
            "tx" => gen_tx(&mut rng, profile, &mut st, &mut steps),
            "arm" => gen_arm(&mut rng, profile, &mut steps),
            "fault" => gen_fault(&mut rng, &mut st, &mut steps),
            "property" => gen_property(&mut rng, profile, &mut st, &mut steps),
            _ => unreachable!("validator pinned kinds"),
        }
    }
    if st.in_tx {
        steps.push(Step::Tx(TxCtl::Commit));
    }

    Plan {
        header: PlanHeader {
            seed,
            profile: profile.name.clone(),
            profile_sha256: profile_sha256.to_string(),
            generator: generator.to_string(),
        },
        steps,
    }
}

fn pick_table(rng: &mut ChaCha8Rng, st: &GenState) -> String {
    st.tables[rng.gen_range(0..st.tables.len())].clone()
}

fn gen_ddl(rng: &mut ChaCha8Rng, st: &mut GenState, steps: &mut Vec<Step>) {
    // Scaffold DDL: create an index sometimes (safe, idempotent-ish naming
    // by counter drawn from the stream).
    let t = pick_table(rng, st);
    let n = rng.gen_range(0..10_000u32);
    steps.push(Step::Ddl(Sql {
        text: format!("CREATE INDEX IF NOT EXISTS idx_{}_{} ON {} (v)", t, n, qual(&t)),
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
    }));
}

fn gen_dml(rng: &mut ChaCha8Rng, profile: &Profile, st: &mut GenState, steps: &mut Vec<Step>) {
    let t = pick_table(rng, st);
    match rng.gen_range(0..3u32) {
        0 => {
            let k = rng.gen_range(0..profile.table_shape.rows_max);
            let v = rng.gen_range(0..1000u32);
            steps.push(Step::Dml(Sql {
                text: format!(
                    "INSERT INTO {} VALUES ({}, {}, 's{}') ON CONFLICT (k) DO UPDATE SET v = {}",
                    qual(&t), k, v, v, v
                ),
                mark: Mark::Mutation,
                meta: SqlMeta::default(),
            }));
        }
        1 => {
            let k = rng.gen_range(0..profile.table_shape.rows_max);
            let v = rng.gen_range(0..1000u32);
            steps.push(Step::Dml(Sql {
                text: format!("UPDATE {} SET v = {} WHERE k = {}", qual(&t), v, k),
                mark: Mark::Mutation,
                meta: SqlMeta::default(),
            }));
        }
        _ => {
            let k = rng.gen_range(0..profile.table_shape.rows_max);
            steps.push(Step::Dml(Sql {
                text: format!("DELETE FROM {} WHERE k = {}", qual(&t), k),
                mark: Mark::Mutation,
                meta: SqlMeta::default(),
            }));
        }
    }
}

fn gen_query(rng: &mut ChaCha8Rng, st: &mut GenState, steps: &mut Vec<Step>) {
    let t = pick_table(rng, st);
    match rng.gen_range(0..3u32) {
        0 => {
            // R2-clean: LIMIT under ORDER BY over the unique key.
            let lim = rng.gen_range(1..20u32);
            steps.push(Step::Query(Sql {
                text: format!("SELECT k, v, s FROM {} ORDER BY k LIMIT {}", qual(&t), lim),
                mark: Mark::Read,
                meta: SqlMeta::default(),
            }));
        }
        1 => {
            steps.push(Step::Query(Sql {
                text: format!("SELECT count(*), sum(v) FROM {}", qual(&t)),
                mark: Mark::Read,
                meta: SqlMeta::default(),
            }));
        }
        _ => {
            // ORDER-less read: marked order-underdetermined (R2 posture).
            let v = rng.gen_range(0..1000u32);
            steps.push(Step::Query(Sql {
                text: format!("SELECT k, v FROM {} WHERE v < {}", qual(&t), v),
                mark: Mark::Read,
                meta: SqlMeta { order_underdetermined: true, float_lenient: false },
            }));
        }
    }
}

fn gen_tx(rng: &mut ChaCha8Rng, profile: &Profile, st: &mut GenState, steps: &mut Vec<Step>) {
    if !st.in_tx {
        let iso_items: Vec<(&str, u32)> =
            profile.iso_mix.iter().map(|(k, w)| (k.as_str(), *w)).collect();
        let iso = match *weighted(rng, &iso_items) {
            "read-committed" => IsoLevel::ReadCommitted,
            "repeatable-read" => IsoLevel::RepeatableRead,
            _ => IsoLevel::Serializable,
        };
        steps.push(Step::Tx(TxCtl::Begin(iso)));
        st.in_tx = true;
        st.savepoints = 0;
    } else {
        match rng.gen_range(0..4u32) {
            0 => {
                st.savepoints += 1;
                steps.push(Step::Tx(TxCtl::Savepoint(format!("sp{}", st.savepoints))));
            }
            1 if st.savepoints > 0 => {
                steps.push(Step::Tx(TxCtl::RollbackTo(format!("sp{}", st.savepoints))));
            }
            2 => {
                steps.push(Step::Tx(TxCtl::Rollback));
                st.in_tx = false;
            }
            _ => {
                steps.push(Step::Tx(TxCtl::Commit));
                st.in_tx = false;
            }
        }
    }
}

fn gen_arm(rng: &mut ChaCha8Rng, profile: &Profile, steps: &mut Vec<Step>) {
    if profile.arm_sets.is_empty() {
        return;
    }
    let set = &profile.arm_sets[rng.gen_range(0..profile.arm_sets.len())];
    if set.is_empty() {
        steps.push(Step::Arm(ArmCtl::ResetAll));
        return;
    }
    for arm in set {
        let (k, v) = arm.split_once('=').expect("validator pinned guc=value");
        steps.push(Step::Arm(ArmCtl::SetGuc(k.to_string(), v.to_string())));
    }
}

fn gen_fault(rng: &mut ChaCha8Rng, st: &mut GenState, steps: &mut Vec<Step>) {
    // Disconnect ends any open tx (server aborts it) — model that.
    if rng.gen_range(0..10u32) == 0 {
        steps.push(Step::Fault(FaultPoint::ReconnectServer));
    } else {
        steps.push(Step::Fault(FaultPoint::Disconnect));
    }
    st.in_tx = false;
    st.savepoints = 0;
}

/// Scaffold properties (real ones are WS-ORACLE structs driven by WS-GEN):
///   InsertCountDelta — count, key-addressed upsert, count again, assert
///                      bounded growth via rowcount identity on a probe read.
///   ReadYourWrites  (M1, serial form) — in-tx upsert then key-addressed
///                      read of the same key must see exactly one row.
fn gen_property(rng: &mut ChaCha8Rng, profile: &Profile, st: &mut GenState, steps: &mut Vec<Step>) {
    let t = pick_table(rng, st);
    let seq = st.next_prop_seq;
    st.next_prop_seq += 1;
    let props: Vec<(&str, u32)> = {
        let mut v: Vec<(&str, u32)> = profile
            .property_weights
            .iter()
            .filter(|(k, _)| !profile.kill_switches.contains(k))
            .map(|(k, w)| (k.as_str(), *w))
            .collect();
        if v.iter().all(|(_, w)| *w == 0) || v.is_empty() {
            v = vec![("ReadYourWrites", 1)];
        }
        v
    };
    match *weighted(rng, &props) {
        "InsertCountDelta" => {
            let k = rng.gen_range(0..profile.table_shape.rows_max);
            steps.push(Step::BeginProperty {
                name: "InsertCountDelta".into(),
                seq,
                tables: vec![t.clone()],
            });
            steps.push(Step::Dml(Sql {
                text: format!(
                    "INSERT INTO {} VALUES ({}, 1, 'p') ON CONFLICT (k) DO UPDATE SET v = 1",
                    qual(&t), k
                ),
                mark: Mark::Mutation,
                meta: SqlMeta::default(),
            }));
            steps.push(Step::Query(Sql {
                text: format!("SELECT k FROM {} WHERE k = {}", qual(&t), k),
                mark: Mark::Read,
                meta: SqlMeta::default(),
            }));
            steps.push(Step::Assertion("{\"op\":\"rowcount-eq\",\"value\":1}".into()));
            steps.push(Step::EndProperty { seq });
        }
        _ /* ReadYourWrites */ => {
            let k = rng.gen_range(0..profile.table_shape.rows_max);
            let own_tx = !st.in_tx;
            steps.push(Step::BeginProperty {
                name: "ReadYourWrites".into(),
                seq,
                tables: vec![t.clone()],
            });
            if own_tx {
                steps.push(Step::Tx(TxCtl::Begin(IsoLevel::ReadCommitted)));
            }
            steps.push(Step::Dml(Sql {
                text: format!(
                    "INSERT INTO {} VALUES ({}, 7, 'ryw') ON CONFLICT (k) DO UPDATE SET v = 7",
                    qual(&t), k
                ),
                mark: Mark::Mutation,
                meta: SqlMeta::default(),
            }));
            steps.push(Step::Query(Sql {
                text: format!("SELECT v FROM {} WHERE k = {}", qual(&t), k),
                mark: Mark::Read,
                meta: SqlMeta::default(),
            }));
            steps.push(Step::Assertion("{\"op\":\"rowcount-eq\",\"value\":1}".into()));
            if own_tx {
                steps.push(Step::Tx(TxCtl::Commit));
            }
            steps.push(Step::EndProperty { seq });
        }
    }
}
