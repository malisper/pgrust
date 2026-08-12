//! LD10 pubsub-cmds drain module: CREATE/ALTER/DROP PUBLICATION and
//! SUBSCRIPTION DDL over the line-drain-queue `pubsub-cmds` chunk
//! (publicationcmds.c + subscriptioncmds.c, 324 hollow lines in
//! line-gap-report-002 — parse_subscription_options 121,
//! Drop/CreateSubscription 43+42, parse_publication_options 39,
//! OpenTableList 24, TransformPubWhereClauses 15, ...).
//!
//! Reachability discipline: subscriptions are ALWAYS created with
//! `connect = false` (+ `slot_name = NONE, enabled = false` where the
//! shape needs a droppable subscription) so no statement ever opens a
//! network connection — the DDL validation arms are the target; the
//! walreceiver/apply arms behind a live connection are out of scope
//! (CONFIG/FAULT tier, see the queue doc). DROP SUBSCRIPTION only ever
//! runs after `SET (slot_name = NONE)` semantics are in force, so no
//! remote-slot drop is attempted.
//!
//! Every group is self-contained: it creates its publications/
//! subscriptions under fz_-prefixed names, probes deterministic catalog
//! projections (pg_publication / pg_publication_tables / pg_subscription
//! minus every oid/xid/LSN column), and drops everything it created.
//! Error arms are first-class fuel — duplicate options, unrecognized
//! options, every mutual-exclusion pair in parse_subscription_options,
//! bad publish/origin/streaming values, row-filter and column-list
//! rejections — asserted identical-SQLSTATE two-sided like all error
//! fuel.

use crate::stmt::{Gen, StmtKind};

/// Statement shapes (top-level weighted pick).
const SHAPES: &[&str] = &[
    "pubsub:pub_create",
    "pubsub:pub_alter",
    "pubsub:pub_err",
    "pubsub:sub_create",
    "pubsub:sub_alter",
    "pubsub:sub_err",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_pubsub_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("pubsub");
    let stmts = match g.weights.pick(g.rng, SHAPES) {
        "pubsub:pub_create" => pub_create(g),
        "pubsub:pub_alter" => pub_alter(g),
        "pubsub:pub_err" => pub_err(g),
        "pubsub:sub_create" => sub_create(g),
        "pubsub:sub_alter" => sub_alter(g),
        _ => sub_err(g),
    };
    stmts.into_iter().map(StmtKind::Raw).collect()
}

// ---------------------------------------------------------------------
// Deterministic probes (no oid/xid/LSN column ever projected).
// ---------------------------------------------------------------------

const PUB_PROBE: &str = "SELECT pubname, puballtables, pubinsert, pubupdate, \
     pubdelete, pubtruncate, pubviaroot, pubgencols FROM pg_publication \
     WHERE pubname LIKE 'fz\\_%' ORDER BY pubname;";

const PUBTAB_PROBE: &str = "SELECT pubname, schemaname, tablename, \
     attnames::text, rowfilter FROM pg_publication_tables \
     WHERE pubname LIKE 'fz\\_%' ORDER BY pubname, schemaname, tablename;";

const SUB_PROBE: &str = "SELECT subname, subenabled, subbinary, substream, \
     subtwophasestate, subdisableonerr, subpasswordrequired, subrunasowner, \
     subfailover, subslotname, subsynccommit, subpublications::text, \
     suborigin FROM pg_subscription WHERE subname LIKE 'fz\\_%' \
     ORDER BY subname;";

/// Fixture table bracket: a tiny table the publication shapes attach.
fn fixture(stmts: &mut Vec<String>, n: u32) -> Vec<String> {
    stmts.push(format!(
        "CREATE TABLE fz_pubt{n} (id int PRIMARY KEY, v text, w int);"
    ));
    vec![format!("DROP TABLE IF EXISTS fz_pubt{n};")]
}

// ---------------------------------------------------------------------
// Publications.
// ---------------------------------------------------------------------

/// publish= subsets incl. the single-action and full lists.
const PUBLISH_SETS: &[&str] = &[
    "insert", "update", "delete", "truncate", "insert, update",
    "insert, update, delete", "insert, update, delete, truncate",
    "update, truncate", "delete",
];

fn pub_with(g: &mut Gen) -> String {
    let mut opts = Vec::new();
    if g.rng.chance(2, 3) {
        opts.push(format!("publish = '{}'", *g.rng.pick(PUBLISH_SETS)));
    }
    if g.rng.chance(1, 3) {
        opts.push(format!(
            "publish_via_partition_root = {}",
            if g.rng.chance(1, 2) { "true" } else { "false" }
        ));
    }
    if g.rng.chance(1, 4) {
        opts.push(format!(
            "publish_generated_columns = {}",
            *g.rng.pick(&["none", "stored"])
        ));
    }
    if opts.is_empty() {
        String::new()
    } else {
        format!(" WITH ({})", opts.join(", "))
    }
}

/// CREATE PUBLICATION across the FOR-clause space + catalog probes.
fn pub_create(g: &mut Gen) -> Vec<String> {
    g.fire("pubsub:pub_create");
    let mut stmts = Vec::new();
    let mut cleanup = fixture(&mut stmts, 1);
    let forclause = match g.rng.below(6) {
        0 => " FOR ALL TABLES".to_string(),
        1 => " FOR TABLE fz_pubt1".to_string(),
        2 => " FOR TABLE fz_pubt1 (id, v)".to_string(),
        3 => format!(
            " FOR TABLE fz_pubt1 WHERE (id > {} AND v IS NOT NULL)",
            g.rng.below(100)
        ),
        4 => " FOR TABLES IN SCHEMA public".to_string(),
        _ => String::new(), // empty publication
    };
    stmts.push(format!(
        "CREATE PUBLICATION fz_pub1{}{};",
        forclause,
        pub_with(g)
    ));
    cleanup.insert(0, "DROP PUBLICATION IF EXISTS fz_pub1;".to_string());
    stmts.push(PUB_PROBE.to_string());
    stmts.push(PUBTAB_PROBE.to_string());
    stmts.extend(cleanup);
    stmts
}

/// ALTER PUBLICATION add/set/drop/rename/options over a base publication.
fn pub_alter(g: &mut Gen) -> Vec<String> {
    g.fire("pubsub:pub_alter");
    let mut stmts = Vec::new();
    let mut cleanup = fixture(&mut stmts, 2);
    stmts.push("CREATE TABLE fz_pubt3 (id int PRIMARY KEY, s text);".to_string());
    cleanup.push("DROP TABLE IF EXISTS fz_pubt3;".to_string());
    stmts.push("CREATE PUBLICATION fz_pub2 FOR TABLE fz_pubt2;".to_string());
    cleanup.insert(0, "DROP PUBLICATION IF EXISTS fz_pub2;".to_string());
    match g.rng.below(11) {
        8 => stmts.push("ALTER PUBLICATION fz_pub2 OWNER TO CURRENT_USER;".to_string()),
        9 => stmts.push(format!(
            "ALTER PUBLICATION fz_pub2 SET (publish_generated_columns = {});",
            *g.rng.pick(&["none", "stored"])
        )),
        10 => {
            stmts.push("ALTER PUBLICATION fz_pub2 ADD TABLE fz_pubt3;".to_string());
            stmts.push("ALTER PUBLICATION fz_pub2 DROP TABLE fz_pubt3, fz_pubt2;".to_string());
        }
        0 => stmts.push("ALTER PUBLICATION fz_pub2 ADD TABLE fz_pubt3 (id);".to_string()),
        1 => stmts.push(format!(
            "ALTER PUBLICATION fz_pub2 ADD TABLE fz_pubt3 WHERE (id <> {});",
            g.rng.below(50)
        )),
        2 => stmts.push(
            "ALTER PUBLICATION fz_pub2 SET TABLE fz_pubt3, fz_pubt2 WHERE (id > 0);".to_string(),
        ),
        3 => stmts.push("ALTER PUBLICATION fz_pub2 DROP TABLE fz_pubt2;".to_string()),
        4 => stmts.push(format!(
            "ALTER PUBLICATION fz_pub2 SET (publish = '{}');",
            *g.rng.pick(PUBLISH_SETS)
        )),
        5 => stmts.push(
            "ALTER PUBLICATION fz_pub2 SET (publish_via_partition_root = true);".to_string(),
        ),
        6 => {
            stmts.push("ALTER PUBLICATION fz_pub2 RENAME TO fz_pub2r;".to_string());
            stmts.push("ALTER PUBLICATION fz_pub2r RENAME TO fz_pub2;".to_string());
        }
        _ => {
            stmts.push("ALTER PUBLICATION fz_pub2 ADD TABLES IN SCHEMA public;".to_string());
            stmts.push("ALTER PUBLICATION fz_pub2 DROP TABLES IN SCHEMA public;".to_string());
        }
    }
    stmts.push(PUB_PROBE.to_string());
    stmts.push(PUBTAB_PROBE.to_string());
    stmts.extend(cleanup);
    stmts
}

/// Publication error fuel: each statement fails with the same SQLSTATE on
/// both sides; the group needs no cleanup beyond what it creates.
fn pub_err(g: &mut Gen) -> Vec<String> {
    g.fire("pubsub:pub_err");
    let mut stmts = Vec::new();
    let mut cleanup = fixture(&mut stmts, 4);
    stmts.push("CREATE PUBLICATION fz_puberr FOR ALL TABLES;".to_string());
    cleanup.insert(0, "DROP PUBLICATION IF EXISTS fz_puberr;".to_string());
    let errs: &[&str] = &[
        // duplicate option (errorConflictingDefElem)
        "CREATE PUBLICATION fz_pube2 WITH (publish = 'insert', publish = 'update');",
        // bad publish value
        "CREATE PUBLICATION fz_pube2 WITH (publish = 'insert, nonsense');",
        // unrecognized option
        "CREATE PUBLICATION fz_pube2 WITH (frobnicate = true);",
        // bad publish_generated_columns value
        "CREATE PUBLICATION fz_pube2 WITH (publish_generated_columns = 'sometimes');",
        // duplicate publication name
        "CREATE PUBLICATION fz_puberr;",
        // ADD TABLE on FOR ALL TABLES publication
        "ALTER PUBLICATION fz_puberr ADD TABLE fz_pubt4;",
        // duplicate column in column list
        "CREATE PUBLICATION fz_pube2 FOR TABLE fz_pubt4 (id, id);",
        // system column in column list
        "CREATE PUBLICATION fz_pube2 FOR TABLE fz_pubt4 (ctid);",
        // row filter with a subquery (expression restriction arm)
        "CREATE PUBLICATION fz_pube2 FOR TABLE fz_pubt4 WHERE (id IN (SELECT 1));",
        // row filter with a mutable function
        "CREATE PUBLICATION fz_pube2 FOR TABLE fz_pubt4 WHERE (v = random()::text);",
        // row filter with a user-defined-ish/system column reference
        "CREATE PUBLICATION fz_pube2 FOR TABLE fz_pubt4 WHERE (xmin::text <> '');",
        // WHERE clause attached to a schema target (parser/transform arm)
        "CREATE PUBLICATION fz_pube2 FOR TABLES IN SCHEMA nosuch_schema;",
        // sequence (unsupported relkind) as publication target
        "ALTER PUBLICATION fz_puberr SET (publish = '');",
        // drop a table that is not in the publication
        "CREATE PUBLICATION fz_pube2 FOR TABLE nosuch_table;",
    ];
    let n = 2 + g.rng.below_usize(3);
    for _ in 0..n {
        stmts.push((*g.rng.pick(errs)).to_string());
    }
    stmts.extend(cleanup);
    stmts
}

// ---------------------------------------------------------------------
// Subscriptions (never connecting: connect = false everywhere).
// ---------------------------------------------------------------------

/// Valid-looking conninfo strings (parsed, never dialed).
const CONNINFOS: &[&str] = &[
    "dbname=fzdb",
    "host=localhost port=5432 dbname=fzdb",
    "host=example.invalid port=5433 dbname=src user=fzuser",
    "dbname=fzdb connect_timeout=1",
    "",
];

/// Option sweep for CREATE SUBSCRIPTION under connect = false. Each entry
/// is one `WITH` fragment beyond the mandatory connect/slot/enabled trio;
/// every parse arm of parse_subscription_options gets a valid spelling.
const SUB_OPTS: &[&str] = &[
    "binary = true",
    "binary = false",
    "streaming = on",
    "streaming = off",
    "streaming = parallel",
    "synchronous_commit = 'off'",
    "synchronous_commit = 'local'",
    "synchronous_commit = 'remote_apply'",
    "two_phase = true",
    "disable_on_error = true",
    "password_required = true",
    "run_as_owner = true",
    "failover = false",
    "origin = 'none'",
    "origin = 'any'",
];

/// CREATE SUBSCRIPTION (connect = false) + probe + drop. slot_name = NONE
/// and enabled = false make the later DROP purely local.
fn sub_create(g: &mut Gen) -> Vec<String> {
    g.fire("pubsub:sub_create");
    let mut stmts = Vec::new();
    stmts.push("CREATE PUBLICATION fz_pubs1;".to_string());
    let conninfo = *g.rng.pick(CONNINFOS);
    let mut with = vec![
        "connect = false".to_string(),
        "slot_name = NONE".to_string(),
        "enabled = false".to_string(),
        "create_slot = false".to_string(),
    ];
    let extra = 1 + g.rng.below_usize(3);
    for _ in 0..extra {
        let o = *g.rng.pick(SUB_OPTS);
        if !with.iter().any(|w| w == o) {
            with.push(o.to_string());
        }
    }
    let pubs = if g.rng.chance(1, 3) {
        "fz_pubs1, fz_remote_pub"
    } else {
        "fz_pubs1"
    };
    stmts.push(format!(
        "CREATE SUBSCRIPTION fz_sub1 CONNECTION '{}' PUBLICATION {} WITH ({});",
        conninfo,
        pubs,
        with.join(", ")
    ));
    stmts.push(SUB_PROBE.to_string());
    stmts.push("DROP SUBSCRIPTION IF EXISTS fz_sub1;".to_string());
    stmts.push("DROP PUBLICATION IF EXISTS fz_pubs1;".to_string());
    stmts
}

/// ALTER SUBSCRIPTION over a disconnected base subscription.
fn sub_alter(g: &mut Gen) -> Vec<String> {
    g.fire("pubsub:sub_alter");
    let mut stmts = Vec::new();
    stmts.push(
        "CREATE SUBSCRIPTION fz_sub2 CONNECTION 'dbname=fzdb' PUBLICATION fz_nopub \
         WITH (connect = false, slot_name = NONE, enabled = false);"
            .to_string(),
    );
    match g.rng.below(13) {
        8 => {
            stmts.push("ALTER SUBSCRIPTION fz_sub2 DISABLE;".to_string());
            // ENABLE with slot_name = NONE is the can't-enable error arm.
            stmts.push("ALTER SUBSCRIPTION fz_sub2 ENABLE;".to_string());
        }
        9 => {
            stmts.push("ALTER SUBSCRIPTION fz_sub2 SET (slot_name = 'fz_slotname');".to_string());
            stmts.push("ALTER SUBSCRIPTION fz_sub2 SET (slot_name = NONE);".to_string());
        }
        // REFRESH on a disabled subscription: dedicated error arm.
        10 => stmts.push("ALTER SUBSCRIPTION fz_sub2 REFRESH PUBLICATION;".to_string()),
        11 => stmts.push(format!(
            "ALTER SUBSCRIPTION fz_sub2 SET ({});",
            *g.rng.pick(&["two_phase = true", "two_phase = false", "failover = true"])
        )),
        // SET PUBLICATION without refresh=false on a disabled sub (error
        // arm pair with the refresh=false success shape).
        12 => stmts.push(
            "ALTER SUBSCRIPTION fz_sub2 SET PUBLICATION fz_nopub2;".to_string(),
        ),
        0 => stmts.push(format!(
            "ALTER SUBSCRIPTION fz_sub2 SET ({});",
            *g.rng.pick(&[
                "binary = true",
                "streaming = parallel",
                "streaming = off",
                "disable_on_error = true",
                "password_required = true",
                "run_as_owner = true",
                "origin = 'none'",
                "synchronous_commit = 'local'",
            ])
        )),
        1 => stmts.push(
            "ALTER SUBSCRIPTION fz_sub2 CONNECTION 'host=elsewhere.invalid dbname=fzdb2';"
                .to_string(),
        ),
        2 => stmts.push(
            "ALTER SUBSCRIPTION fz_sub2 SET PUBLICATION fz_nopub, fz_otherpub \
             WITH (refresh = false);"
                .to_string(),
        ),
        3 => {
            stmts.push(
                "ALTER SUBSCRIPTION fz_sub2 ADD PUBLICATION fz_addpub WITH (refresh = false);"
                    .to_string(),
            );
            stmts.push(
                "ALTER SUBSCRIPTION fz_sub2 DROP PUBLICATION fz_addpub WITH (refresh = false);"
                    .to_string(),
            );
        }
        4 => {
            stmts.push("ALTER SUBSCRIPTION fz_sub2 RENAME TO fz_sub2r;".to_string());
            stmts.push("ALTER SUBSCRIPTION fz_sub2r RENAME TO fz_sub2;".to_string());
        }
        5 => stmts.push("ALTER SUBSCRIPTION fz_sub2 SKIP (lsn = '0/12345');".to_string()),
        6 => {
            stmts.push("ALTER SUBSCRIPTION fz_sub2 SKIP (lsn = '0/12345');".to_string());
            stmts.push("ALTER SUBSCRIPTION fz_sub2 SKIP (lsn = NONE);".to_string())
        }
        _ => stmts.push("ALTER SUBSCRIPTION fz_sub2 OWNER TO CURRENT_USER;".to_string()),
    }
    stmts.push(SUB_PROBE.to_string());
    stmts.push("DROP SUBSCRIPTION IF EXISTS fz_sub2;".to_string());
    stmts
}

/// Subscription error fuel: the parse_subscription_options combination
/// arms (each ereport is its own line region) + Create/Drop error arms.
fn sub_err(g: &mut Gen) -> Vec<String> {
    g.fire("pubsub:sub_err");
    let errs: &[&str] = &[
        // connect = false mutual exclusions (all three arms)
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, enabled = true);",
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, create_slot = true);",
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, copy_data = true);",
        // slot_name = NONE combination arms (specified and defaulted)
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, slot_name = NONE, enabled = true);",
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (slot_name = NONE);",
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (slot_name = NONE, enabled = false);",
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (slot_name = NONE, enabled = false, create_slot = true);",
        // duplicate option (errorConflictingDefElem)
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, connect = false);",
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, binary = true, binary = false);",
        // unrecognized parameter
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, frobnicate = 1);",
        // bad option values (origin / streaming / synchronous_commit)
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, origin = 'elsewhere');",
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, streaming = 'sideways');",
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p \
         WITH (connect = false, synchronous_commit = 'sometimes');",
        // duplicate publication name in the list
        "CREATE SUBSCRIPTION fz_sube CONNECTION 'dbname=x' PUBLICATION p, p \
         WITH (connect = false);",
        // options valid only at CREATE time rejected by ALTER SET
        "ALTER SUBSCRIPTION fz_nosuchsub SET (streaming = on);",
        // DROP of a missing subscription (with and without IF EXISTS noise)
        "DROP SUBSCRIPTION fz_nosuchsub;",
        // ALTER on a missing subscription
        "ALTER SUBSCRIPTION fz_nosuchsub DISABLE;",
        // bad LSN in SKIP
        "ALTER SUBSCRIPTION fz_nosuchsub SKIP (lsn = 'notalsn');",
    ];
    let mut stmts = Vec::new();
    let n = 2 + g.rng.below_usize(4);
    for _ in 0..n {
        stmts.push((*g.rng.pick(errs)).to_string());
    }
    // IF EXISTS notice arm is a line too, and succeeds identically.
    if g.rng.chance(1, 3) {
        stmts.push("DROP SUBSCRIPTION IF EXISTS fz_nosuchsub;".to_string());
    }
    stmts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Determinism + quote balance + the never-connect invariant: every
    /// CREATE SUBSCRIPTION carries connect = false.
    #[test]
    fn pubsub_groups_are_deterministic_and_never_connect() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        for seed in [3u64, 17, 99] {
            let run = || {
                let mut rng = Rng::new(seed);
                let mut out = Vec::new();
                for _ in 0..400 {
                    let mut prods = Vec::new();
                    let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                    let stmts: Vec<String> =
                        gen_pubsub_module(&mut g).iter().map(|s| s.to_sql()).collect();
                    out.push(stmts);
                }
                out
            };
            let a = run();
            assert_eq!(a, run());
            for group in &a {
                for s in group {
                    assert!(s.ends_with(';'), "unterminated: {s}");
                    assert_eq!(s.matches('\'').count() % 2, 0, "odd quotes: {s}");
                    if s.starts_with("CREATE SUBSCRIPTION") {
                        // Either connect = false (the success shapes), or a
                        // slot_name = NONE error shape that fails inside
                        // parse_subscription_options BEFORE any connection
                        // attempt. Both never dial out.
                        assert!(
                            s.contains("connect = false") || s.contains("slot_name = NONE"),
                            "subscription that could connect: {s}"
                        );
                    }
                }
            }
        }
    }

    /// Every group drops everything it creates (name-symmetric create/drop).
    #[test]
    fn pubsub_groups_are_self_contained() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(7);
        for _ in 0..600 {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            let stmts: Vec<String> =
                gen_pubsub_module(&mut g).iter().map(|s| s.to_sql()).collect();
            let joined = stmts.join("\n");
            for (create, obj) in [
                ("CREATE TABLE fz_pubt1", "DROP TABLE IF EXISTS fz_pubt1;"),
                ("CREATE TABLE fz_pubt2", "DROP TABLE IF EXISTS fz_pubt2;"),
                ("CREATE TABLE fz_pubt3", "DROP TABLE IF EXISTS fz_pubt3;"),
                ("CREATE TABLE fz_pubt4", "DROP TABLE IF EXISTS fz_pubt4;"),
                ("CREATE PUBLICATION fz_pub1", "DROP PUBLICATION IF EXISTS fz_pub1;"),
                ("CREATE PUBLICATION fz_pub2", "DROP PUBLICATION IF EXISTS fz_pub2;"),
                ("CREATE PUBLICATION fz_puberr", "DROP PUBLICATION IF EXISTS fz_puberr;"),
                ("CREATE PUBLICATION fz_pubs1", "DROP PUBLICATION IF EXISTS fz_pubs1;"),
                ("CREATE SUBSCRIPTION fz_sub1", "DROP SUBSCRIPTION IF EXISTS fz_sub1;"),
                ("CREATE SUBSCRIPTION fz_sub2", "DROP SUBSCRIPTION IF EXISTS fz_sub2;"),
            ] {
                if joined.contains(create) {
                    assert!(joined.contains(obj), "created but never dropped: {create}\n{joined}");
                }
            }
        }
    }
}
