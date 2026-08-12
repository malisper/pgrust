//! U1 object/utility DDL module: COMMENT ON + object-address deparse
//! probes, roles, CREATE TYPE (enum/composite/range/shell), and extended
//! statistics — the top needs-DDL-or-utility cluster of gap-report-004
//! (`getObjectIdentityParts` rank 1, `CreateRole` rank 5, `DefineType`
//! rank 6, `CreateStatistics` rank 15; `AlterRole`, `DefineRange`,
//! `does_not_exist_skipping`, `record_cmp`/`record_in` ride along).
//!
//! `ObjState` (session-persistent, swapped in and out of `Gen` exactly like
//! `DdlState`) tracks every object this module creates — roles, enum/
//! composite/range/shell types, statistics objects — so later statements
//! are valid by construction: nothing is referenced after its drop, names
//! come from monotonic counters and are never reused, and drops target
//! fuzz-created objects only (the bootstrap superuser and builtin objects
//! are never dropped or altered).
//!
//! Validity/safety disciplines (hand-verified on both engines 2026-08-11;
//! every family below produced byte-identical psql output on C 18.4 and
//! pgrust @ main):
//!   - roles are CLUSTER-global while types/stats/comments are per-DB, so
//!     role names carry a session tag drawn from the session PRNG (same
//!     seed = same tag) — concurrent/sequential seeds sharing one cluster
//!     (the gapreport corpus rig) cannot collide;
//!   - SET ROLE is always bracketed [SET ROLE r; SELECT current_user;
//!     RESET ROLE;] in a single group, so a restricted role can never leak
//!     into later groups (state probes landing inside the bracket see the
//!     same permission errors on both sides — matched);
//!   - role-membership GRANTs only ever flow from an older pool entry to a
//!     newer one, so membership cycles (0LP01) are impossible;
//!   - fuzz roles own no objects and hold no object-level grants, so
//!     DROP ROLE can never hit 2BP01 dependency errors;
//!   - every created-type output is cast to a builtin type (::text, bool,
//!     int) — custom-type OIDs differ across engines and the differ
//!     compares RowDescription OIDs strictly;
//!   - range constructors always receive ordered (or NULL) bounds; enum
//!     probes only reference labels that exist at generation time;
//!   - object-address describe probes with attnums target fixture tables
//!     only (never ALTERed, so attnum == column position + 1 forever);
//!   - DROP STATISTICS always says IF EXISTS: a statistics object dies
//!     silently with its table when the ddl module drops it;
//!   - the base-type shape (CREATE TYPE (INPUT = int4in, ...)) errors
//!     identically on both engines ("type does not exist" — no shell
//!     pre-declaration); it stays at a token weight purely for
//!     DefineType's option-parsing error path and is never registered.

use crate::catalog::{SqlType, Table};
use crate::stmt::{Gen, StmtKind};

/// Caps keeping the live-object population bounded over long streams.
const MAX_LIVE_ROLES: usize = 6;
const MAX_LIVE_TYPES: usize = 9;
const MAX_LIVE_STATS: usize = 6;
/// Enum labels stop growing here (ALTER TYPE ADD VALUE turns no-op).
const MAX_ENUM_LABELS: u64 = 12;

#[derive(Clone, Debug)]
pub struct ObjRole {
    pub name: String,
    pub live: bool,
}

#[derive(Clone, Debug)]
pub struct ObjEnum {
    pub name: String,
    /// Labels are always v0..v{nvals-1} in creation order (BEFORE/AFTER
    /// placement changes sort order, not the label set).
    pub nvals: u64,
    pub live: bool,
}

#[derive(Clone, Debug)]
pub struct ObjComp {
    pub name: String,
    /// Field i is named f{i} with this type.
    pub fields: Vec<SqlType>,
    pub live: bool,
}

#[derive(Clone, Debug)]
pub struct ObjRange {
    pub name: String,
    pub subtype: SqlType,
    pub live: bool,
}

#[derive(Clone, Debug)]
pub struct ObjShell {
    pub name: String,
    pub live: bool,
}

#[derive(Clone, Debug)]
pub struct ObjStat {
    pub name: String,
    pub live: bool,
}

/// Session-persistent object/utility DDL model.
#[derive(Clone, Debug, Default)]
pub struct ObjState {
    /// Session tag for cluster-global names (roles). Drawn lazily from the
    /// session PRNG on the module's first pick, so it is part of the seeded
    /// stream and unique-per-seed with overwhelming probability.
    tag: Option<u64>,
    pub roles: Vec<ObjRole>,
    next_role: u32,
    pub enums: Vec<ObjEnum>,
    pub comps: Vec<ObjComp>,
    pub ranges: Vec<ObjRange>,
    pub shells: Vec<ObjShell>,
    next_type: u32,
    pub stats: Vec<ObjStat>,
    next_stat: u32,
    next_objt: u32,
    /// Q6 opclass-ddl name counter (fz_qopc_/fz_qopf_/fz_qtab_/fz_qam_/
    /// fz_qen_/fz_qcv_/fz_qlg_/fz_qag_/fz_qfn_ names — every Q6 group is
    /// self-contained, so only the counter persists).
    next_q: u32,
}

impl ObjState {
    pub fn new() -> ObjState {
        ObjState::default()
    }

    fn live_roles(&self) -> Vec<usize> {
        self.roles.iter().enumerate().filter(|(_, r)| r.live).map(|(i, _)| i).collect()
    }

    fn live_stats(&self) -> Vec<usize> {
        self.stats.iter().enumerate().filter(|(_, s)| s.live).map(|(i, _)| i).collect()
    }

    fn n_live_types(&self) -> usize {
        self.enums.iter().filter(|t| t.live).count()
            + self.comps.iter().filter(|t| t.live).count()
            + self.ranges.iter().filter(|t| t.live).count()
            + self.shells.iter().filter(|t| t.live).count()
    }

    /// Names of all live created types (comment/describe targets).
    fn live_type_names(&self) -> Vec<String> {
        let mut out = Vec::new();
        for t in &self.enums {
            if t.live {
                out.push(t.name.clone());
            }
        }
        for t in &self.comps {
            if t.live {
                out.push(t.name.clone());
            }
        }
        for t in &self.ranges {
            if t.live {
                out.push(t.name.clone());
            }
        }
        // Shell types deliberately excluded: a shell type has no identity
        // deparse surface worth probing and no I/O for use-probes.
        out
    }
}

fn session_tag(g: &mut Gen) -> u64 {
    if g.obj.tag.is_none() {
        g.obj.tag = Some(1 + g.rng.below(0xFF_FFFF));
    }
    g.obj.tag.unwrap()
}

/// Builtin comment/describe fuel: never created, never dropped.
const BUILTIN_FUNCS: &[&str] =
    &["abs(int4)", "length(text)", "lower(text)", "now()", "int4pl(int4,int4)"];
const BUILTIN_TYPES: &[&str] = &["int4", "text", "date", "numeric", "jsonb"];
const BUILTIN_OPS: &[&str] = &["=(int4,int4)", "<(text,text)", "+(int4,int4)"];
const ACCESS_METHODS: &[&str] = &["heap", "btree", "hash", "gin", "gist", "brin", "spgist"];
const LANGUAGES: &[&str] = &["sql", "plpgsql"];

/// GUCs a role may carry as per-role defaults (ALTER ROLE ... SET): the
/// same session-local deterministic bar as the util module's curated list.
const ROLE_GUCS: &[(&str, &[&str])] = &[
    ("work_mem", &["'64kB'", "'1MB'", "'16MB'"]),
    ("enable_seqscan", &["on", "off"]),
    ("extra_float_digits", &["0", "2", "-3"]),
];

const SHAPES: &[&str] = &[
    "objddl:comment",
    "objddl:describe",
    "objddl:role:create",
    "objddl:role:alter",
    "objddl:role:grant",
    "objddl:role:setrole",
    "objddl:role:drop",
    "objddl:type:create",
    "objddl:type:alter_enum",
    "objddl:type:use",
    "objddl:type:coltab",
    "objddl:type:drop",
    "objddl:stats:create",
    "objddl:stats:drop",
    "objddl:opc",
    "objddl:opcerr",
    "objddl:am",
    "objddl:cast",
    "objddl:conv",
    "objddl:plang",
    "objddl:xform",
    "objddl:seclabel",
    "objddl:opshell",
    "objddl:aggmod",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_objddl_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl");
    let shape = g.weights.pick(g.rng, SHAPES);
    match shape {
        "objddl:comment" => gen_comment(g),
        "objddl:describe" => gen_describe(g),
        "objddl:role:create" => gen_role_create(g),
        "objddl:role:alter" => gen_role_alter(g),
        "objddl:role:grant" => gen_role_grant(g),
        "objddl:role:setrole" => gen_role_setrole(g),
        "objddl:role:drop" => gen_role_drop(g),
        "objddl:type:create" => gen_type_create(g),
        "objddl:type:alter_enum" => gen_type_alter_enum(g),
        "objddl:type:use" => gen_type_use(g),
        "objddl:type:coltab" => gen_type_coltab(g),
        "objddl:type:drop" => gen_type_drop(g),
        "objddl:stats:create" => gen_stats_create(g),
        "objddl:stats:drop" => gen_stats_drop(g),
        "objddl:opc" => gen_opc(g),
        "objddl:opcerr" => gen_opcerr(g),
        "objddl:am" => gen_am(g),
        "objddl:cast" => gen_cast(g),
        "objddl:conv" => gen_conv(g),
        "objddl:plang" => gen_plang(g),
        "objddl:xform" => gen_xform(g),
        "objddl:seclabel" => gen_seclabel(g),
        "objddl:opshell" => gen_opshell(g),
        "objddl:aggmod" => gen_aggmod(g),
        other => unreachable!("unknown objddl shape {other}"),
    }
}

// ---------------------------------------------------------- COMMENT ON ----

fn comment_text(g: &mut Gen) -> String {
    if g.weights.pick(g.rng, &["objddl:comment:text", "objddl:comment:null"])
        == "objddl:comment:null"
    {
        g.fire("objddl:comment:null");
        "NULL".to_string()
    } else {
        g.fire("objddl:comment:text");
        format!("'u1 note {}'", g.rng.below(4))
    }
}

/// Fixture tables only: never ALTERed by any module, so attnums are stable
/// and `<name>_pkey` always exists.
fn fixture_tables<'a>(g: &Gen<'a>) -> Vec<&'a Table> {
    g.catalog
        .tables
        .iter()
        .filter(|t| !t.name.starts_with("fz_ddl_") && !t.name.starts_with("fz_part_"))
        .collect()
}

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn gen_comment(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:comment");
    let kind = g.weights.pick(
        g.rng,
        &[
            "objddl:comment:table",
            "objddl:comment:column",
            "objddl:comment:index",
            "objddl:comment:sequence",
            "objddl:comment:function",
            "objddl:comment:type",
            "objddl:comment:schema",
            "objddl:comment:constraint",
            "objddl:comment:am",
            "objddl:comment:role",
            "objddl:comment:language",
        ],
    );
    // Object-kind targets that may not exist yet fall back to a table
    // comment (the fired production records what actually ran).
    let target: Option<(String, String)> = match kind {
        "objddl:comment:table" => {
            let t = g.pick_table().name.clone();
            Some(("TABLE".to_string(), t))
        }
        "objddl:comment:column" => {
            let t = g.pick_table();
            let name = t.name.clone();
            let c = t.columns[g.rng.below_usize(t.columns.len())].name.clone();
            Some(("COLUMN".to_string(), format!("{}.{}", name, c)))
        }
        "objddl:comment:index" => {
            let live: Vec<String> = g
                .ddl
                .indexes
                .iter()
                .filter(|x| x.live)
                .map(|x| x.name.clone())
                .collect();
            if live.is_empty() {
                None
            } else {
                Some(("INDEX".to_string(), live[g.rng.below_usize(live.len())].clone()))
            }
        }
        "objddl:comment:sequence" => {
            let live: Vec<String> = g
                .ddl
                .seqs
                .iter()
                .filter(|s| s.live)
                .map(|s| s.name.clone())
                .collect();
            if live.is_empty() {
                None
            } else {
                Some(("SEQUENCE".to_string(), live[g.rng.below_usize(live.len())].clone()))
            }
        }
        "objddl:comment:function" => {
            Some(("FUNCTION".to_string(), pick_str(g, BUILTIN_FUNCS).to_string()))
        }
        "objddl:comment:type" => {
            let mut pool: Vec<String> =
                BUILTIN_TYPES.iter().map(|s| s.to_string()).collect();
            pool.extend(g.obj.live_type_names());
            Some(("TYPE".to_string(), pool[g.rng.below_usize(pool.len())].clone()))
        }
        "objddl:comment:schema" => Some(("SCHEMA".to_string(), "public".to_string())),
        "objddl:comment:constraint" => {
            let cands: Vec<String> = fixture_tables(g)
                .iter()
                .filter(|t| t.pk.is_some() && t.pk_unique)
                .map(|t| t.name.clone())
                .collect();
            if cands.is_empty() {
                None
            } else {
                let t = cands[g.rng.below_usize(cands.len())].clone();
                Some(("CONSTRAINT".to_string(), format!("{}_pkey ON {}", t, t)))
            }
        }
        "objddl:comment:am" => {
            Some(("ACCESS METHOD".to_string(), pick_str(g, ACCESS_METHODS).to_string()))
        }
        "objddl:comment:role" => {
            let live = g.obj.live_roles();
            if live.is_empty() {
                None
            } else {
                let r = g.obj.roles[live[g.rng.below_usize(live.len())]].name.clone();
                Some(("ROLE".to_string(), r))
            }
        }
        "objddl:comment:language" => {
            Some(("LANGUAGE".to_string(), pick_str(g, LANGUAGES).to_string()))
        }
        other => unreachable!("unknown comment kind {other}"),
    };
    let (obj_kind, obj_name) = match target {
        Some(t) => {
            g.fire(kind);
            t
        }
        None => {
            g.fire("objddl:comment:fallback_table");
            ("TABLE".to_string(), g.pick_table().name.clone())
        }
    };
    let txt = comment_text(g);
    vec![StmtKind::Raw(format!("COMMENT ON {} {} IS {};", obj_kind, obj_name, txt))]
}

// ---------------------------------------- object-address describe probes ----

fn gen_describe(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:describe");
    let fixtures = fixture_tables(g);
    let t = fixtures[g.rng.below_usize(fixtures.len())];
    let tname = t.name.clone();
    let ncols = t.columns.len();
    let attnum = 1 + g.rng.below_usize(ncols);
    let sql = match g.rng.below(9) {
        0 => {
            g.fire("objddl:describe:obj_description");
            format!("SELECT obj_description('{}'::regclass, 'pg_class');", tname)
        }
        1 => {
            g.fire("objddl:describe:col_description");
            format!("SELECT col_description('{}'::regclass, {});", tname, attnum)
        }
        2 => {
            g.fire("objddl:describe:rel");
            let sub = if g.rng.chance(1, 2) { 0 } else { attnum };
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT pg_describe_object('pg_class'::regclass, '{}'::regclass, {});",
                    tname, sub
                )
            } else {
                format!(
                    "SELECT * FROM pg_identify_object('pg_class'::regclass, '{}'::regclass, {});",
                    tname, sub
                )
            }
        }
        3 => {
            g.fire("objddl:describe:type");
            let mut pool: Vec<String> =
                BUILTIN_TYPES.iter().map(|s| s.to_string()).collect();
            pool.extend(g.obj.live_type_names());
            let ty = pool[g.rng.below_usize(pool.len())].clone();
            format!(
                "SELECT * FROM pg_identify_object_as_address('pg_type'::regclass, '{}'::regtype, 0);",
                ty
            )
        }
        4 => {
            g.fire("objddl:describe:operator");
            format!(
                "SELECT pg_describe_object('pg_operator'::regclass, '{}'::regoperator, 0);",
                pick_str(g, BUILTIN_OPS)
            )
        }
        5 => {
            g.fire("objddl:describe:proc");
            format!(
                "SELECT * FROM pg_identify_object('pg_proc'::regclass, '{}'::regprocedure, 0);",
                pick_str(g, BUILTIN_FUNCS)
            )
        }
        6 => {
            g.fire("objddl:describe:namespace");
            "SELECT pg_describe_object('pg_namespace'::regclass, 'public'::regnamespace, 0);"
                .to_string()
        }
        7 => {
            g.fire("objddl:describe:constraint");
            format!(
                "SELECT pg_describe_object('pg_constraint'::regclass, oid, 0) FROM pg_constraint WHERE conname = '{}_pkey';",
                tname
            )
        }
        _ => {
            g.fire("objddl:describe:type_addr");
            let mut pool: Vec<String> =
                BUILTIN_TYPES.iter().map(|s| s.to_string()).collect();
            pool.extend(g.obj.live_type_names());
            let ty = pool[g.rng.below_usize(pool.len())].clone();
            format!(
                "SELECT * FROM pg_identify_object('pg_type'::regclass, '{}'::regtype, 0);",
                ty
            )
        }
    };
    vec![StmtKind::Raw(sql)]
}

// -------------------------------------------------------------- roles ----

fn gen_role_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.obj.live_roles().len() >= MAX_LIVE_ROLES {
        g.fire("objddl:cap:roles");
        return gen_role_drop(g);
    }
    g.fire("objddl:role:create");
    let tag = session_tag(g);
    let name = format!("fz_role_{:x}_{}", tag, g.obj.next_role);
    g.obj.next_role += 1;
    let mut opts = String::new();
    if g.rng.chance(1, 3) {
        opts.push_str(if g.rng.chance(1, 2) { " LOGIN" } else { " NOLOGIN" });
    }
    if g.rng.chance(1, 5) {
        opts.push_str(" SUPERUSER");
    }
    if g.rng.chance(1, 4) {
        opts.push_str(" CREATEDB");
    }
    if g.rng.chance(1, 5) {
        opts.push_str(" CREATEROLE");
    }
    if g.rng.chance(1, 4) {
        opts.push_str(if g.rng.chance(1, 2) { " INHERIT" } else { " NOINHERIT" });
    }
    if g.rng.chance(1, 4) {
        opts.push_str(&format!(" CONNECTION LIMIT {}", pick_str(g, &["-1", "0", "1", "5"])));
    }
    if g.rng.chance(1, 4) {
        opts.push_str(&format!(
            " VALID UNTIL {}",
            pick_str(g, &["'2030-01-01'", "'infinity'", "'2026-01-01 00:00:00'"])
        ));
    }
    if g.rng.chance(1, 5) {
        opts.push_str(if g.rng.chance(1, 2) { " PASSWORD 'fzpw'" } else { " PASSWORD NULL" });
    }
    let live = g.obj.live_roles();
    if !live.is_empty() && g.rng.chance(1, 4) {
        g.fire("objddl:role:create:in_role");
        let r = g.obj.roles[live[g.rng.below_usize(live.len())]].name.clone();
        opts.push_str(&format!(" IN ROLE {}", r));
    }
    g.obj.roles.push(ObjRole { name: name.clone(), live: true });
    vec![StmtKind::Raw(format!("CREATE ROLE {}{};", name, opts))]
}

fn gen_role_alter(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.obj.live_roles();
    if live.is_empty() {
        g.fire("objddl:fallback:role_create");
        return gen_role_create(g);
    }
    g.fire("objddl:role:alter");
    let name = g.obj.roles[live[g.rng.below_usize(live.len())]].name.clone();
    let sql = match g.rng.below(4) {
        0 => {
            // Attribute mix (always at least one option).
            let mut opts = String::new();
            if g.rng.chance(1, 2) {
                opts.push_str(if g.rng.chance(1, 2) { " LOGIN" } else { " NOLOGIN" });
            }
            if g.rng.chance(1, 3) {
                opts.push_str(if g.rng.chance(1, 2) { " CREATEDB" } else { " NOCREATEDB" });
            }
            if g.rng.chance(1, 3) {
                opts.push_str(&format!(
                    " CONNECTION LIMIT {}",
                    pick_str(g, &["-1", "0", "2"])
                ));
            }
            if opts.is_empty() {
                opts.push_str(if g.rng.chance(1, 2) { " INHERIT" } else { " NOINHERIT" });
            }
            format!("ALTER ROLE {}{};", name, opts)
        }
        1 => {
            let (guc, vals) = ROLE_GUCS[g.rng.below_usize(ROLE_GUCS.len())];
            format!("ALTER ROLE {} SET {} TO {};", name, guc, pick_str(g, vals))
        }
        2 => {
            if g.rng.chance(1, 3) {
                format!("ALTER ROLE {} RESET ALL;", name)
            } else {
                let (guc, _) = ROLE_GUCS[g.rng.below_usize(ROLE_GUCS.len())];
                format!("ALTER ROLE {} RESET {};", name, guc)
            }
        }
        _ => format!(
            "ALTER ROLE {} VALID UNTIL {};",
            name,
            pick_str(g, &["'2031-06-15'", "'infinity'"])
        ),
    };
    vec![StmtKind::Raw(sql)]
}

fn gen_role_grant(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.obj.live_roles();
    if live.len() < 2 {
        g.fire("objddl:fallback:role_create");
        return gen_role_create(g);
    }
    g.fire("objddl:role:grant");
    // Membership always flows old -> new (pool order), so cycles are
    // impossible by construction.
    let jpos = 1 + g.rng.below_usize(live.len() - 1);
    let ipos = g.rng.below_usize(jpos);
    let granted = g.obj.roles[live[ipos]].name.clone();
    let grantee = g.obj.roles[live[jpos]].name.clone();
    let sql = if g.rng.chance(1, 3) {
        g.fire("objddl:role:revoke");
        format!("REVOKE {} FROM {};", granted, grantee)
    } else if g.rng.chance(1, 4) {
        format!("GRANT {} TO {} WITH ADMIN OPTION;", granted, grantee)
    } else {
        format!("GRANT {} TO {};", granted, grantee)
    };
    vec![StmtKind::Raw(sql)]
}

fn gen_role_setrole(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.obj.live_roles();
    if live.is_empty() {
        g.fire("objddl:fallback:role_create");
        return gen_role_create(g);
    }
    g.fire("objddl:role:setrole");
    let name = g.obj.roles[live[g.rng.below_usize(live.len())]].name.clone();
    vec![
        StmtKind::Raw(format!("SET ROLE {};", name)),
        StmtKind::Raw("SELECT current_user;".to_string()),
        StmtKind::Raw("RESET ROLE;".to_string()),
    ]
}

fn gen_role_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.obj.live_roles();
    if live.is_empty() {
        g.fire("objddl:fallback:role_create");
        return gen_role_create(g);
    }
    g.fire("objddl:role:drop");
    let ri = live[g.rng.below_usize(live.len())];
    let name = g.obj.roles[ri].name.clone();
    g.obj.roles[ri].live = false;
    let if_exists = g.rng.chance(1, 2);
    vec![StmtKind::Raw(format!(
        "DROP ROLE {}{};",
        if if_exists { "IF EXISTS " } else { "" },
        name
    ))]
}

// -------------------------------------------------------------- types ----

/// Composite-field / range-subtype palette: btree-ordered scalar types with
/// simple deterministic literals.
const FIELD_TYPES: &[SqlType] = &[
    SqlType::Int4,
    SqlType::Int8,
    SqlType::Text,
    SqlType::Date,
    SqlType::Numeric,
    SqlType::Bool,
];
const RANGE_SUBTYPES: &[SqlType] = &[
    SqlType::Int4,
    SqlType::Int8,
    SqlType::Date,
    SqlType::Numeric,
    SqlType::Text,
    SqlType::Timestamp,
];

fn gen_type_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.obj.n_live_types() >= MAX_LIVE_TYPES {
        g.fire("objddl:cap:types");
        return gen_type_drop(g);
    }
    g.fire("objddl:type:create");
    let kind = g.weights.pick(
        g.rng,
        &[
            "objddl:type:enum",
            "objddl:type:composite",
            "objddl:type:range",
            "objddl:type:shell",
            "objddl:type:base",
        ],
    );
    let n = g.obj.next_type;
    g.obj.next_type += 1;
    match kind {
        "objddl:type:enum" => {
            g.fire("objddl:type:enum");
            let name = format!("fz_ety_{}", n);
            let nvals = 2 + g.rng.below(4);
            let labels: Vec<String> = (0..nvals).map(|i| format!("'v{}'", i)).collect();
            g.obj.enums.push(ObjEnum { name: name.clone(), nvals, live: true });
            vec![StmtKind::Raw(format!(
                "CREATE TYPE {} AS ENUM ({});",
                name,
                labels.join(", ")
            ))]
        }
        "objddl:type:composite" => {
            g.fire("objddl:type:composite");
            let name = format!("fz_cty_{}", n);
            let nfields = 2 + g.rng.below_usize(3);
            let mut fields = Vec::with_capacity(nfields);
            let mut defs = Vec::with_capacity(nfields);
            for i in 0..nfields {
                let ty = FIELD_TYPES[g.rng.below_usize(FIELD_TYPES.len())];
                defs.push(format!("f{} {}", i, ty.name()));
                fields.push(ty);
            }
            g.obj.comps.push(ObjComp { name: name.clone(), fields, live: true });
            vec![StmtKind::Raw(format!(
                "CREATE TYPE {} AS ({});",
                name,
                defs.join(", ")
            ))]
        }
        "objddl:type:range" => {
            g.fire("objddl:type:range");
            let name = format!("fz_rty_{}", n);
            let subtype = RANGE_SUBTYPES[g.rng.below_usize(RANGE_SUBTYPES.len())];
            g.obj.ranges.push(ObjRange { name: name.clone(), subtype, live: true });
            vec![StmtKind::Raw(format!(
                "CREATE TYPE {} AS RANGE (SUBTYPE = {});",
                name,
                subtype.name()
            ))]
        }
        "objddl:type:shell" => {
            g.fire("objddl:type:shell");
            let name = format!("fz_sty_{}", n);
            g.obj.shells.push(ObjShell { name: name.clone(), live: true });
            vec![StmtKind::Raw(format!("CREATE TYPE {};", name))]
        }
        _ => {
            // Deliberate matched-error fuel: no shell pre-declaration, so
            // both engines reject identically after walking DefineType's
            // option parsing. Never registered (the type never exists).
            g.fire("objddl:type:base");
            let name = format!("fz_bty_{}", n);
            vec![StmtKind::Raw(format!(
                "CREATE TYPE {} (INPUT = int4in, OUTPUT = int4out, LIKE = int4);",
                name
            ))]
        }
    }
}

fn gen_type_alter_enum(g: &mut Gen) -> Vec<StmtKind> {
    let live: Vec<usize> = g
        .obj
        .enums
        .iter()
        .enumerate()
        .filter(|(_, t)| t.live)
        .map(|(i, _)| i)
        .collect();
    if live.is_empty() {
        g.fire("objddl:fallback:type_create");
        return gen_type_create(g);
    }
    g.fire("objddl:type:alter_enum");
    let ei = live[g.rng.below_usize(live.len())];
    let (name, nvals) = (g.obj.enums[ei].name.clone(), g.obj.enums[ei].nvals);
    if nvals >= MAX_ENUM_LABELS || g.rng.chance(1, 6) {
        // No-op re-add of an existing label (NOTICE, not an error).
        let existing = g.rng.below(nvals);
        return vec![StmtKind::Raw(format!(
            "ALTER TYPE {} ADD VALUE IF NOT EXISTS 'v{}';",
            name, existing
        ))];
    }
    let placement = match g.rng.below(3) {
        0 => " BEFORE 'v0'".to_string(),
        1 => format!(" AFTER 'v{}'", nvals - 1),
        _ => String::new(),
    };
    g.obj.enums[ei].nvals += 1;
    vec![StmtKind::Raw(format!(
        "ALTER TYPE {} ADD VALUE 'v{}'{};",
        name, nvals, placement
    ))]
}

/// Ordered literal pair (lo < hi) for a range subtype.
fn range_bounds(g: &mut Gen, ty: SqlType) -> (String, String) {
    match ty {
        SqlType::Int4 | SqlType::Int8 => {
            let a = g.rng.below(100);
            let b = a + 1 + g.rng.below(100);
            (a.to_string(), b.to_string())
        }
        SqlType::Numeric => {
            let a = g.rng.below(50);
            let b = a + 1 + g.rng.below(50);
            (format!("{}.5", a), format!("{}.25", b))
        }
        SqlType::Date => (
            pick_str(g, &["'2020-01-01'", "'2021-06-15'"]).to_string(),
            pick_str(g, &["'2024-02-29'", "'2030-12-31'"]).to_string(),
        ),
        SqlType::Timestamp => (
            "'2020-01-01 00:00:00'".to_string(),
            pick_str(g, &["'2022-03-03 12:00:00'", "'2030-01-01 23:59:59'"]).to_string(),
        ),
        _ => (
            pick_str(g, &["'a'", "'b'"]).to_string(),
            pick_str(g, &["'m'", "'z'"]).to_string(),
        ),
    }
}

fn gen_type_use(g: &mut Gen) -> Vec<StmtKind> {
    // Uniform pick over all live enum/composite/range types.
    let mut pool: Vec<(char, usize)> = Vec::new();
    for (i, t) in g.obj.enums.iter().enumerate() {
        if t.live {
            pool.push(('e', i));
        }
    }
    for (i, t) in g.obj.comps.iter().enumerate() {
        if t.live {
            pool.push(('c', i));
        }
    }
    for (i, t) in g.obj.ranges.iter().enumerate() {
        if t.live {
            pool.push(('r', i));
        }
    }
    if pool.is_empty() {
        g.fire("objddl:fallback:type_create");
        return gen_type_create(g);
    }
    g.fire("objddl:type:use");
    let (class, idx) = pool[g.rng.below_usize(pool.len())];
    let sql = match class {
        'e' => {
            let (name, nvals) = (g.obj.enums[idx].name.clone(), g.obj.enums[idx].nvals);
            let vi = g.rng.below(nvals);
            let vj = g.rng.below(nvals);
            match g.rng.below(4) {
                0 => format!("SELECT ('v{}'::{})::text;", vi, name),
                1 => format!("SELECT 'v{}'::{} < 'v{}'::{};", vi, name, vj, name),
                2 => format!("SELECT (enum_range(NULL::{}))::text;", name),
                _ => format!(
                    "SELECT (enum_first(NULL::{}))::text, (enum_last(NULL::{}))::text;",
                    name, name
                ),
            }
        }
        'c' => {
            let (name, fields) =
                (g.obj.comps[idx].name.clone(), g.obj.comps[idx].fields.clone());
            let lits = |g: &mut Gen| -> String {
                let vals: Vec<String> = fields.iter().map(|&ty| g.gen_literal(ty)).collect();
                vals.join(", ")
            };
            match g.rng.below(4) {
                0 => {
                    let fi = g.rng.below_usize(fields.len());
                    let l = lits(g);
                    format!("SELECT (ROW({})::{}).f{};", l, name, fi)
                }
                1 => {
                    let (a, b) = (lits(g), lits(g));
                    format!("SELECT ROW({})::{} = ROW({})::{};", a, name, b, name)
                }
                2 => {
                    let (a, b) = (lits(g), lits(g));
                    format!("SELECT ROW({})::{} < ROW({})::{};", a, name, b, name)
                }
                _ => {
                    let l = lits(g);
                    format!("SELECT (ROW({})::{})::text;", l, name)
                }
            }
        }
        _ => {
            let (name, subtype) =
                (g.obj.ranges[idx].name.clone(), g.obj.ranges[idx].subtype);
            let (lo, hi) = range_bounds(g, subtype);
            match g.rng.below(5) {
                0 => format!("SELECT ({}({}, {}))::text;", name, lo, hi),
                1 => format!("SELECT isempty({}({}, {}));", name, lo, lo),
                2 => format!(
                    "SELECT (lower({}({}, {})))::text, (upper({}({}, {})))::text;",
                    name, lo, hi, name, lo, hi
                ),
                3 => format!("SELECT ('empty'::{})::text;", name),
                _ => format!("SELECT ({}(NULL, {}))::text;", name, hi),
            }
        }
    };
    vec![StmtKind::Raw(sql)]
}

/// Create/use/drop a table with an enum column in one self-contained group
/// (the created-type-in-column-DDL surface without cross-module hazards).
fn gen_type_coltab(g: &mut Gen) -> Vec<StmtKind> {
    let live: Vec<usize> = g
        .obj
        .enums
        .iter()
        .enumerate()
        .filter(|(_, t)| t.live)
        .map(|(i, _)| i)
        .collect();
    if live.is_empty() {
        g.fire("objddl:fallback:type_create");
        return gen_type_create(g);
    }
    g.fire("objddl:type:coltab");
    let ei = live[g.rng.below_usize(live.len())];
    let (ty, nvals) = (g.obj.enums[ei].name.clone(), g.obj.enums[ei].nvals);
    let t = format!("fz_objt_{}", g.obj.next_objt);
    g.obj.next_objt += 1;
    vec![
        StmtKind::Raw(format!("CREATE TABLE {} (pk int4 PRIMARY KEY, c {});", t, ty)),
        StmtKind::Raw(format!(
            "INSERT INTO {} VALUES (1, 'v0'), (2, 'v{}');",
            t,
            nvals - 1
        )),
        StmtKind::Raw(format!("SELECT pk, c::text FROM {} ORDER BY pk;", t)),
        StmtKind::Raw(format!("DROP TABLE {};", t)),
    ]
}

fn gen_type_drop(g: &mut Gen) -> Vec<StmtKind> {
    let mut pool: Vec<(char, usize, String)> = Vec::new();
    for (i, t) in g.obj.enums.iter().enumerate() {
        if t.live {
            pool.push(('e', i, t.name.clone()));
        }
    }
    for (i, t) in g.obj.comps.iter().enumerate() {
        if t.live {
            pool.push(('c', i, t.name.clone()));
        }
    }
    for (i, t) in g.obj.ranges.iter().enumerate() {
        if t.live {
            pool.push(('r', i, t.name.clone()));
        }
    }
    for (i, t) in g.obj.shells.iter().enumerate() {
        if t.live {
            pool.push(('s', i, t.name.clone()));
        }
    }
    if pool.is_empty() {
        g.fire("objddl:fallback:type_create");
        return gen_type_create(g);
    }
    g.fire("objddl:type:drop");
    let (class, idx, name) = pool[g.rng.below_usize(pool.len())].clone();
    match class {
        'e' => g.obj.enums[idx].live = false,
        'c' => g.obj.comps[idx].live = false,
        'r' => g.obj.ranges[idx].live = false,
        _ => g.obj.shells[idx].live = false,
    }
    let if_exists = g.rng.chance(1, 3);
    vec![StmtKind::Raw(format!(
        "DROP TYPE {}{};",
        if if_exists { "IF EXISTS " } else { "" },
        name
    ))]
}

// --------------------------------------------------- extended statistics ----

const STAT_KINDS: &[&str] = &[
    "",
    " (ndistinct)",
    " (dependencies)",
    " (mcv)",
    " (ndistinct, dependencies)",
    " (mcv, dependencies)",
    " (ndistinct, dependencies, mcv)",
];

fn gen_stats_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.obj.live_stats().len() >= MAX_LIVE_STATS {
        g.fire("objddl:cap:stats");
        return gen_stats_drop(g);
    }
    // Any catalog table with >= 2 comparable columns (json has no equality
    // operator and cannot appear in a statistics column list).
    let cands: Vec<usize> = g
        .catalog
        .tables
        .iter()
        .enumerate()
        .filter(|(_, t)| {
            t.columns.iter().filter(|c| c.ty != SqlType::Json).count() >= 2
        })
        .map(|(i, _)| i)
        .collect();
    if cands.is_empty() {
        g.fire("objddl:fallback:describe");
        return gen_describe(g);
    }
    g.fire("objddl:stats:create");
    let t = &g.catalog.tables[cands[g.rng.below_usize(cands.len())]];
    let tname = t.name.clone();
    let eligible: Vec<(String, SqlType)> = t
        .columns
        .iter()
        .filter(|c| c.ty != SqlType::Json)
        .map(|c| (c.name.clone(), c.ty))
        .collect();
    // 2-3 distinct columns, deterministic sample without replacement.
    let want = 2 + g.rng.below_usize(2).min(eligible.len() - 2);
    let mut picked: Vec<(String, SqlType)> = Vec::new();
    let mut rest = eligible;
    for _ in 0..want {
        let i = g.rng.below_usize(rest.len());
        picked.push(rest.remove(i));
    }
    let name = format!("fz_stx_{}", g.obj.next_stat);
    g.obj.next_stat += 1;
    g.obj.stats.push(ObjStat { name: name.clone(), live: true });
    let kinds = pick_str(g, STAT_KINDS);
    let collist: Vec<String> = picked.iter().map(|(n, _)| n.clone()).collect();
    let mut out = vec![
        StmtKind::Raw(format!(
            "CREATE STATISTICS {}{} ON {} FROM {};",
            name,
            kinds,
            collist.join(", "),
            tname
        )),
        StmtKind::Raw(format!("ANALYZE {};", tname)),
    ];
    // Planner probe: multi-column equality drives the extended-stats
    // selectivity paths (dependencies/mcv clauselist selectivity).
    let mut preds = Vec::with_capacity(picked.len());
    for (cname, ty) in &picked {
        let lit = g.gen_literal(*ty);
        preds.push(format!("{} = {}", cname, lit));
    }
    out.push(StmtKind::Raw(format!(
        "SELECT count(*) FROM {} WHERE {};",
        tname,
        preds.join(" AND ")
    )));
    out
}

fn gen_stats_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.obj.live_stats();
    if live.is_empty() {
        g.fire("objddl:fallback:stats_create");
        return gen_stats_create(g);
    }
    g.fire("objddl:stats:drop");
    let si = live[g.rng.below_usize(live.len())];
    let name = g.obj.stats[si].name.clone();
    g.obj.stats[si].live = false;
    // Always IF EXISTS: the object dies silently with its table when the
    // ddl module drops a table this statistics object was built on.
    vec![StmtKind::Raw(format!("DROP STATISTICS IF EXISTS {};", name))]
}

// ------------------------------------------- Q6: opclass DDL breadth ----
//
// The sql-reachable-queue `opclass-ddl` chunk: CREATE OPERATOR CLASS /
// OPERATOR FAMILY with full member lists over every AM (btree/hash/gist/
// gin/spgist/brin), ALTER OPERATOR FAMILY ADD/DROP incl. cross-type
// members, amvalidate() probes (warnings go to the server log, the
// boolean result is compared), access methods, casts, conversions,
// languages, transform/security-label error surfaces, forward-referenced
// shell operators and aggregate FINALFUNC_MODIFY options.
//
// Every group is SELF-CONTAINED: it creates, exercises and drops its
// objects (names from the monotonic `next_q` counter — never reused), so
// nothing leaks across groups and the zero-42xxx discipline holds; the
// deliberate matched-error statements below were each hand-verified
// byte-identical on both engines first (2026-08-11, scratchpad/
// q6-hv1-opclass.sql + q6-hv2-miscddl.sql).
//
// Banked exclusion (finding, not a weight choice): CREATE FUNCTION in a
// user-created plpgsql-handler LANGUAGE — pgrust errors "language ... is
// not supported yet" where C accepts it. The language groups create/drop
// the language but never define functions in it.

fn qn(g: &mut Gen) -> u32 {
    let n = g.obj.next_q;
    g.obj.next_q += 1;
    n
}

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

/// CREATE OPERATOR CLASS with a full member list, per AM; the btree/hash
/// arms optionally attach to an explicit family and weave cross-type
/// members in/out via ALTER OPERATOR FAMILY; btree/gist arms optionally
/// build an index through the new opclass and query it.
fn gen_opc(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:opc");
    let am = g.weights.pick(
        g.rng,
        &[
            "objddl:opc:bt",
            "objddl:opc:hash",
            "objddl:opc:gist",
            "objddl:opc:gin",
            "objddl:opc:spgist",
            "objddl:opc:brin",
        ],
    );
    g.fire(am);
    match am {
        "objddl:opc:bt" => opc_bt(g),
        "objddl:opc:hash" => opc_hash(g),
        "objddl:opc:gist" => opc_gist(g),
        "objddl:opc:gin" => opc_gin(g),
        "objddl:opc:spgist" => opc_spgist(g),
        _ => opc_brin(g),
    }
}

fn amvalidate_class(name: &str) -> StmtKind {
    raw(format!("SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = '{}';", name))
}

fn amvalidate_family(name: &str) -> StmtKind {
    raw(format!("SELECT amvalidate(oid) FROM pg_opfamily WHERE opfname = '{}';", name))
}

fn opc_bt(g: &mut Gen) -> Vec<StmtKind> {
    let n = qn(g);
    let fam = format!("fz_qopf_{n}");
    let opc = format!("fz_qopc_{n}");
    let with_family =
        g.weights.pick(g.rng, &["objddl:opc:fam", "objddl:opc:nofam"]) == "objddl:opc:fam";
    let mut out = Vec::new();
    if with_family {
        g.fire("objddl:opc:fam");
        out.push(raw(format!("CREATE OPERATOR FAMILY {fam} USING btree;")));
    }
    // Optional support procs beyond the required cmp (sortsupport /
    // equalimage — btvalidate's optional-proc arms).
    let mut procs = String::from("FUNCTION 1 btint4cmp(int4, int4)");
    if g.weights.pick(g.rng, &["objddl:opc:sort", "objddl:opc:nosort"]) == "objddl:opc:sort" {
        g.fire("objddl:opc:sort");
        procs.push_str(", FUNCTION 2 btint4sortsupport(internal)");
    }
    if g.weights.pick(g.rng, &["objddl:opc:eqimg", "objddl:opc:noeqimg"]) == "objddl:opc:eqimg" {
        g.fire("objddl:opc:eqimg");
        procs.push_str(", FUNCTION 4 btequalimage(oid)");
    }
    out.push(raw(format!(
        "CREATE OPERATOR CLASS {opc} FOR TYPE int4 USING btree{} AS \
         OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >, {};",
        if with_family { format!(" FAMILY {fam}") } else { String::new() },
        procs
    )));
    out.push(amvalidate_class(&opc));
    let cross = with_family
        && g.weights.pick(g.rng, &["objddl:opc:xtype", "objddl:opc:noxtype"])
            == "objddl:opc:xtype";
    if cross {
        g.fire("objddl:opc:xtype");
        out.push(raw(format!(
            "ALTER OPERATOR FAMILY {fam} USING btree ADD \
             OPERATOR 1 < (int4, int8), OPERATOR 2 <= (int4, int8), OPERATOR 3 = (int4, int8), \
             OPERATOR 4 >= (int4, int8), OPERATOR 5 > (int4, int8), \
             FUNCTION 1 btint48cmp(int4, int8);"
        )));
        out.push(amvalidate_family(&fam));
    }
    if g.weights.pick(g.rng, &["objddl:opc:index", "objddl:opc:noindex"]) == "objddl:opc:index" {
        g.fire("objddl:opc:index");
        let tab = format!("fz_qtab_{n}");
        let rows = 300 + g.rng.below(400);
        let a = 1 + g.rng.below(rows);
        out.push(raw(format!("CREATE TABLE {tab} (pk int4);")));
        out.push(raw(format!(
            "INSERT INTO {tab} SELECT i FROM generate_series(1, {rows}) i;"
        )));
        out.push(raw(format!("CREATE INDEX {tab}_i ON {tab} (pk {opc});")));
        out.push(raw("SET enable_seqscan TO off;"));
        out.push(raw(format!("SELECT count(*) FROM {tab} WHERE pk < {a};")));
        out.push(raw(format!(
            "SELECT pk FROM {tab} WHERE pk < {} ORDER BY pk;",
            1 + g.rng.below(12)
        )));
        out.push(raw("RESET enable_seqscan;"));
        out.push(raw(format!("DROP TABLE {tab};")));
    }
    if cross {
        out.push(raw(format!(
            "ALTER OPERATOR FAMILY {fam} USING btree DROP \
             OPERATOR 1 (int4, int8), OPERATOR 2 (int4, int8), OPERATOR 3 (int4, int8), \
             OPERATOR 4 (int4, int8), OPERATOR 5 (int4, int8), FUNCTION 1 (int4, int8);"
        )));
    }
    let (mut opc_f, mut fam_f) = (opc.clone(), fam.clone());
    if g.weights.pick(g.rng, &["objddl:opc:rename", "objddl:opc:norename"])
        == "objddl:opc:rename"
    {
        g.fire("objddl:opc:rename");
        opc_f = format!("{opc}_r");
        out.push(raw(format!("ALTER OPERATOR CLASS {opc} USING btree RENAME TO {opc_f};")));
        if with_family {
            fam_f = format!("{fam}_r");
            out.push(raw(format!("ALTER OPERATOR FAMILY {fam} USING btree RENAME TO {fam_f};")));
        }
    }
    out.push(raw(format!("DROP OPERATOR CLASS {opc_f} USING btree;")));
    if with_family {
        out.push(raw(format!("DROP OPERATOR FAMILY {fam_f} USING btree;")));
    }
    out
}

fn opc_hash(g: &mut Gen) -> Vec<StmtKind> {
    let n = qn(g);
    let fam = format!("fz_qopf_{n}");
    let opc = format!("fz_qopc_{n}");
    let mut out = vec![
        raw(format!("CREATE OPERATOR FAMILY {fam} USING hash;")),
        raw(format!(
            "CREATE OPERATOR CLASS {opc} FOR TYPE int4 USING hash FAMILY {fam} AS \
             OPERATOR 1 =, FUNCTION 1 hashint4(int4), FUNCTION 2 hashint4extended(int4, int8);"
        )),
    ];
    if g.weights.pick(g.rng, &["objddl:opc:xtype", "objddl:opc:noxtype"]) == "objddl:opc:xtype" {
        g.fire("objddl:opc:xtype");
        out.push(raw(format!(
            "ALTER OPERATOR FAMILY {fam} USING hash ADD OPERATOR 1 = (int4, int8), \
             FUNCTION 1 (int4, int8) hashint8(int8), \
             FUNCTION 2 (int4, int8) hashint8extended(int8, int8);"
        )));
    }
    out.push(amvalidate_class(&opc));
    out.push(amvalidate_family(&fam));
    out.push(raw(format!("DROP OPERATOR CLASS {opc} USING hash;")));
    // Cross-type loose members may still be attached: CASCADE drops them
    // with the family (hand-verified identical).
    out.push(raw(format!("DROP OPERATOR FAMILY {fam} USING hash CASCADE;")));
    out
}

fn opc_gist(g: &mut Gen) -> Vec<StmtKind> {
    let n = qn(g);
    let opc = format!("fz_qopc_{n}");
    // Optional ordering member: OPERATOR 15 <-> (box, point) FOR ORDER BY +
    // FUNCTION 8 gist_box_distance — a custom-opclass KNN path.
    let dist = g.weights.pick(g.rng, &["objddl:opc:dist", "objddl:opc:nodist"])
        == "objddl:opc:dist";
    let mut members = String::from(
        "OPERATOR 3 &&, OPERATOR 7 @>, OPERATOR 8 <@, \
         FUNCTION 1 gist_box_consistent(internal, box, smallint, oid, internal), \
         FUNCTION 2 gist_box_union(internal, internal), \
         FUNCTION 5 gist_box_penalty(internal, internal, internal), \
         FUNCTION 6 gist_box_picksplit(internal, internal), \
         FUNCTION 7 gist_box_same(box, box, internal)",
    );
    if dist {
        g.fire("objddl:opc:dist");
        members = format!(
            "OPERATOR 15 <-> (box, point) FOR ORDER BY pg_catalog.float_ops, {}, \
             FUNCTION 8 gist_box_distance(internal, box, smallint, oid, internal)",
            members
        );
    }
    let mut out = vec![
        raw(format!("CREATE OPERATOR CLASS {opc} FOR TYPE box USING gist AS {members};")),
        amvalidate_class(&opc),
    ];
    if g.weights.pick(g.rng, &["objddl:opc:index", "objddl:opc:noindex"]) == "objddl:opc:index" {
        g.fire("objddl:opc:index");
        let tab = format!("fz_qtab_{n}");
        let rows = 400 + g.rng.below(500);
        out.push(raw(format!("CREATE TABLE {tab} (pk int4 PRIMARY KEY, bx box);")));
        out.push(raw(format!(
            "INSERT INTO {tab} SELECT i, box(point((i * 7) % 97, (i * 11) % 89), \
             point((i * 7) % 97 + 2, (i * 11) % 89 + 2)) FROM generate_series(1, {rows}) i;"
        )));
        out.push(raw(format!("CREATE INDEX {tab}_i ON {tab} USING gist (bx {opc});")));
        let x1 = g.rng.below(80);
        let y1 = g.rng.below(70);
        out.push(raw("SET enable_seqscan TO off;"));
        out.push(raw(format!(
            "SELECT count(*) FROM {tab} WHERE bx && box(point({x1}, {y1}), point({}, {}));",
            x1 + 5 + g.rng.below(30),
            y1 + 5 + g.rng.below(30)
        )));
        if dist {
            out.push(raw(format!(
                "SELECT pk FROM {tab} ORDER BY bx <-> point({}, {}), pk LIMIT {};",
                g.rng.below(97),
                g.rng.below(89),
                5 + g.rng.below(10)
            )));
        }
        out.push(raw("RESET enable_seqscan;"));
        out.push(raw(format!("DROP TABLE {tab};")));
    }
    out.push(raw(format!("DROP OPERATOR CLASS {opc} USING gist;")));
    out
}

fn opc_gin(g: &mut Gen) -> Vec<StmtKind> {
    let n = qn(g);
    let opc = format!("fz_qopc_{n}");
    vec![
        raw(format!(
            "CREATE OPERATOR CLASS {opc} FOR TYPE int4[] USING gin AS \
             OPERATOR 1 &&, OPERATOR 2 @>, OPERATOR 3 <@, OPERATOR 4 =, \
             FUNCTION 1 btint4cmp(int4, int4), \
             FUNCTION 2 ginarrayextract(anyarray, internal, internal), \
             FUNCTION 3 ginqueryarrayextract(anyarray, internal, smallint, internal, internal, internal, internal), \
             FUNCTION 4 ginarrayconsistent(internal, smallint, anyarray, integer, internal, internal, internal, internal), \
             FUNCTION 6 ginarraytriconsistent(internal, smallint, anyarray, integer, internal, internal, internal), \
             STORAGE int4;"
        )),
        amvalidate_class(&opc),
        raw(format!("DROP OPERATOR CLASS {opc} USING gin;")),
    ]
}

fn opc_spgist(g: &mut Gen) -> Vec<StmtKind> {
    let n = qn(g);
    let opc = format!("fz_qopc_{n}");
    vec![
        raw(format!(
            "CREATE OPERATOR CLASS {opc} FOR TYPE point USING spgist AS \
             OPERATOR 11 >^, OPERATOR 1 <<, OPERATOR 5 >>, OPERATOR 6 ~=, OPERATOR 10 <^, \
             OPERATOR 8 <@ (point, box), \
             FUNCTION 1 spg_quad_config(internal, internal), \
             FUNCTION 2 spg_quad_choose(internal, internal), \
             FUNCTION 3 spg_quad_picksplit(internal, internal), \
             FUNCTION 4 spg_quad_inner_consistent(internal, internal), \
             FUNCTION 5 spg_quad_leaf_consistent(internal, internal);"
        )),
        amvalidate_class(&opc),
        raw(format!("DROP OPERATOR CLASS {opc} USING spgist;")),
    ]
}

fn opc_brin(g: &mut Gen) -> Vec<StmtKind> {
    let n = qn(g);
    let opc = format!("fz_qopc_{n}");
    vec![
        raw(format!(
            "CREATE OPERATOR CLASS {opc} FOR TYPE int4 USING brin AS \
             OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >, \
             FUNCTION 1 brin_minmax_opcinfo(internal), \
             FUNCTION 2 brin_minmax_add_value(internal, internal, internal, internal), \
             FUNCTION 3 brin_minmax_consistent(internal, internal, internal), \
             FUNCTION 4 brin_minmax_union(internal, internal, internal);"
        )),
        amvalidate_class(&opc),
        raw(format!("DROP OPERATOR CLASS {opc} USING brin;")),
    ]
}

/// Matched-error fuel (every statement hand-verified to error identically):
/// wrong support-fn signature, duplicate DEFAULT opclass, member drops on
/// an empty family, drops of nonexistent classes/families, and the
/// incomplete-opclass amvalidate=false path.
fn gen_opcerr(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:opcerr");
    let n = qn(g);
    let kind = g.weights.pick(
        g.rng,
        &[
            "objddl:opcerr:sig",
            "objddl:opcerr:default",
            "objddl:opcerr:member",
            "objddl:opcerr:nosuch",
            "objddl:opcerr:incomplete",
        ],
    );
    g.fire(kind);
    match kind {
        "objddl:opcerr:sig" => vec![raw(format!(
            "CREATE OPERATOR CLASS fz_qopc_{n} FOR TYPE int4 USING btree AS \
             OPERATOR 1 <, FUNCTION 1 length(text);"
        ))],
        "objddl:opcerr:default" => vec![raw(format!(
            "CREATE OPERATOR CLASS fz_qopc_{n} DEFAULT FOR TYPE int4 USING btree AS \
             OPERATOR 1 <, FUNCTION 1 btint4cmp(int4, int4);"
        ))],
        "objddl:opcerr:member" => vec![
            raw(format!("CREATE OPERATOR FAMILY fz_qopf_{n} USING btree;")),
            raw(format!(
                "ALTER OPERATOR FAMILY fz_qopf_{n} USING btree DROP OPERATOR 1 (int4, int4);"
            )),
            raw(format!("ALTER OPERATOR FAMILY fz_qopf_{n} USING btree ADD FUNCTION 1 now();")),
            raw(format!("DROP OPERATOR FAMILY fz_qopf_{n} USING btree;")),
        ],
        "objddl:opcerr:nosuch" => {
            let am = ["btree", "hash", "gist", "gin", "spgist", "brin"][g.rng.below_usize(6)];
            match g.rng.below(3) {
                0 => vec![raw(format!("DROP OPERATOR CLASS fz_qnosuch_{n} USING {am};"))],
                1 => vec![raw(format!("DROP OPERATOR FAMILY fz_qnosuch_{n} USING {am};"))],
                _ => vec![raw(format!(
                    "DROP OPERATOR CLASS IF EXISTS fz_qnosuch_{n} USING {am};"
                ))],
            }
        }
        _ => {
            let opc = format!("fz_qopc_{n}");
            vec![
                raw(format!(
                    "CREATE OPERATOR CLASS {opc} FOR TYPE int4 USING btree AS \
                     OPERATOR 1 <, FUNCTION 1 btint4cmp(int4, int4);"
                )),
                amvalidate_class(&opc),
                raw(format!("DROP OPERATOR CLASS {opc} USING btree;")),
            ]
        }
    }
}

/// CREATE/DROP ACCESS METHOD: an index AM wrapping gisthandler (with an
/// index built through it), a table AM wrapping heap_tableam_handler
/// (with ALTER TABLE SET ACCESS METHOD both ways), the partitioned
/// no-storage SET ACCESS METHOD path, and handler-mismatch error fuel.
fn gen_am(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:am");
    let n = qn(g);
    let kind = g.weights.pick(
        g.rng,
        &["objddl:am:index", "objddl:am:table", "objddl:am:part", "objddl:am:err"],
    );
    g.fire(kind);
    let am = format!("fz_qam_{n}");
    let tab = format!("fz_qtab_{n}");
    match kind {
        "objddl:am:index" => {
            let rows = 200 + g.rng.below(201);
            let x1 = g.rng.below(40);
            let y1 = g.rng.below(30);
            vec![
                raw(format!("CREATE ACCESS METHOD {am} TYPE INDEX HANDLER gisthandler;")),
                raw(format!("CREATE TABLE {tab} (pk int4 PRIMARY KEY, bx box);")),
                raw(format!(
                    "INSERT INTO {tab} SELECT i, box(point(i % 50, i % 40), \
                     point(i % 50 + 2, i % 40 + 2)) FROM generate_series(1, {rows}) i;"
                )),
                raw(format!("CREATE INDEX {tab}_i ON {tab} USING {am} (bx);")),
                raw("SET enable_seqscan TO off;"),
                raw(format!(
                    "SELECT count(*) FROM {tab} WHERE bx && box(point({x1}, {y1}), point({}, {}));",
                    x1 + 5 + g.rng.below(15),
                    y1 + 5 + g.rng.below(15)
                )),
                raw("RESET enable_seqscan;"),
                raw(format!("COMMENT ON ACCESS METHOD {am} IS 'q6 am';")),
                raw(format!("DROP ACCESS METHOD {am} CASCADE;")),
                raw(format!("DROP TABLE {tab};")),
            ]
        }
        "objddl:am:table" => vec![
            raw(format!("CREATE ACCESS METHOD {am} TYPE TABLE HANDLER heap_tableam_handler;")),
            raw(format!("CREATE TABLE {tab} (pk int4) USING {am};")),
            raw(format!(
                "INSERT INTO {tab} SELECT i FROM generate_series(1, {}) i;",
                20 + g.rng.below(60)
            )),
            raw(format!("ALTER TABLE {tab} SET ACCESS METHOD heap;")),
            raw(format!("ALTER TABLE {tab} SET ACCESS METHOD {am};")),
            raw(format!("SELECT count(*) FROM {tab};")),
            raw(format!("DROP TABLE {tab};")),
            raw(format!("DROP ACCESS METHOD {am};")),
        ],
        "objddl:am:part" => vec![
            raw(format!("CREATE TABLE {tab} (pk int4) PARTITION BY RANGE (pk);")),
            raw(format!("ALTER TABLE {tab} SET ACCESS METHOD heap;")),
            raw(format!("ALTER TABLE {tab} SET ACCESS METHOD DEFAULT;")),
            raw(format!("DROP TABLE {tab};")),
        ],
        _ => match g.rng.below(3) {
            0 => vec![raw(format!(
                "CREATE ACCESS METHOD {am} TYPE INDEX HANDLER heap_tableam_handler;"
            ))],
            1 => vec![raw(format!("CREATE ACCESS METHOD {am} TYPE TABLE HANDLER int4pl;"))],
            _ => vec![raw(format!("DROP ACCESS METHOD fz_qnosuch_{n};"))],
        },
    }
}

/// CREATE CAST over a group-local enum: WITH INOUT both directions
/// (weighted IMPLICIT), a WITH FUNCTION cast, use-probes through each,
/// and duplicate-builtin-cast / missing-cast error fuel.
fn gen_cast(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:cast");
    let n = qn(g);
    if g.weights.pick(g.rng, &["objddl:cast:err", "objddl:cast:ok"]) == "objddl:cast:err" {
        g.fire("objddl:cast:err");
        return if g.rng.chance(1, 2) {
            vec![raw("CREATE CAST (int4 AS bool) WITH INOUT;")]
        } else {
            vec![raw("DROP CAST (int4 AS text);")]
        };
    }
    let en = format!("fz_qen_{n}");
    let mut out = vec![
        raw(format!("CREATE TYPE {en} AS ENUM ('sad', 'ok', 'happy');")),
        raw(format!("CREATE CAST ({en} AS text) WITH INOUT;")),
    ];
    let implicit = g.weights.pick(g.rng, &["objddl:cast:implicit", "objddl:cast:plain"])
        == "objddl:cast:implicit";
    if implicit {
        g.fire("objddl:cast:implicit");
        out.push(raw(format!("CREATE CAST (text AS {en}) WITH INOUT AS IMPLICIT;")));
    } else {
        out.push(raw(format!("CREATE CAST (text AS {en}) WITH INOUT;")));
    }
    out.push(raw(format!("SELECT (('ok'::text)::{en})::text;")));
    if g.weights.pick(g.rng, &["objddl:cast:fn", "objddl:cast:nofn"]) == "objddl:cast:fn" {
        g.fire("objddl:cast:fn");
        let f = format!("fz_qfn_{n}");
        out.push(raw(format!(
            "CREATE FUNCTION {f}({en}) RETURNS int4 LANGUAGE sql IMMUTABLE \
             RETURN CASE $1 WHEN 'sad' THEN 0 WHEN 'ok' THEN 1 ELSE 2 END;"
        )));
        out.push(raw(format!("CREATE CAST ({en} AS int4) WITH FUNCTION {f}({en}) AS ASSIGNMENT;")));
        out.push(raw(format!("SELECT ('happy'::{en})::int4;")));
        out.push(raw(format!("DROP CAST ({en} AS int4);")));
        out.push(raw(format!("DROP FUNCTION {f}({en});")));
    }
    out.push(raw(format!("DROP CAST ({en} AS text);")));
    out.push(raw(format!("DROP CAST (text AS {en});")));
    out.push(raw(format!("DROP TYPE {en};")));
    out
}

/// CREATE [DEFAULT] CONVERSION over the builtin iso8859_1_to_utf8 proc,
/// with comment/rename churn and bad-function / missing-object error fuel.
fn gen_conv(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:conv");
    let n = qn(g);
    if g.weights.pick(g.rng, &["objddl:conv:err", "objddl:conv:ok"]) == "objddl:conv:err" {
        g.fire("objddl:conv:err");
        return if g.rng.chance(1, 2) {
            vec![raw(format!(
                "CREATE CONVERSION fz_qcv_{n} FOR 'LATIN1' TO 'UTF8' FROM int4pl;"
            ))]
        } else {
            vec![raw(format!("DROP CONVERSION fz_qnosuch_{n};"))]
        };
    }
    let cv = format!("fz_qcv_{n}");
    let default = g.weights.pick(g.rng, &["objddl:conv:default", "objddl:conv:plain"])
        == "objddl:conv:default";
    let mut out = vec![raw(format!(
        "CREATE {}CONVERSION {cv} FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;",
        if default {
            g.fire("objddl:conv:default");
            "DEFAULT "
        } else {
            ""
        }
    ))];
    out.push(raw(format!("COMMENT ON CONVERSION {cv} IS 'q6 conv';")));
    let mut fin = cv.clone();
    if g.rng.chance(1, 2) {
        fin = format!("{cv}_r");
        out.push(raw(format!("ALTER CONVERSION {cv} RENAME TO {fin};")));
    }
    out.push(raw(format!("DROP CONVERSION {fin};")));
    out
}

/// CREATE LANGUAGE over the builtin plpgsql handler triple. Functions in
/// the created language are NEVER defined (banked finding: pgrust errors
/// "language ... is not supported yet" where C accepts).
fn gen_plang(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:plang");
    let n = qn(g);
    if g.weights.pick(g.rng, &["objddl:plang:err", "objddl:plang:ok"]) == "objddl:plang:err" {
        g.fire("objddl:plang:err");
        return vec![raw(format!("CREATE LANGUAGE fz_qlg_{n} HANDLER int4pl;"))];
    }
    let lg = format!("fz_qlg_{n}");
    let full = g.rng.chance(2, 3);
    vec![
        raw(format!(
            "CREATE TRUSTED LANGUAGE {lg} HANDLER plpgsql_call_handler{};",
            if full { " INLINE plpgsql_inline_handler VALIDATOR plpgsql_validator" } else { "" }
        )),
        raw(format!("COMMENT ON LANGUAGE {lg} IS 'q6 lang';")),
        raw(format!("DROP LANGUAGE {lg};")),
    ]
}

/// CREATE TRANSFORM signature-check error surface + DROP TRANSFORM of a
/// missing transform (all matched errors, hand-verified).
fn gen_xform(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:xform");
    match g.rng.below(3) {
        0 => vec![raw(
            "CREATE TRANSFORM FOR int4 LANGUAGE plpgsql (FROM SQL WITH FUNCTION int4pl(int4, int4));",
        )],
        1 => vec![raw("CREATE TRANSFORM FOR int4 LANGUAGE plpgsql (TO SQL WITH FUNCTION now());")],
        _ => vec![raw("DROP TRANSFORM FOR int4 LANGUAGE plpgsql;")],
    }
}

/// SECURITY LABEL: no providers are loaded, so every form is a matched
/// error — but ExecSecLabelStmt's object resolution still executes.
fn gen_seclabel(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:seclabel");
    let target = {
        let tabs = fixture_tables(g);
        let pick = g.rng.below_usize(tabs.len() + 2);
        if pick < tabs.len() {
            format!("TABLE {}", tabs[pick].name)
        } else if pick == tabs.len() {
            "FUNCTION length(text)".to_string()
        } else {
            "TYPE int4".to_string()
        }
    };
    if g.weights.pick(g.rng, &["objddl:seclabel:provider", "objddl:seclabel:plain"])
        == "objddl:seclabel:provider"
    {
        g.fire("objddl:seclabel:provider");
        vec![raw(format!("SECURITY LABEL FOR selinux ON {} IS 'classified';", target))]
    } else {
        vec![raw(format!("SECURITY LABEL ON {} IS 'classified';", target))]
    }
}

/// Forward-referenced COMMUTATOR/NEGATOR: the first CREATE OPERATOR makes
/// shell operators (OperatorShellMake), the second fills the commutator
/// in; the negator shell is probed via pg_operator and dropped explicitly.
fn gen_opshell(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:opshell");
    let neg = g.weights.pick(g.rng, &["objddl:opshell:neg", "objddl:opshell:noneg"])
        == "objddl:opshell:neg";
    let mut first = String::from(
        "CREATE OPERATOR <%% (LEFTARG = int4, RIGHTARG = int4, FUNCTION = int4lt, \
         COMMUTATOR = OPERATOR(public.%%>)",
    );
    if neg {
        g.fire("objddl:opshell:neg");
        first.push_str(", NEGATOR = OPERATOR(public.>=%%)");
    }
    first.push_str(", RESTRICT = scalarltsel, JOIN = scalarltjoinsel);");
    let mut out = vec![
        raw(first),
        raw(
            "CREATE OPERATOR %%> (LEFTARG = int4, RIGHTARG = int4, FUNCTION = int4gt, \
             COMMUTATOR = OPERATOR(public.<%%));",
        ),
        raw(format!("SELECT 1 <%% {}, {} %%> 1;", g.rng.below(4), g.rng.below(4))),
    ];
    if neg {
        out.push(raw("SELECT oprname, oprcode FROM pg_operator WHERE oprname = '>=%%';"));
    }
    out.push(raw("DROP OPERATOR <%% (int4, int4);"));
    out.push(raw("DROP OPERATOR %%> (int4, int4);"));
    if neg {
        out.push(raw("DROP OPERATOR >=%% (int4, int4);"));
    }
    out
}

/// CREATE AGGREGATE with the FINALFUNC_MODIFY/MFINALFUNC_MODIFY option
/// surface (extractModify), a use-probe, and the invalid-value error arm.
fn gen_aggmod(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("objddl:aggmod");
    let n = qn(g);
    let ag = format!("fz_qag_{n}");
    let kind = g.weights.pick(
        g.rng,
        &["objddl:aggmod:plain", "objddl:aggmod:moving", "objddl:aggmod:err"],
    );
    g.fire(kind);
    match kind {
        "objddl:aggmod:plain" => {
            let modify = ["READ_ONLY", "SHAREABLE", "READ_WRITE"][g.rng.below_usize(3)];
            vec![
                raw(format!(
                    "CREATE AGGREGATE {ag} (int4) (SFUNC = int4pl, STYPE = int4, \
                     FINALFUNC_MODIFY = {modify});"
                )),
                raw(format!(
                    "SELECT {ag}(pk) FROM generate_series(1, {}) pk;",
                    5 + g.rng.below(15)
                )),
                raw(format!("DROP AGGREGATE {ag}(int4);")),
            ]
        }
        "objddl:aggmod:moving" => vec![
            raw(format!(
                "CREATE AGGREGATE {ag} (int4) (SFUNC = int4pl, STYPE = int4, \
                 MSFUNC = int4pl, MINVFUNC = int4mi, MSTYPE = int4, \
                 MFINALFUNC_MODIFY = {});",
                ["READ_ONLY", "SHAREABLE"][g.rng.below_usize(2)]
            )),
            raw(format!(
                "SELECT {ag}(pk) FROM generate_series(1, {}) pk;",
                5 + g.rng.below(15)
            )),
            raw(format!("DROP AGGREGATE {ag}(int4);")),
        ],
        _ => vec![raw(format!(
            "CREATE AGGREGATE {ag} (int4) (SFUNC = int4pl, STYPE = int4, \
             FINALFUNC_MODIFY = SOMETIMES);"
        ))],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Generate n groups with session-persistent ObjState (mirrors the
    /// session loop's swap), returning the rendered groups + productions.
    fn gen_groups(seed: u64, n: usize, w: &WeightTable) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut rng = Rng::new(seed);
        let mut state = ObjState::new();
        let mut groups = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, w, &mut prods, 3);
            std::mem::swap(&mut g.obj, &mut state);
            let stmts = gen_objddl_module(&mut g);
            std::mem::swap(&mut g.obj, &mut state);
            groups.push(stmts.iter().map(|s| s.to_sql()).collect::<Vec<_>>());
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    #[test]
    fn shapes_and_invariants() {
        let (groups, prods) = gen_groups(0x0B1u64, 900, &WeightTable::defaults());
        for group in &groups {
            for sql in group {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                // The bootstrap superuser is never named in role DDL.
                assert!(!sql.contains("ROLE postgres"), "{sql}");
                // Drops touch fuzz-created objects only.
                if let Some(rest) = sql.strip_prefix("DROP ROLE ") {
                    let r = rest.trim_start_matches("IF EXISTS ");
                    assert!(r.starts_with("fz_role_"), "{sql}");
                }
                if let Some(rest) = sql.strip_prefix("DROP TYPE ") {
                    let r = rest.trim_start_matches("IF EXISTS ");
                    assert!(
                        r.starts_with("fz_ety_")
                            || r.starts_with("fz_cty_")
                            || r.starts_with("fz_rty_")
                            || r.starts_with("fz_sty_")
                            || r.starts_with("fz_qen_"),
                        "{sql}"
                    );
                }
                if let Some(rest) = sql.strip_prefix("DROP STATISTICS ") {
                    assert!(rest.starts_with("IF EXISTS fz_stx_"), "{sql}");
                }
                // Role DDL only ever names fuzz roles.
                for pfx in ["CREATE ROLE ", "ALTER ROLE ", "GRANT ", "REVOKE ", "SET ROLE "] {
                    if let Some(rest) = sql.strip_prefix(pfx) {
                        if pfx == "GRANT " || pfx == "REVOKE " {
                            assert!(rest.starts_with("fz_role_"), "{sql}");
                        } else {
                            assert!(rest.starts_with("fz_role_"), "{sql}");
                        }
                    }
                }
            }
            // SET ROLE is always a closed bracket within its group.
            if group[0].starts_with("SET ROLE ") {
                assert_eq!(group.len(), 3, "{group:?}");
                assert_eq!(group[1], "SELECT current_user;");
                assert_eq!(group[2], "RESET ROLE;");
            }
            // The coltab group is fully self-contained.
            if group[0].starts_with("CREATE TABLE fz_objt_") {
                assert_eq!(group.len(), 4, "{group:?}");
                assert!(group[3].starts_with("DROP TABLE fz_objt_"), "{group:?}");
            }
        }
        // Every shape fires under default weights in 900 groups.
        for p in SHAPES {
            assert!(prods.iter().any(|q| q == p), "shape {p} never fired");
        }
        for p in ["objddl", "objddl:comment:text", "objddl:comment:null",
                  "objddl:type:enum", "objddl:type:composite", "objddl:type:range"] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    /// Q6 opclass-ddl families: every sub-production fires and the SQL
    /// carries each chartered statement surface; Q6 groups are
    /// self-contained (each CREATE of a q-object has a same-group DROP,
    /// except the deliberate matched-error creates, which create nothing).
    #[test]
    fn q6_opclass_ddl_variety() {
        let w = WeightTable::parse(
            "objddl:opc=8,objddl:opcerr=4,objddl:am=6,objddl:cast=6,objddl:conv=5,\
             objddl:plang=4,objddl:xform=3,objddl:seclabel=3,objddl:opshell=4,objddl:aggmod=5",
        )
        .unwrap();
        let (groups, prods) = gen_groups(0x6ADD, 1500, &w);
        let all: Vec<String> = groups.iter().flatten().cloned().collect();
        let joined = all.join("\n");
        for frag in [
            "CREATE OPERATOR FAMILY fz_qopf_",
            "CREATE OPERATOR CLASS fz_qopc_",
            "FOR TYPE int4 USING btree",
            "FOR TYPE int4 USING hash",
            "FOR TYPE box USING gist",
            "FOR TYPE int4[] USING gin",
            "FOR TYPE point USING spgist",
            "FOR TYPE int4 USING brin",
            "FUNCTION 2 btint4sortsupport(internal)",
            "FUNCTION 4 btequalimage(oid)",
            "FUNCTION 1 btint48cmp(int4, int8)",
            "FUNCTION 2 hashint4extended(int4, int8)",
            "OPERATOR 15 <-> (box, point) FOR ORDER BY pg_catalog.float_ops",
            "FUNCTION 8 gist_box_distance(internal, box, smallint, oid, internal)",
            "STORAGE int4;",
            "brin_minmax_opcinfo(internal)",
            "spg_quad_config(internal, internal)",
            "SELECT amvalidate(oid) FROM pg_opclass WHERE opcname = 'fz_qopc_",
            "SELECT amvalidate(oid) FROM pg_opfamily WHERE opfname = 'fz_qopf_",
            "ALTER OPERATOR FAMILY fz_qopf_",
            " USING btree ADD ",
            " USING btree DROP ",
            " USING hash ADD OPERATOR 1 = (int4, int8)",
            "RENAME TO fz_qopc_",
            "RENAME TO fz_qopf_",
            "DROP OPERATOR CLASS fz_qopc_",
            "DROP OPERATOR FAMILY fz_qopf_",
            " USING hash CASCADE;",
            "(pk fz_qopc_",
            "USING gist (bx fz_qopc_",
            "ORDER BY bx <-> point(",
            "FUNCTION 1 length(text)",
            "DEFAULT FOR TYPE int4 USING btree",
            "ADD FUNCTION 1 now()",
            "DROP OPERATOR CLASS IF EXISTS fz_qnosuch_",
            "CREATE ACCESS METHOD fz_qam_",
            "TYPE INDEX HANDLER gisthandler",
            "TYPE TABLE HANDLER heap_tableam_handler",
            "TYPE INDEX HANDLER heap_tableam_handler",
            "TYPE TABLE HANDLER int4pl",
            "SET ACCESS METHOD heap;",
            "SET ACCESS METHOD DEFAULT;",
            "PARTITION BY RANGE (pk)",
            "COMMENT ON ACCESS METHOD fz_qam_",
            "DROP ACCESS METHOD fz_qam_",
            "CREATE TYPE fz_qen_",
            "WITH INOUT AS IMPLICIT;",
            "WITH INOUT;",
            "AS ASSIGNMENT;",
            "CREATE CAST (int4 AS bool) WITH INOUT;",
            "DROP CAST (int4 AS text);",
            "CREATE CONVERSION fz_qcv_",
            "CREATE DEFAULT CONVERSION fz_qcv_",
            "FROM iso8859_1_to_utf8;",
            "FROM int4pl;",
            "ALTER CONVERSION fz_qcv_",
            "DROP CONVERSION fz_qcv_",
            "CREATE TRUSTED LANGUAGE fz_qlg_",
            "INLINE plpgsql_inline_handler VALIDATOR plpgsql_validator",
            "CREATE LANGUAGE fz_qlg_",
            "DROP LANGUAGE fz_qlg_",
            "CREATE TRANSFORM FOR int4 LANGUAGE plpgsql (FROM SQL WITH FUNCTION int4pl(int4, int4));",
            "CREATE TRANSFORM FOR int4 LANGUAGE plpgsql (TO SQL WITH FUNCTION now());",
            "DROP TRANSFORM FOR int4 LANGUAGE plpgsql;",
            "SECURITY LABEL ON ",
            "SECURITY LABEL FOR selinux ON ",
            "COMMUTATOR = OPERATOR(public.%%>)",
            "NEGATOR = OPERATOR(public.>=%%)",
            "DROP OPERATOR <%% (int4, int4);",
            "DROP OPERATOR >=%% (int4, int4);",
            "FINALFUNC_MODIFY = READ_WRITE",
            "FINALFUNC_MODIFY = SHAREABLE",
            "MFINALFUNC_MODIFY = ",
            "FINALFUNC_MODIFY = SOMETIMES",
            "DROP AGGREGATE fz_qag_",
        ] {
            assert!(joined.contains(frag), "q6 flavor {frag:?} never generated");
        }
        for p in [
            "objddl:opc",
            "objddl:opc:bt",
            "objddl:opc:hash",
            "objddl:opc:gist",
            "objddl:opc:gin",
            "objddl:opc:spgist",
            "objddl:opc:brin",
            "objddl:opc:fam",
            "objddl:opc:sort",
            "objddl:opc:eqimg",
            "objddl:opc:xtype",
            "objddl:opc:index",
            "objddl:opc:rename",
            "objddl:opc:dist",
            "objddl:opcerr:sig",
            "objddl:opcerr:default",
            "objddl:opcerr:member",
            "objddl:opcerr:nosuch",
            "objddl:opcerr:incomplete",
            "objddl:am:index",
            "objddl:am:table",
            "objddl:am:part",
            "objddl:am:err",
            "objddl:cast:err",
            "objddl:cast:implicit",
            "objddl:cast:fn",
            "objddl:conv:err",
            "objddl:conv:default",
            "objddl:plang:err",
            "objddl:xform",
            "objddl:seclabel",
            "objddl:seclabel:provider",
            "objddl:opshell",
            "objddl:opshell:neg",
            "objddl:aggmod:plain",
            "objddl:aggmod:moving",
            "objddl:aggmod:err",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        // Self-containment: every successfully-created q-object is dropped
        // in the SAME group (error-fuel creates never create anything).
        for group in &groups {
            for (create, drop_pfx, err_ok) in [
                ("CREATE OPERATOR CLASS fz_qopc_", "DROP OPERATOR CLASS fz_qopc_", true),
                ("CREATE OPERATOR FAMILY fz_qopf_", "DROP OPERATOR FAMILY fz_qopf_", false),
                ("CREATE TABLE fz_qtab_", "DROP TABLE fz_qtab_", false),
                ("CREATE TYPE fz_qen_", "DROP TYPE fz_qen_", false),
                ("CREATE TRUSTED LANGUAGE fz_qlg_", "DROP LANGUAGE fz_qlg_", false),
            ] {
                let c = group.iter().filter(|s| s.starts_with(create)).count();
                let d = group.iter().filter(|s| s.starts_with(drop_pfx)).count();
                if err_ok {
                    // opcerr sig/default/incomplete arms: at most one create
                    // may be a deliberate failing statement.
                    assert!(c <= d + 1, "unbalanced {create}: {group:?}");
                } else {
                    assert_eq!(c, d, "unbalanced {create}: {group:?}");
                }
            }
            // GUC brackets balanced within the group.
            let sets = group.iter().filter(|s| s.starts_with("SET enable_seqscan")).count();
            let resets = group.iter().filter(|s| *s == "RESET enable_seqscan;").count();
            assert_eq!(sets, resets, "unbalanced seqscan bracket: {group:?}");
        }
    }

    #[test]
    fn names_are_never_reused_and_grants_are_acyclic() {
        let (groups, _) = gen_groups(7, 1200, &WeightTable::defaults());
        let mut created: Vec<String> = Vec::new();
        let mut role_order: Vec<String> = Vec::new();
        for group in &groups {
            for sql in group {
                let name = if let Some(r) = sql.strip_prefix("CREATE ROLE ") {
                    let n = r.split([' ', ';']).next().unwrap().to_string();
                    role_order.push(n.clone());
                    Some(n)
                } else if let Some(r) = sql.strip_prefix("CREATE TYPE ") {
                    Some(r.split([' ', ';']).next().unwrap().to_string())
                } else if let Some(r) = sql.strip_prefix("CREATE STATISTICS ") {
                    Some(r.split([' ', ';']).next().unwrap().to_string())
                } else {
                    None
                };
                if let Some(n) = name {
                    assert!(!created.contains(&n), "name reused: {n}");
                    created.push(n);
                }
                if let Some(r) = sql.strip_prefix("GRANT ") {
                    let granted = r.split(' ').next().unwrap();
                    let grantee =
                        r.split(" TO ").nth(1).unwrap().split([' ', ';']).next().unwrap();
                    let gi = role_order.iter().position(|x| x == granted).unwrap();
                    let gj = role_order.iter().position(|x| x == grantee).unwrap();
                    assert!(gi < gj, "grant does not flow old->new: {sql}");
                }
            }
        }
        assert!(created.iter().any(|n| n.starts_with("fz_role_")));
        assert!(created.iter().any(|n| n.starts_with("fz_ety_")));
        assert!(created.iter().any(|n| n.starts_with("fz_stx_")));
    }

    #[test]
    fn no_reference_after_drop() {
        // After a DROP of a role/type/statistics object, no later statement
        // references it (word-boundary match), mirroring the ddl module's
        // ordering-hazard gate. IF EXISTS drops of already-dead objects
        // cannot occur because the model marks objects dead exactly once.
        let (groups, _) = gen_groups(21, 1200, &WeightTable::defaults());
        let stmts: Vec<String> = groups.into_iter().flatten().collect();
        let mut dropped: Vec<(usize, String)> = Vec::new();
        for (i, sql) in stmts.iter().enumerate() {
            for pfx in ["DROP ROLE ", "DROP TYPE ", "DROP STATISTICS "] {
                if let Some(rest) = sql.strip_prefix(pfx) {
                    let name =
                        rest.trim_start_matches("IF EXISTS ").trim_end_matches(';').to_string();
                    dropped.push((i, name));
                }
            }
        }
        assert!(!dropped.is_empty(), "no drops in 1200 groups");
        for (i, name) in &dropped {
            for later in &stmts[i + 1..] {
                let clean = !later.match_indices(name.as_str()).any(|(p, _)| {
                    later[p + name.len()..]
                        .chars()
                        .next()
                        .is_none_or(|ch| !ch.is_alphanumeric() && ch != '_')
                });
                assert!(clean, "statement after drop references {name}: {later}");
            }
        }
    }

    #[test]
    fn objddl_is_deterministic() {
        let w = WeightTable::defaults();
        let (a, _) = gen_groups(5, 120, &w);
        let (b, _) = gen_groups(5, 120, &w);
        assert_eq!(a, b);
        let (c, _) = gen_groups(6, 120, &w);
        assert_ne!(a, c);
    }
}
