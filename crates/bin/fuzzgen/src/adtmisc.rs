//! A3 adt-misc breadth module: four backend/utils/adt long-tail families
//! that the round-3 gap report (gap-report-005) ranks under the adt
//! rollup — ACL/privilege functions (`acl.c`, pairs with the objddl role
//! machinery), the varbit + varlena long tail (`varbit.c`, `varlena.c`,
//! `text_format`), multirange completion (`multirangetypes.c`), and the
//! float/numeric edge matrix (`float.c`, `numeric.c`, `cash.c`, incl. the
//! `cash_out` / `width_bucket` / gcd-lcm-factorial gap entries).
//!
//! Everything is emitted as `StmtKind::Raw` over literal inputs (the
//! expr/rich productions already carry the column-borne surface), so the
//! AST scoping checker has nothing to check and the differ's multiset
//! compare covers the few SRF shapes (aclexplode, unnest(multirange)).
//!
//! Validity/safety disciplines (hand-verified on both engines 2026-08-11;
//! every family produced byte-identical psql output on C 18.4 and pgrust
//! @ this branch — scratchpad hv1..hv6 legs):
//!   - scalar float/numeric outputs are always cast `::text`: the
//!     campaign's float-ulp rulings apply to aggregate plan-order only,
//!     scalar arithmetic must compare byte-identical (B1 scope), and the
//!     text cast keeps the wire compare exact on both the value and the
//!     type oid;
//!   - GRANT/REVOKE always forms a self-contained bracket in one group —
//!     GRANT, privilege probe, REVOKE — so fuzz roles never carry
//!     object-level grants past their group (the objddl DROP ROLE
//!     invariant "roles hold no object grants" stays intact) and no other
//!     module can interleave;
//!   - the module reads objddl's live-role pool (`Gen::obj`) and never
//!     mutates it; with no live role, role-needing shapes fall back to
//!     probe shapes (the fired production records what actually ran);
//!   - the SET ROLE denial bracket is closed [SET ROLE r; probe;
//!     RESET ROLE] and its write probes are WHERE-false UPDATE/DELETE:
//!     privilege checks fire before execution, so a denied role raises
//!     42501 while a SUPERUSER fuzz role touches zero rows — either way
//!     no state mutation, matched on both sides;
//!   - aclitem machinery only ever names role oid 10 (the bootstrap
//!     superuser, a fixed oid on both engines) or oid 0 (PUBLIC): fuzz
//!     role oids differ across engines and must never reach an acl
//!     output;
//!   - deliberate error fuel (bogus privilege strings, invalid acldefault
//!     kinds, out-of-range get_bit/get_byte, bad base64/hex, domain-error
//!     trig/log, div-mod by zero, overflow gcd/factorial/money) rides a
//!     single `adtm:ok`/`adtm:err` weight pair, biased away from
//!     both-sides-error per the findings-budget rule;
//!   - no nondeterministic function is ever emitted (no now(), random(),
//!     gen_random_uuid()); `has_database_privilege` spells the database
//!     as `current_database()` so the statement text is rig-independent.

use crate::catalog::Table;
use crate::stmt::{Gen, StmtKind};

/// Statement shapes (top-level weighted pick).
const SHAPES: &[&str] = &[
    "adtm:acl:probe",
    "adtm:acl:aclitem",
    "adtm:acl:grant",
    "adtm:acl:denial",
    "adtm:bit",
    "adtm:vltext",
    "adtm:vlbytea",
    "adtm:mr",
    "adtm:num",
    "adtm:money",
    // Q2 expr-misc-adt breadth (sql-reachable-queue chunk, 340 fns):
    // pseudotype IO, binary_upgrade ereport stubs, ruleutils deparse over
    // fuzz-created objects, record/tid/oidvector/aclitem comparisons,
    // uuid family, hashed IN lists, enum DDL brackets, misc probe fns,
    // and hand-verified parser/executor error paths. Every family was
    // verified byte-identical on both engines (scratchpad hv-misc/hv2
    // legs 2026-08-11) before banking; the xml family (schema_to_xml &
    // co) is DELIBERATELY absent — the pinned C reference is built
    // without libxml while pgrust implements XML natively, so that
    // surface is not differential-comparable on this rig.
    "adtm:pseudo",
    "adtm:binupg",
    "adtm:deparse",
    "adtm:rec",
    "adtm:uuid",
    "adtm:inlist",
    "adtm:enum",
    "adtm:probe2",
    "adtm:errpath",
    // Q5 acl-grant breadth (sql-reachable-queue chunk, 142 fns): the full
    // has_*_privilege arity matrix (name/oid crossings via role oid 10 and
    // reg* casts), per-object-class grant brackets (language, sequence,
    // large object, foreign-data wrapper + server, parameter), GRANTED BY
    // spellings, DROP OWNED / REASSIGN OWNED brackets, row-level-security
    // policy brackets, and the aclitem long tail. Hand-verified
    // byte-identical on both engines 2026-08-11 (scratch deck hv6) before
    // banking; extends the standing adtm:acl families — same invariants
    // (self-contained brackets, fuzz roles only, oid 10/PUBLIC only in
    // acl outputs).
    "adtm:acl:arity",
    "adtm:acl:objx",
    "adtm:acl:grantby",
    "adtm:acl:owned",
    "adtm:acl:rls",
    "adtm:acl:aclx",
    // LD10 adt-misc-residue drain (line-drain-queue chunk, 297 fns /
    // 2,126 hollow lines): numutils safe-parse arms via
    // pg_input_is_valid/pg_input_error_info (error detail without
    // erroring), the SIMILAR TO escape machinery + regexp flag parsing,
    // advisory locks feeding pg_lock_status, pg_stat_activity /
    // pg_hba_file_rules deterministic projections, the amutils property
    // matrix, inet/cidr abbreviated forms + operator tail,
    // format_type_extended typmods, oracle_compat trim/pad tail,
    // levenshtein cost variants, parse_ident, varbit typmod/shift edges,
    // cash_in/cash_out signs and separators, the SP-GiST box operator
    // matrix (geo_spgist.c), expandedrecord plpgsql assignment, and the
    // pg_stat_get_progress_info command sweep.
    "adtm:strsafe",
    "adtm:regex2",
    "adtm:locks",
    "adtm:activity",
    "adtm:hba",
    "adtm:amprop",
    "adtm:inet2",
    "adtm:fmtty",
    "adtm:trim2",
    "adtm:lev",
    "adtm:ident",
    "adtm:bit2",
    "adtm:cash2",
    "adtm:spgbox",
    "adtm:xrec",
    "adtm:progress",
    // Q7 expr-misc-adt + expr-strings breadth (sql-reachable-queue
    // GEN-GAP-SIBLING chunks, 196 + 90 remaining fns after gap-010).
    // Every family hand-verified byte-identical on both engines
    // 2026-08-12 (scratch decks deck-misc1/pseudo/deparse/ri/strings;
    // A = own cpg-ref REL_18_3 build, B = pgrust origin/main@cc3b6bc550e)
    // EXCEPT adtm:xml: the pinned C reference is built without libxml
    // (every xml.c entry errors "unsupported XML feature") while pgrust
    // implements XML natively — the family exists for COVERAGE ONLY and
    // must run with --weight adtm:xml=0 on every differential leg.
    "adtm:misc2",
    "adtm:errhint",
    "adtm:regex3",
    "adtm:enum2",
    "adtm:rec2",
    "adtm:uuid2",
    "adtm:bit3",
    "adtm:poly",
    "adtm:toastslice",
    "adtm:ri",
    "adtm:part3",
    "adtm:deparse3",
    "adtm:pseudo3",
    "adtm:tid3",
    "adtm:xml",
    "adtm:char2",
    "adtm:bpchar",
    "adtm:nametext",
    "adtm:strfns",
    "adtm:byteax",
    "adtm:patidx",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_adtmisc_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtmisc");
    match g.weights.pick(g.rng, SHAPES) {
        "adtm:acl:probe" => gen_acl_probe(g),
        "adtm:acl:aclitem" => gen_aclitem(g),
        "adtm:acl:grant" => gen_acl_grant(g),
        "adtm:acl:denial" => gen_acl_denial(g),
        "adtm:bit" => gen_bit(g),
        "adtm:vltext" => gen_vltext(g),
        "adtm:vlbytea" => gen_vlbytea(g),
        "adtm:mr" => gen_multirange(g),
        "adtm:num" => gen_num_edges(g),
        "adtm:money" => gen_money(g),
        "adtm:pseudo" => gen_pseudo(g),
        "adtm:binupg" => gen_binupg(g),
        "adtm:deparse" => gen_deparse(g),
        "adtm:rec" => gen_rec(g),
        "adtm:uuid" => gen_uuid(g),
        "adtm:inlist" => gen_inlist(g),
        "adtm:enum" => gen_enum(g),
        "adtm:probe2" => gen_probe2(g),
        "adtm:errpath" => gen_errpath(g),
        "adtm:acl:arity" => gen_acl_arity(g),
        "adtm:acl:objx" => gen_acl_objx(g),
        "adtm:acl:grantby" => gen_acl_grantby(g),
        "adtm:acl:owned" => gen_acl_owned(g),
        "adtm:acl:rls" => gen_acl_rls(g),
        "adtm:acl:aclx" => gen_acl_aclx(g),
        "adtm:strsafe" => gen_strsafe(g),
        "adtm:regex2" => gen_regex2(g),
        "adtm:locks" => gen_locks(g),
        "adtm:activity" => gen_activity(g),
        "adtm:hba" => gen_hba(g),
        "adtm:amprop" => gen_amprop(g),
        "adtm:inet2" => gen_inet2(g),
        "adtm:fmtty" => gen_fmtty(g),
        "adtm:trim2" => gen_trim2(g),
        "adtm:lev" => gen_lev(g),
        "adtm:ident" => gen_ident(g),
        "adtm:bit2" => gen_bit2(g),
        "adtm:cash2" => gen_cash2(g),
        "adtm:spgbox" => gen_spgbox(g),
        "adtm:xrec" => gen_xrec(g),
        "adtm:progress" => gen_progress(g),
        "adtm:misc2" => gen_misc2(g),
        "adtm:errhint" => gen_errhint(g),
        "adtm:regex3" => gen_regex3(g),
        "adtm:enum2" => gen_enum2(g),
        "adtm:rec2" => gen_rec2(g),
        "adtm:uuid2" => gen_uuid2(g),
        "adtm:bit3" => gen_bit3(g),
        "adtm:poly" => gen_poly(g),
        "adtm:toastslice" => gen_toastslice(g),
        "adtm:ri" => gen_ri(g),
        "adtm:part3" => gen_part3(g),
        "adtm:deparse3" => gen_deparse3(g),
        "adtm:pseudo3" => gen_pseudo3(g),
        "adtm:tid3" => gen_tid3(g),
        "adtm:xml" => gen_xmlfam(g),
        "adtm:char2" => gen_char2(g),
        "adtm:bpchar" => gen_bpchar(g),
        "adtm:nametext" => gen_nametext(g),
        "adtm:strfns" => gen_strfns(g),
        "adtm:byteax" => gen_byteax(g),
        "adtm:patidx" => gen_patidx(g),
        other => unreachable!("unknown adtmisc shape {other}"),
    }
}

/// One-knob error-fuel bias: err arms host deliberate matched errors.
fn err_arm(g: &mut Gen) -> bool {
    if g.weights.pick(g.rng, &["adtm:ok", "adtm:err"]) == "adtm:err" {
        g.fire("adtm:err");
        true
    } else {
        false
    }
}

fn pick_str<'x>(g: &mut Gen, opts: &[&'x str]) -> &'x str {
    opts[g.rng.below_usize(opts.len())]
}

fn raw(sql: String) -> Vec<StmtKind> {
    vec![StmtKind::Raw(sql)]
}

/// Live fuzz-role names from the objddl pool (never mutated here).
fn live_roles(g: &Gen) -> Vec<String> {
    g.obj
        .roles
        .iter()
        .filter(|r| r.live)
        .map(|r| r.name.clone())
        .collect()
}

/// Catalog tables whose attnums are stable (never ALTERed by any module).
fn stable_tables<'a>(g: &Gen<'a>) -> Vec<&'a Table> {
    g.catalog
        .tables
        .iter()
        .filter(|t| !t.name.starts_with("fz_ddl_") && !t.name.starts_with("fz_part_"))
        .collect()
}

// ------------------------------------------------------------- family 1 ----
// ACL / privilege functions (acl.c): has_*_privilege across name/oid
// arities, pg_has_role, and the aclitem machinery.

const TBL_PRIVS: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN",
];
const BUILTIN_FUNCS: &[&str] =
    &["abs(int4)", "length(text)", "lower(text)", "upper(text)", "int4pl(int4,int4)"];

/// Privilege-string list: 1-2 privileges, optional WITH GRANT OPTION
/// suffix; err arm yields an unrecognized privilege name (22023 fuel).
fn priv_list(g: &mut Gen, pool: &[&str], err: bool) -> String {
    if err {
        return "BOGUS".to_string();
    }
    let mut s = pick_str(g, pool).to_string();
    if g.rng.chance(1, 3) {
        let extra = pick_str(g, pool);
        s.push_str(", ");
        s.push_str(extra);
    }
    if g.rng.chance(1, 4) {
        s.push_str(" WITH GRANT OPTION");
    }
    s
}

/// Role argument for a has_*_privilege probe: a live fuzz role (quoted),
/// the current_user keyword, or (err arm) a missing role.
fn role_arg(g: &mut Gen, err: bool) -> String {
    if err {
        return "'fz_no_such_role'".to_string();
    }
    let roles = live_roles(g);
    if !roles.is_empty() && g.rng.chance(1, 2) {
        format!("'{}'", roles[g.rng.below_usize(roles.len())])
    } else {
        "current_user".to_string()
    }
}

fn gen_acl_probe(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:acl:probe");
    let err = err_arm(g);
    let t = g.pick_table().name.clone();
    let sql = match g.rng.below(10) {
        0 => {
            let p = priv_list(g, TBL_PRIVS, err);
            format!("SELECT has_table_privilege('{}', '{}');", t, p)
        }
        1 => {
            let who = role_arg(g, err);
            let p = priv_list(g, TBL_PRIVS, false);
            format!("SELECT has_table_privilege({}, '{}', '{}');", who, t, p)
        }
        2 => {
            // regclass (oid) arity.
            let p = priv_list(g, TBL_PRIVS, err);
            if g.rng.chance(1, 2) {
                format!("SELECT has_table_privilege('{}'::regclass, '{}');", t, p)
            } else {
                format!(
                    "SELECT has_table_privilege(current_user, '{}'::regclass, '{}');",
                    t, p
                )
            }
        }
        3 => {
            let stable = stable_tables(g);
            let st = &stable[g.rng.below_usize(stable.len())];
            let c = st.columns[g.rng.below_usize(st.columns.len())].name.clone();
            let p = pick_str(g, &["SELECT", "INSERT", "UPDATE", "REFERENCES"]);
            let who = role_arg(g, err);
            if g.rng.chance(1, 3) {
                format!(
                    "SELECT has_column_privilege({}, '{}', '{}', '{}');",
                    who, st.name, c, p
                )
            } else {
                format!("SELECT has_column_privilege('{}', '{}', '{}');", st.name, c, p)
            }
        }
        4 => {
            // attnum arity over stable tables only (attnums never shift).
            let stable = stable_tables(g);
            let st = &stable[g.rng.below_usize(stable.len())];
            let attnum: i64 = if err {
                *[0, -1, 999].get(g.rng.below_usize(3)).unwrap()
            } else {
                1 + g.rng.below(st.columns.len() as u64) as i64
            };
            let p = pick_str(g, &["SELECT", "UPDATE"]);
            format!(
                "SELECT has_column_privilege('{}', {}::int2, '{}');",
                st.name, attnum, p
            )
        }
        5 => {
            let schema = if err { "fz_no_such_schema" } else { pick_str(g, &["public", "pg_catalog", "information_schema"]) };
            let p = priv_list(g, &["CREATE", "USAGE"], false);
            if g.rng.chance(1, 3) {
                format!(
                    "SELECT has_schema_privilege(current_user, '{}', '{}');",
                    schema, p
                )
            } else {
                format!("SELECT has_schema_privilege('{}', '{}');", schema, p)
            }
        }
        6 => {
            let p = priv_list(g, &["CREATE", "CONNECT", "TEMPORARY", "TEMP"], err);
            if g.rng.chance(1, 3) {
                format!(
                    "SELECT has_database_privilege(current_user, current_database(), '{}');",
                    p
                )
            } else {
                format!("SELECT has_database_privilege(current_database(), '{}');", p)
            }
        }
        7 => {
            let f = pick_str(g, BUILTIN_FUNCS);
            let p = if err { "SELECT" } else { "EXECUTE" };
            if g.rng.chance(1, 3) {
                format!("SELECT has_function_privilege(current_user, '{}', '{}');", f, p)
            } else {
                format!("SELECT has_function_privilege('{}', '{}');", f, p)
            }
        }
        8 => {
            let who = role_arg(g, err);
            let p = pick_str(
                g,
                &["USAGE", "MEMBER", "SET", "MEMBER WITH ADMIN OPTION", "USAGE, SET"],
            );
            if g.rng.chance(1, 3) {
                format!("SELECT pg_has_role(current_user, {}, '{}');", who, p)
            } else {
                format!("SELECT pg_has_role({}, '{}');", who, p)
            }
        }
        _ => match g.rng.below(4) {
            0 => format!(
                "SELECT has_any_column_privilege('{}', '{}');",
                t,
                pick_str(g, &["SELECT", "UPDATE", "INSERT"])
            ),
            1 => {
                let p = pick_str(g, &["SET", "ALTER SYSTEM", "SET, ALTER SYSTEM"]);
                if g.rng.chance(1, 2) {
                    format!("SELECT has_parameter_privilege(current_user, 'work_mem', '{}');", p)
                } else {
                    format!("SELECT has_parameter_privilege('work_mem', '{}');", p)
                }
            }
            2 => format!(
                "SELECT has_tablespace_privilege('pg_default', '{}');",
                if err { "SELECT" } else { "CREATE" }
            ),
            _ => format!(
                "SELECT has_type_privilege('{}', 'USAGE');",
                pick_str(g, &["int4", "text", "numeric", "jsonb"])
            ),
        },
    };
    raw(sql)
}

fn gen_aclitem(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:acl:aclitem");
    let err = err_arm(g);
    // acldefault object kinds — valid kinds only: an invalid kind raises
    // XX000 in C Postgres itself (elog, not ereport), and XX000 traffic is
    // triage noise under the panic-fatality taxonomy even when matched.
    // Err fuel rides the makeaclitem/aclitemin arms instead.
    let kinds = ["c", "r", "s", "d", "f", "l", "L", "n", "t", "T"];
    let kind = pick_str(g, &kinds);
    let sql = match g.rng.below(6) {
        0 => format!("SELECT acldefault('{}', 10)::text;", kind),
        1 => {
            // SRF expansion: multiset compare covers the rowset.
            format!(
                "SELECT a.grantor, a.grantee, a.privilege_type, a.is_grantable FROM aclexplode(acldefault('{}', 10)) a;",
                kind
            )
        }
        2 => {
            let p = priv_list(g, TBL_PRIVS, err);
            let grantee = if g.rng.chance(1, 4) { 0 } else { 10 };
            let grantable = if g.rng.chance(1, 2) { "true" } else { "false" };
            format!(
                "SELECT makeaclitem({}, 10, '{}', {})::text;",
                grantee, p, grantable
            )
        }
        3 => format!(
            "SELECT makeaclitem(10, 10, 'SELECT', true) = makeaclitem(10, 10, 'SELECT', {});",
            if g.rng.chance(1, 2) { "true" } else { "false" }
        ),
        4 => {
            let p = pick_str(g, &["SELECT", "UPDATE", "DELETE"]);
            format!(
                "SELECT aclcontains(acldefault('r', 10), makeaclitem(10, 10, '{}', false));",
                p
            )
        }
        _ => {
            let lit = if err { "not-an-aclitem" } else { pick_str(g, &["postgres=r/postgres", "postgres=arwdDxtm/postgres", "=r/postgres"]) };
            format!("SELECT aclitemin('{}')::text;", lit)
        }
    };
    raw(sql)
}

fn gen_acl_grant(g: &mut Gen) -> Vec<StmtKind> {
    let roles = live_roles(g);
    if roles.is_empty() {
        g.fire("adtm:fallback:probe");
        return gen_acl_probe(g);
    }
    g.fire("adtm:acl:grant");
    let r = roles[g.rng.below_usize(roles.len())].clone();
    let t = g.pick_table().name.clone();
    match g.rng.below(6) {
        0 => {
            // Table-level bracket.
            let p = priv_list(g, TBL_PRIVS, false);
            let probe_p = pick_str(g, TBL_PRIVS);
            vec![
                StmtKind::Raw(format!("GRANT {} ON {} TO {};", p, t, r)),
                StmtKind::Raw(format!(
                    "SELECT has_table_privilege('{}', '{}', '{}');",
                    r, t, probe_p
                )),
                StmtKind::Raw(format!("REVOKE ALL PRIVILEGES ON {} FROM {};", t, r)),
            ]
        }
        1 => {
            // Column-level bracket over a stable table.
            let stable = stable_tables(g);
            let st = &stable[g.rng.below_usize(stable.len())];
            let stn = st.name.clone();
            let c1 = st.columns[g.rng.below_usize(st.columns.len())].name.clone();
            let c2 = st.columns[g.rng.below_usize(st.columns.len())].name.clone();
            let go = if g.rng.chance(1, 3) { " WITH GRANT OPTION" } else { "" };
            let probe_p = if g.rng.chance(1, 3) { "SELECT WITH GRANT OPTION" } else { "SELECT" };
            let revoke = if g.rng.chance(1, 3) && !go.is_empty() {
                format!(
                    "REVOKE GRANT OPTION FOR SELECT ({}) ON {} FROM {} CASCADE;",
                    c1, stn, r
                )
            } else {
                format!("REVOKE ALL PRIVILEGES ON {} FROM {};", stn, r)
            };
            let mut out = vec![
                StmtKind::Raw(format!(
                    "GRANT SELECT ({}, {}), UPDATE ({}) ON {} TO {}{};",
                    c1, c2, c1, stn, r, go
                )),
                StmtKind::Raw(format!(
                    "SELECT has_column_privilege('{}', '{}', '{}', '{}');",
                    r, stn, c1, probe_p
                )),
                StmtKind::Raw(revoke),
            ];
            // A REVOKE GRANT OPTION leaves the base grants; close them out.
            if out[2].to_sql().starts_with("REVOKE GRANT OPTION") {
                out.push(StmtKind::Raw(format!(
                    "REVOKE ALL PRIVILEGES ON {} FROM {};",
                    stn, r
                )));
            }
            out
        }
        2 => {
            // Schema bracket.
            let p = priv_list(g, &["USAGE", "CREATE"], false);
            vec![
                StmtKind::Raw(format!("GRANT {} ON SCHEMA public TO {};", p, r)),
                StmtKind::Raw(format!(
                    "SELECT has_schema_privilege('{}', 'public', 'CREATE');",
                    r
                )),
                StmtKind::Raw(format!("REVOKE ALL ON SCHEMA public FROM {};", r)),
            ]
        }
        3 => {
            // Builtin-function bracket.
            let f = pick_str(g, BUILTIN_FUNCS);
            vec![
                StmtKind::Raw(format!("GRANT EXECUTE ON FUNCTION {} TO {};", f, r)),
                StmtKind::Raw(format!(
                    "SELECT has_function_privilege('{}', '{}', 'EXECUTE');",
                    r, f
                )),
                StmtKind::Raw(format!("REVOKE EXECUTE ON FUNCTION {} FROM {};", f, r)),
            ]
        }
        4 => {
            // ALTER DEFAULT PRIVILEGES bracket (SetDefaultACL /
            // ExecAlterDefaultPrivilegesStmt). GRANT and REVOKE mirror the
            // same IN SCHEMA clause so the default-acl entry is removed.
            let in_schema = if g.rng.chance(1, 2) { " IN SCHEMA public" } else { "" };
            let (obj, p) = *pick_str2(
                g,
                &[("TABLES", "SELECT"), ("TABLES", "SELECT, INSERT"), ("SEQUENCES", "USAGE"), ("FUNCTIONS", "EXECUTE"), ("TYPES", "USAGE")],
            );
            vec![
                StmtKind::Raw(format!(
                    "ALTER DEFAULT PRIVILEGES{} GRANT {} ON {} TO {};",
                    in_schema, p, obj, r
                )),
                StmtKind::Raw("SELECT count(*) FROM pg_default_acl;".to_string()),
                StmtKind::Raw(format!(
                    "ALTER DEFAULT PRIVILEGES{} REVOKE {} ON {} FROM {};",
                    in_schema, p, obj, r
                )),
            ]
        }
        _ => {
            if err_arm(g) {
                // Missing grantee: matched 42704.
                return raw(format!("GRANT SELECT ON {} TO fz_no_such_role;", t));
            }
            // PUBLIC bracket.
            vec![
                StmtKind::Raw(format!("GRANT SELECT ON {} TO PUBLIC;", t)),
                StmtKind::Raw(format!(
                    "SELECT has_table_privilege('{}', '{}', 'SELECT');",
                    r, t
                )),
                StmtKind::Raw(format!("REVOKE SELECT ON {} FROM PUBLIC;", t)),
            ]
        }
    }
}

/// Typed pair pick (rng-uniform).
fn pick_str2<'x, T>(g: &mut Gen, opts: &'x [T]) -> &'x T {
    &opts[g.rng.below_usize(opts.len())]
}

fn gen_acl_denial(g: &mut Gen) -> Vec<StmtKind> {
    let roles = live_roles(g);
    if roles.is_empty() {
        g.fire("adtm:fallback:probe");
        return gen_acl_probe(g);
    }
    g.fire("adtm:acl:denial");
    let r = roles[g.rng.below_usize(roles.len())].clone();
    let t = g.pick_table();
    let tname = t.name.clone();
    let col = t.columns[0].name.clone();
    // Privilege checks fire before execution: denied roles raise 42501,
    // privileged ones touch zero rows (WHERE false) — no mutation either
    // way, matched on both sides. The runner's setup recreates schema
    // public with a bare ACL (no PUBLIC USAGE), so a bare bracket denies
    // at the SCHEMA aclcheck (42P01 unknown-relation); granting the role
    // schema USAGE first pushes the denial down to the TABLE aclcheck
    // (42501) — both halves of aclcheck_error, split half and half.
    let probe = match g.rng.below(3) {
        0 => format!("SELECT count(*) FROM {};", tname),
        1 => format!("UPDATE {} SET {} = {} WHERE false;", tname, col, col),
        _ => format!("DELETE FROM {} WHERE false;", tname),
    };
    if g.rng.chance(1, 2) {
        g.fire("adtm:acl:denial:table");
        vec![
            StmtKind::Raw(format!("GRANT USAGE ON SCHEMA public TO {};", r)),
            StmtKind::Raw(format!("SET ROLE {};", r)),
            StmtKind::Raw(probe),
            StmtKind::Raw("RESET ROLE;".to_string()),
            StmtKind::Raw(format!("REVOKE USAGE ON SCHEMA public FROM {};", r)),
        ]
    } else {
        vec![
            StmtKind::Raw(format!("SET ROLE {};", r)),
            StmtKind::Raw(probe),
            StmtKind::Raw("RESET ROLE;".to_string()),
        ]
    }
}

// --------------------------------------------------------- Q5 acl-grant ----
// Full arity matrix + object-class breadth for acl.c/aclchk.c. Fixed
// names throughout (fz_adtm_seq/fdw/srv/rls, large-object oid 543210) so
// brackets are idempotent under shared-cluster corpus rigs; every acl
// output still only ever names role oid 10 or PUBLIC (standing law).

/// Full has_*_privilege arity matrix: oid arities via role oid 10 (the
/// bootstrap superuser, fixed on both engines), reg* casts, and catalog
/// subquery oids. All raw-compared booleans (hand-verified).
fn gen_acl_arity(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:acl:arity");
    let t = g.pick_table().name.clone();
    let sql = match g.rng.below(10) {
        0 => format!(
            "SELECT has_table_privilege(10, '{}'::regclass, '{}'), has_table_privilege('{}'::regclass, 'SELECT WITH GRANT OPTION');",
            t,
            pick_str(g, TBL_PRIVS),
            t
        ),
        1 => {
            let stable = stable_tables(g);
            let st = &stable[g.rng.below_usize(stable.len())];
            let c = st.columns[g.rng.below_usize(st.columns.len())].name.clone();
            let attnum = 1 + g.rng.below(st.columns.len() as u64);
            format!(
                "SELECT has_column_privilege(10, '{}'::regclass, '{}', 'SELECT'), has_column_privilege(10, '{}'::regclass, {}::int2, 'UPDATE'), has_column_privilege('{}'::regclass, {}::int2, 'INSERT');",
                st.name, c, st.name, attnum, st.name, attnum
            )
        }
        2 => format!(
            "SELECT has_any_column_privilege(10, '{}'::regclass, 'SELECT'), has_any_column_privilege('{}'::regclass, 'UPDATE');",
            t, t
        ),
        3 => format!(
            "SELECT has_schema_privilege(10, (SELECT oid FROM pg_namespace WHERE nspname = 'public'), 'CREATE'), has_schema_privilege((SELECT oid FROM pg_namespace WHERE nspname = '{}'), 'USAGE');",
            pick_str(g, &["pg_catalog", "public", "information_schema"])
        ),
        4 => format!(
            "SELECT has_database_privilege(10, (SELECT oid FROM pg_database WHERE datname = current_database()), '{}'), has_database_privilege((SELECT oid FROM pg_database WHERE datname = current_database()), 'TEMP');",
            pick_str(g, &["CONNECT", "CREATE", "TEMPORARY"])
        ),
        5 => {
            let f = pick_str(g, BUILTIN_FUNCS);
            format!(
                "SELECT has_function_privilege(10, '{}'::regprocedure, 'EXECUTE'), has_function_privilege('{}'::regprocedure, 'EXECUTE WITH GRANT OPTION');",
                f, f
            )
        }
        6 => format!(
            "SELECT has_type_privilege(10, '{}'::regtype, 'USAGE'), has_type_privilege('{}'::regtype, 'USAGE');",
            pick_str(g, &["int4", "text", "numeric"]),
            pick_str(g, &["jsonb", "uuid", "int8"])
        ),
        7 => "SELECT has_tablespace_privilege(10, (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'), 'CREATE');".to_string(),
        8 => "SELECT pg_has_role(10, 10, 'USAGE'), pg_has_role(10, 'MEMBER');".to_string(),
        _ => format!(
            "SELECT has_parameter_privilege(10, 'work_mem', 'SET'), has_parameter_privilege('{}', '{}');",
            pick_str(g, &["search_path", "work_mem", "shared_buffers"]),
            pick_str(g, &["SET", "ALTER SYSTEM"])
        ),
    };
    raw(sql)
}

/// Per-object-class grant brackets: language, sequence, large object,
/// foreign-data wrapper + server, parameter. Each bracket is one group
/// (create if needed, grant, has_*_privilege probes, revoke, drop).
fn gen_acl_objx(g: &mut Gen) -> Vec<StmtKind> {
    let roles = live_roles(g);
    if roles.is_empty() {
        g.fire("adtm:fallback:probe");
        return gen_acl_arity(g);
    }
    g.fire("adtm:acl:objx");
    let r = roles[g.rng.below_usize(roles.len())].clone();
    match g.rng.below(5) {
        0 => {
            // Language bracket (builtin languages only — never dropped).
            let lang = pick_str(g, &["sql", "plpgsql"]);
            vec![
                StmtKind::Raw(format!("GRANT USAGE ON LANGUAGE {} TO {};", lang, r)),
                StmtKind::Raw(format!(
                    "SELECT has_language_privilege('{}', '{}', 'USAGE'), has_language_privilege('{}', (SELECT oid FROM pg_language WHERE lanname = '{}'), 'USAGE'), has_language_privilege('c', 'USAGE');",
                    r, lang, r, lang
                )),
                StmtKind::Raw(format!("REVOKE ALL PRIVILEGES ON LANGUAGE {} FROM {};", lang, r)),
            ]
        }
        1 => {
            // Sequence bracket: all six has_sequence_privilege arities.
            let go = if g.rng.chance(1, 3) { " WITH GRANT OPTION" } else { "" };
            vec![
                StmtKind::Raw("DROP SEQUENCE IF EXISTS fz_adtm_seq;".to_string()),
                StmtKind::Raw("CREATE SEQUENCE fz_adtm_seq;".to_string()),
                StmtKind::Raw(format!(
                    "GRANT USAGE, SELECT ON SEQUENCE fz_adtm_seq TO {}{};",
                    r, go
                )),
                StmtKind::Raw(format!(
                    "SELECT has_sequence_privilege('{}', 'fz_adtm_seq', 'USAGE'), has_sequence_privilege('{}', 'fz_adtm_seq'::regclass, 'SELECT'), has_sequence_privilege('fz_adtm_seq', 'UPDATE'), has_sequence_privilege(10, 'fz_adtm_seq'::regclass, 'UPDATE');",
                    r, r
                )),
                StmtKind::Raw(format!(
                    "SELECT has_sequence_privilege('{}', '{}', 'USAGE');",
                    r,
                    g.pick_table().name
                )),
                StmtKind::Raw(format!(
                    "REVOKE ALL PRIVILEGES ON SEQUENCE fz_adtm_seq FROM {};",
                    r
                )),
                StmtKind::Raw("DROP SEQUENCE fz_adtm_seq;".to_string()),
            ]
        }
        2 => {
            // Large-object bracket: fixed oid, guarded pre-unlink.
            vec![
                StmtKind::Raw("SELECT lo_unlink(oid) FROM pg_largeobject_metadata WHERE oid = 543210;".to_string()),
                StmtKind::Raw("SELECT lo_create(543210);".to_string()),
                StmtKind::Raw(format!(
                    "GRANT SELECT, UPDATE ON LARGE OBJECT 543210 TO {};",
                    r
                )),
                StmtKind::Raw(format!(
                    "SELECT has_largeobject_privilege('{}', 543210, 'SELECT'), has_largeobject_privilege(10, 543210, 'UPDATE'), has_largeobject_privilege(543210, 'SELECT WITH GRANT OPTION');",
                    r
                )),
                StmtKind::Raw(format!(
                    "REVOKE ALL PRIVILEGES ON LARGE OBJECT 543210 FROM {};",
                    r
                )),
                StmtKind::Raw("SELECT lo_unlink(543210);".to_string()),
                StmtKind::Raw("SELECT has_largeobject_privilege(543210, 'SELECT');".to_string()),
            ]
        }
        3 => {
            // FDW + server bracket.
            let go = if g.rng.chance(1, 3) { " WITH GRANT OPTION" } else { "" };
            vec![
                StmtKind::Raw("DROP SERVER IF EXISTS fz_adtm_srv CASCADE;".to_string()),
                StmtKind::Raw("DROP FOREIGN DATA WRAPPER IF EXISTS fz_adtm_fdw CASCADE;".to_string()),
                StmtKind::Raw("CREATE FOREIGN DATA WRAPPER fz_adtm_fdw;".to_string()),
                StmtKind::Raw("CREATE SERVER fz_adtm_srv FOREIGN DATA WRAPPER fz_adtm_fdw;".to_string()),
                StmtKind::Raw(format!(
                    "GRANT USAGE ON FOREIGN DATA WRAPPER fz_adtm_fdw TO {};",
                    r
                )),
                StmtKind::Raw(format!(
                    "GRANT USAGE ON FOREIGN SERVER fz_adtm_srv TO {}{};",
                    r, go
                )),
                StmtKind::Raw(format!(
                    "SELECT has_foreign_data_wrapper_privilege('{}', 'fz_adtm_fdw', 'USAGE'), has_server_privilege('{}', 'fz_adtm_srv', 'USAGE WITH GRANT OPTION'), has_server_privilege('fz_adtm_srv', 'USAGE');",
                    r, r
                )),
                StmtKind::Raw(format!("REVOKE ALL ON FOREIGN SERVER fz_adtm_srv FROM {};", r)),
                StmtKind::Raw(format!(
                    "REVOKE ALL ON FOREIGN DATA WRAPPER fz_adtm_fdw FROM {};",
                    r
                )),
                StmtKind::Raw("DROP SERVER fz_adtm_srv;".to_string()),
                StmtKind::Raw("DROP FOREIGN DATA WRAPPER fz_adtm_fdw;".to_string()),
            ]
        }
        _ => {
            // Parameter bracket (ParameterAclCreate; the pg_parameter_acl
            // catalog row persists after the revoke — identical on both
            // sides, so the count probe stays deterministic).
            let param = pick_str(g, &["work_mem", "shared_buffers", "search_path"]);
            let p = pick_str(g, &["SET", "ALTER SYSTEM", "SET, ALTER SYSTEM", "ALL"]);
            vec![
                StmtKind::Raw(format!("GRANT {} ON PARAMETER {} TO {};", p, param, r)),
                StmtKind::Raw(format!(
                    "SELECT has_parameter_privilege('{}', '{}', 'SET'), has_parameter_privilege('{}', '{}', 'ALTER SYSTEM');",
                    r, param, r, param
                )),
                StmtKind::Raw(format!("REVOKE ALL ON PARAMETER {} FROM {};", param, r)),
            ]
        }
    }
}

/// GRANTED BY spellings (CURRENT_USER / CURRENT_ROLE / SESSION_USER) on
/// grant and revoke, ALL PRIVILEGES included.
fn gen_acl_grantby(g: &mut Gen) -> Vec<StmtKind> {
    let roles = live_roles(g);
    if roles.is_empty() {
        g.fire("adtm:fallback:probe");
        return gen_acl_arity(g);
    }
    g.fire("adtm:acl:grantby");
    let r = roles[g.rng.below_usize(roles.len())].clone();
    let t = g.pick_table().name.clone();
    let by1 = pick_str(g, &["CURRENT_USER", "CURRENT_ROLE", "SESSION_USER"]);
    let by2 = pick_str(g, &["CURRENT_USER", "CURRENT_ROLE", "SESSION_USER"]);
    let p = if g.rng.chance(1, 3) { "ALL PRIVILEGES".to_string() } else { priv_list(g, TBL_PRIVS, false) };
    vec![
        StmtKind::Raw(format!("GRANT {} ON {} TO {} GRANTED BY {};", p, t, r, by1)),
        StmtKind::Raw(format!(
            "SELECT has_table_privilege('{}', '{}', '{}');",
            r,
            t,
            pick_str(g, TBL_PRIVS)
        )),
        StmtKind::Raw(format!(
            "REVOKE ALL PRIVILEGES ON {} FROM {} GRANTED BY {};",
            t, r, by2
        )),
    ]
}

/// DROP OWNED / REASSIGN OWNED brackets: fuzz roles own no objects
/// (objddl invariant), so DROP OWNED only revokes grants and REASSIGN
/// OWNED is a no-op — both deterministic; probes confirm the revoke.
fn gen_acl_owned(g: &mut Gen) -> Vec<StmtKind> {
    let roles = live_roles(g);
    if roles.is_empty() {
        g.fire("adtm:fallback:probe");
        return gen_acl_arity(g);
    }
    g.fire("adtm:acl:owned");
    let r = roles[g.rng.below_usize(roles.len())].clone();
    let t = g.pick_table().name.clone();
    let mut out = vec![
        StmtKind::Raw(format!(
            "GRANT {} ON {} TO {};",
            priv_list(g, TBL_PRIVS, false),
            t, r
        )),
        StmtKind::Raw(format!("GRANT CREATE ON SCHEMA public TO {};", r)),
        StmtKind::Raw(format!("DROP OWNED BY {};", r)),
        StmtKind::Raw(format!(
            "SELECT has_table_privilege('{}', '{}', 'SELECT'), has_schema_privilege('{}', 'public', 'CREATE');",
            r, t, r
        )),
    ];
    if g.rng.chance(1, 2) {
        out.push(StmtKind::Raw(format!("REASSIGN OWNED BY {} TO postgres;", r)));
    }
    // Close the bracket per the residual-grants invariant (no-ops after
    // DROP OWNED, but the schema + table grants are provably gone).
    out.push(StmtKind::Raw(format!("REVOKE ALL ON SCHEMA public FROM {};", r)));
    out.push(StmtKind::Raw(format!("REVOKE ALL PRIVILEGES ON {} FROM {};", t, r)));
    out
}

/// Row-level-security bracket: own table, ENABLE RLS, SELECT/INSERT
/// policies, grants, a SET ROLE probe window (rowsecurity.c policy
/// application incl. the WITH CHECK violation error), DROP TABLE.
/// A SUPERUSER/BYPASSRLS fuzz role sees unfiltered rows — the role's
/// attribute set is identical on both sides, so the output still matches.
fn gen_acl_rls(g: &mut Gen) -> Vec<StmtKind> {
    let roles = live_roles(g);
    if roles.is_empty() {
        g.fire("adtm:fallback:probe");
        return gen_acl_arity(g);
    }
    g.fire("adtm:acl:rls");
    let r = roles[g.rng.below_usize(roles.len())].clone();
    let cut = 1 + g.rng.below(3);
    let mut out = vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_adtm_rls CASCADE;".to_string()),
        StmtKind::Raw("CREATE TABLE fz_adtm_rls (a int PRIMARY KEY, b text);".to_string()),
        StmtKind::Raw("INSERT INTO fz_adtm_rls VALUES (1, 'one'), (2, 'two'), (3, 'three');".to_string()),
        StmtKind::Raw("ALTER TABLE fz_adtm_rls ENABLE ROW LEVEL SECURITY;".to_string()),
        StmtKind::Raw(format!(
            "CREATE POLICY fz_adtm_p1 ON fz_adtm_rls FOR SELECT TO PUBLIC USING (a > {});",
            cut
        )),
        StmtKind::Raw("CREATE POLICY fz_adtm_p2 ON fz_adtm_rls FOR INSERT TO PUBLIC WITH CHECK (a < 100);".to_string()),
        StmtKind::Raw(format!("GRANT SELECT, INSERT ON fz_adtm_rls TO {};", r)),
        StmtKind::Raw(format!("GRANT USAGE ON SCHEMA public TO {};", r)),
        StmtKind::Raw(format!("SET ROLE {};", r)),
        StmtKind::Raw("SELECT a, b FROM fz_adtm_rls ORDER BY a;".to_string()),
        StmtKind::Raw(format!("INSERT INTO fz_adtm_rls VALUES ({}, 'new');", 4 + g.rng.below(20))),
    ];
    if g.rng.chance(1, 2) {
        // Matched RLS WITH CHECK violation (42501) for a plain role; a
        // SUPERUSER role inserts the row — dropped with the table either
        // way, identical on both sides.
        out.push(StmtKind::Raw("INSERT INTO fz_adtm_rls VALUES (200, 'nope');".to_string()));
    }
    out.push(StmtKind::Raw("RESET ROLE;".to_string()));
    out.push(StmtKind::Raw("DROP TABLE fz_adtm_rls;".to_string()));
    out.push(StmtKind::Raw(format!("REVOKE ALL ON SCHEMA public FROM {};", r)));
    out
}

/// aclitem long tail: the aclinsert/aclremove "no longer supported"
/// stubs (matched errors), hash functions (equality-probed — the values
/// agree today but the probe form is future-proof), multi-privilege
/// makeaclitem, and the acldefault kinds the base family leaves rare.
fn gen_acl_aclx(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:acl:aclx");
    let sql = match g.rng.below(5) {
        0 => "SELECT aclinsert(ARRAY[]::aclitem[], makeaclitem(10, 10, 'SELECT', false));".to_string(),
        1 => "SELECT aclremove(ARRAY[]::aclitem[], makeaclitem(10, 10, 'SELECT', false));".to_string(),
        2 => format!(
            "SELECT hash_aclitem(makeaclitem(10, 10, '{}', false)) = hash_aclitem(makeaclitem(10, 10, '{}', false)), hash_aclitem_extended(makeaclitem(0, 10, 'UPDATE', true), {}) = hash_aclitem_extended(makeaclitem(0, 10, 'UPDATE', true), {});",
            pick_str(g, &["SELECT", "UPDATE", "TRUNCATE"]),
            pick_str(g, &["SELECT", "UPDATE", "TRUNCATE"]),
            g.rng.below(2),
            g.rng.below(2)
        ),
        3 => format!(
            "SELECT makeaclitem({}, 10, 'SELECT, UPDATE, INSERT', {})::text;",
            if g.rng.chance(1, 3) { 0 } else { 10 },
            if g.rng.chance(1, 2) { "true" } else { "false" }
        ),
        _ => format!(
            "SELECT count(*) FROM aclexplode(acldefault('{}', 10)) a;",
            pick_str(g, &["n", "T", "d", "L"])
        ),
    };
    raw(sql)
}

// ------------------------------------------------------------- family 2 ----
// varbit (varbit.c) + varlena text long tail (varlena.c) + bytea.

/// Random bit-string literal `B'...'` of `lo..=hi` bits.
fn bit_lit(g: &mut Gen, lo: u64, hi: u64) -> String {
    let n = lo + g.rng.below(hi - lo + 1);
    let mut s = String::from("B'");
    for _ in 0..n {
        s.push(if g.rng.chance(1, 2) { '1' } else { '0' });
    }
    s.push('\'');
    s
}

fn gen_bit(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:bit");
    let err = err_arm(g);
    let sql = match g.rng.below(12) {
        0 => {
            // Binary logical ops need equal lengths; err arm mismatches.
            let n = 1 + g.rng.below(10);
            let a = bit_lit(g, n, n);
            let b = if err { bit_lit(g, n + 1, n + 3) } else { bit_lit(g, n, n) };
            let op = pick_str(g, &["&", "|", "#"]);
            format!("SELECT ({} {} {})::text;", a, op, b)
        }
        1 => {
            let a = bit_lit(g, 1, 10);
            match g.rng.below(3) {
                0 => format!("SELECT (~ {})::text;", a),
                1 => format!("SELECT ({} << {})::text;", a, g.rng.below(14)),
                _ => format!("SELECT ({} >> {})::text;", a, g.rng.below(14)),
            }
        }
        2 => {
            let a = bit_lit(g, 0, 6);
            let b = bit_lit(g, 0, 6);
            format!("SELECT ({} || {})::text;", a, b)
        }
        3 => {
            let a = bit_lit(g, 1, 8);
            let b = bit_lit(g, 1, 8);
            let op = pick_str(g, &["=", "<>", "<", "<=", ">", ">="]);
            format!("SELECT {} {} {};", a, op, b)
        }
        4 => {
            // bit(n) cast: extension, truncation, int conversions.
            let n = 1 + g.rng.below(12);
            match g.rng.below(4) {
                0 => {
                    let a = bit_lit(g, 1, 10);
                    format!("SELECT ({}::bit({}))::text;", a, n)
                }
                1 => format!(
                    "SELECT ({}::bit({}))::text;",
                    g.rng.range_i64(-300, 300),
                    if g.rng.chance(1, 3) { 64 } else { 8 + g.rng.below(24) }
                ),
                2 => {
                    let a = bit_lit(g, 8, 8);
                    format!("SELECT ({}::bit(8))::int;", a)
                }
                _ => {
                    let a = bit_lit(g, 1, 6);
                    format!("SELECT ({}::varbit({}))::text;", a, 1 + g.rng.below(6))
                }
            }
        }
        5 => {
            let a = bit_lit(g, 1, 12);
            format!(
                "SELECT length({}), octet_length({}), bit_length({});",
                a, a, a
            )
        }
        6 => {
            let n = 1 + g.rng.below(10);
            let a = bit_lit(g, n, n);
            let i = if err { n as i64 + g.rng.below(3) as i64 } else { g.rng.below(n) as i64 };
            if g.rng.chance(1, 2) {
                format!("SELECT get_bit({}, {});", a, i)
            } else {
                format!("SELECT set_bit({}, {}, {})::text;", a, i, g.rng.below(2))
            }
        }
        7 => {
            let a = bit_lit(g, 4, 10);
            let b = bit_lit(g, 1, 4);
            let from = if err { 12 + g.rng.below(4) as i64 } else { 1 + g.rng.below(6) as i64 };
            if g.rng.chance(1, 2) {
                format!("SELECT overlay({} placing {} from {})::text;", a, b, from)
            } else {
                format!(
                    "SELECT overlay({} placing {} from {} for {})::text;",
                    a,
                    b,
                    from,
                    g.rng.range_i64(0, 6)
                )
            }
        }
        8 => {
            let a = bit_lit(g, 2, 10);
            let b = bit_lit(g, 1, 3);
            format!("SELECT position({} in {});", b, a)
        }
        9 => {
            let a = bit_lit(g, 2, 12);
            format!(
                "SELECT substring({} from {} for {})::text;",
                a,
                g.rng.range_i64(-2, 6),
                g.rng.range_i64(0, 8)
            )
        }
        10 => {
            let a = bit_lit(g, 1, 16);
            format!("SELECT bit_count({});", a)
        }
        _ => {
            let n = 1 + g.rng.below(6);
            let a = bit_lit(g, n, n);
            let b = bit_lit(g, n, n);
            let c = bit_lit(g, n, n);
            let agg = pick_str(g, &["bit_and", "bit_or", "bit_xor"]);
            format!(
                "SELECT {}(v)::text FROM (VALUES ({}), ({}), ({})) t(v);",
                agg, a, b, c
            )
        }
    };
    raw(sql)
}

/// Small quote-free text-literal pool (unicode included).
const TEXT_POOL: &[&str] = &[
    "", "a", "abc,def,ghi", "Hello World", "MiXeD CaSe", "  pad  ", "aaa", "x,y",
    "Ünïcødé tex", "abcabcabc", "tab\tless", "comma,,double",
];

fn text_lit(g: &mut Gen) -> String {
    format!("'{}'", pick_str(g, TEXT_POOL))
}

fn gen_vltext(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:vltext");
    let err = err_arm(g);
    let sql = match g.rng.below(16) {
        0 => {
            let t = text_lit(g);
            let d = pick_str(g, &["','", "' '", "''", "'abc'"]);
            let n = if err { 0 } else { g.rng.range_i64(-3, 4) };
            format!("SELECT split_part({}, {}, {});", t, d, n)
        }
        1 => {
            let t = text_lit(g);
            let (from, to) = *pick_str2(g, &[("abc", "xy"), ("", "xy"), ("a", ""), ("abcdef", "ABCDEF"), ("aeiou", "*")]);
            format!("SELECT translate({}, '{}', '{}');", t, from, to)
        }
        2 => {
            let t = pick_str(g, &["ab", "x", ""]);
            format!("SELECT repeat('{}', {});", t, g.rng.range_i64(-2, 5))
        }
        3 => {
            let t = text_lit(g);
            let p = text_lit(g);
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT overlay({} placing {} from {});",
                    t,
                    p,
                    g.rng.range_i64(-1, 8)
                )
            } else {
                format!(
                    "SELECT overlay({} placing {} from {} for {});",
                    t,
                    p,
                    g.rng.range_i64(1, 6),
                    g.rng.range_i64(-1, 8)
                )
            }
        }
        4 => {
            let t = text_lit(g);
            let n = text_lit(g);
            if g.rng.chance(1, 2) {
                format!("SELECT position({} in {});", n, t)
            } else {
                format!("SELECT strpos({}, {});", t, n)
            }
        }
        5 => {
            let t = pick_str(g, &["sel ect", "abc", "MiXed", "with\"quote", "1abc", ""]);
            match g.rng.below(3) {
                0 => format!("SELECT quote_ident('{}');", t.replace('"', "\"\"")),
                1 => format!("SELECT quote_literal('{}');", t.replace('"', "")),
                _ => {
                    if g.rng.chance(1, 3) {
                        "SELECT quote_nullable(NULL::text);".to_string()
                    } else {
                        format!("SELECT quote_nullable('{}');", t.replace('"', ""))
                    }
                }
            }
        }
        6 => {
            // format(): %s/%I/%L conversions, widths, positional refs.
            if err {
                let bad = pick_str(g, &["%<invalid", "%1$", "%s %s missing", "%I"]);
                if bad == "%s %s missing" {
                    "SELECT format('%s %s', 'only-one');".to_string()
                } else if bad == "%I" {
                    "SELECT format('%I', NULL);".to_string()
                } else {
                    format!("SELECT format('{}');", bad)
                }
            } else {
                let pic = pick_str(g, &[
                    "%s|%I|%L",
                    "%10s|%-10s",
                    "%1$s %1$s %2$L",
                    "100%%",
                    "%2$s %1$s",
                    "%-5I end",
                ]);
                let a = text_lit(g);
                let b = text_lit(g);
                if pic == "100%%" {
                    "SELECT format('100%%');".to_string()
                } else if pic == "%s|%I|%L" {
                    format!("SELECT format('{}', {}, 'a b', NULL);", pic, a)
                } else {
                    format!("SELECT format('{}', {}, {});", pic, a, b)
                }
            }
        }
        7 => {
            let a = text_lit(g);
            let b = text_lit(g);
            match g.rng.below(3) {
                0 => format!("SELECT concat_ws(',', {}, NULL, {}, '');", a, b),
                1 => format!("SELECT concat_ws(NULL, {}, {});", a, b),
                _ => format!("SELECT concat({}, NULL, 1, true, {});", a, b),
            }
        }
        8 => {
            let t = text_lit(g);
            let n = g.rng.range_i64(-4, 8);
            let f = pick_str(g, &["left", "right"]);
            format!("SELECT {}({}, {});", f, t, n)
        }
        9 => {
            let t = text_lit(g);
            let n = g.rng.range_i64(-1, 10);
            match g.rng.below(4) {
                0 => format!("SELECT lpad({}, {}, 'xy');", t, n),
                1 => format!("SELECT rpad({}, {}, 'xy');", t, n),
                2 => format!("SELECT lpad({}, {});", t, n),
                _ => format!("SELECT rpad({}, {});", t, n),
            }
        }
        10 => {
            let u = if err {
                pick_str(g, &["\\dfff", "\\+110000", "\\00zz"])
            } else {
                pick_str(g, &["\\0041\\0042", "d\\0061t\\+000061", "", "\\00e9", "\\+01F600ok"])
            };
            format!("SELECT unistr('{}');", u)
        }
        11 => {
            let t = text_lit(g);
            match g.rng.below(4) {
                0 => format!("SELECT initcap({});", t),
                1 => format!("SELECT casefold({});", t),
                2 => format!("SELECT reverse({});", t),
                _ => format!("SELECT ascii({});", t),
            }
        }
        12 => {
            let n = if err { 0 } else { *pick_str2(g, &[65, 97, 233, 8364, 128512]) };
            format!("SELECT chr({});", n)
        }
        13 => {
            let t = text_lit(g);
            let set = pick_str(g, &["'x'", "'ax '", "''"]);
            let f = pick_str(g, &["ltrim", "rtrim", "btrim"]);
            if g.rng.chance(1, 3) {
                format!("SELECT {}({});", f, t)
            } else {
                format!("SELECT {}({}, {});", f, t, set)
            }
        }
        14 => {
            let t = text_lit(g);
            match g.rng.below(5) {
                0 => format!("SELECT starts_with({}, 'a');", t),
                1 => format!("SELECT md5({});", t),
                2 => format!("SELECT to_hex({});", g.rng.range_i64(-1, 1000000)),
                3 => format!(
                    "SELECT to_bin({}), to_oct({});",
                    g.rng.range_i64(-2, 300),
                    g.rng.range_i64(-2, 300)
                ),
                _ => {
                    let id = if err { "a..b" } else { pick_str(g, &["a.b.c", "\"A b\".c", "abc"]) };
                    format!("SELECT parse_ident('{}')::text;", id.replace('"', "\"\""))
                }
            }
        }
        _ => {
            let t = text_lit(g);
            format!(
                "SELECT substr({}, {}, {});",
                t,
                g.rng.range_i64(-3, 5),
                if err { -1 } else { g.rng.range_i64(0, 6) }
            )
        }
    };
    raw(sql)
}

/// Random bytea literal `'\x..'` of `lo..=hi` bytes.
fn bytea_lit(g: &mut Gen, lo: u64, hi: u64) -> String {
    let n = lo + g.rng.below(hi - lo + 1);
    let mut s = String::from("'\\x");
    for _ in 0..n {
        s.push_str(&format!("{:02x}", g.rng.below(256)));
    }
    s.push_str("'::bytea");
    s
}

fn gen_vlbytea(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:vlbytea");
    let err = err_arm(g);
    let sql = match g.rng.below(11) {
        0 => {
            let n = 1 + g.rng.below(6);
            let a = bytea_lit(g, n, n);
            let i = if err { n as i64 + g.rng.below(3) as i64 } else { g.rng.below(n) as i64 };
            format!("SELECT get_byte({}, {});", a, i)
        }
        1 => {
            let n = 1 + g.rng.below(6);
            let a = bytea_lit(g, n, n);
            let i = if err { n as i64 } else { g.rng.below(n) as i64 };
            format!(
                "SELECT set_byte({}, {}, {})::text;",
                a,
                i,
                g.rng.below(600)
            )
        }
        2 => {
            let a = bytea_lit(g, 3, 8);
            let b = bytea_lit(g, 1, 3);
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT overlay({} placing {} from {})::text;",
                    a,
                    b,
                    g.rng.range_i64(-1, 8)
                )
            } else {
                format!(
                    "SELECT overlay({} placing {} from {} for {})::text;",
                    a,
                    b,
                    g.rng.range_i64(1, 6),
                    g.rng.range_i64(0, 6)
                )
            }
        }
        3 => {
            let a = bytea_lit(g, 2, 8);
            let b = bytea_lit(g, 1, 2);
            format!("SELECT position({} in {});", b, a)
        }
        4 => {
            let a = pick_str(g, &["'\\x0011220000'", "'\\x000000'", "'\\xaabb'"]);
            let set = pick_str(g, &["'\\x00'", "'\\x00aa'", "'\\x'"]);
            let f = pick_str(g, &["ltrim", "rtrim", "btrim"]);
            format!("SELECT {}({}::bytea, {}::bytea)::text;", f, a, set)
        }
        5 => {
            // encode/decode escape.
            if err {
                format!(
                    "SELECT decode('{}', 'escape');",
                    pick_str(g, &["ab\\9", "trail\\", "\\40x\\999"])
                )
            } else if g.rng.chance(1, 2) {
                let a = bytea_lit(g, 0, 8);
                format!("SELECT encode({}, 'escape');", a)
            } else {
                format!(
                    "SELECT decode('{}', 'escape')::text;",
                    pick_str(g, &["ab\\\\000cd", "plain", "\\\\134x", ""])
                )
            }
        }
        6 => {
            // encode/decode base64 (incl. the 76-col line-wrap path).
            if err {
                format!(
                    "SELECT decode('{}', 'base64');",
                    pick_str(g, &["####", "3q2+7w=", "AB"])
                )
            } else if g.rng.chance(1, 2) {
                let a = bytea_lit(g, 0, 80);
                format!("SELECT encode({}, 'base64');", a)
            } else {
                format!(
                    "SELECT decode('{}', 'base64')::text;",
                    pick_str(g, &["3q2+7w==", "AA==", "", "QUJDREVGRw=="])
                )
            }
        }
        7 => {
            if err {
                format!(
                    "SELECT decode('{}', '{}');",
                    pick_str(g, &["zz", "123", "0x"]),
                    if g.rng.chance(1, 4) { "bogus" } else { "hex" }
                )
            } else if g.rng.chance(1, 2) {
                let a = bytea_lit(g, 0, 6);
                format!("SELECT encode({}, 'hex');", a)
            } else {
                format!(
                    "SELECT decode('{}', 'hex')::text;",
                    pick_str(g, &["12AB", "deadBEEF", "", "00ff"])
                )
            }
        }
        8 => {
            let a = bytea_lit(g, 0, 8);
            let f = pick_str(g, &["sha224", "sha256", "sha384", "sha512"]);
            format!("SELECT {}({})::text;", f, a)
        }
        9 => {
            let a = bytea_lit(g, 0, 8);
            format!(
                "SELECT bit_count({}), length({}), octet_length({});",
                a, a, a
            )
        }
        _ => {
            let a = bytea_lit(g, 2, 8);
            if g.rng.chance(1, 2) {
                format!(
                    "SELECT substring({} from {} for {})::text;",
                    a,
                    g.rng.range_i64(-2, 5),
                    g.rng.range_i64(0, 5)
                )
            } else {
                format!("SELECT reverse({})::text;", a)
            }
        }
    };
    raw(sql)
}

// ------------------------------------------------------------- family 3 ----
// Multirange completion (multirangetypes.c).

/// The five base kinds with both engines' built-in multirange types.
struct MrKind {
    rangef: &'static str,
    mrf: &'static str,
    mrty: &'static str,
}

const MR_KINDS: &[MrKind] = &[
    MrKind { rangef: "int4range", mrf: "int4multirange", mrty: "int4multirange" },
    MrKind { rangef: "int8range", mrf: "int8multirange", mrty: "int8multirange" },
    MrKind { rangef: "numrange", mrf: "nummultirange", mrty: "nummultirange" },
    MrKind { rangef: "daterange", mrf: "datemultirange", mrty: "datemultirange" },
    MrKind { rangef: "tsrange", mrf: "tsmultirange", mrty: "tsmultirange" },
];

/// Ordered (lo < hi) bound pair for a base kind; occasionally NULL bounds.
fn mr_bounds(g: &mut Gen, k: &MrKind) -> (String, String) {
    let null_lo = g.rng.chance(1, 8);
    let null_hi = g.rng.chance(1, 8);
    let (lo, hi) = match k.rangef {
        "int4range" | "int8range" => {
            let a = g.rng.range_i64(-20, 50);
            let b = a + g.rng.below(30) as i64;
            (a.to_string(), b.to_string())
        }
        "numrange" => {
            let a = g.rng.range_i64(-9, 9);
            let b = a + 1 + g.rng.below(9) as i64;
            (format!("{}.{}", a, g.rng.below(100)), format!("{}.5", b))
        }
        "daterange" => (
            pick_str(g, &["'2020-01-01'", "'2021-06-15'", "'1999-12-31'"]).to_string(),
            pick_str(g, &["'2024-02-29'", "'2030-12-31'"]).to_string(),
        ),
        _ => (
            "'2020-01-01 00:00:00'".to_string(),
            pick_str(g, &["'2022-03-03 12:00:00'", "'2030-01-01 23:59:59'"]).to_string(),
        ),
    };
    (
        if null_lo { "NULL".to_string() } else { lo },
        if null_hi { "NULL".to_string() } else { hi },
    )
}

/// One range-constructor expression, occasionally empty / single-point /
/// with explicit bound-inclusivity flags.
fn mr_range(g: &mut Gen, k: &MrKind) -> String {
    let (lo, hi) = mr_bounds(g, k);
    // Bound-inclusivity flags stay paren/bracket-balanced ('[]' / '()'):
    // the stream-wide textual invariants count parens per statement.
    match g.rng.below(6) {
        0 => format!("{}({}, {}, '[]')", k.rangef, lo, hi),
        1 => format!("{}({}, {}, '()')", k.rangef, lo, hi),
        2 => format!("{}({}, {})", k.rangef, lo, lo), // empty
        _ => format!("{}({}, {})", k.rangef, lo, hi),
    }
}

/// A multirange expression: constructor of 0-3 ranges (overlap/adjacency
/// collapse included by construction) or a literal.
fn mr_expr(g: &mut Gen, k: &MrKind) -> String {
    match g.rng.below(6) {
        0 => format!("{}()", k.mrf),
        1 => format!("{}({})", k.mrf, mr_range(g, k)),
        2 | 3 => format!("{}({}, {})", k.mrf, mr_range(g, k), mr_range(g, k)),
        4 => format!(
            "{}({}, {}, {})",
            k.mrf,
            mr_range(g, k),
            mr_range(g, k),
            mr_range(g, k)
        ),
        _ => {
            if k.rangef == "int4range" {
                // Literal spellings stay paren-balanced (closed-bracket or
                // paired-paren bounds) for the stream textual invariants.
                format!(
                    "'{}'::{}",
                    pick_str(g, &["{}", "{[1,3], [5,9]}", "{(1,3), (2,9)}", "{(,5), (8,)}", "{empty}"]),
                    k.mrty
                )
            } else {
                format!("{}({})", k.mrf, mr_range(g, k))
            }
        }
    }
}

/// An element literal for containment probes.
fn mr_elem(g: &mut Gen, k: &MrKind) -> String {
    match k.rangef {
        "int4range" => g.rng.range_i64(-20, 60).to_string(),
        "int8range" => format!("{}::int8", g.rng.range_i64(-20, 60)),
        "numrange" => format!("{}.5::numeric", g.rng.range_i64(-9, 20)),
        "daterange" => "'2020-06-01'::date".to_string(),
        _ => "'2021-01-01 00:00:00'::timestamp".to_string(),
    }
}

fn gen_multirange(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:mr");
    let err = err_arm(g);
    let k = &MR_KINDS[g.rng.below_usize(MR_KINDS.len())];
    let sql = match g.rng.below(12) {
        0 => format!("SELECT ({})::text;", mr_expr(g, k)),
        1 => {
            if err {
                // Reversed bounds (22000) or a broken literal (22P02).
                if g.rng.chance(1, 2) {
                    "SELECT int4multirange(int4range(5, 1))::text;".to_string()
                } else {
                    format!(
                        "SELECT '{}'::int4multirange;",
                        pick_str(g, &["{[1,", "[1,3]", "{(1,3) (4,5)}", "{(b,c)}"])
                    )
                }
            } else {
                format!("SELECT ({})::text;", mr_expr(g, k))
            }
        }
        2 => {
            let op = pick_str(g, &["@>", "<@", "&&", "<<", ">>", "-|-"]);
            let a = mr_expr(g, k);
            let b = mr_expr(g, k);
            format!("SELECT {} {} {};", a, op, b)
        }
        3 => {
            // multirange vs range / element mixed-operand forms.
            let a = mr_expr(g, k);
            match g.rng.below(4) {
                0 => format!("SELECT {} @> {};", a, mr_elem(g, k)),
                1 => format!("SELECT {} <@ {};", mr_elem(g, k), a),
                2 => format!("SELECT {} @> {};", a, mr_range(g, k)),
                _ => format!("SELECT {} && {};", mr_range(g, k), a),
            }
        }
        4 => {
            let op = pick_str(g, &["+", "*", "-"]);
            let a = mr_expr(g, k);
            let b = mr_expr(g, k);
            format!("SELECT ({} {} {})::text;", a, op, b)
        }
        5 => {
            let a = mr_expr(g, k);
            let f = pick_str(g, &["lower", "upper"]);
            format!("SELECT ({}({}))::text;", f, a)
        }
        6 => {
            let a = mr_expr(g, k);
            let f = pick_str(g, &[
                "isempty", "lower_inc", "upper_inc", "lower_inf", "upper_inf",
            ]);
            format!("SELECT {}({});", f, a)
        }
        7 => {
            let a = mr_expr(g, k);
            match g.rng.below(3) {
                0 => format!("SELECT range_merge({})::text;", a),
                1 => format!("SELECT multirange({})::text;", mr_range(g, k)),
                _ => format!("SELECT hash_multirange({}) IS NOT NULL;", a),
            }
        }
        8 => {
            // unnest in FROM: multiset compare covers the rowset.
            let a = mr_expr(g, k);
            let alias = g.next_alias();
            format!(
                "SELECT {a}.x::text FROM unnest({m}) AS {a}(x);",
                a = alias,
                m = a
            )
        }
        9 => {
            // range_agg / range_intersect_agg over VALUES (order-blind).
            let r1 = mr_range(g, k);
            let r2 = mr_range(g, k);
            let r3 = mr_range(g, k);
            let f = pick_str(g, &["range_agg", "range_intersect_agg"]);
            format!(
                "SELECT {}(r)::text FROM (VALUES ({}), ({}), ({})) v(r);",
                f, r1, r2, r3
            )
        }
        10 => {
            let a = mr_expr(g, k);
            let b = mr_expr(g, k);
            let op = pick_str(g, &["=", "<>", "<", "<=", ">", ">="]);
            format!("SELECT {} {} {};", a, op, b)
        }
        _ => {
            // Cross with an objddl-created custom range type when live:
            // range_agg over anyrange returns the auto-created multirange.
            let ranges: Vec<(String, crate::catalog::SqlType)> = g
                .obj
                .ranges
                .iter()
                .filter(|r| r.live)
                .map(|r| (r.name.clone(), r.subtype))
                .collect();
            if ranges.is_empty() {
                format!("SELECT ({})::text;", mr_expr(g, k))
            } else {
                let (name, subtype) = ranges[g.rng.below_usize(ranges.len())].clone();
                let (lo1, hi1) = custom_range_bounds(g, subtype);
                let (lo2, hi2) = custom_range_bounds(g, subtype);
                g.fire("adtm:mr:custom");
                if g.rng.chance(1, 2) {
                    format!(
                        "SELECT range_agg(x)::text FROM (VALUES ({}({}, {})), ({}({}, {}))) t(x);",
                        name, lo1, hi1, name, lo2, hi2
                    )
                } else {
                    format!("SELECT multirange({}({}, {}))::text;", name, lo1, hi1)
                }
            }
        }
    };
    raw(sql)
}

/// Ordered literal pair for an objddl custom-range subtype (mirrors the
/// objddl module's palette; both halves must stay orderable).
fn custom_range_bounds(g: &mut Gen, ty: crate::catalog::SqlType) -> (String, String) {
    use crate::catalog::SqlType;
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
        SqlType::Date => ("'2020-01-01'".to_string(), "'2030-12-31'".to_string()),
        SqlType::Timestamp => (
            "'2020-01-01 00:00:00'".to_string(),
            "'2030-01-01 23:59:59'".to_string(),
        ),
        _ => ("'a'".to_string(), "'z'".to_string()),
    }
}

// ------------------------------------------------------------- family 4 ----
// float/numeric edge matrix (float.c / numeric.c) + money (cash.c).

/// Special float8 values (the always-legal pool).
const F8_SPECIALS: &[&str] = &[
    "'NaN'", "'Infinity'", "'-Infinity'", "'-0'", "0", "1", "-1", "0.5", "2", "2.5",
    "'1e308'", "'1e-300'", "100",
];

/// One-arg float functions total over the special pool (no domain error
/// for any pool value — errors ride the dedicated err arms instead).
const F8_TOTAL_FNS: &[&str] = &[
    "exp", "atan", "sinh", "cosh", "tanh", "asinh", "degrees", "radians", "sign",
    "trunc", "floor", "ceil", "round", "abs", "cbrt", "erf", "erfc",
];

/// (fn, arg) pairs raising domain/overflow errors identically both sides.
const F8_ERR_CALLS: &[(&str, &str)] = &[
    ("ln", "0"), ("ln", "-1"), ("sqrt", "-1"), ("asin", "2"), ("acos", "-2"),
    ("acosh", "0.5"), ("atanh", "2"), ("gamma", "0"), ("gamma", "-1"), ("lgamma", "0"),
    ("sin", "'Infinity'"), ("cos", "'-Infinity'"), ("tan", "'Infinity'"),
];

fn gen_num_edges(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:num");
    let err = err_arm(g);
    let sql = match g.rng.below(14) {
        0 => {
            if err {
                let (f, a) = *pick_str2(g, F8_ERR_CALLS);
                format!("SELECT {}({}::float8);", f, a)
            } else {
                let f = pick_str(g, F8_TOTAL_FNS);
                let a = pick_str(g, F8_SPECIALS);
                format!("SELECT {}({}::float8)::text;", f, a)
            }
        }
        1 => {
            // Positive-domain fns over safe args; ln/sqrt/log10 family.
            let f = pick_str(g, &["ln", "sqrt", "log"]);
            let a = pick_str(g, &["1", "2", "0.5", "'Infinity'", "'NaN'", "100"]);
            format!("SELECT {}({}::float8)::text;", f, a)
        }
        2 => {
            // sin/cos/tan over finite + NaN; asin/acos/atan in-domain.
            let f = pick_str(g, &["sin", "cos", "tan", "atan"]);
            let a = pick_str(g, &["0", "1", "-1", "0.5", "'NaN'", "2.5"]);
            format!("SELECT {}({}::float8)::text;", f, a)
        }
        3 => {
            // Degree-based trig (exact at the special angles).
            if err {
                format!("SELECT {}(90);", pick_str(g, &["tand", "cotd"]))
            } else {
                let f = pick_str(g, &["sind", "cosd", "tand", "cotd", "asind", "acosd", "atand"]);
                let a = pick_str(g, &["0", "30", "45", "60", "0.5", "1", "'NaN'::float8"]);
                format!("SELECT {}({})::text;", f, a)
            }
        }
        4 => {
            let a = pick_str(g, F8_SPECIALS);
            let b = pick_str(g, F8_SPECIALS);
            if g.rng.chance(1, 2) {
                format!("SELECT atan2({}::float8, {}::float8)::text;", a, b)
            } else {
                format!("SELECT atan2d({}::float8, {}::float8)::text;", a, b)
            }
        }
        5 => {
            // power: float and numeric arities incl. the 0^-1 / (-1)^0.5
            // error pairs.
            if err {
                let ty = pick_str(g, &["float8", "numeric"]);
                let (a, b) = *pick_str2(g, &[("0", "-1"), ("-1", "0.5")]);
                format!("SELECT power({}::{ty}, {}::{ty});", a, b, ty = ty)
            } else {
                let (a, b) = *pick_str2(g, &[
                    ("'NaN'", "0"), ("1", "'NaN'"), ("'Infinity'", "0"), ("2", "10"),
                    ("'-Infinity'", "-1"), ("0", "'Infinity'"), ("2", "-3"),
                ]);
                format!("SELECT power({}::float8, {}::float8)::text;", a, b)
            }
        }
        6 => {
            // Inf/NaN arithmetic + zero division (float and numeric).
            if err {
                let d = pick_str(g, &[
                    "1::float8 / 0::float8",
                    "0::float8 / 0::float8",
                    "1::numeric / 0::numeric",
                    "mod(5, 0)",
                    "mod(5::numeric, 0::numeric)",
                    "div(5::numeric, 0::numeric)",
                    "(-2147483648) / (-1)",
                    "mod(-2147483648, -1)::text",
                ]);
                format!("SELECT {};", d)
            } else {
                let e = pick_str(g, &[
                    "'Infinity'::float8 - 'Infinity'::float8",
                    "'Infinity'::float8 + '-Infinity'::float8",
                    "'Infinity'::float8 * 0",
                    "'-Infinity'::float8 * '-0'::float8",
                    "'Infinity'::float8 / 'Infinity'::float8",
                    "'Infinity'::numeric + 'NaN'::numeric",
                    "'Infinity'::numeric - 'Infinity'::numeric",
                    "'Infinity'::numeric * 0::numeric",
                    "'-Infinity'::numeric / 3::numeric",
                    "div('NaN'::numeric, 1)",
                ]);
                format!("SELECT ({})::text;", e)
            }
        }
        7 => {
            if err {
                let c = pick_str(g, &[
                    "gcd(-2147483648, -2147483648)",
                    "gcd((-9223372036854775808)::int8, (-9223372036854775808)::int8)",
                    "lcm(2147483647, 2147483646)",
                    "factorial(-1)",
                ]);
                format!("SELECT {};", c)
            } else {
                match g.rng.below(3) {
                    0 => format!(
                        "SELECT gcd({}, {}), lcm({}, {});",
                        g.rng.range_i64(-40, 40),
                        g.rng.range_i64(-40, 40),
                        g.rng.range_i64(-8, 8),
                        g.rng.range_i64(-8, 8)
                    ),
                    1 => format!("SELECT factorial({})::text;", g.rng.below(41)),
                    _ => "SELECT gcd(0, 0), lcm(0, 5);".to_string(),
                }
            }
        }
        8 => {
            // width_bucket: 4-arg float8/numeric + array arity.
            if err {
                let c = pick_str(g, &[
                    "width_bucket(5::float8, 10::float8, 0::float8, 0)",
                    "width_bucket(5::float8, 0::float8, 0::float8, 5)",
                    "width_bucket('NaN'::float8, 0::float8, 10::float8, 5)",
                    "width_bucket('NaN'::numeric, 0::numeric, 10::numeric, 5)",
                    "width_bucket(5::numeric, 0::numeric, 10::numeric, -1)",
                ]);
                format!("SELECT {};", c)
            } else {
                match g.rng.below(4) {
                    0 => format!(
                        "SELECT width_bucket({}::float8, 0::float8, 10::float8, {});",
                        g.rng.range_i64(-3, 14),
                        1 + g.rng.below(8)
                    ),
                    1 => format!(
                        "SELECT width_bucket({}.5::numeric, 0::numeric, 10::numeric, {});",
                        g.rng.range_i64(-3, 14),
                        1 + g.rng.below(8)
                    ),
                    2 => format!(
                        "SELECT width_bucket({}, ARRAY[1, 3, 4, 10]);",
                        g.rng.range_i64(-2, 12)
                    ),
                    _ => "SELECT width_bucket(5, ARRAY[]::int4[]);".to_string(),
                }
            }
        }
        9 => {
            // numeric round/trunc with extreme scales.
            if err {
                "SELECT round(1.5e100::numeric, -101)::text;".to_string()
            } else {
                let c = pick_str(g, &[
                    "round(2.5::numeric)", "round(-2.5::numeric)",
                    "round(1.234567::numeric, 3)", "round(123456.789::numeric, -2)",
                    "round(123456.789::numeric, -7)", "round(1.5e100::numeric, -99)",
                    "trunc(123.456::numeric, 1)", "trunc(123.456::numeric, -1)",
                    "round('NaN'::numeric, 2)", "round('Infinity'::numeric, -5)",
                    "trunc('NaN'::numeric)", "round(2.5::float8)", "round(-2.5::float8)",
                ]);
                format!("SELECT ({})::text;", c)
            }
        }
        10 => {
            // scale/min_scale/trim_scale + NaN/Inf probes.
            if err {
                format!(
                    "SELECT min_scale('{}'::numeric);",
                    pick_str(g, &["Infinity", "-Infinity"])
                )
            } else {
                let c = pick_str(g, &[
                    "scale(1.230::numeric)", "scale('NaN'::numeric)",
                    "min_scale(1.2300::numeric)", "min_scale(100::numeric)",
                    "trim_scale(1.2300::numeric)::text", "trim_scale(0.0000::numeric)::text",
                ]);
                format!("SELECT {};", c)
            }
        }
        11 => {
            // Extreme numeric magnitudes: 1e131071 boundary, scale 1000.
            if err {
                let c = pick_str(g, &[
                    "'1e131072'::numeric",
                    "('1e131071'::numeric * 10::numeric)::text",
                    "'1e-16384'::numeric",
                    "(10.0::numeric ^ 131072::numeric)::text",
                ]);
                format!("SELECT {};", c)
            } else {
                let c = pick_str(g, &[
                    "numeric_out('1e131071'::numeric) IS NOT NULL",
                    "('1e-16383'::numeric)::text IS NOT NULL",
                    "(10.0::numeric ^ 131071::numeric) IS NOT NULL",
                    "('0.5'::numeric ^ 131072::numeric)::text",
                    "exp(-20000::numeric)::text",
                    "(1.0::numeric / 3::numeric)::text",
                    "round(1.0::numeric / 3::numeric, 1000)::text",
                ]);
                format!("SELECT {};", c)
            }
        }
        12 => {
            // numeric ln/log/sqrt domain parity.
            if err {
                let c = pick_str(g, &[
                    "ln(0::numeric)", "log(0::numeric)", "log(-1::numeric, 10::numeric)",
                    "sqrt(-1::numeric)", "ln(-2::numeric)",
                ]);
                format!("SELECT {};", c)
            } else {
                let c = pick_str(g, &[
                    "ln(1e-100::numeric)", "sqrt('NaN'::numeric)", "sqrt(2::numeric)",
                    "log(2.5::numeric, 100::numeric)", "exp(1::numeric)", "ln(10::numeric)",
                ]);
                format!("SELECT ({})::text;", c)
            }
        }
        _ => {
            // int8/int2 overflow edges + misc exact probes.
            if err {
                let c = pick_str(g, &[
                    "(-9223372036854775808)::int8 / (-1)::int8",
                    "32767::int2 + 1::int2",
                    "2147483647 + 1",
                    "abs((-9223372036854775808)::int8)",
                ]);
                format!("SELECT {};", c)
            } else {
                let c = pick_str(g, &[
                    "((-9223372036854775808)::int8 % (-1)::int8)::text",
                    "(gamma(5::float8))::text",
                    "(lgamma(100::float8))::text",
                    "sign('-0'::float8)::text",
                    "abs('-0'::float8)::text",
                ]);
                format!("SELECT {};", c)
            }
        }
    };
    raw(sql)
}

/// Money literal pool (C locale on both servers by charter).
const MONEY_LITS: &[&str] = &[
    "'12.34'", "'0'", "'-1.50'", "'$1,000.00'", "'-$4,564,321.98'", "'12.345'", "'0.005'",
];

fn gen_money(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:money");
    let err = err_arm(g);
    let a = pick_str(g, MONEY_LITS);
    let b = pick_str(g, MONEY_LITS);
    let sql = match g.rng.below(7) {
        0 => format!("SELECT ({}::money)::text;", a),
        1 => {
            let op = pick_str(g, &["+", "-"]);
            format!("SELECT ({}::money {} {}::money)::text;", a, op, b)
        }
        2 => {
            if err {
                let d = pick_str(g, &["0", "0.0::float8", "0::int2"]);
                format!("SELECT {}::money / {};", a, d)
            } else {
                let k = pick_str(g, &["2", "4", "1.5::float8", "3::int2", "10::int8"]);
                let op = pick_str(g, &["*", "/"]);
                format!("SELECT ({}::money {} {})::text;", a, op, k)
            }
        }
        3 => {
            let op = pick_str(g, &["=", "<>", "<", "<=", ">", ">="]);
            format!("SELECT {}::money {} {}::money;", a, op, b)
        }
        4 => {
            if err {
                "SELECT (92233720368547758.08::numeric)::money;".to_string()
            } else {
                let c = pick_str(g, &[
                    "('12.34'::money)::numeric::text",
                    "(7::int4)::money::text",
                    "(7.7::float8)::money::text",
                    "(92233720368547758.07::numeric)::money::text",
                    "(1234.567::numeric)::money::text",
                ]);
                format!("SELECT {};", c)
            }
        }
        5 => format!("SELECT cash_words({}::money);", a),
        _ => {
            let f = pick_str(g, &["min", "max", "sum"]);
            format!(
                "SELECT {}(v)::text FROM (VALUES ({}::money), ({}::money), ('0.10'::money)) t(v);",
                f, a, b
            )
        }
    };
    raw(sql)
}

// ================================================================ Q2 ====
// expr-misc-adt breadth (sql-reachable-queue chunk). Shapes are literal-
// pool Raw statements or self-contained DDL brackets; every family
// hand-verified byte-identical on both engines before banking (see the
// SHAPES comment). Deliberate errors are matched errors by construction.

/// Pseudotype in/out fns called directly with cstring args: the *_in
/// stubs raise their matched "cannot accept a value of type" ereports
/// (which IS the coverage — the stub executes), void/cstring round-trip
/// for real.
fn gen_pseudo(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:pseudo");
    let sql = match g.rng.below(8) {
        0 => {
            let f = pick_str(g, &[
                "any_in", "trigger_in", "internal_in", "language_handler_in",
                "fdw_handler_in", "index_am_handler_in", "tsm_handler_in",
                "table_am_handler_in", "event_trigger_in", "pg_ddl_command_in",
                "anyarray_in", "anyelement_in", "anynonarray_in", "anyenum_in",
                "anyrange_in", "anymultirange_in", "anycompatible_in",
                "anycompatiblearray_in", "anycompatiblenonarray_in",
                "anycompatiblerange_in", "anycompatiblemultirange_in",
                "shell_in", "pg_node_tree_in",
            ]);
            format!("SELECT {}('x'::cstring);", f)
        }
        1 => "SELECT void_out(''::void), ''::void::text;".to_string(),
        2 => "SELECT cstring_out(cstring_in('abc')), 'roundtrip'::text::cstring::text;".to_string(),
        3 => {
            let (v, t) = match g.rng.below(6) {
                0 => ("pg_class", "regclass"),
                1 => ("int4pl", "regproc"),
                2 => ("sum(int4)", "regprocedure"),
                3 => ("integer", "regtype"),
                4 => ("pg_catalog", "regnamespace"),
                _ => ("=(integer,integer)", "regoperator"),
            };
            format!("SELECT ('{}'::{})::text, ('{}'::{})::oid::int8;", v, t, v, t)
        }
        4 => {
            let t = pick_str(g, &["regconfig", "regdictionary", "regrole"]);
            let v = match t {
                "regconfig" => "english",
                "regdictionary" => "simple",
                _ => "postgres",
            };
            format!("SELECT ('{}'::{})::text;", v, t)
        }
        5 => {
            // Cast-to-pseudotype error path (type-lookup naming the type).
            let t = pick_str(g, &["internal", "trigger", "anyelement", "language_handler"]);
            format!("SELECT 'x'::text::{};", t)
        }
        6 => "SELECT ('(0,1)'::tid)::text, ('10'::text::xid)::text, (cid_in('5'::cstring))::text;".to_string(),
        _ => {
            // "char" / name IO + nameconcatoid.
            format!(
                "SELECT ('{}'::\"char\")::text, (name '{}' = name '{}')::text, nameconcatoid('{}', {});",
                pick_str(g, &["c", "x", "!"]),
                pick_str(g, &["nm", "pg_x"]),
                pick_str(g, &["nm", "pg_x"]),
                pick_str(g, &["pg_class", "t"]),
                g.rng.below(100000)
            )
        }
    };
    raw(sql)
}

/// binary_upgrade_* called outside binary-upgrade mode: each raises the
/// matched "function can only be called when server is in binary upgrade
/// mode" ereport — executing the whole family.
fn gen_binupg(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:binupg");
    let sql = pick_str(g, &[
        "SELECT binary_upgrade_set_next_pg_type_oid('10'::oid);",
        "SELECT binary_upgrade_set_next_array_pg_type_oid('10'::oid);",
        "SELECT binary_upgrade_set_next_heap_pg_class_oid('10'::oid);",
        "SELECT binary_upgrade_set_next_index_pg_class_oid('10'::oid);",
        "SELECT binary_upgrade_set_next_toast_pg_class_oid('10'::oid);",
        "SELECT binary_upgrade_set_next_pg_enum_oid('10'::oid);",
        "SELECT binary_upgrade_set_next_pg_authid_oid('10'::oid);",
        "SELECT binary_upgrade_set_next_pg_tablespace_oid('10'::oid);",
        "SELECT binary_upgrade_set_record_init_privs(true);",
        "SELECT binary_upgrade_set_missing_value('pg_class'::regclass, 'relname', 'x');",
        "SELECT binary_upgrade_create_empty_extension('x', 'pg_catalog', false, '1.0', NULL, NULL, NULL);",
        "SELECT binary_upgrade_logical_slot_has_caught_up('none');",
        "SELECT binary_upgrade_add_sub_rel_state('s', 1, 'r', NULL);",
        "SELECT binary_upgrade_replorigin_advance('s', NULL);",
    ])
    .to_string();
    raw(sql)
}

/// Ruleutils deparse breadth over fuzz-created objects: self-contained
/// bracket (create, pg_get_*def probes, drop) with fixed names — the
/// bracket is one statement group, so no other module interleaves.
fn gen_deparse(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:deparse");
    match g.rng.below(4) {
        0 => {
            // SQL-body function with DEFAULTs + OUT-args proc.
            let body = pick_str(g, &["a + b", "a * b - 1", "abs(a) + abs(b)"]);
            vec![
                StmtKind::Raw("DROP FUNCTION IF EXISTS fz_adtm_fn(int, int);".to_string()),
                StmtKind::Raw(format!(
                    "CREATE FUNCTION fz_adtm_fn(a int, b int DEFAULT {}) RETURNS int LANGUAGE sql IMMUTABLE RETURN {};",
                    g.rng.below(100),
                    body
                )),
                StmtKind::Raw("SELECT pg_get_functiondef('fz_adtm_fn'::regproc);".to_string()),
                StmtKind::Raw("SELECT pg_get_function_arguments('fz_adtm_fn'::regproc), pg_get_function_identity_arguments('fz_adtm_fn'::regproc), pg_get_function_result('fz_adtm_fn'::regproc);".to_string()),
                StmtKind::Raw(format!("SELECT fz_adtm_fn({});", g.rng.below(50))),
                StmtKind::Raw("DROP FUNCTION fz_adtm_fn(int, int);".to_string()),
            ]
        }
        1 => {
            // Table with constraints + expression index WITH (reloptions):
            // indexdef/constraintdef/pg_get_expr(adbin) breadth.
            let ff = 50 + g.rng.below(50);
            vec![
                StmtKind::Raw("DROP TABLE IF EXISTS fz_adtm_dep CASCADE;".to_string()),
                StmtKind::Raw(format!(
                    "CREATE TABLE fz_adtm_dep (a int PRIMARY KEY, b text CHECK (b <> ''), c int DEFAULT {}, UNIQUE (b, c));",
                    g.rng.below(1000)
                )),
                StmtKind::Raw(format!(
                    "CREATE INDEX fz_adtm_dep_idx ON fz_adtm_dep ((a + c)) WITH (fillfactor = {});",
                    ff
                )),
                StmtKind::Raw("SELECT pg_get_indexdef(indexrelid), pg_get_indexdef(indexrelid, 1, true) FROM pg_index WHERE indrelid = 'fz_adtm_dep'::regclass ORDER BY indexrelid::regclass::text;".to_string()),
                StmtKind::Raw("SELECT conname, pg_get_constraintdef(oid), pg_get_constraintdef(oid, true) FROM pg_constraint WHERE conrelid = 'fz_adtm_dep'::regclass ORDER BY conname;".to_string()),
                StmtKind::Raw("SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid = 'fz_adtm_dep'::regclass;".to_string()),
                StmtKind::Raw("DROP TABLE fz_adtm_dep;".to_string()),
            ]
        }
        2 => {
            // Trigger (suppress_redundant_updates_trigger) + triggerdef.
            vec![
                StmtKind::Raw("DROP TABLE IF EXISTS fz_adtm_trg CASCADE;".to_string()),
                StmtKind::Raw("CREATE TABLE fz_adtm_trg (a int);".to_string()),
                StmtKind::Raw("CREATE TRIGGER fz_adtm_zz BEFORE UPDATE ON fz_adtm_trg FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger();".to_string()),
                StmtKind::Raw("SELECT pg_get_triggerdef(oid), pg_get_triggerdef(oid, true) FROM pg_trigger WHERE tgrelid = 'fz_adtm_trg'::regclass ORDER BY tgname;".to_string()),
                StmtKind::Raw("INSERT INTO fz_adtm_trg VALUES (1);".to_string()),
                StmtKind::Raw("UPDATE fz_adtm_trg SET a = a;".to_string()),
                StmtKind::Raw("UPDATE fz_adtm_trg SET a = a + 1;".to_string()),
                StmtKind::Raw("SELECT a FROM fz_adtm_trg ORDER BY a;".to_string()),
                StmtKind::Raw("DROP TABLE fz_adtm_trg;".to_string()),
            ]
        }
        _ => {
            // Catalog-probe forms needing no created objects.
            let sql = pick_str(g, &[
                "SELECT pg_get_userbyid(10);",
                "SELECT pg_get_functiondef('pg_get_keywords'::regproc) IS NOT NULL;",
                "SELECT pg_get_viewdef('pg_roles'::regclass) IS NOT NULL, length(pg_get_viewdef('pg_roles', true)) > 0;",
                "SELECT pg_get_indexdef('pg_class_oid_index'::regclass);",
                "SELECT word, catcode FROM pg_get_keywords() WHERE word IN ('select', 'abort', 'between', 'xmlattributes') ORDER BY word;",
                "SELECT count(*) > 400 FROM pg_get_keywords();",
                "SELECT pg_basetype('int4'::regtype)::text, format_type('numeric'::regtype::oid, 655366);",
            ])
            .to_string();
            raw(sql)
        }
    }
}

/// Composite/record comparisons + record image ops + tid ops + oidvector
/// + aclitem machinery (role oid 10 only — fuzz-role oids differ across
/// engines and never reach an acl output; standing adtmisc law).
fn gen_rec(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:rec");
    let sql = match g.rng.below(8) {
        0 => {
            let op = pick_str(g, &["<", "<=", "=", ">=", ">", "<>"]);
            format!(
                "SELECT (ROW({}, '{}') {} ROW({}, '{}'))::text;",
                g.rng.below(5),
                pick_str(g, &["a", "b"]),
                op,
                g.rng.below(5),
                pick_str(g, &["a", "b"])
            )
        }
        1 => {
            let op = pick_str(g, &["*=", "*<>", "*<", "*<=", "*>", "*>="]);
            format!(
                "SELECT (ROW({}, '{}') {} ROW({}, '{}'))::text;",
                g.rng.below(3),
                pick_str(g, &["x", "y"]),
                op,
                g.rng.below(3),
                pick_str(g, &["x", "y"])
            )
        }
        2 => format!(
            "SELECT max(v)::text, min(v)::text FROM (VALUES (ROW(1, {})), (ROW(1, {})), (ROW(0, 9))) t(v);",
            g.rng.below(10),
            g.rng.below(10)
        ),
        3 => format!(
            "SELECT (ROW({}, {}) IS DISTINCT FROM ROW({}, {}))::text, (ROW(1, NULL) IS NULL)::text;",
            g.rng.below(3), g.rng.below(3), g.rng.below(3), g.rng.below(3)
        ),
        4 => {
            let t1 = format!("({},{})", g.rng.below(5), g.rng.below(10));
            let t2 = format!("({},{})", g.rng.below(5), g.rng.below(10));
            format!(
                "SELECT ('{}'::tid)::text, ('{}'::tid {} '{}'::tid)::text, hashtid('{}'::tid) = hashtid('{}'::tid);",
                t1, t1, pick_str(g, &["<", "=", ">", "<>"]), t2, t1, t1
            )
        }
        5 => format!(
            "SELECT ('{}'::oidvector)::text, oidvectorin('23 25'::cstring)::text, ('1 2'::oidvector = '1 2'::oidvector)::text;",
            pick_str(g, &["1 2 3", "23 25 16", ""])
        ),
        6 => "SELECT aclitemout(aclitemin('postgres=arwdDxtm/postgres'::cstring)), ('postgres=r/postgres'::aclitem = 'postgres=r/postgres'::aclitem)::text;".to_string(),
        _ => {
            // Whole-row/star expansion + attribute-on-noncomposite error.
            if g.rng.chance(1, 3) {
                "SELECT (1).foo;".to_string()
            } else {
                let t = g.pick_table().name.clone();
                format!("SELECT ({}.*)::text FROM {} ORDER BY 1 LIMIT 3;", t, t)
            }
        }
    };
    raw(sql)
}

/// uuid family: v4/v7 generation compared through stable predicates only
/// (values are per-engine random), literal cmp/hash/extract compared raw.
fn gen_uuid(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:uuid");
    let u1 = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    let u2 = "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    let u7 = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
    let sql = match g.rng.below(6) {
        0 => format!(
            "SELECT ('{}'::uuid {} '{}'::uuid)::text, uuid_hash('{}') = uuid_hash('{}');",
            u1, pick_str(g, &["<", "<=", "=", ">=", ">", "<>"]), u2, u1, u1
        ),
        1 => format!(
            "SELECT uuid_extract_version('{}')::text, uuid_extract_timestamp('{}')::text;",
            u7, u7
        ),
        2 => "SELECT (uuid_extract_version(uuidv7()) = 7)::text, uuid_extract_timestamp(uuidv7()) IS NOT NULL;".to_string(),
        3 => "SELECT (uuidv7(interval '1 hour') IS NOT NULL)::text, (uuidv7(interval '-1 hour') IS NOT NULL)::text;".to_string(),
        4 => "SELECT (gen_random_uuid() <> gen_random_uuid())::text, uuid_extract_version(uuidv4())::text;".to_string(),
        _ => format!(
            "SELECT max(u)::text, min(u)::text FROM (VALUES ('{}'::uuid), ('{}'::uuid)) v(u);",
            u1, u2
        ),
    };
    raw(sql)
}

/// Long IN (...) constant lists — the hashed-ScalarArrayOp threshold; int
/// and text element types, hit and miss probes.
fn gen_inlist(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:inlist");
    let n = 20 + g.rng.below(40);
    let probe_hit = g.rng.chance(1, 2);
    let sql = if g.rng.chance(1, 2) {
        let list: Vec<String> = (1..=n).map(|i| i.to_string()).collect();
        let probe = if probe_hit { 1 + g.rng.below(n) } else { n + 1 + g.rng.below(50) };
        format!("SELECT {} IN ({});", probe, list.join(", "))
    } else {
        let list: Vec<String> = (0..n).map(|i| format!("'k{}'", i)).collect();
        let probe = if probe_hit { format!("'k{}'", g.rng.below(n)) } else { "'zz'".to_string() };
        format!("SELECT {} IN ({});", probe, list.join(", "))
    };
    raw(sql)
}

/// CREATE TYPE AS ENUM bracket: comparisons, min/max, enum_range/first/
/// last, matched invalid-value error; fixed name, self-contained group.
fn gen_enum(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:enum");
    let labels = ["sad", "ok", "happy", "ecstatic"];
    let k = 2 + g.rng.below_usize(3).min(labels.len() - 2);
    let quoted: Vec<String> = labels[..k].iter().map(|l| format!("'{}'", l)).collect();
    let a = labels[g.rng.below_usize(k)];
    let b = labels[g.rng.below_usize(k)];
    let mut out = vec![
        StmtKind::Raw("DROP TYPE IF EXISTS fz_adtm_mood CASCADE;".to_string()),
        StmtKind::Raw(format!("CREATE TYPE fz_adtm_mood AS ENUM ({});", quoted.join(", "))),
        StmtKind::Raw(format!(
            "SELECT ('{}'::fz_adtm_mood)::text, ('{}'::fz_adtm_mood {} '{}'::fz_adtm_mood)::text, enum_range(NULL::fz_adtm_mood)::text;",
            a, a, pick_str(g, &["<", "<=", "=", ">=", ">", "<>"]), b
        )),
        StmtKind::Raw(format!(
            "SELECT enum_first(NULL::fz_adtm_mood)::text, enum_last(NULL::fz_adtm_mood)::text, enum_range('{}'::fz_adtm_mood, '{}'::fz_adtm_mood)::text;",
            labels[0], a
        )),
        StmtKind::Raw(format!(
            "SELECT min(m)::text, max(m)::text FROM (VALUES ('{}'::fz_adtm_mood), ('{}')) v(m);",
            a, b
        )),
    ];
    if g.rng.chance(1, 4) {
        // Matched 22P02: invalid input value for enum.
        out.push(StmtKind::Raw("SELECT 'grumpy'::fz_adtm_mood;".to_string()));
    }
    out.push(StmtKind::Raw("DROP TYPE fz_adtm_mood;".to_string()));
    out
}

/// Misc probe functions: md5/crc32/bit_count, num_nulls, pg_typeof,
/// input-validity introspection, index-property fns, SIMILAR TO,
/// IS DISTINCT/IS TRUE, memory-context shape probes, pg_sleep(0).
fn gen_probe2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:probe2");
    let s = pick_str(g, &["abc", "", "two words", "0123456789abcdef"]).to_string();
    let sql = match g.rng.below(12) {
        0 => format!("SELECT md5('{}'), md5('{}'::bytea);", s, s),
        1 => format!(
            "SELECT crc32('{}'::bytea)::text, crc32c('{}'::bytea)::text, bit_count('\\xdeadbeef'::bytea)::text;",
            s, s
        ),
        2 => "SELECT num_nulls(1, NULL, 'x'), num_nonnulls(1, NULL, 'x'), num_nulls(VARIADIC ARRAY[1, NULL, 3]);".to_string(),
        3 => format!(
            "SELECT pg_typeof({})::text, pg_collation_for('x'::text)::text;",
            pick_str(g, &["1.5", "'a'::text", "'2024-01-01'::date", "ARRAY[1]", "NULL"])
        ),
        4 => {
            let (v, t) = match g.rng.below(4) {
                0 => ("42", "integer"),
                1 => ("xx", "integer"),
                2 => ("123456789", "numeric(5,2)"),
                _ => ("2024-02-30", "date"),
            };
            format!(
                "SELECT pg_input_is_valid('{}', '{}')::text, (pg_input_error_info('{}', '{}')).sql_error_code;",
                v, t, v, t
            )
        }
        5 => "SELECT current_query();".to_string(),
        6 => {
            let p = pick_str(g, &["clusterable", "index_scan", "bitmap_scan", "backward_scan", "bogus"]);
            let cp = pick_str(g, &["asc", "desc", "nulls_first", "orderable", "distance_orderable", "returnable"]);
            format!(
                "SELECT pg_index_has_property('pg_class_oid_index'::regclass, '{}')::text, pg_index_column_has_property('pg_class_oid_index'::regclass, 1, '{}')::text, pg_indexam_has_property((SELECT oid FROM pg_am WHERE amname = 'btree'), 'can_order')::text;",
                p, cp
            )
        }
        7 => {
            let pat = pick_str(g, &["a%", "_bc", "(a|b)%", "%(b|d)%", "a{2,}"]);
            format!(
                "SELECT ('abc' SIMILAR TO '{}')::text, similar_escape('a|b%', '|'), similar_to_escape('x%');",
                pat
            )
        }
        8 => format!(
            "SELECT ({} IS DISTINCT FROM {})::text, (true IS TRUE)::text, (NULL::bool IS UNKNOWN)::text, (false IS NOT FALSE)::text;",
            pick_str(g, &["1", "NULL", "2"]),
            pick_str(g, &["1", "NULL"])
        ),
        9 => "SELECT count(*) >= 0 FROM pg_backend_memory_contexts;".to_string(),
        10 => format!("SELECT to_ascii('{}'), to_ascii('{}', 'LATIN1');", s, s),
        _ => "SELECT pg_sleep(0);".to_string(),
    };
    raw(sql)
}

/// Hand-verified parser/executor error paths: fuzzy-match column HINTs,
/// unknown-function signature formatting, attribute-on-scalar, cast to
/// missing type, >1GB palloc, lexer \u escapes, oversized tokens,
/// out-of-int-range numeric literal promotion.
fn gen_errpath(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:errpath");
    let sql = match g.rng.below(9) {
        0 => {
            // Misspelled column -> fuzzy-match HINT (per-table).
            let t = g.pick_table().name.clone();
            let c = g.pick_table().columns[0].name.clone();
            format!("SELECT {}x FROM {};", c, t)
        }
        1 => "SELECT lengthh('x');".to_string(),
        2 => format!("SELECT length('x', {});", g.rng.below(5)),
        3 => "SELECT ''::fz_adtm_no_such_type;".to_string(),
        4 => "SELECT repeat('x', 2000000000)::text || repeat('y', 2000000000);".to_string(),
        5 => pick_str(g, &["SELECT E'\\ud800';", "SELECT E'\\uzzzz';", "SELECT U&'\\d800';"]).to_string(),
        6 => "SELECT 99999999999999999999 + 0.5;".to_string(),
        7 => format!("SELECT length(repeat('{}', 8192) || 'tail');", pick_str(g, &["z", "ab", "\u{00e9}"])),
        _ => {
            // Qualified-reference / RTE-namespace error paths.
            let t = g.pick_table().name.clone();
            match g.rng.below(3) {
                0 => format!("SELECT {}.nosuchcol FROM {};", t, t),
                1 => format!("SELECT badalias.{} FROM {} realalias;", "pk", t),
                _ => format!("SELECT public.{}.pk FROM {} LIMIT 1;", t, t),
            }
        }
    };
    raw(sql)
}

// =====================================================================
// LD10 adt-misc-residue shapes.
// =====================================================================

fn rawv(stmts: Vec<String>) -> Vec<StmtKind> {
    stmts.into_iter().map(StmtKind::Raw).collect()
}

/// Types whose input functions carry safe-error arms worth draining.
const SAFE_TYPES: &[&str] = &[
    "int2", "int4", "int8", "money", "bit(8)", "varbit", "inet", "cidr",
    "uuid", "numeric", "macaddr", "macaddr8", "date", "interval", "box",
    "bytea", "oid", "regtype",
];

/// Edge inputs for the numutils/cash/varbit/inet safe parsers: hex/octal/
/// binary radix forms, digit-group underscores (valid and malformed),
/// whitespace, signs, overflow at each width, and junk.
const SAFE_INPUTS: &[&str] = &[
    "0x1A", "0X7fff", "0o17", "0O777", "0b101", "0B11", "1_000_000",
    "0x7fff_ffff", " 42 ", "  +7", "-32768", "32768", "-32769",
    "2147483648", "-2147483649", "9223372036854775808",
    "-9223372036854775809", "99999999999999999999999999", "12abc", "",
    "   ", "+", "-", "0x", "0b", "1__0", "_1", "1_", "1e5", "1.5",
    "0x8000000000000000", "nan", "10.0.0.1/33", "not-a-uuid",
    "1.2.3.4.5", "$1,2,3", "--5", "£7",
];

/// pg_input_is_valid / pg_input_error_info: reaches every soft-error arm
/// of pg_strtoint{16,32,64}_safe & friends without erroring, plus direct
/// casts of the VALID radix/underscore forms (the hard-path twins).
fn gen_strsafe(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:strsafe");
    let n = 3 + g.rng.below(3);
    let cols: Vec<String> = (0..n)
        .map(|_| {
            let ty = *g.rng.pick(SAFE_TYPES);
            let inp = esc_s(g.rng.pick(SAFE_INPUTS));
            if g.rng.chance(1, 3) {
                format!(
                    "(pg_input_error_info('{inp}', '{ty}')).sql_error_code"
                )
            } else {
                format!("pg_input_is_valid('{inp}', '{ty}')")
            }
        })
        .collect();
    let mut stmts = vec![format!("SELECT {};", cols.join(", "))];
    if g.rng.chance(1, 2) {
        // Direct-cast twins of the valid radix forms (non-safe arms).
        stmts.push(
            "SELECT '0x1A'::int2::text, '0o17'::int4::text, '0b101'::int8::text, \
             '1_000_000'::int4::text, '0x7fff_ffff'::int4::text, \
             '-0x8000'::int2::text;"
                .to_string(),
        );
    }
    rawv(stmts)
}

/// SIMILAR TO escape machinery (similar_escape_internal) + regexp flag
/// parsing (parse_re_flags) + the regexp_* argument tail.
fn gen_regex2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:regex2");
    let pats: &[&str] = &[
        "%(b|d)%", "_a_", "%[abc]%", "(a|b)*c+", "a{2,4}b", "ab|cd",
        "%#\"o#\"%", "a#%b", "50#%", "a_c%", "[^x]+", "(ab)?cd",
    ];
    let flags: &[&str] = &["", "i", "g", "n", "p", "w", "x", "gi", "ic", "q", "s", "m", "t", "b", "z", "1"];
    let subj: &[&str] = &["abcde", "banana", "50%", "aXcYe", "", "aaa bbb ccc", "AbCdAb"];
    let sql = match g.rng.below(7) {
        0 => {
            let e = *g.rng.pick(&["#", "\\", "", "x"]);
            format!(
                "SELECT '{}' SIMILAR TO '{}' ESCAPE '{}';",
                esc_s(g.rng.pick(subj)),
                esc_s(g.rng.pick(pats)),
                e
            )
        }
        1 => format!(
            "SELECT '{}' NOT SIMILAR TO '{}';",
            esc_s(g.rng.pick(subj)),
            esc_s(g.rng.pick(pats))
        ),
        2 => format!(
            "SELECT substring('{}' SIMILAR '{}' ESCAPE '#');",
            esc_s(g.rng.pick(subj)),
            esc_s(g.rng.pick(pats))
        ),
        3 => format!(
            "SELECT regexp_replace('{}', '(a|b)', 'Z', '{}');",
            esc_s(g.rng.pick(subj)),
            *g.rng.pick(flags)
        ),
        4 => format!(
            "SELECT regexp_count('{s}', 'a', {p}), regexp_instr('{s}', '(a)(b)?', {p}, {o}, {e}, '', {sub})",
            s = esc_s(g.rng.pick(subj)),
            p = 1 + g.rng.below(3),
            o = 1 + g.rng.below(2),
            e = g.rng.below(2),
            sub = g.rng.below(3),
        ) + ";",
        5 => format!(
            "SELECT regexp_substr('{s}', '[ab]+', {p}, {o}, '{f}'), regexp_like('{s}', 'a.c', '{f}');",
            s = esc_s(g.rng.pick(subj)),
            p = 1 + g.rng.below(2),
            o = 1 + g.rng.below(2),
            f = *g.rng.pick(&["", "i", "s", "n"]),
        ),
        _ => format!(
            "SELECT regexp_split_to_array('{}', '[b ]', '{}')::text;",
            esc_s(g.rng.pick(subj)),
            *g.rng.pick(&["", "i", "x", "g"])
        ),
    };
    raw(sql)
}

/// Advisory-lock bracket feeding pg_lock_status: take a deterministic set
/// of session + xact advisory locks, project the advisory rows of
/// pg_locks (keys and modes are deterministic; pids never selected), then
/// release everything in-group.
fn gen_locks(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:locks");
    let k = g.rng.below(1000);
    rawv(vec![
        format!(
            "SELECT pg_advisory_lock({k}), pg_advisory_lock({k}, {}), \
             pg_advisory_lock_shared({}), pg_try_advisory_lock({}), \
             pg_try_advisory_lock_shared({k}, 7);",
            k + 1,
            k + 2,
            k + 3
        ),
        format!(
            "SELECT pg_try_advisory_xact_lock({}), pg_advisory_xact_lock_shared({}, 9);",
            k + 4,
            k
        ),
        "SELECT locktype, database IS NOT NULL AS hasdb, classid, objid, objsubid, \
         mode, granted, fastpath FROM pg_locks \
         WHERE locktype = 'advisory' ORDER BY classid, objid, objsubid, mode;"
            .to_string(),
        "SELECT pg_advisory_unlock_all();".to_string(),
    ])
}

/// pg_stat_get_activity via deterministic own-backend projections.
fn gen_activity(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:activity");
    let sql = match g.rng.below(4) {
        0 => "SELECT datname, usename IS NOT NULL, state, backend_type, \
              wait_event_type IS NOT NULL, query_id IS NULL \
              FROM pg_stat_activity WHERE pid = pg_backend_pid();"
            .to_string(),
        1 => "SELECT count(*) >= 1 FROM pg_stat_activity WHERE backend_type = 'client backend';"
            .to_string(),
        2 => "SELECT state, xact_start IS NOT NULL, query <> '' \
              FROM pg_stat_activity WHERE pid = pg_backend_pid();"
            .to_string(),
        _ => "SELECT pg_stat_get_backend_activity(pg_backend_pid()) <> '', \
              pg_stat_get_backend_dbid(pg_backend_pid()) IS NOT NULL;"
            .to_string(),
    };
    raw(sql)
}

/// pg_hba_file_rules / pg_ident_file_mappings: both clusters run the
/// identical C-initdb default files, so full-row projections (minus
/// file_name/line_number paths) compare exactly.
fn gen_hba(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:hba");
    let sql = if g.rng.chance(1, 4) {
        "SELECT count(*) FROM pg_ident_file_mappings;".to_string()
    } else {
        "SELECT type, database::text, user_name::text, address, netmask, \
         auth_method, options::text, error FROM pg_hba_file_rules \
         ORDER BY rule_number;"
            .to_string()
    };
    raw(sql)
}

/// amutils property matrix: every property name (plus a bogus one) against
/// every AM / an index of that AM / its first column.
fn gen_amprop(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:amprop");
    let mut stmts = vec![
        "CREATE TABLE fz_amp (i int, t text, b box);".to_string(),
        "CREATE INDEX fz_amp_bt ON fz_amp USING btree (i DESC NULLS LAST, t);".to_string(),
        "CREATE INDEX fz_amp_hash ON fz_amp USING hash (i);".to_string(),
        "CREATE INDEX fz_amp_gist ON fz_amp USING gist (b);".to_string(),
        "CREATE INDEX fz_amp_spg ON fz_amp USING spgist (b);".to_string(),
        "CREATE INDEX fz_amp_brin ON fz_amp USING brin (i);".to_string(),
        "CREATE INDEX fz_amp_gin ON fz_amp USING gin (to_tsvector('simple', t));".to_string(),
    ];
    let idx = *g.rng.pick(&[
        "fz_amp_bt", "fz_amp_hash", "fz_amp_gist", "fz_amp_spg", "fz_amp_brin", "fz_amp_gin",
    ]);
    stmts.push(format!(
        "SELECT p, pg_index_column_has_property('{idx}'::regclass, 1, p) \
         FROM unnest(ARRAY['asc','desc','nulls_first','nulls_last','orderable',\
'distance_orderable','returnable','search_array','search_nulls','bogus']) p ORDER BY p;"
    ));
    stmts.push(format!(
        "SELECT p, pg_index_has_property('{idx}'::regclass, p) \
         FROM unnest(ARRAY['clusterable','index_scan','bitmap_scan','backward_scan','bogus']) p ORDER BY p;"
    ));
    let am = *g.rng.pick(&["btree", "hash", "gist", "spgist", "brin", "gin"]);
    stmts.push(format!(
        "SELECT p, pg_indexam_has_property((SELECT oid FROM pg_am WHERE amname = '{am}'), p) \
         FROM unnest(ARRAY['can_order','can_unique','can_multi_col','can_exclude','can_include','bogus']) p ORDER BY p;"
    ));
    stmts.push("DROP TABLE fz_amp;".to_string());
    rawv(stmts)
}

/// inet/cidr abbreviated forms (inet_cidr_pton_ipv4 short-form arms) and
/// the network function/operator tail.
fn gen_inet2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:inet2");
    let cidrs: &[&str] = &[
        "10/8", "10.1/16", "192.168/24", "172.16.5/24", "10.1.2.3/32",
        "255.255.255.255/32", "0/0", "128.1.2/24", "2001:db8::/32",
        "::ffff:1.2.3.4/120", "ff00::/8",
    ];
    let inets: &[&str] = &[
        "10.1.2.3/8", "192.168.1.226", "::1", "fe80::1%1/64",
        "2001:db8::8:800:200c:417a/64", "0.0.0.0/32", "255.254.253.252",
        "10.1.2.3/25",
    ];
    let sql = match g.rng.below(5) {
        0 => format!(
            "SELECT ('{}'::cidr)::text, abbrev('{}'::cidr), broadcast('{}'::inet)::text;",
            g.rng.pick(cidrs),
            g.rng.pick(cidrs),
            g.rng.pick(inets)
        ),
        1 => format!(
            "SELECT host('{i}'::inet), masklen('{i}'::inet), family('{i}'::inet), \
             netmask('{i}'::inet)::text, hostmask('{i}'::inet)::text, network('{i}'::inet)::text;",
            i = g.rng.pick(inets)
        ),
        2 => format!(
            "SELECT set_masklen('{}'::inet, {})::text, set_masklen('{}'::cidr, {})::text;",
            g.rng.pick(inets),
            g.rng.below(33),
            g.rng.pick(cidrs),
            g.rng.below(25)
        ),
        3 => format!(
            "SELECT inet_same_family('{a}'::inet, '{b}'::inet), \
             inet_merge('{a}'::inet, '{b}'::inet)::text, \
             ('{a}'::inet << '{b}'::inet), ('{a}'::inet &&  '{b}'::inet), \
             ('{a}'::inet >>= '{b}'::inet);",
            a = g.rng.pick(inets),
            b = g.rng.pick(inets)
        ),
        _ => format!(
            "SELECT ('{i}'::inet + {n})::text, ('{i}'::inet - {n})::text, \
             ('{i}'::inet - '{j}'::inet), (~'{i}'::inet)::text, \
             ('{i}'::inet & '{j}'::inet)::text, ('{i}'::inet | '{j}'::inet)::text;",
            i = g.rng.pick(inets),
            j = g.rng.pick(inets),
            n = g.rng.below(1000)
        ),
    };
    raw(sql)
}

// ===================================================================
// Q7 expr-misc-adt families (hand-verified deck-misc1/pseudo/deparse/ri;
// see the SHAPES comment for the adtm:xml coverage-only exception).
// ===================================================================

/// Misc adt probe tail: to_ascii, pg_current_logfile, current_query,
/// pg_basetype/pg_column_is_updatable, nested typmod casts, array-element
/// coercion + virtual-slot fast paths, multibyte wrappers, random(lo,hi),
/// popcount/bit_count, SIMD-length strings, >1GB palloc.
fn gen_misc2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:misc2");
    let sql = match g.rng.below(12) {
        0 => {
            let s = pick_str(g, &["abc", "Hello", "x y z"]);
            match g.rng.below(3) {
                0 => format!("SELECT to_ascii('{}');", s),
                1 => format!("SELECT to_ascii('{}', 'LATIN1');", s),
                _ => format!("SELECT to_ascii('{}', 8);", s),
            }
        }
        1 => pick_str(g, &[
            "SELECT pg_current_logfile() IS NULL;",
            "SELECT pg_current_logfile('stderr') IS NULL;",
            "SELECT pg_current_logfile('csvlog') IS NULL;",
        ])
        .to_string(),
        2 => "SELECT current_query();".to_string(),
        3 => {
            let t = pick_str(g, &["int4", "varchar", "numeric", "int8", "text"]);
            format!("SELECT pg_basetype('{}'::regtype)::text;", t)
        }
        4 => {
            // Nested typmod casts (relabel_to_typmod via length-coercion
            // support) + varbit_support nesting.
            let n1 = 4 + g.rng.below(16);
            let n2 = 2 + g.rng.below(8);
            match g.rng.below(3) {
                0 => format!("SELECT ('abcdefgh'::varchar({}))::varchar({});", n1, n2),
                1 => format!("SELECT ('ab'::varchar({}))::varchar({})::varchar({});", n2, n1, n2),
                _ => format!("SELECT (B'1101'::varbit)::varbit({})::varbit({});", n1, n2),
            }
        }
        5 => {
            // ArrayCoerce element cast (func-over-CASE-test fast path) and
            // a bare Var over a virtual (VALUES) slot.
            match g.rng.below(3) {
                0 => format!("SELECT '{{{},{}}}'::int4[]::int8[];", g.rng.below(100), g.rng.below(100)),
                1 => format!("SELECT '{{{}}}'::int4[]::numeric[]::text;", g.rng.below(100)),
                _ => format!("SELECT x FROM (VALUES ({}), ({})) v(x);", g.rng.below(50), g.rng.below(50)),
            }
        }
        6 => {
            let s = pick_str(g, &["h\u{00e9}llo w\u{00f6}rld", "\u{65e5}\u{672c}\u{8a9e}abc"]);
            format!(
                "SELECT substr('{}', {}, 3), char_length('{}'), position('\u{672c}' in '\u{65e5}\u{672c}\u{8a9e}');",
                s,
                1 + g.rng.below(3),
                s
            )
        }
        7 => "SELECT length('\\xdead'::bytea, 'LATIN1');".to_string(),
        8 => {
            // Sampling PRNG (pg_prng int paths) through TABLESAMPLE with a
            // REPEATABLE seed; counts stay off the compare surface (the
            // sampler stream is engine-internal), only >= 0 is compared.
            let pct = 10 + g.rng.below(80);
            let seed = g.rng.below(1000);
            let meth = pick_str(g, &["SYSTEM", "BERNOULLI"]);
            return vec![
                StmtKind::Raw("DROP TABLE IF EXISTS fz_q7sam;".to_string()),
                StmtKind::Raw("CREATE TABLE fz_q7sam AS SELECT g AS x FROM generate_series(1, 200) g;".to_string()),
                StmtKind::Raw(format!(
                    "SELECT count(*) >= 0 FROM fz_q7sam TABLESAMPLE {meth} ({pct}) REPEATABLE ({seed});"
                )),
                StmtKind::Raw("DROP TABLE fz_q7sam;".to_string()),
            ];
        }
        9 => {
            let n = 200 + g.rng.below(200);
            format!(
                "SELECT bit_count(repeat('ff', {})::bytea), bit_count(B'10110111011101110111011101110111011101110101');",
                n
            )
        }
        10 => {
            let n = 3000 + g.rng.below(3000);
            match g.rng.below(3) {
                0 => format!("SELECT length(repeat('abcdefgh', {}));", n),
                1 => format!("SELECT length(repeat('h\u{00e9}llo\u{65e5}\u{672c}', {}));", n),
                _ => format!("SELECT octet_length(quote_literal(repeat('a', {})));", n),
            }
        }
        _ => {
            if err_arm(g) {
                "SELECT repeat('xy', 800000000);".to_string()
            } else {
                "SELECT pg_column_is_updatable('pg_class'::regclass, 1, true)::text;".to_string()
            }
        }
    };
    raw(sql)
}

/// Parser/namespace error+hint machinery: fuzzy-match column HINTs over a
/// bracket-local table, the would-be-legal-with-LATERAL hint, qualified
/// schema.table.column resolution, whole-row star expansion, misplaced
/// outer-level vars, shell-type casts, lexer escape errors.
fn gen_errhint(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:errhint");
    let mut v = vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_q7eh CASCADE;".to_string()),
        StmtKind::Raw("CREATE TABLE fz_q7eh (aa int, ab int, zz text);".to_string()),
    ];
    match g.rng.below(8) {
        0 => v.push(StmtKind::Raw(
            format!("SELECT {} FROM fz_q7eh;", pick_str(g, &["ac", "abb", "zzz", "az"])),
        )),
        1 => v.push(StmtKind::Raw(
            format!("SELECT fz_q7eh.{} FROM fz_q7eh;", pick_str(g, &["ba", "zy"])),
        )),
        2 => v.push(StmtKind::Raw(
            "SELECT * FROM fz_q7eh x, (SELECT x.aa) y;".to_string(),
        )),
        3 => v.push(StmtKind::Raw(
            "SELECT public.fz_q7eh.aa FROM public.fz_q7eh LIMIT 0;".to_string(),
        )),
        4 => v.push(StmtKind::Raw("SELECT (t).* FROM fz_q7eh t;".to_string())),
        5 => v.push(StmtKind::Raw(
            "SELECT 1 UNION SELECT 2 ORDER BY fz_q7eh.aa;".to_string(),
        )),
        6 => {
            v.push(StmtKind::Raw("DROP TYPE IF EXISTS fz_q7sh;".to_string()));
            v.push(StmtKind::Raw("CREATE TYPE fz_q7sh;".to_string()));
            v.push(StmtKind::Raw("SELECT '1'::fz_q7sh;".to_string()));
            v.push(StmtKind::Raw("DROP TYPE fz_q7sh;".to_string()));
        }
        _ => v.push(StmtKind::Raw(
            pick_str(g, &[
                "SELECT E'\\uZZZZ';",
                "SELECT E'\\ud800';",
                "SELECT U&'\\+10FFFF' = 'x';",
            ])
            .to_string(),
        )),
    }
    v.push(StmtKind::Raw("DROP TABLE fz_q7eh;".to_string()));
    v
}

/// Regex engine tail: negated/class-mixed brackets, non-greedy quantified
/// captures (creviterdissect), DFA cache pressure from long subjects.
fn gen_regex3(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:regex3");
    let sql = match g.rng.below(5) {
        0 => format!(
            "SELECT '{}' ~ '[^a-c[:digit:]]';",
            pick_str(g, &["x7", "ab", "9", "Z"])
        ),
        1 => format!(
            "SELECT '{}' ~ '[^[:alpha:][:blank:]x-z]';",
            pick_str(g, &["abq", "a b", "x-", "9!"])
        ),
        2 => format!(
            "SELECT regexp_replace('{}', '(x)+?', 'z'), substring('{}' from '((a)+?b)');",
            pick_str(g, &["xxxy", "xyx", "zz"]),
            pick_str(g, &["aaabbb", "ab", "ba"])
        ),
        3 => format!(
            "SELECT regexp_matches('{}', '((x)+?(y)+?)')::text;",
            pick_str(g, &["xxyy", "xy", "yx"])
        ),
        _ => {
            let n = 4000 + g.rng.below(4000);
            format!("SELECT repeat('ab', {}) ~ '(ab)*(cd)?e?f';", n)
        }
    };
    raw(sql)
}

/// Enum comparison breadth over a bracket-local enum: sort, min/max,
/// enum_range/first/last, the enum_cmp support proc.
fn gen_enum2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:enum2");
    let probe = match g.rng.below(4) {
        0 => "SELECT x FROM (VALUES ('hi'::fz_q7en), ('lo'), ('mid')) v(x) ORDER BY x;",
        1 => "SELECT min(x)::text, max(x)::text FROM (VALUES ('hi'::fz_q7en), ('lo'), ('mid')) v(x);",
        2 => "SELECT enum_range('lo'::fz_q7en, 'hi'::fz_q7en)::text, enum_first(NULL::fz_q7en)::text, enum_last(NULL::fz_q7en)::text;",
        _ => "SELECT enum_cmp('lo'::fz_q7en, 'hi'::fz_q7en), enum_cmp('hi'::fz_q7en, 'hi'::fz_q7en);",
    };
    vec![
        StmtKind::Raw("DROP TYPE IF EXISTS fz_q7en;".to_string()),
        StmtKind::Raw("CREATE TYPE fz_q7en AS ENUM ('lo', 'mid', 'hi');".to_string()),
        StmtKind::Raw(probe.to_string()),
        StmtKind::Raw("DROP TYPE fz_q7en;".to_string()),
    ]
}

/// Record tail: full comparison/image-operator sweep, record_in through a
/// bracket-local composite type, record hashing (direct + hashed DISTINCT).
fn gen_rec2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:rec2");
    match g.rng.below(4) {
        0 => {
            let a = g.rng.below(5);
            let b = g.rng.below(5);
            raw(format!(
                "SELECT (ROW({a},'b') > ROW({b},'a'))::text, (ROW({a},'a') >= ROW({b},'a'))::text, (ROW({a},'a') <= ROW({b},'b'))::text, (ROW({a},'a') <> ROW({b},'b'))::text;"
            ))
        }
        1 => {
            let a = g.rng.below(3);
            raw(format!(
                "SELECT (ROW({a},'b') *> ROW(1,'a'))::text, (ROW({a},'a') *>= ROW(1,'a'))::text, (ROW({a},'a') *<= ROW(1,'b'))::text, (ROW({a},'a') *<> ROW(1,'b'))::text, (ROW({a},'a') *< ROW(1,'b'))::text;"
            ))
        }
        2 => raw(format!(
            "SELECT hash_record(ROW({}, 'a'::text)), hash_record_extended(ROW({}, 'a'::text), {});",
            g.rng.below(10),
            g.rng.below(10),
            g.rng.below(100)
        )),
        _ => {
            let a = g.rng.below(9);
            vec![
                StmtKind::Raw("DROP TYPE IF EXISTS fz_q7ct;".to_string()),
                StmtKind::Raw("CREATE TYPE fz_q7ct AS (a int, b text);".to_string()),
                StmtKind::Raw(format!("SELECT '({a},xy)'::fz_q7ct, ('({a},zz)'::fz_q7ct).b;")),
                StmtKind::Raw(format!(
                    "SELECT DISTINCT v::text FROM (VALUES (ROW(1,{a})), (ROW(1,{a})), (ROW(3,4))) t(v) ORDER BY 1;"
                )),
                StmtKind::Raw("DROP TYPE fz_q7ct;".to_string()),
            ]
        }
    }
}

/// uuid tail: comparison/support-proc breadth over fixed literals, v4/v7
/// generators projected through engine-stable extractors only.
fn gen_uuid2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:uuid2");
    let lo = "11111111-2222-3333-4444-555555555555";
    let hi = "ffffffff-2222-3333-4444-555555555555";
    let sql = match g.rng.below(5) {
        0 => format!("SELECT '{lo}'::uuid < '{hi}'::uuid, '{hi}'::uuid > '{lo}'::uuid, '{lo}'::uuid <> '{hi}'::uuid;"),
        1 => format!("SELECT uuid_cmp('{lo}', '{hi}'), uuid_cmp('{hi}', '{lo}'), uuid_cmp('{lo}', '{lo}');"),
        2 => format!("SELECT uuid_hash_extended('{lo}', {}), uuid_hash('{hi}');", g.rng.below(100)),
        3 => pick_str(g, &[
            "SELECT uuid_extract_version(gen_random_uuid());",
            "SELECT uuid_extract_version(uuidv7());",
            "SELECT uuid_extract_version(uuidv4());",
            "SELECT uuid_extract_timestamp(uuidv7()) IS NOT NULL;",
            "SELECT uuid_extract_timestamp('11111111-2222-3333-4444-555555555555'::uuid) IS NULL;",
        ])
        .to_string(),
        _ => format!("SELECT min(u)::text, max(u)::text FROM (VALUES ('{lo}'::uuid), ('{hi}'), ('00000000-2222-3333-4444-555555555555')) v(u);"),
    };
    raw(sql)
}

/// varbit tail: int8<->bit casts, substring(bit) incl. no-length form,
/// typmod display via format_type, bitcmp through min/max.
fn gen_bit3(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:bit3");
    let sql = match g.rng.below(6) {
        0 => format!("SELECT {}::bigint::bit(64);", 1 + g.rng.below(1 << 30)),
        1 => "SELECT (B'101'::bit(64))::bigint, (B'1'::bit(8))::bigint;".to_string(),
        2 => {
            let f = 1 + g.rng.below(5);
            format!("SELECT substring(B'11010111' from {f}), substring(B'11010111' from {f} for 3);")
        }
        3 => format!(
            "SELECT format_type('bit'::regtype::oid, {}), format_type('varbit'::regtype::oid, {}), format_type('bit'::regtype::oid, NULL);",
            4 + g.rng.below(20),
            4 + g.rng.below(20)
        ),
        4 => "SELECT min(b)::text, max(b)::text FROM (VALUES (B'101'), (B'011'), (B'110')) v(b);".to_string(),
        _ => format!(
            "SELECT '{}'::varbit(8), '1010'::bit({});",
            pick_str(g, &["1010", "1", "111111"]),
            4 + g.rng.below(4)
        ),
    };
    raw(sql)
}

/// Polymorphic result-type resolution + hashed ScalarArrayOp IN lists.
fn gen_poly(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:poly");
    let sql = match g.rng.below(4) {
        0 => format!("SELECT array_fill({}, ARRAY[3])::text, array_fill('x'::text, ARRAY[2, 2])::text;", g.rng.below(50)),
        1 => "SELECT range_merge('{[1,2],[5,6]}'::int4multirange)::text, multirange('[1,3]'::int4range)::text;".to_string(),
        2 => {
            // Hashed IN: >8 distinct constants engages the simplehash
            // ScalarArrayOp path; a wide list forces grow/free.
            let n = 30 + g.rng.below(400);
            let consts: Vec<String> = (0..n).map(|i| (i * 3 + 1).to_string()).collect();
            format!(
                "SELECT count(*) FROM generate_series(1, 50) s(x) WHERE x IN ({});",
                consts.join(", ")
            )
        }
        _ => {
            let n = 20 + g.rng.below(100);
            let consts: Vec<String> = (0..n).map(|i| (i * 2 + 1).to_string()).collect();
            format!(
                "SELECT count(*) FROM generate_series(1, 50) s(x) WHERE x NOT IN ({});",
                consts.join(", ")
            )
        }
    };
    raw(sql)
}

/// pglz partial detoast: substr() slices of a compressed toasted value.
fn gen_toastslice(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:toastslice");
    let off = 10 + g.rng.below(1000);
    let far = 150000 + g.rng.below(40000);
    vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_q7ts;".to_string()),
        StmtKind::Raw("CREATE TABLE fz_q7ts (id int, t text);".to_string()),
        StmtKind::Raw(
            "INSERT INTO fz_q7ts VALUES (1, repeat('compressme', 20000) || 'TAIL');".to_string(),
        ),
        StmtKind::Raw(format!("SELECT substr(t, {off}, 20) FROM fz_q7ts WHERE id = 1;")),
        StmtKind::Raw(format!("SELECT substr(t, {far}, 30) FROM fz_q7ts WHERE id = 1;")),
        StmtKind::Raw("DROP TABLE fz_q7ts;".to_string()),
    ]
}

/// Referential-integrity action breadth: ON UPDATE CASCADE / SET NULL /
/// SET DEFAULT / RESTRICT / NO ACTION firing on referenced-key UPDATEs,
/// plus FK-referencing-partitioned-table DETACH (RI_PartitionRemove_Check).
fn gen_ri(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:ri");
    if g.rng.below(3) < 2 {
        let action = pick_str(g, &[
            "ON UPDATE CASCADE ON DELETE CASCADE",
            "ON UPDATE SET NULL ON DELETE SET NULL",
            "ON UPDATE SET DEFAULT",
            "ON UPDATE RESTRICT",
            "",
        ])
        .to_string();
        let k = 1 + g.rng.below(5);
        let mut v = vec![
            StmtKind::Raw("DROP TABLE IF EXISTS fz_q7rc, fz_q7rp CASCADE;".to_string()),
            StmtKind::Raw("CREATE TABLE fz_q7rp (id int PRIMARY KEY, v text);".to_string()),
            StmtKind::Raw(format!(
                "CREATE TABLE fz_q7rc (id int DEFAULT 1 REFERENCES fz_q7rp {}, w int);",
                action
            )),
            StmtKind::Raw(
                "INSERT INTO fz_q7rp SELECT g, 'v' || g FROM generate_series(1, 6) g;".to_string(),
            ),
            StmtKind::Raw(format!("INSERT INTO fz_q7rc VALUES ({k}, 10), ({k}, 11);")),
            StmtKind::Raw(format!("UPDATE fz_q7rp SET id = 20 WHERE id = {k};")),
            StmtKind::Raw(format!("UPDATE fz_q7rp SET id = {k} WHERE id = {k};")),
            StmtKind::Raw(format!("DELETE FROM fz_q7rp WHERE id = {k};")),
            StmtKind::Raw("SELECT id, w FROM fz_q7rc ORDER BY w, id;".to_string()),
            StmtKind::Raw("SELECT id FROM fz_q7rp ORDER BY id;".to_string()),
        ];
        v.push(StmtKind::Raw("DROP TABLE fz_q7rc, fz_q7rp;".to_string()));
        v
    } else {
        vec![
            StmtKind::Raw("DROP TABLE IF EXISTS fz_q7fk, fz_q7pp CASCADE;".to_string()),
            StmtKind::Raw(
                "CREATE TABLE fz_q7pp (a int PRIMARY KEY) PARTITION BY RANGE (a);".to_string(),
            ),
            StmtKind::Raw(
                "CREATE TABLE fz_q7pp1 PARTITION OF fz_q7pp FOR VALUES FROM (0) TO (10);".to_string(),
            ),
            StmtKind::Raw(
                "CREATE TABLE fz_q7pp2 PARTITION OF fz_q7pp FOR VALUES FROM (10) TO (20);".to_string(),
            ),
            StmtKind::Raw("CREATE TABLE fz_q7fk (a int REFERENCES fz_q7pp);".to_string()),
            StmtKind::Raw(format!("INSERT INTO fz_q7pp VALUES ({}), (15);", g.rng.below(10))),
            StmtKind::Raw(format!("INSERT INTO fz_q7fk VALUES ({});", g.rng.below(10))),
            StmtKind::Raw("ALTER TABLE fz_q7pp DETACH PARTITION fz_q7pp1;".to_string()),
            StmtKind::Raw("ALTER TABLE fz_q7pp DETACH PARTITION fz_q7pp2;".to_string()),
            StmtKind::Raw("DELETE FROM fz_q7fk;".to_string()),
            StmtKind::Raw("ALTER TABLE fz_q7pp DETACH PARTITION fz_q7pp1;".to_string()),
            StmtKind::Raw(
                "DROP TABLE fz_q7fk; DROP TABLE IF EXISTS fz_q7pp1, fz_q7pp2, fz_q7pp;".to_string(),
            ),
        ]
    }
}

/// pg_partition_tree / _root / _ancestors probes over a bracket-local
/// two-level partition hierarchy (plus the non-partition empty answers).
fn gen_part3(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:part3");
    let probe = match g.rng.below(4) {
        0 => "SELECT relid::text, parentrelid::text, isleaf, level FROM pg_partition_tree('fz_q7pt') ORDER BY 1;",
        1 => "SELECT pg_partition_root('fz_q7pt2a')::text, pg_partition_root('fz_q7pt')::text;",
        2 => "SELECT relid::text FROM pg_partition_ancestors('fz_q7pt2a') ORDER BY 1;",
        _ => "SELECT count(*) FROM pg_partition_tree('pg_class'); SELECT pg_partition_root('pg_class')::text;",
    };
    vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_q7pt CASCADE;".to_string()),
        StmtKind::Raw(
            "CREATE TABLE fz_q7pt (a int, b text) PARTITION BY RANGE (a);".to_string(),
        ),
        StmtKind::Raw(
            "CREATE TABLE fz_q7pt1 PARTITION OF fz_q7pt FOR VALUES FROM (0) TO (10);".to_string(),
        ),
        StmtKind::Raw(
            "CREATE TABLE fz_q7pt2 PARTITION OF fz_q7pt FOR VALUES FROM (10) TO (20) PARTITION BY LIST (b);"
                .to_string(),
        ),
        StmtKind::Raw(
            "CREATE TABLE fz_q7pt2a PARTITION OF fz_q7pt2 FOR VALUES IN ('x');".to_string(),
        ),
        StmtKind::Raw(probe.to_string()),
        StmtKind::Raw("DROP TABLE fz_q7pt CASCADE;".to_string()),
    ]
}

/// Ruleutils deparse tail: viewdef-by-name, recursive/VALUES/grouping-set/
/// sublink/cast-wrapper views, SQL-body fns (MERGE bodies, arg defaults,
/// OUT-arg procs), pg_get_indexdef column arms, partition-key/constraint
/// deparse over a schema-qualified enum partkey, ATTACH/DETACH deparse.
fn gen_deparse3(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:deparse3");
    match g.rng.below(5) {
        0 => {
            let body = match g.rng.below(4) {
                0 => "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r",
                1 => "VALUES (1, 'a'), (2, 'b')",
                2 => "SELECT a, count(*) AS n FROM fz_q7dp GROUP BY GROUPING SETS ((a), (b), ())",
                _ => "SELECT a FROM fz_q7dp x WHERE EXISTS (SELECT 1 FROM fz_q7dp y WHERE y.a = x.a + 1) AND a IN (SELECT a FROM fz_q7dp) AND a > ANY (SELECT a FROM fz_q7dp)",
            };
            vec![
                StmtKind::Raw("DROP TABLE IF EXISTS fz_q7dp CASCADE;".to_string()),
                StmtKind::Raw("CREATE TABLE fz_q7dp (a int, b text, c numeric(10,2));".to_string()),
                StmtKind::Raw(format!("CREATE VIEW fz_q7dpv AS {};", body)),
                StmtKind::Raw("SELECT pg_get_viewdef('fz_q7dpv');".to_string()),
                StmtKind::Raw("SELECT pg_get_viewdef('fz_q7dpv', true);".to_string()),
                StmtKind::Raw(format!("SELECT pg_get_viewdef('fz_q7dpv', {});", 10 + g.rng.below(60))),
                StmtKind::Raw("DROP TABLE fz_q7dp CASCADE;".to_string()),
            ]
        }
        1 => {
            vec![
                StmtKind::Raw("DROP TABLE IF EXISTS fz_q7dp CASCADE;".to_string()),
                StmtKind::Raw("CREATE TABLE fz_q7dp (a int, b text, c numeric(10,2));".to_string()),
                StmtKind::Raw(
                    "CREATE VIEW fz_q7dpv AS SELECT a::int8 AS a8, b::varchar(10) AS bv, c::float8 AS cf, (a + c)::numeric(12,3) AS m FROM fz_q7dp;"
                        .to_string(),
                ),
                StmtKind::Raw("SELECT pg_get_viewdef('fz_q7dpv', true);".to_string()),
                StmtKind::Raw("DROP TABLE fz_q7dp CASCADE;".to_string()),
            ]
        }
        2 => {
            let d = g.rng.below(100);
            vec![
                StmtKind::Raw("DROP TABLE IF EXISTS fz_q7dp CASCADE;".to_string()),
                StmtKind::Raw("CREATE TABLE fz_q7dp (a int PRIMARY KEY, b text);".to_string()),
                StmtKind::Raw(
                    "CREATE FUNCTION fz_q7dpm(v int) RETURNS void LANGUAGE sql BEGIN ATOMIC MERGE INTO fz_q7dp t USING (SELECT v AS a) s ON t.a = s.a WHEN MATCHED THEN UPDATE SET b = 'm' WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.a, 'i'); END;"
                        .to_string(),
                ),
                StmtKind::Raw("SELECT pg_get_functiondef('fz_q7dpm'::regproc);".to_string()),
                StmtKind::Raw("SELECT pg_get_function_sqlbody('fz_q7dpm'::regproc);".to_string()),
                StmtKind::Raw(format!(
                    "CREATE FUNCTION fz_q7dpd(a int, b text DEFAULT 'dd', c numeric DEFAULT {d}.5) RETURNS text LANGUAGE sql RETURN b || a::text;"
                )),
                StmtKind::Raw(
                    "SELECT pg_get_function_arg_default('fz_q7dpd'::regproc, 2), pg_get_function_arg_default('fz_q7dpd'::regproc, 3), pg_get_function_arg_default('fz_q7dpd'::regproc, 1);"
                        .to_string(),
                ),
                StmtKind::Raw(
                    "CREATE PROCEDURE fz_q7dpp(IN x int, OUT y int, INOUT z int) LANGUAGE sql BEGIN ATOMIC SELECT x + 1, z * 2; END;"
                        .to_string(),
                ),
                StmtKind::Raw("SELECT pg_get_functiondef('fz_q7dpp'::regproc);".to_string()),
                StmtKind::Raw(
                    "SELECT pg_get_function_arguments('fz_q7dpp'::regproc), pg_get_function_identity_arguments('fz_q7dpp'::regproc), pg_get_function_result('fz_q7dpp'::regproc);"
                        .to_string(),
                ),
                StmtKind::Raw(
                    "DROP PROCEDURE fz_q7dpp; DROP FUNCTION fz_q7dpd; DROP FUNCTION fz_q7dpm; DROP TABLE fz_q7dp CASCADE;"
                        .to_string(),
                ),
            ]
        }
        3 => {
            vec![
                StmtKind::Raw("DROP TABLE IF EXISTS fz_q7dp CASCADE;".to_string()),
                StmtKind::Raw("CREATE TABLE fz_q7dp (a int, b text, c numeric(10,2));".to_string()),
                StmtKind::Raw(
                    "CREATE INDEX fz_q7dpi ON fz_q7dp (b DESC NULLS LAST, (a + 1)) INCLUDE (c) WHERE a > 0;"
                        .to_string(),
                ),
                StmtKind::Raw("SELECT pg_get_indexdef('fz_q7dpi'::regclass, 0, true);".to_string()),
                StmtKind::Raw(
                    "SELECT pg_get_indexdef('fz_q7dpi'::regclass, 1, true), pg_get_indexdef('fz_q7dpi'::regclass, 2, true), pg_get_indexdef('fz_q7dpi'::regclass, 3, true);"
                        .to_string(),
                ),
                StmtKind::Raw("DROP TABLE fz_q7dp CASCADE;".to_string()),
            ]
        }
        _ => {
            vec![
                StmtKind::Raw("DROP TABLE IF EXISTS fz_q7dq CASCADE; DROP SCHEMA IF EXISTS fz_q7ds CASCADE;".to_string()),
                StmtKind::Raw("CREATE SCHEMA fz_q7ds;".to_string()),
                StmtKind::Raw("CREATE TYPE fz_q7ds.en AS ENUM ('a', 'b');".to_string()),
                StmtKind::Raw(
                    "CREATE TABLE fz_q7dq (a int, b fz_q7ds.en, ts timestamp) PARTITION BY RANGE (a, (a + 1));"
                        .to_string(),
                ),
                StmtKind::Raw(
                    "CREATE TABLE fz_q7dq1 PARTITION OF fz_q7dq FOR VALUES FROM (0, 0) TO (10, 10);".to_string(),
                ),
                StmtKind::Raw(
                    "CREATE TABLE fz_q7dq2 PARTITION OF fz_q7dq FOR VALUES FROM (10, 10) TO (MAXVALUE, MAXVALUE);"
                        .to_string(),
                ),
                StmtKind::Raw(
                    "CREATE TABLE fz_q7dql (b fz_q7ds.en, x int) PARTITION BY LIST (b);".to_string(),
                ),
                StmtKind::Raw(
                    "CREATE TABLE fz_q7dql1 PARTITION OF fz_q7dql FOR VALUES IN ('a');".to_string(),
                ),
                StmtKind::Raw(
                    "SELECT pg_get_partkeydef('fz_q7dq'::regclass), pg_get_partkeydef('fz_q7dql'::regclass);"
                        .to_string(),
                ),
                StmtKind::Raw(
                    "SELECT pg_get_partition_constraintdef('fz_q7dq1'::regclass), pg_get_partition_constraintdef('fz_q7dql1'::regclass);"
                        .to_string(),
                ),
                StmtKind::Raw(
                    "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE relname IN ('fz_q7dq1', 'fz_q7dq2', 'fz_q7dql1') ORDER BY relname;"
                        .to_string(),
                ),
                StmtKind::Raw("CREATE TABLE fz_q7dq3 (a int, b fz_q7ds.en, ts timestamp);".to_string()),
                StmtKind::Raw(
                    "ALTER TABLE fz_q7dq ATTACH PARTITION fz_q7dq3 FOR VALUES FROM (-10, -10) TO (0, 0);"
                        .to_string(),
                ),
                StmtKind::Raw("ALTER TABLE fz_q7dq DETACH PARTITION fz_q7dq3;".to_string()),
                StmtKind::Raw(
                    "DROP TABLE fz_q7dq3; DROP TABLE fz_q7dq CASCADE; DROP TABLE fz_q7dql CASCADE; DROP SCHEMA fz_q7ds CASCADE;"
                        .to_string(),
                ),
            ]
        }
    }
}

/// Pseudotype IO tail: the _out stubs (polymorphic heads bound to real
/// values; non-polymorphic heads through NULL/unknown coercion — both
/// sides raise the matched stub error) and the remaining _in stubs.
fn gen_pseudo3(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:pseudo3");
    let sql = match g.rng.below(4) {
        0 => {
            let f = pick_str(g, &[
                "any_out(42)",
                "anyarray_out('{1,2}'::int4[])",
                "anyelement_out(42)",
                "anynonarray_out(42)",
                "anycompatible_out(42)",
                "anycompatiblearray_out('{1,2}'::int4[])",
                "anycompatiblenonarray_out(42)",
                "anyrange_out('[1,2]'::int4range)",
                "anycompatiblerange_out('[1,2]'::int4range)",
                "anymultirange_out('{[1,2]}'::int4multirange)",
                "anycompatiblemultirange_out('{[1,2]}'::int4multirange)",
            ]);
            format!("SELECT {};", f)
        }
        1 => {
            let f = pick_str(g, &[
                "internal_out", "trigger_out", "event_trigger_out", "fdw_handler_out",
                "index_am_handler_out", "table_am_handler_out", "tsm_handler_out",
                "language_handler_out", "pg_ddl_command_out", "shell_out",
            ]);
            let arg = pick_str(g, &["NULL", "'x'"]);
            format!("SELECT {}({});", f, arg)
        }
        2 => {
            let f = pick_str(g, &[
                "any_in", "anyarray_in", "anycompatible_in", "anycompatiblemultirange_in",
                "anycompatiblerange_in", "anyenum_in", "anymultirange_in", "anynonarray_in",
                "anyrange_in", "fdw_handler_in", "index_am_handler_in", "internal_in",
                "pg_ddl_command_in", "table_am_handler_in", "tsm_handler_in",
            ]);
            format!("SELECT {}('x'::cstring);", f)
        }
        _ => {
            // anyenum_out bound to a real enum value (bracket-local type).
            return vec![
                StmtKind::Raw("DROP TYPE IF EXISTS fz_q7pe;".to_string()),
                StmtKind::Raw("CREATE TYPE fz_q7pe AS ENUM ('a', 'b');".to_string()),
                StmtKind::Raw("SELECT anyenum_out('a'::fz_q7pe);".to_string()),
                StmtKind::Raw("DROP TYPE fz_q7pe;".to_string()),
            ];
        }
    };
    raw(sql)
}

/// currtid2 over a fresh single-row table and over a simple view carrying
/// a _RETURN rule (currtid_for_view path).
fn gen_tid3(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:tid3");
    let k = g.rng.below(100);
    vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_q7tt CASCADE;".to_string()),
        StmtKind::Raw("CREATE TABLE fz_q7tt (a int PRIMARY KEY, b text);".to_string()),
        StmtKind::Raw(format!("INSERT INTO fz_q7tt VALUES ({k}, 'one');")),
        StmtKind::Raw("SELECT currtid2('fz_q7tt', '(0,1)');".to_string()),
        StmtKind::Raw("CREATE VIEW fz_q7tv AS SELECT * FROM fz_q7tt;".to_string()),
        StmtKind::Raw("SELECT currtid2('fz_q7tv', '(0,1)');".to_string()),
        StmtKind::Raw("DROP TABLE fz_q7tt CASCADE;".to_string()),
    ]
}

/// COVERAGE-ONLY xml mapping family (schema_to_xml & co): the C reference
/// is a no-libxml build and errors "unsupported XML feature" where pgrust
/// succeeds natively, so this family must be weighted to 0 on every
/// differential leg (--weight adtm:xml=0) — the covloop is its consumer.
fn gen_xmlfam(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:xml");
    let probe = match g.rng.below(7) {
        0 => "SELECT schema_to_xmlschema('fz_q7xe', true, true, '');",
        1 => "SELECT database_to_xmlschema(true, true, '');",
        2 => "SELECT schema_to_xml('fz_q7xs', true, true, '');",
        3 => "SELECT database_to_xml(true, true, '');",
        4 => "SELECT database_to_xml_and_xmlschema(true, true, '');",
        5 => "SELECT schema_to_xml_and_xmlschema('fz_q7xe', true, true, 'tgt');",
        _ => "SELECT query_to_xml('SELECT 1 AS one, ''a''::text AS two', true, true, '');",
    };
    vec![
        StmtKind::Raw("DROP SCHEMA IF EXISTS fz_q7xe CASCADE; DROP SCHEMA IF EXISTS fz_q7xs CASCADE;".to_string()),
        StmtKind::Raw("CREATE SCHEMA fz_q7xe;".to_string()),
        StmtKind::Raw("CREATE SCHEMA fz_q7xs;".to_string()),
        StmtKind::Raw("CREATE DOMAIN fz_q7xs.dom AS int CHECK (VALUE > 0);".to_string()),
        StmtKind::Raw("CREATE TABLE fz_q7xs.t1 (a int, b text, d fz_q7xs.dom, ts timestamp);".to_string()),
        StmtKind::Raw("INSERT INTO fz_q7xs.t1 VALUES (1, 'x', 5, '2020-01-01');".to_string()),
        StmtKind::Raw(probe.to_string()),
        StmtKind::Raw("DROP SCHEMA fz_q7xs CASCADE; DROP SCHEMA fz_q7xe CASCADE;".to_string()),
    ]
}

// ===================================================================
// Q7 expr-strings families (hand-verified deck-strings).
// ===================================================================

/// "char" comparison operators and int4 crossings.
fn gen_char2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:char2");
    let a = pick_str(g, &["a", "b", "z", "!"]);
    let b = pick_str(g, &["a", "m", "0"]);
    let sql = match g.rng.below(3) {
        0 => format!(
            "SELECT '{a}'::\"char\" < '{b}'::\"char\", '{a}'::\"char\" <= '{b}'::\"char\", '{a}'::\"char\" > '{b}'::\"char\", '{a}'::\"char\" >= '{b}'::\"char\", '{a}'::\"char\" <> '{b}'::\"char\", '{a}'::\"char\" = '{b}'::\"char\";"
        ),
        1 => format!("SELECT ('{a}'::\"char\")::int, ({})::\"char\"::int;", 33 + g.rng.below(90)),
        _ => format!("SELECT ({})::\"char\", ('{a}'::\"char\")::text;", 33 + g.rng.below(90)),
    };
    raw(sql)
}

/// bpchar breadth: full comparison matrix, length/octet/typmod display,
/// min/max, name/"char" crossings.
fn gen_bpchar(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:bpchar");
    let x = pick_str(g, &["ab", "ac", "zz"]);
    let y = pick_str(g, &["ab", "ba"]);
    let n = 3 + g.rng.below(6);
    let sql = match g.rng.below(6) {
        0 => format!(
            "SELECT '{x}'::char({n}) = '{y}'::char(3), '{x}'::char({n}) <> '{y}'::char(3), '{x}'::char({n}) < '{y}'::char(3), '{x}'::char({n}) <= '{y}'::char(3), '{x}'::char({n}) > '{y}'::char(3), '{x}'::char({n}) >= '{y}'::char(3);"
        ),
        1 => format!(
            "SELECT bpcharcmp('{x}'::char(4), '{y}'::char(2)), length('{x}  '::char({n})), octet_length('{x}'::char({n})), char_length('{x}'::char({n}));"
        ),
        2 => "SELECT min(c)::text, max(c)::text FROM (VALUES ('aa'::char(4)), ('zz'), ('mm')) v(c);".to_string(),
        3 => format!(
            "SELECT ('{x}'::char({n}))::name, ('{x}'::name)::char({n}), ('x'::\"char\")::char(2), ('{x}'::char(3))::\"char\";"
        ),
        4 => format!(
            "SELECT format_type('bpchar'::regtype::oid, {}), format_type('varchar'::regtype::oid, {});",
            4 + n,
            4 + n
        ),
        _ => format!(
            "SELECT '{x}'::char(3) ~<~ '{y}'::char(3), '{x}'::char(3) ~<=~ '{y}'::char(3), '{x}'::char(3) ~>~ '{y}'::char(3), '{x}'::char(3) ~>=~ '{y}'::char(3);"
        ),
    };
    raw(sql)
}

/// format_type_extended over builtin type oids x typmods (fixed oids are
/// identical on both engines; 0/unknown oids hit the "-"/??? arms).
fn gen_fmtty(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:fmtty");
    let oids: &[u32] = &[
        16, 17, 18, 19, 20, 21, 23, 25, 700, 701, 790, 1042, 1043, 1082,
        1083, 1114, 1184, 1186, 1266, 1560, 1562, 1700, 0, 999999,
    ];
    let typmods: &[i64] = &[-1, 0, 4, 6, 8, 104, 458759, 1048576, 65540];
    let n = 3 + g.rng.below(4);
    let cols: Vec<String> = (0..n)
        .map(|_| {
            format!(
                "format_type({}, {})",
                *g.rng.pick(oids),
                *g.rng.pick(typmods)
            )
        })
        .collect();
    raw(format!("SELECT {};", cols.join(", ")))
}

/// oracle_compat tail: text + bytea trims, pads, translate, initcap,
/// chr/ascii edges.
fn gen_trim2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:trim2");
    let sql = match g.rng.below(6) {
        0 => "SELECT btrim('xxhellozz', 'xz'), ltrim('xxhello', 'x'), rtrim('helloz', 'z'), \
              btrim('  padded  '), ltrim('él fin', 'lé '), rtrim('日本語だ', 'だ');"
            .to_string(),
        1 => "SELECT btrim('\\x001122330000'::bytea, '\\x00'::bytea)::text, \
              ltrim('\\x0011'::bytea, '\\x00'::bytea)::text, \
              rtrim('\\x110000'::bytea, '\\x00'::bytea)::text, \
              btrim(''::bytea, '\\x00'::bytea)::text;"
            .to_string(),
        2 => format!(
            "SELECT lpad('abc', {n}), rpad('abc', {n}, 'xy'), lpad('日本', {n}, '語'), \
             rpad('', {n}, 'z'), lpad('abc', -1), rpad('abc', 0);",
            n = g.rng.below(30)
        ),
        3 => "SELECT translate('12345abc', '143', 'ax'), translate('', 'a', 'b'), \
              translate('日本語', '本', '書'), repeat('ab', 3), repeat('x', 0), repeat('', 5);"
            .to_string(),
        4 => "SELECT initcap('hello tHE WORLD 3rd time'), initcap(''), \
              initcap('über älter'), ascii('A'), ascii('日'), chr(66), chr(955);"
            .to_string(),
        _ => "SELECT chr(0);".to_string(), // matched error arm
    };
    raw(sql)
}

/// varstr_levenshtein(_less_equal) drain: the SQL-callable levenshtein()
/// lives in contrib (out of reach, no-dlopen rig), but core calls the
/// same code for parser "Perhaps you meant ..." HINTs. Near-miss column
/// references over a purpose-named table are deterministic identical
/// errors whose hint computation walks the cost matrix.
fn gen_lev(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:lev");
    let miss = *g.rng.pick(&[
        "colcount", "columnn", "colcnt", "the_column_counter", "clm",
        "column_conut", "colun_count", "c", "kolumncount",
        "日本語のカラム", "extremely_long_identifier_no_match_at_all_qq",
    ]);
    rawv(vec![
        "CREATE TABLE fz_lev (column_count int, colname text, col2 int);".to_string(),
        format!("SELECT {miss} FROM fz_lev;"),
        format!("UPDATE fz_lev SET {miss} = 1 WHERE false;"),
        "DROP TABLE fz_lev;".to_string(),
    ])
}

/// parse_ident over quoting/whitespace/strict-tail variants + error arms.
fn gen_ident(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:ident");
    let idents: &[(&str, &str)] = &[
        ("a.b.c", "true"),
        ("\"Sch ema\".\"Ta.ble\"", "true"),
        ("  spaced  .  out  ", "true"),
        ("x.y.z junk", "false"),
        ("\"quoted\"", "true"),
        ("a.\"b\"\"q\".c", "true"),
        ("trailing.", "true"),   // error: no valid identifier after "."
        (".leading", "true"),    // error: string is not a valid identifier
        ("", "true"),            // error
        ("a..b", "true"),        // error
        ("x.y.z junk", "true"),  // error under strict
        ("\"\"", "true"),        // error: empty quoted ident
    ];
    let (s, strict) = *g.rng.pick(idents);
    raw(format!(
        "SELECT parse_ident('{}', {})::text;",
        esc_s(&s),
        strict
    ))
}

/// varbit residue: typmod length coercions, shifts at/beyond width,
/// get/set/overlay/position, int<->bit casts.
fn gen_bit2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:bit2");
    let sql = match g.rng.below(7) {
        0 => "SELECT B'101'::bit(3)::text, B'10'::bit varying(5)::text, \
              X'1F'::bit(8)::text, X'F'::bit(4)::text, B''::varbit::text;"
            .to_string(),
        1 => format!(
            "SELECT (B'1011001110001111' >> {n})::text, (B'1011001110001111' << {n})::text, \
             (B'101' >> 64)::text, (B'101' << 65)::text;",
            n = g.rng.below(20)
        ),
        2 => "SELECT get_bit(B'101101', 3), set_bit(B'101101', 2, 0)::text, \
              overlay(B'10111000' placing B'01' from 3 for 2)::text, \
              position(B'11' in B'00110'), bit_count(B'1101101');"
            .to_string(),
        3 => format!(
            "SELECT ({v}::bit(16))::text, ({v}::bit(32))::text, (B'1000'::int4), \
             (B'111111111111111111111111111111111111'::int8);",
            v = g.rng.below(70000)
        ),
        4 => "SELECT (B'101' || B'0110')::text, (B'101' & B'110')::text, \
              (B'101' | B'110')::text, (B'101' # B'110')::text, (~B'1010')::text, \
              bit_length(B'10101');"
            .to_string(),
        5 => "SELECT B'101'::bit(8);".to_string(), // matched length error
        _ => "SELECT get_bit(B'101', 7);".to_string(), // matched range error
    };
    raw(sql)
}

/// name vs text cross-type comparison operators, both directions, plus
/// text::name truncation.
fn gen_nametext(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:nametext");
    let x = pick_str(g, &["abc", "abd", "zz"]);
    let y = pick_str(g, &["abc", "abd"]);
    let sql = match g.rng.below(3) {
        0 => format!(
            "SELECT '{x}'::name = '{y}'::text, '{x}'::name <> '{y}'::text, '{x}'::name < '{y}'::text, '{x}'::name <= '{y}'::text, '{x}'::name > '{y}'::text, '{x}'::name >= '{y}'::text;"
        ),
        1 => format!(
            "SELECT '{x}'::text = '{y}'::name, '{x}'::text <> '{y}'::name, '{x}'::text < '{y}'::name, '{x}'::text <= '{y}'::name, '{x}'::text > '{y}'::name, '{x}'::text >= '{y}'::name;"
        ),
        _ => format!("SELECT ('{x}'::text)::name, (repeat('long', 30)::text)::name;"),
    };
    raw(sql)
}

/// cash_in sign/parenthesis/separator matrix + cash_out negatives +
/// arithmetic tail (money never crosses the ulp rulings: exact int64).
fn gen_cash2(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:cash2");
    let ins: &[&str] = &[
        "$1,234.56", "(1234.56)", "($55.00)", "-$1.23", "$-1.23", "+$0.05",
        "$0.009", "1234", ".55", "-.05", "92233720368547758.07",
        "-92233720368547758.08", "$ 12 . 34", "  $99  ",
    ];
    let sql = match g.rng.below(5) {
        0 => {
            let n = 3 + g.rng.below(3);
            let cols: Vec<String> = (0..n)
                .map(|_| format!("('{}'::money)::text", esc_s(g.rng.pick(ins))))
                .collect();
            format!("SELECT {};", cols.join(", "))
        }
        1 => "SELECT ('1.23'::money * 2)::text, ('1.23'::money * 2.5::float8)::text, \
              ('10'::money / 4)::text, ('10'::money / '2.50'::money), \
              ('1.23'::money + '4.56'::money)::text, ('1.23'::money * -1)::text;"
            .to_string(),
        2 => "SELECT ('12.34'::money::numeric)::text, (12.345::numeric::money)::text, \
              (7::int4::money)::text, (9::int8::money)::text, \
              ('12.34'::money::numeric::money)::text;"
            .to_string(),
        3 => "SELECT cash_words('12.34'::money), cash_words('-0.05'::money);".to_string(),
        _ => "SELECT ('92233720368547758.08'::money)::text;".to_string(), // overflow error
    };
    raw(sql)
}

/// String function tail: trims (incl. bytea forms), array_to_string,
/// string_to_array/string_to_table with null-string arms, to_bin/oct/hex,
/// format(VARIADIC), unknown-type literals, to_char(float4)/TM templates.
fn gen_strfns(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:strfns");
    let sql = match g.rng.below(8) {
        0 => "SELECT ltrim('xxabcxx', 'x'), ltrim('  ab'), rtrim('abxx', 'x'), btrim('xxabxx', 'x'), trim(LEADING 'x' FROM 'xxax'), trim(TRAILING 'y' FROM 'ayy');".to_string(),
        1 => "SELECT ltrim('\\x61616263'::bytea, '\\x61'::bytea)::text, rtrim('\\x61626161'::bytea, '\\x61'::bytea)::text, btrim('\\x6161626161'::bytea, '\\x61'::bytea)::text;".to_string(),
        2 => {
            let d = pick_str(g, &[",", "-", ""]);
            format!(
                "SELECT array_to_string(ARRAY[1, 2, 3], '{d}'), array_to_string(ARRAY['a', NULL, 'c'], '{d}'), array_to_string(ARRAY['a', NULL, 'c'], '{d}', 'NUL');"
            )
        }
        3 => {
            let ns = pick_str(g, &["a", "b", "zz"]);
            match g.rng.below(3) {
                0 => format!(
                    "SELECT string_to_array('a,b,a', ',', '{ns}')::text, string_to_array('a,b', ',', NULL)::text, string_to_array('abc', NULL, '{ns}')::text;"
                ),
                1 => "SELECT x FROM string_to_table('a,b,c', ',') t(x);".to_string(),
                _ => format!("SELECT x FROM string_to_table('a,b,c', ',', '{ns}') t(x);"),
            }
        }
        4 => {
            let v = g.rng.below(1 << 20) as i64;
            format!(
                "SELECT to_bin({v}), to_bin(-{v}), to_bin({v}::bigint), to_oct({v}), to_oct({v}::bigint), to_oct(-9000000000::bigint), to_hex({v}), to_hex(-{v}::bigint), to_bin(-9000000000::bigint);"
            )
        }
        5 => "SELECT format('%s-%s', VARIADIC ARRAY['a', 'b']), format('%2$s %1$s', VARIADIC ARRAY['x', 'y']), format('%I %L', VARIADIC ARRAY['col nm', NULL]);".to_string(),
        6 => pick_str(g, &[
            "SELECT 'abc'::unknown;",
            "SELECT ('abc'::unknown)::text;",
        ])
        .to_string(),
        _ => match g.rng.below(4) {
            0 => "SELECT to_char(1.5::float4, '9.99'), to_char(3.25e5::float4, '9.99EEEE'), to_char(0.1::float4, 'FM990.000');".to_string(),
            1 => "SELECT to_char('2020-01-05'::date, 'TMDay TMMonth TMDy TMMon'), to_char('2020-03-07'::date, 'tmday tmmonth'), to_char('2020-03-07'::date, 'TMDAY TMMONTH');".to_string(),
            2 => "SELECT to_date('05 January 2020', 'DD TMMonth YYYY'), to_date('Sat 07 Mar 2020', 'TMDy DD TMMon YYYY');".to_string(),
            _ => "SELECT to_timestamp('05 January 2020', 'DD TMMonth YYYY')::text;".to_string(),
        },
    };
    raw(sql)
}

/// bytea breadth: get/set bit/byte, int2/4/8 crossings both directions,
/// min/max, no-length substr, overlay.
fn gen_byteax(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:byteax");
    let sql = match g.rng.below(5) {
        0 => {
            let b = g.rng.below(16);
            format!(
                "SELECT get_bit('\\xdeadbeef'::bytea, {b}), set_bit('\\xdeadbeef'::bytea, {b}, 1)::text, get_byte('\\xdeadbeef'::bytea, 2), set_byte('\\xdeadbeef'::bytea, 1, 255)::text;"
            )
        }
        1 => format!(
            "SELECT ({}::int2)::bytea::text, ({})::bytea::text, ({}000000000::int8)::bytea::text;",
            g.rng.below(30000),
            g.rng.below(2000000),
            1 + g.rng.below(9)
        ),
        2 => "SELECT ('\\x0102'::bytea)::int2, ('\\x00010203'::bytea)::int4, ('\\x0000000218711a00'::bytea)::int8;".to_string(),
        3 => "SELECT min(v)::text, max(v)::text FROM (VALUES ('\\xaa'::bytea), ('\\x01'), ('\\xff')) t(v);".to_string(),
        _ => {
            let f = 1 + g.rng.below(4);
            format!(
                "SELECT substr('\\xdeadbeef'::bytea, {f})::text, substring('\\xdeadbeef'::bytea from {f})::text, substring('\\xdeadbeef'::bytea from {f} for 2)::text, overlay('\\xdeadbeef'::bytea placing '\\x00'::bytea from {f})::text;"
            )
        }
    };
    raw(sql)
}

/// SP-GiST box operator matrix (geo_spgist.c quad_inner_consistent):
/// deterministic box grid + every strategy operator under forced index
/// scans, ORDER BY area for stable output.
fn gen_spgbox(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:spgbox");
    let op = *g.rng.pick(&[
        "&&", "<<", ">>", "<@", "@>", "&<", "&>", "<<|", "|>>", "&<|", "|&>", "~=",
    ]);
    let q = format!(
        "box(point({a},{b}), point({c},{d}))",
        a = g.rng.below(8),
        b = g.rng.below(8),
        c = 8 + g.rng.below(8),
        d = 8 + g.rng.below(8)
    );
    rawv(vec![
        "CREATE TABLE fz_spg (b box);".to_string(),
        "INSERT INTO fz_spg SELECT box(point(x, y), point(x + w, y + w)) \
         FROM generate_series(0, 12, 3) x, generate_series(0, 12, 3) y, \
         generate_series(1, 5, 2) w;"
            .to_string(),
        "CREATE INDEX fz_spg_idx ON fz_spg USING spgist (b);".to_string(),
        "SET enable_seqscan = off;".to_string(),
        format!(
            "SELECT b::text FROM fz_spg WHERE b {op} {q} \
             ORDER BY area(b), b::text;"
        ),
        "RESET enable_seqscan;".to_string(),
        "DROP TABLE fz_spg;".to_string(),
    ])
}

/// expandedrecord drain: plpgsql composite-typed variable assignment /
/// field mutation / whole-record copy, observed through a probe table.
fn gen_xrec(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:xrec");
    let k = g.rng.below(100);
    rawv(vec![
        "CREATE TABLE fz_xrec_out (id int, r text);".to_string(),
        "CREATE TYPE fz_xrec_t AS (a int, b text, c numeric);".to_string(),
        format!(
            "DO $$ DECLARE x fz_xrec_t; y fz_xrec_t; z fz_xrec_out%ROWTYPE; BEGIN \
             x := ROW({k}, 'seed', 1.25); \
             x.a := x.a + 1; x.b := x.b || '-x'; \
             y := x; y.c := y.c * 2; y.b := NULL; \
             z.id := 1; z.r := x::text || '|' || y::text; \
             INSERT INTO fz_xrec_out VALUES (z.id, z.r); \
             x := NULL; \
             INSERT INTO fz_xrec_out VALUES (2, coalesce(x::text, '<null>')); \
             END $$;"
        ),
        "SELECT id, r FROM fz_xrec_out ORDER BY id;".to_string(),
        "DROP TABLE fz_xrec_out;".to_string(),
        "DROP TYPE fz_xrec_t;".to_string(),
    ])
}

/// pg_stat_get_progress_info over every command kind (all empty here —
/// the per-command dispatch and the empty-result arms are the lines).
fn gen_progress(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:progress");
    let v = *g.rng.pick(&[
        "pg_stat_progress_vacuum", "pg_stat_progress_analyze",
        "pg_stat_progress_cluster", "pg_stat_progress_create_index",
        "pg_stat_progress_basebackup", "pg_stat_progress_copy",
    ]);
    raw(format!("SELECT count(*) FROM {v};"))
}

/// Single-quote escape for literal material.
fn esc_s(s: &str) -> String {
    s.replace('\'', "''")
}

/// Pattern-ops index bracket: bpchar_pattern_ops + text_pattern_ops btree
/// indexes (build sorts through the pattern sortsupport), LIKE-prefix and
/// explicit ~<~-family probes under enable_seqscan=off, plus hash/sort
/// grouping on the char(n) column.
fn gen_patidx(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("adtm:patidx");
    let k = 1 + g.rng.below(3);
    vec![
        StmtKind::Raw("DROP TABLE IF EXISTS fz_q7px;".to_string()),
        StmtKind::Raw("CREATE TABLE fz_q7px (c char(6), t text);".to_string()),
        StmtKind::Raw(
            "INSERT INTO fz_q7px SELECT 'c' || g % 20, 't' || g % 20 FROM generate_series(1, 100) g;"
                .to_string(),
        ),
        StmtKind::Raw("CREATE INDEX fz_q7pxc ON fz_q7px (c bpchar_pattern_ops);".to_string()),
        StmtKind::Raw("CREATE INDEX fz_q7pxt ON fz_q7px (t text_pattern_ops);".to_string()),
        StmtKind::Raw("SET enable_seqscan = off;".to_string()),
        StmtKind::Raw(format!("SELECT count(*) FROM fz_q7px WHERE c LIKE 'c{k}%';")),
        StmtKind::Raw(format!("SELECT count(*) FROM fz_q7px WHERE t LIKE 't{k}%';")),
        StmtKind::Raw(format!(
            "SELECT count(*) FROM fz_q7px WHERE c ~<~ 'c{}' AND c ~>~ 'c0';",
            k + 1
        )),
        StmtKind::Raw("SELECT 'ab' ~>~ 'aa', 'ab' ~<=~ 'ab', 'ab' ~<~ 'ac', 'ac' ~>=~ 'ab';".to_string()),
        StmtKind::Raw("RESET enable_seqscan;".to_string()),
        StmtKind::Raw("SELECT c FROM fz_q7px GROUP BY c ORDER BY c LIMIT 3;".to_string()),
        StmtKind::Raw("SELECT DISTINCT c FROM fz_q7px ORDER BY c DESC LIMIT 3;".to_string()),
        StmtKind::Raw("DROP TABLE fz_q7px;".to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::objddl::{ObjRole, ObjState};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Generate n groups with a session-persistent ObjState carrying live
    /// roles/ranges (mirrors the session loop's swap).
    fn gen_groups(
        seed: u64,
        n: usize,
        with_roles: bool,
    ) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut state = ObjState::new();
        if with_roles {
            state.roles.push(ObjRole { name: "fz_role_t_0".to_string(), live: true });
            state.roles.push(ObjRole { name: "fz_role_t_1".to_string(), live: false });
        }
        let mut groups = Vec::new();
        let mut prods_all = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            std::mem::swap(&mut g.obj, &mut state);
            let stmts = gen_adtmisc_module(&mut g);
            std::mem::swap(&mut g.obj, &mut state);
            groups.push(stmts.iter().map(|s| s.to_sql()).collect::<Vec<_>>());
            prods_all.extend(prods);
        }
        (groups, prods_all)
    }

    #[test]
    fn shapes_and_textual_invariants() {
        let (groups, prods) = gen_groups(0xA3, 4000, true);
        for group in &groups {
            for sql in group {
                assert!(sql.ends_with(';'), "{sql}");
                assert!(!sql.contains('\n'), "{sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
                // Determinism: no volatile functions ever — except the Q2
                // uuid-generation shapes, whose volatile values are wrapped
                // in engine-stable predicates and never reach the compare
                // surface raw (adtm:uuid, hand-verified).
                for banned in ["random(", "now()"] {
                    assert!(!sql.contains(banned), "volatile fn in {sql}");
                }
                if sql.contains("gen_random_uuid") || sql.contains("uuidv4()") || sql.contains("uuidv7(") {
                    let stable = sql.contains("<> gen_random_uuid()")
                        || sql.contains("uuid_extract_version(")
                        || sql.contains("IS NOT NULL");
                    assert!(stable, "raw volatile uuid on compare surface: {sql}");
                }
                // Role DDL touches fuzz roles / PUBLIC / the deliberate
                // missing-role error only.
                if let Some(rest) = sql.strip_prefix("SET ROLE ") {
                    assert!(rest.starts_with("fz_role_"), "{sql}");
                }
                if sql.starts_with("GRANT ")
                    || sql.starts_with("REVOKE ")
                    || sql.starts_with("ALTER DEFAULT PRIVILEGES")
                {
                    let ok = sql.contains(" fz_role_")
                        || sql.contains(" PUBLIC;")
                        || sql.contains(" fz_no_such_role;");
                    assert!(ok, "role-ddl names a non-fuzz role: {sql}");
                }
            }
            // Brackets are closed within their group.
            if group[0].starts_with("SET ROLE ") {
                assert_eq!(group.len(), 3, "{group:?}");
                assert_eq!(group[2], "RESET ROLE;");
            }
            if group[0].starts_with("GRANT ") && !group[0].contains("fz_no_such_role") {
                assert!(
                    group.last().unwrap().starts_with("REVOKE "),
                    "unclosed grant bracket: {group:?}"
                );
            }
            if group[0].starts_with("ALTER DEFAULT PRIVILEGES") {
                let last = group.last().unwrap();
                assert!(
                    last.starts_with("ALTER DEFAULT PRIVILEGES") && last.contains(" REVOKE "),
                    "unclosed default-privileges bracket: {group:?}"
                );
                // GRANT/REVOKE must mirror the same IN SCHEMA clause.
                assert_eq!(
                    group[0].contains(" IN SCHEMA public"),
                    last.contains(" IN SCHEMA public"),
                    "{group:?}"
                );
            }
        }
        // Every shape fires under default weights (roles present).
        for p in SHAPES {
            assert!(prods.iter().any(|q| q == p), "shape {p} never fired");
        }
        assert!(prods.iter().any(|q| q == "adtm:err"), "err arm never fired");
    }

    #[test]
    fn roleless_state_falls_back_cleanly() {
        let (groups, prods) = gen_groups(7, 800, false);
        for group in &groups {
            for sql in group {
                assert!(!sql.starts_with("SET ROLE"), "{sql}");
                assert!(!sql.starts_with("GRANT"), "{sql}");
            }
        }
        assert!(prods.iter().any(|q| q == "adtm:fallback:probe"));
        // Non-role families unaffected.
        for p in ["adtm:bit", "adtm:mr", "adtm:num", "adtm:vltext"] {
            assert!(prods.iter().any(|q| q == p), "{p} never fired");
        }
    }

    #[test]
    fn generation_is_deterministic() {
        let (a, pa) = gen_groups(5, 200, true);
        let (b, pb) = gen_groups(5, 200, true);
        assert_eq!(a, b);
        assert_eq!(pa, pb);
        let (c, _) = gen_groups(6, 200, true);
        assert_ne!(a, c);
    }

    #[test]
    fn grant_brackets_leave_no_residual_grants() {
        // Every group that GRANTs on an object ends with a REVOKE naming
        // the same object, so fuzz roles never carry object grants across
        // groups (the objddl DROP ROLE invariant).
        let (groups, _) = gen_groups(11, 1500, true);
        let mut saw_grant = false;
        for group in &groups {
            let first = &group[0];
            if !first.starts_with("GRANT ") {
                continue;
            }
            // The deliberate missing-grantee error statement grants
            // nothing, so it carries no closing REVOKE.
            if first.contains("fz_no_such_role") {
                assert_eq!(group.len(), 1, "{group:?}");
                continue;
            }
            saw_grant = true;
            let on = first.split(" ON ").nth(1).map(|s| {
                s.split(" TO ").next().unwrap().trim().to_string()
            });
            let last = group.last().unwrap();
            assert!(last.starts_with("REVOKE "), "{group:?}");
            if let Some(on_obj) = on {
                assert!(last.contains(&format!(" ON {} FROM ", on_obj)), "{group:?}");
            }
        }
        assert!(saw_grant, "no grant bracket in 1500 groups");
    }

    #[test]
    fn custom_range_cross_fires_when_ranges_live() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(21);
        let mut state = ObjState::new();
        state.ranges.push(crate::objddl::ObjRange {
            name: "fz_rty_t".to_string(),
            subtype: crate::catalog::SqlType::Int4,
            live: true,
        });
        let mut fired = false;
        for _ in 0..3000 {
            let mut prods = Vec::new();
            let mut g = crate::stmt::Gen::new(&mut rng, &cat, &w, &mut prods, 3);
            std::mem::swap(&mut g.obj, &mut state);
            let stmts = gen_adtmisc_module(&mut g);
            std::mem::swap(&mut g.obj, &mut state);
            if prods.iter().any(|p| p == "adtm:mr:custom") {
                fired = true;
                for s in &stmts {
                    assert!(s.to_sql().contains("fz_rty_t"), "{}", s.to_sql());
                }
            }
        }
        assert!(fired, "custom-range cross never fired");
    }
}
