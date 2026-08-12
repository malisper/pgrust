//! Feature-module registry and toggle vector. Modules are the swarm-testing
//! axis: each has an on/off toggle and a selection weight, sampled per
//! session (seeded) or pinned via `--modules`.

use crate::rng::Rng;

#[derive(Clone, Copy, Debug)]
pub struct ModuleSpec {
    pub name: &'static str,
    pub default_weight: f64,
}

/// All known statement modules (names parallel to `stmt::STMT_MODULES`;
/// a sync test enforces it). Later milestones append here (DML, DDL, ...).
pub const REGISTRY: &[ModuleSpec] = &[
    ModuleSpec { name: "expr", default_weight: 1.0 },
    ModuleSpec { name: "joins", default_weight: 1.0 },
    ModuleSpec { name: "subq", default_weight: 1.0 },
    ModuleSpec { name: "agg", default_weight: 1.0 },
    ModuleSpec { name: "win", default_weight: 1.0 },
    ModuleSpec { name: "dml", default_weight: 1.0 },
    // MERGE is one statement per pick but touches the same shared-table
    // state as dml; below-default weight keeps the write mix balanced.
    ModuleSpec { name: "merge", default_weight: 0.7 },
    // txn emits a whole BEGIN..COMMIT bracket (typically ~5 statements)
    // per pick, so its selection weight sits below the one-statement
    // modules to keep the statement mix balanced.
    ModuleSpec { name: "txn", default_weight: 0.5 },
    // ddl churns persistent objects (and view/aggregate/trigger picks emit
    // 2-3 statement groups); below-default weight keeps the object churn
    // from starving query statements over the shared catalog.
    ModuleSpec { name: "ddl", default_weight: 0.7 },
    // Statement-level rich-type shapes (SRFs in FROM over literal inputs);
    // most T1 surface lives in the expr productions, so the module weight
    // stays low.
    ModuleSpec { name: "types", default_weight: 0.5 },
    ModuleSpec { name: "explain", default_weight: 1.0 },
    // util occasionally emits PREPARE/EXECUTE brackets and SET+SHOW
    // pairs; still ~1 statement per pick on average.
    ModuleSpec { name: "util", default_weight: 1.0 },
    // part creates whole partition trees (3-6 statement groups) and churns
    // persistent objects like ddl; the deep coverage comes from OTHER
    // modules targeting its parents, so the selection weight stays low.
    ModuleSpec { name: "part", default_weight: 0.6 },
    // objddl churns cluster-global objects (roles) and per-db types/stats;
    // several picks emit 3-4 statement groups (SET ROLE brackets, stats
    // create+ANALYZE+probe, coltab), so the weight sits below default.
    ModuleSpec { name: "objddl", default_weight: 0.6 },
    // idx creates whole indexed-table populations (a create group is
    // ~8-20 statements of table + bulk load + indexes) and its query
    // brackets are 3-5 statements each; low selection weight keeps the
    // statement mix balanced.
    ModuleSpec { name: "idx", default_weight: 0.5 },
    // par creates big bulk-loaded tables (a create group is ~24k rows +
    // three index builds) and every query pick is a 5-10 statement GUC
    // bracket; low selection weight keeps the statement mix balanced.
    ModuleSpec { name: "par", default_weight: 0.5 },
    // tsdl churns text-search configs/dictionaries (usually 2-statement
    // groups: one DDL + one probe); the expression-level ts productions
    // carry most of the stemmer surface, so the module weight stays low.
    ModuleSpec { name: "tsdl", default_weight: 0.5 },
    // cursor emits whole transaction brackets (DECLARE + 2-5 FETCH/MOVE +
    // terminator) or PREPARE/EXECUTE bursts, so several statements per
    // pick; below-default weight keeps the statement mix balanced.
    ModuleSpec { name: "cursor", default_weight: 0.6 },
    // views creates whole view/base/rule-table object sets (2-8 statement
    // groups) and churns persistent objects like ddl; the deep rewriter
    // coverage comes from OTHER modules' DML through the registered views,
    // so the selection weight stays low.
    ModuleSpec { name: "views", default_weight: 0.6 },
    // geo creates whole indexed-table populations (create groups are ~5
    // statements with bulk loads) but most picks are single scalar/query
    // statements; slightly below default keeps the create churn balanced.
    ModuleSpec { name: "geo", default_weight: 0.8 },
    // dtm emits one literal-only scalar SELECT per pick (A2 datetime
    // cross-type matrix); it targets a deep but narrow adt surface, so a
    // moderate weight buys its coverage without starving table-driven
    // modules.
    ModuleSpec { name: "dtm", default_weight: 0.8 },
    // adtmisc emits mostly single literal-driven SELECTs (grant/denial
    // brackets are 3-4 statements); slightly below default keeps the
    // breadth families from crowding the stateful modules.
    ModuleSpec { name: "adtmisc", default_weight: 0.8 },
    // sqljson emits one SQL/JSON statement per pick (J1: constructors,
    // query functions, JSON_TABLE, jsonpath item methods) — a deep parser/
    // executor surface over mostly literal inputs, so a moderate weight
    // buys its coverage without starving table-driven modules.
    ModuleSpec { name: "sqljson", default_weight: 0.8 },
    // plpg emits whole self-contained object groups (create + exercise +
    // probe + drop, typically 3-15 statements); below-default weight keeps
    // the group churn from crowding the single-statement modules.
    ModuleSpec { name: "plpg", default_weight: 0.6 },
    // coll (C3) emits self-contained collation groups (create collation +
    // table/index/range type, exercise, drop — typically 5-14 statements);
    // below-default weight keeps the group churn from crowding the
    // single-statement modules.
    ModuleSpec { name: "coll", default_weight: 0.6 },
    // mbconv is one statement per pick over pre-verified literal tables
    // (encoding-conversion sweep, Q2); moderate weight — the surface is
    // wide (128 conversion pairs) but each statement is cheap.
    ModuleSpec { name: "mbconv", default_weight: 0.7 },
    // xnum is one statement per pick over typed-literal operator
    // matrices (cross-type numeric sweep, Q2).
    ModuleSpec { name: "xnum", default_weight: 0.8 },
    // nodes emits whole debug-print bracket groups (SETs + 1-9 utility
    // statements + RESETs, ~4-13 statements per pick) and churns
    // persistent objects; below-default weight keeps the group churn from
    // crowding the single-statement modules.
    ModuleSpec { name: "nodes", default_weight: 0.5 },
    // obs emits observability probe groups (typically 1-10 wrapped
    // pg_stat probes per pick, occasionally with a table/function
    // create+drop); moderate weight — the surface is wide but cheap.
    ModuleSpec { name: "obs", default_weight: 0.6 },
    // admin is mostly one projected statement per pick (Q5 admin-funcs)
    // over a WIDE arm surface (11 families); default weight keeps its
    // statement share ~2% — multi-statement modules dilute per-pick
    // modules, and below 1.0 the sparse arms (hba/mcxt) never fire in a
    // 2000-statement leg. The backup/summarizer brackets still carry real
    // WAL cost (switch + CHECKPOINT) but sit at token family weights.
    ModuleSpec { name: "admin", default_weight: 1.0 },
    // objid emits whole hand-verified identity/deparse deck batteries
    // (60-200 statements per group — every OBJECT_* class + every
    // deparsable expr node, probed through pg_identify_object/
    // pg_describe_object/pg_get_*def); a low weight keeps the giant
    // groups from crowding the statement mix (each pick lands ~100-200
    // statements, so even a token weight buys a large statement share;
    // drain arms enable it explicitly via --modules objid=on:N).
    ModuleSpec { name: "objid", default_weight: 0.05 },
    // einterp emits whole self-contained drain groups (fixture create +
    // 1-10 statements + drop) targeting the ExecInterpExpr opcode arms and
    // the raw/analyzed tree-walker node kinds (LD2); below-default weight
    // keeps the object churn balanced like nodes/plpg.
    ModuleSpec { name: "einterp", default_weight: 0.5 },
    // exd emits EXPLAIN plan-node/option drain groups (LD4): each pick is
    // a self-contained fixture + 6-25 EXPLAIN probes over the missing
    // explain.c node/option arms. Moderate weight — groups are chunky
    // (the bitmap arm bulk-loads 20k rows) and the surface is fixed-shape.
    ModuleSpec { name: "exd", default_weight: 0.4 },
    // spill (LD5) emits GUC-bracketed spill/fallback groups (typically
    // 3-9 statements: SETs + 1-2 queries + RESETs) over one big bulk
    // table; below-default weight keeps the heavyweight spilling queries
    // from crowding the statement mix.
    ModuleSpec { name: "spill", default_weight: 0.5 },
    // earm emits whole self-contained ERROR-ARM drain groups (fixtures +
    // 6-14 deliberately-invalid probes + drops, ~15-40 statements per
    // pick) targeting the DDL/parser/catalog ereport validation arms
    // (LD6). The groups are the largest in the registry, so the selection
    // weight sits well below the bracket modules to keep the statement
    // mix balanced (at 0.5 the module carried >20% of a default stream).
    ModuleSpec { name: "earm", default_weight: 0.3 },
    // plansel (LD7) emits GUC-profile plan-selection sweeps: one
    // deterministic query repeated under 2-3 forced-plan GUC brackets
    // (~10-30 statements per pick), plus chunky fixture-set creates
    // (~35-statement groups with three bulk loads); below-default weight
    // keeps the sweep groups from crowding the statement mix.
    ModuleSpec { name: "plansel", default_weight: 0.4 },
    // earm2 (LD8) emits whole VERBATIM hand-verified ERROR-ARM round-2
    // sections (12-60 statements per pick, fixtures + probes + drops);
    // like earm the groups are large, so the weight sits low.
    ModuleSpec { name: "earm2", default_weight: 0.2 },
    // exr (LD9) emits executor-residue drain groups (runtime pruning
    // brackets, window frame-option probes, MERGE/ON CONFLICT rollback
    // brackets, transition-table trigger groups) over a persistent
    // fixture suite; below-default weight like spill — the create group
    // is chunky and the bracket groups run heavyweight nodes.
    ModuleSpec { name: "exr", default_weight: 0.5 },
    // numx (LD9) is one boundary-value statement per pick over the
    // numeric.c arithmetic/format surface (plus in-group sort/window
    // fixture families); xnum-like weight.
    ModuleSpec { name: "numx", default_weight: 0.7 },
    // LD10: publication/subscription DDL drain (never-connecting; see
    // crate::pubsub module docs).
    ModuleSpec { name: "pubsub", default_weight: 0.3 },
];

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Toggle {
    pub on: bool,
    pub weight: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToggleVector {
    /// Parallel to `REGISTRY`.
    entries: Vec<Toggle>,
}

impl ToggleVector {
    pub fn all_on() -> ToggleVector {
        ToggleVector {
            entries: REGISTRY
                .iter()
                .map(|m| Toggle { on: true, weight: m.default_weight })
                .collect(),
        }
    }

    /// Swarm-random sampling: each module independently on with probability
    /// 1/2, re-rolled until at least one module is on. Draws only from the
    /// session PRNG.
    pub fn swarm(rng: &mut Rng) -> ToggleVector {
        loop {
            let entries: Vec<Toggle> = REGISTRY
                .iter()
                .map(|m| Toggle { on: rng.chance(1, 2), weight: m.default_weight })
                .collect();
            if entries.iter().any(|t| t.on) {
                return ToggleVector { entries };
            }
        }
    }

    /// Parse a `--modules` spec like `expr=on,joins=off` or
    /// `expr=on:2.5` (weight after the colon). Unknown modules are errors.
    pub fn parse(spec: &str) -> Result<ToggleVector, String> {
        let mut tv = ToggleVector::all_on();
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (name, value) = part
                .split_once('=')
                .ok_or_else(|| format!("bad module spec {:?}: expected name=on|off", part))?;
            let idx = REGISTRY
                .iter()
                .position(|m| m.name == name)
                .ok_or_else(|| format!("unknown module {:?}", name))?;
            let (state, weight) = match value.split_once(':') {
                Some((s, w)) => {
                    let w: f64 = w
                        .parse()
                        .map_err(|_| format!("bad weight {:?} for module {:?}", w, name))?;
                    if !(w > 0.0 && w.is_finite()) {
                        return Err(format!("weight for module {:?} must be finite and > 0", name));
                    }
                    (s, Some(w))
                }
                None => (value, None),
            };
            let on = match state {
                "on" => true,
                "off" => false,
                _ => {
                    return Err(format!(
                        "bad state {:?} for module {:?}: expected on or off",
                        state, name
                    ))
                }
            };
            tv.entries[idx].on = on;
            if let Some(w) = weight {
                tv.entries[idx].weight = w;
            }
        }
        if !tv.entries.iter().any(|t| t.on) {
            return Err("toggle vector disables every module".to_string());
        }
        Ok(tv)
    }

    /// Canonical spec string; `parse(spec_string(tv)) == tv`.
    pub fn spec_string(&self) -> String {
        let mut out = String::new();
        for (m, t) in REGISTRY.iter().zip(&self.entries) {
            if !out.is_empty() {
                out.push(',');
            }
            out.push_str(m.name);
            out.push('=');
            out.push_str(if t.on { "on" } else { "off" });
            if t.weight != m.default_weight {
                out.push(':');
                out.push_str(&format!("{}", t.weight));
            }
        }
        out
    }

    pub fn is_on(&self, name: &str) -> bool {
        REGISTRY
            .iter()
            .position(|m| m.name == name)
            .is_some_and(|i| self.entries[i].on)
    }

    /// Weighted pick among enabled modules; returns the module name.
    pub fn pick_module(&self, rng: &mut Rng) -> &'static str {
        let enabled: Vec<(usize, f64)> = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, t)| t.on)
            .map(|(i, t)| (i, t.weight))
            .collect();
        debug_assert!(!enabled.is_empty());
        let total: f64 = enabled.iter().map(|(_, w)| w).sum();
        let mut x = rng.f64_unit() * total;
        for &(i, w) in &enabled {
            if x < w {
                return REGISTRY[i].name;
            }
            x -= w;
        }
        REGISTRY[enabled.last().unwrap().0].name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trip() {
        let tv = ToggleVector::parse("expr=on,joins=off").unwrap();
        assert_eq!(ToggleVector::parse(&tv.spec_string()).unwrap(), tv);
        assert!(!tv.is_on("joins"));
        assert!(tv.is_on("subq"));
        let tv = ToggleVector::parse("expr=on:2.5").unwrap();
        assert_eq!(ToggleVector::parse(&tv.spec_string()).unwrap(), tv);
        assert!(tv.is_on("expr"));
    }

    #[test]
    fn parse_rejects_unknown_and_bad() {
        assert!(ToggleVector::parse("windows=on").is_err());
        assert!(ToggleVector::parse("expr=maybe").is_err());
        assert!(ToggleVector::parse("expr=on:0").is_err());
        // All-off is rejected.
        assert!(
            ToggleVector::parse(
                "expr=off,joins=off,subq=off,agg=off,win=off,dml=off,merge=off,\
                 txn=off,ddl=off,types=off,explain=off,util=off,part=off,\
                 objddl=off,idx=off,par=off,tsdl=off,cursor=off,views=off,geo=off,\
                 dtm=off,adtmisc=off,sqljson=off,plpg=off,coll=off,mbconv=off,\
             xnum=off,nodes=off,obs=off,admin=off,objid=off,einterp=off,exd=off,spill=off,earm=off,\
                 plansel=off,earm2=off,exr=off,numx=off,pubsub=off"
            )
            .is_err()
        );
    }

    #[test]
    fn disabled_modules_are_never_picked() {
        let tv = ToggleVector::parse(
            "expr=on,joins=off,subq=off,agg=off,win=off,dml=off,merge=off,\
             txn=off,ddl=off,types=off,explain=off,util=off,part=off,\
             objddl=off,idx=off,par=off,tsdl=off,cursor=off,views=off,geo=off,\
             dtm=off,adtmisc=off,sqljson=off,plpg=off,coll=off,mbconv=off,\
             xnum=off,nodes=off,obs=off,admin=off,objid=off,einterp=off,exd=off,spill=off,earm=off,\
             plansel=off,earm2=off,exr=off,numx=off,pubsub=off",
        )
        .unwrap();
        let mut rng = Rng::new(5);
        for _ in 0..64 {
            assert_eq!(tv.pick_module(&mut rng), "expr");
        }
    }

    #[test]
    fn swarm_is_seed_deterministic() {
        let a = ToggleVector::swarm(&mut Rng::new(9));
        let b = ToggleVector::swarm(&mut Rng::new(9));
        assert_eq!(a, b);
        assert!(a.entries.iter().any(|t| t.on));
    }

    #[test]
    fn pick_module_returns_enabled() {
        let tv = ToggleVector::all_on();
        let mut rng = Rng::new(3);
        for _ in 0..64 {
            let m = tv.pick_module(&mut rng);
            assert!(tv.is_on(m));
        }
    }
}
