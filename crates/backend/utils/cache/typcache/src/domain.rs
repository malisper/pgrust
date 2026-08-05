use std::rc::Rc;

use lsyscache::TYPTYPE_DOMAIN;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::Node;

use crate::{
    lookup_type_cache, TypeCacheEntry, TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS,
    TYPECACHE_DOMAIN_CONSTR_INFO,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DomConstraintType {
    NotNull,
    Check,
}

pub struct DomainConstraintState {
    pub constrainttype: DomConstraintType,
    pub name: &'static str,
    pub check_expr: Option<Node<'static>>,
}

pub struct DomainConstraintCache {
    pub constraints: &'static [DomainConstraintState],
}

// C parents the dcc context under CacheMemoryContext and frees it when
// dccRefCount drops to zero; here it is leaked (constraint-set changes are
// DDL-rare and consumers borrow &'static into it — C's ref-list leak comment
// is the precedent).
fn leak_dcc_mcx() -> Mcx<'static> {
    ::mcx::session_root("Domain constraints").mcx()
}

fn str_in(mcx: Mcx<'static>, s: &str) -> PgResult<&'static str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

pub(crate) fn load_domaintype_info(entry: &TypeCacheEntry) -> PgResult<()> {
    let mut type_oid = entry.type_id;
    let mut not_null = false;
    let mut dcc_mcx: Option<Mcx<'static>> = None;
    let mut constraints: Vec<DomainConstraintState> = Vec::new();

    loop {
        let Some(t) = syscache_seams::pg_type_domain_shape::call(type_oid)? else {
            panic!("cache lookup failed for type {type_oid}");
        };
        if t.typtype != TYPTYPE_DOMAIN {
            break;
        }
        if t.typnotnull {
            not_null = true;
        }

        let scan_mcx = MemoryContext::new("load_domaintype_info");
        let rows = typcache_seams::scan_domain_check_constraints::call(scan_mcx.mcx(), type_oid)?;
        if !rows.is_empty() {
            let mcx = *dcc_mcx.get_or_insert_with(leak_dcc_mcx);
            let mut level: Vec<DomainConstraintState> = Vec::with_capacity(rows.len());
            for row in rows.iter() {
                let name_str = core::str::from_utf8(row.conname.name_str())
                    .unwrap_or_else(|_| panic!("non-UTF-8 constraint name"));
                let name: &'static str = str_in(mcx, name_str)?;
                let check_expr = readfuncs::stringToNode(mcx, row.conbin)?;
                let check_expr = expression_planner(mcx, check_expr)?;
                level.push(DomainConstraintState {
                    constrainttype: DomConstraintType::Check,
                    name,
                    check_expr: Some(check_expr),
                });
            }
            // C: per-level qsort by name, then lcons — ancestors first.
            level.sort_by(|a, b| a.name.cmp(b.name));
            level.append(&mut constraints);
            constraints = level;
        }

        type_oid = t.typbasetype;
    }

    if not_null {
        let mut level = vec![DomainConstraintState {
            constrainttype: DomConstraintType::NotNull,
            name: "NOT NULL",
            check_expr: None,
        }];
        level.append(&mut constraints);
        constraints = level;
    }

    if constraints.is_empty() {
        entry.domain_data.set(None);
    } else {
        let mcx = dcc_mcx.unwrap_or_else(leak_dcc_mcx);
        let mut v: PgVec<'static, DomainConstraintState> =
            mcx::vec_with_capacity_in(mcx, constraints.len())?;
        for c in constraints {
            v.push(c);
        }
        let dcc: &'static DomainConstraintCache = mcx::leak_in(mcx::alloc_in(
            mcx,
            DomainConstraintCache { constraints: v.leak() },
        )?);
        entry.domain_data.set(Some(dcc));
    }
    entry.set_flags(TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS);
    Ok(())
}

// C expression_planner (planner.c) = eval_const_expressions + fix_opfuncids;
// the planner-hook/PlannerGlobal surface does not apply to a bare expression.
fn expression_planner(mcx: Mcx<'static>, expr: Node<'static>) -> PgResult<Node<'static>> {
    let expr = clauses_seams::eval_const_expressions::call(mcx, expr)?;
    nodes_core::fix_opfuncids(expr)?;
    Ok(expr)
}

/// C DomainConstraintRef; need_exprstate is always false here — compiled
/// exprstates belong to the consumers (execexpr bakes steps, the domain_in
/// engine caches its own programs keyed by dcc identity).
pub struct DomainConstraintRef {
    entry: Rc<TypeCacheEntry>,
    dcc: Option<&'static DomainConstraintCache>,
}

impl DomainConstraintRef {
    pub fn init(type_id: Oid) -> PgResult<DomainConstraintRef> {
        let entry = lookup_type_cache(type_id, TYPECACHE_DOMAIN_CONSTR_INFO)?;
        let dcc = entry.domain_data.get();
        Ok(DomainConstraintRef { entry, dcc })
    }

    /// C UpdateDomainConstraintRef; returns true when the constraint set
    /// changed since init/the last update.
    pub fn update(&mut self) -> PgResult<bool> {
        if self.entry.flags_raw() & TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS == 0
            && self.entry.typtype() == TYPTYPE_DOMAIN
        {
            load_domaintype_info(&self.entry)?;
            self.entry.set_ready(crate::compute_ready(&self.entry));
        }
        let current = self.entry.domain_data.get();
        let changed = !match (self.dcc, current) {
            (Some(a), Some(b)) => core::ptr::eq(a, b),
            (None, None) => true,
            _ => false,
        };
        self.dcc = current;
        Ok(changed)
    }

    pub fn constraints(&self) -> &'static [DomainConstraintState] {
        match self.dcc {
            Some(dcc) => dcc.constraints,
            None => &[],
        }
    }

    /// Identity of the current dcc, for consumer-side compiled-program memos.
    pub fn dcc_addr(&self) -> usize {
        self.dcc.map_or(0, |d| d as *const _ as usize)
    }

    pub fn typlen(&self) -> i16 {
        self.entry.typlen()
    }
}

pub fn DomainHasConstraints(type_id: Oid) -> PgResult<bool> {
    let entry = lookup_type_cache(type_id, TYPECACHE_DOMAIN_CONSTR_INFO)?;
    Ok(entry.domain_data.get().is_some())
}
