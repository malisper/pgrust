//! `network_subset_support`'s `SupportRequestIndexCondition` leg (network.c:973-
//! 1131): the planner support function that converts a subnet subset/superset
//! operator/function call (`i << cidr`, `i <<= cidr`, `i >> cidr`, `i >>= cidr`)
//! into a pair of btree-rangeable index conditions
//! `key >= network_scan_first(rhs) AND key <= network_scan_last(rhs)`.
//!
//! This crate is the bridge that breaks the dep cycle the value crate
//! `backend-utils-adt-network` cannot: that crate owns the inet helpers
//! (`network_scan_first`/`network_scan_last`, themselves pure inet arithmetic)
//! but must not depend on `backend-nodes-core`/`lsyscache` to build the bound
//! `OpExpr` trees. This crate already has both (it ports the inet selectivity
//! estimators), so it ports the index-condition leg here:
//!
//!   * the `inet_struct` arithmetic (`network_scan_first`/`network_scan_last`)
//!     is the value crate's pure code, called directly;
//!   * the inet→on-disk-varlena→`Datum` re-serialization for the bound `Const`s
//!     is done here (the canonical header-ful varlena image the rest of the
//!     codebase carries on `Datum::ByRef`), reading the RHS `Const`'s inet word
//!     through the same `datum_get_inet_pp` detoast seam the estimators use;
//!   * the catalog lookups (`get_opfamily_member_for_cmptype`) and node
//!     construction (`make_const`/`make_opclause`) cross the lsyscache /
//!     nodes-core dependencies this crate holds.
//!
//! Ported faithfully from `network_subset_support` / `match_network_function` /
//! `match_network_subset` (utils/adt/network.c).

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{InvalidOid, Oid};
use types_network::inet_struct;
use nodes::primnodes::Expr;
use pathnodes::{IndexOptInfo, NodeId, PlannerInfo};

use nodes_core::makefuncs::{make_const, make_opclause};
use adt_network::planner::{network_scan_first, network_scan_last};
use network_seams::inet::datum_get_inet_pp;
use lsyscache_seams::get_opfamily_member_for_cmptype;
use fmgr_support_seams::index_support_registry as reg;

// The canonical unified `Datum` (the by-reference inet varlena image), carried
// on `Const.constvalue` and produced for the new bound `Const`s here.
use types_tuple::heaptuple::Datum as DatumV;

/* ---------------------------------------------------------------------------
 * Catalog OIDs.
 * ------------------------------------------------------------------------- */

/// `INETOID` (pg_type.dat oid 869) — the `inet` datatype.
const INETOID: Oid = 869;
/// `BOOLOID` (pg_type.dat oid 16).
const BOOLOID: Oid = 16;

/// `network_subset_support`'s prosupport OID (pg_proc.dat oid 1173).
pub const F_NETWORK_SUBSET_SUPPORT: Oid = 1173;

/* `match_network_function` recognized funcids (fmgroids.h). */
/// `F_NETWORK_SUB` — `network_sub` (`<<`).
const F_NETWORK_SUB: Oid = 927;
/// `F_NETWORK_SUBEQ` — `network_subeq` (`<<=`).
const F_NETWORK_SUBEQ: Oid = 928;
/// `F_NETWORK_SUP` — `network_sup` (`>>`).
const F_NETWORK_SUP: Oid = 929;
/// `F_NETWORK_SUPEQ` — `network_supeq` (`>>=`).
const F_NETWORK_SUPEQ: Oid = 930;

/* `CompareType` codes (access/cmptype.h). */
/// `COMPARE_LE`.
const COMPARE_LE: i32 = 2;
/// `COMPARE_GE`.
const COMPARE_GE: i32 = 4;
/// `COMPARE_GT`.
const COMPARE_GT: i32 = 5;

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// Build the canonical header-ful `inet` varlena `Datum::ByRef` for an
/// `inet_struct`: a 4-byte `SET_VARSIZE` length word + the 18-byte
/// `inet_struct::to_datum_bytes` image, exactly the image `datum_get_inet_pp`
/// decodes (which strips the 4-byte header). This is the inet→Datum
/// re-serialization the bound `Const`s need.
fn inet_to_datum<'mcx>(mcx: mcx::Mcx<'mcx>, addr: &inet_struct) -> types_error::PgResult<DatumV<'mcx>> {
    let payload = addr.to_datum_bytes();
    let total = payload.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&payload);
    DatumV::from_byref_bytes_in(mcx, &img)
}

/// `match_network_subset` (network.c:1068) — try to generate an indexqual for a
/// network subset function. `leftop` is the indexkey expression; `rightopval` is
/// the already-decoded RHS `inet_struct` (the C `((Const *) rightop)->constvalue`
/// decoded through `network_scan_first`/`network_scan_last`'s `DatumGetInetPP`).
/// Returns the derived bare index-condition `Expr`s (empty ⇒ C `NIL`).
fn match_network_subset<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    leftop: &Expr<'_>,
    rightopval: &inet_struct,
    is_eq: bool,
    opfamily: Oid,
) -> types_error::PgResult<Vec<Expr<'mcx>>> {
    let datatype = INETOID;

    // create clause "key >= network_scan_first(rightopval)", or ">" if the
    // operator disallows equality.
    let opr1oid = get_opfamily_member_for_cmptype::call(
        opfamily,
        datatype,
        datatype,
        if is_eq { COMPARE_GE } else { COMPARE_GT },
    )?;
    if opr1oid == InvalidOid {
        return Ok(Vec::new());
    }

    let opr1right = network_scan_first(rightopval);
    let opr1right_datum = inet_to_datum(mcx, &opr1right)?;
    let const1 = make_const(
        mcx,
        datatype,
        -1,
        InvalidOid, /* not collatable */
        -1,
        opr1right_datum,
        false,
        false,
    )?;
    let expr1 = make_opclause(
        opr1oid,
        BOOLOID,
        false,
        leftop.clone_in(mcx)?,
        Some(Expr::Const(const1)),
        InvalidOid,
        InvalidOid,
    );
    let mut result = alloc::vec![expr1];

    // create clause "key <= network_scan_last(rightopval)".
    let opr2oid = get_opfamily_member_for_cmptype::call(opfamily, datatype, datatype, COMPARE_LE)?;
    if opr2oid == InvalidOid {
        return Ok(Vec::new());
    }

    let opr2right = network_scan_last(rightopval)?;
    let opr2right_datum = inet_to_datum(mcx, &opr2right)?;
    let const2 = make_const(
        mcx,
        datatype,
        -1,
        InvalidOid, /* not collatable */
        -1,
        opr2right_datum,
        false,
        false,
    )?;
    let expr2 = make_opclause(
        opr2oid,
        BOOLOID,
        false,
        leftop.clone_in(mcx)?,
        Some(Expr::Const(const2)),
        InvalidOid,
        InvalidOid,
    );
    result.push(expr2);

    Ok(result)
}

/// `match_network_function` (network.c:1020) — identify the function and swap the
/// arguments if necessary, then delegate to `match_network_subset`. `leftop` /
/// `rightop` are the clause's two argument expressions in their original order;
/// `indexarg` (0 = left, 1 = right) is the side the indexkey is on.
fn match_network_function<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    leftop: &Expr<'_>,
    rightop: &Expr<'_>,
    indexarg: i32,
    funcid: Oid,
    opfamily: Oid,
) -> types_error::PgResult<Vec<Expr<'mcx>>> {
    // Resolve the side that must be a non-NULL inet `Const` (the RHS of the
    // subset test in canonical orientation) into its decoded `inet_struct`.
    // Returns `None` (the C `NIL`) when it is not a usable constant.
    let decode_const = |c_expr: &Expr<'_>| -> types_error::PgResult<Option<inet_struct>> {
        match c_expr {
            Expr::Const(c) if !c.constisnull => Ok(Some(datum_get_inet_pp::call(mcx, &c.constvalue)?)),
            _ => Ok(None),
        }
    };

    match funcid {
        F_NETWORK_SUB => {
            // indexkey must be on the left
            if indexarg != 0 {
                return Ok(Vec::new());
            }
            match decode_const(rightop)? {
                Some(rv) => match_network_subset(mcx, leftop, &rv, false, opfamily),
                None => Ok(Vec::new()),
            }
        }
        F_NETWORK_SUBEQ => {
            if indexarg != 0 {
                return Ok(Vec::new());
            }
            match decode_const(rightop)? {
                Some(rv) => match_network_subset(mcx, leftop, &rv, true, opfamily),
                None => Ok(Vec::new()),
            }
        }
        F_NETWORK_SUP => {
            // indexkey must be on the right
            if indexarg != 1 {
                return Ok(Vec::new());
            }
            // match_network_subset(rightop, leftop, ...) — the indexkey is rightop,
            // the constant is leftop.
            match decode_const(leftop)? {
                Some(lv) => match_network_subset(mcx, rightop, &lv, false, opfamily),
                None => Ok(Vec::new()),
            }
        }
        F_NETWORK_SUPEQ => {
            if indexarg != 1 {
                return Ok(Vec::new());
            }
            match decode_const(leftop)? {
                Some(lv) => match_network_subset(mcx, rightop, &lv, true, opfamily),
                None => Ok(Vec::new()),
            }
        }
        // Attached to an unexpected function: do nothing (C `default: return NIL`).
        _ => Ok(Vec::new()),
    }
}

/// `network_subset_support`'s `SupportRequestIndexCondition` leg (network.c:973).
/// Registered in the OID-keyed index-condition registry under
/// `F_NETWORK_SUBSET_SUPPORT`; the dispatcher routes the seam call here when the
/// function's `prosupport` is `network_subset_support`.
///
/// The C body inspects `is_opclause(req->node) || is_funcclause(req->node)` (a
/// 2-arg `OpExpr` or `FuncExpr`) and runs `match_network_function` over the two
/// operands. The transient `OpExpr` nodes are built in a per-call context and the
/// derived `Expr`s erased to `'static` at the seam handoff (the seam caller
/// re-allocates each into the planner arena, exactly as the pattern kernel does).
fn network_subset_index_condition(
    root: &PlannerInfo,
    _prosupport: Oid,
    funcid: Oid,
    clause: NodeId,
    indexarg: i32,
    index: &IndexOptInfo,
    indexcol: i32,
) -> (Vec<Expr<'static>>, bool) {
    let node = root.node(clause);
    // is_opclause / is_funcclause: a 2-arg OpExpr or FuncExpr (indexkey, const).
    let (left, right) = match node {
        Expr::OpExpr(op) if op.args.len() == 2 => (&op.args[0], &op.args[1]),
        Expr::FuncExpr(f) if f.args.len() == 2 => (&f.args[0], &f.args[1]),
        _ => return (Vec::new(), false),
    };

    let opfamily = index.opfamily[indexcol as usize];

    // The derived OpExpr/Const nodes are built in a per-call context; the seam
    // caller clones each out into the planner arena (allocs into `root` + wraps
    // in a RestrictInfo), so erase the per-call lifetime at the handoff boundary.
    let cx = mcx::MemoryContext::new("network_subset_support match_network_function");
    let result = match match_network_function(cx.mcx(), left, right, indexarg, funcid, opfamily) {
        Ok(v) => v,
        // C's support function only does catalog lookups + pure inet arithmetic
        // here; a hard error (e.g. a detoast ereport) surfaces as a panic
        // (mirror-PG-and-abort), as for the other index-support boundaries.
        Err(e) => panic!("network_subset_support failed: {}", e.message()),
    };
    let result: Vec<Expr<'static>> = result.into_iter().map(Expr::erase_lifetime).collect();

    // `get_index_clause_from_support` (indxpath.c) sets `req.lossy = true` as the
    // default, and `network_subset_support` never overrides it. The derived
    // `>=`/`<=` range quals only bound the search of an `inet` btree by the
    // canonical scan-first/scan-last endpoints; the original subset operator must
    // be retained as a recheck `Filter` (a `<<` clause is not equivalent to the
    // address-range bound when masklens differ), so the clause stays lossy.
    (result, true)
}

/// Register `network_subset_support`'s index-condition kernel in the OID-keyed
/// registry and install the dispatcher on the `oid_function_call1_index_support`
/// seam (idempotent — the same dispatcher; harmless if selfuncs already did it).
pub fn init_support_seam() {
    reg::register_index_condition(F_NETWORK_SUBSET_SUPPORT, network_subset_index_condition);
    reg::install_dispatch();
}
