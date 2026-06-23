//! Planner support for `inet`/`cidr` (network.c:973-1131, 1654-1674):
//! `network_subset_support`, `match_network_function`, `match_network_subset`,
//! and the indexscan-limit helpers `network_scan_first` / `network_scan_last`.
//!
//! The indexscan-limit helpers are *pure* (they only call already-ported network
//! functions) and are implemented here 1:1. `match_network_function`'s function
//! dispatch (which funcid maps to which subset shape + the `indexarg` side
//! checks) is ported faithfully in [`classify_network_function`]. The index
//! condition `OpExpr` tree construction (catalog lookups, `makeConst`,
//! `make_opclause`) is genuinely external (planner / catalog / nodes) and crosses
//! the [`::network_seams::planner`] seam.

use ::network_seams::planner;
use ::types_error::PgResult;
use ::types_network::inet_struct;

use crate::{inet_set_masklen, network_broadcast, network_network};

/// `network_scan_first` (network.c:1654). Minimal value for an IP on a given
/// network: `network_network(in)`.
pub fn network_scan_first(input: &inet_struct) -> inet_struct {
    network_network(input)
}

/// `network_scan_last` (network.c:1668). "Last" IP on a given network: the
/// broadcast address with masklen maxed out
/// (`inet_set_masklen(network_broadcast(in), -1)`).
pub fn network_scan_last(input: &inet_struct) -> PgResult<inet_struct> {
    // DirectFunctionCall1(network_broadcast, in)
    let broadcast = network_broadcast(input);
    // DirectFunctionCall2(inet_set_masklen, broadcast, Int32GetDatum(-1))
    inet_set_masklen(&broadcast, -1)
}

/// The four containment funcids `match_network_function` recognizes
/// (network.c:1026, `F_NETWORK_SUB`/`SUBEQ`/`SUP`/`SUPEQ` from `fmgroids.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkFunc {
    /// `F_NETWORK_SUB` (`<<`).
    Sub,
    /// `F_NETWORK_SUBEQ` (`<<=`).
    Subeq,
    /// `F_NETWORK_SUP` (`>>`).
    Sup,
    /// `F_NETWORK_SUPEQ` (`>>=`).
    Supeq,
}

/// The outcome of `match_network_function`'s dispatch (network.c:1019): which
/// arguments to feed `match_network_subset`, and whether equality is allowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubsetMatch {
    /// `true` if the function allows equality (subeq/supeq): C `is_eq`.
    pub is_eq: bool,
    /// `true` if the operands were swapped (sup/supeq put the indexkey on the
    /// right, so `match_network_subset(rightop, leftop, ...)`): C arg order.
    pub swap_args: bool,
}

/// `match_network_function` (network.c:1019), the pure dispatch.
///
/// Given the recognized [`NetworkFunc`] and the `indexarg` position (0 = left,
/// 1 = right), returns how `match_network_subset` should be invoked, or `None`
/// when the indexkey is on the wrong side (C `return NIL`) or the funcid is not
/// one of the four (C `default: return NIL`).
pub fn classify_network_function(func: Option<NetworkFunc>, indexarg: i32) -> Option<SubsetMatch> {
    match func {
        Some(NetworkFunc::Sub) => {
            // indexkey must be on the left
            if indexarg != 0 {
                return None;
            }
            Some(SubsetMatch { is_eq: false, swap_args: false })
        }
        Some(NetworkFunc::Subeq) => {
            if indexarg != 0 {
                return None;
            }
            Some(SubsetMatch { is_eq: true, swap_args: false })
        }
        Some(NetworkFunc::Sup) => {
            // indexkey must be on the right
            if indexarg != 1 {
                return None;
            }
            Some(SubsetMatch { is_eq: false, swap_args: true })
        }
        Some(NetworkFunc::Supeq) => {
            if indexarg != 1 {
                return None;
            }
            Some(SubsetMatch { is_eq: true, swap_args: true })
        }
        // We'd only get here if somebody attached this support function to an
        // unexpected function.  Do nothing.
        None => None,
    }
}

/// `network_subset_support` (network.c:973). Planner support function.
///
/// The `SupportRequestIndexCondition` request inspection + index-condition
/// `OpExpr` tree construction (`get_opfamily_member_for_cmptype`, `makeConst`,
/// `make_opclause`, using [`network_scan_first`] / [`network_scan_last`]) is
/// genuinely external (planner / catalog / nodes subsystems) and is delegated to
/// the [`::network_seams::planner::network_subset_support`] seam,
/// which models "did we derive index conditions" (declining is always a valid
/// planner answer).
pub fn network_subset_support() -> bool {
    planner::network_subset_support::call()
}
