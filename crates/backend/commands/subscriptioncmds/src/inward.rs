//! Installation of this unit's two inward seams.
//!
//! The seam contracts (`backend-commands-subscriptioncmds-seams`) are
//! `Mcx`-free: `AlterSubscriptionOwner` is reached from the ALTER ... OWNER TO
//! utility dispatch (`backend-commands-alter`) and `alter_subscription_owner_oid`
//! from `backend-catalog-pg-shdepend`'s REASSIGN OWNED, neither of which has an
//! `Mcx` in scope. Each installer wrapper spins up a fresh `MemoryContext` to
//! obtain an `Mcx` for the ported body (the established bridging idiom, cf.
//! `backend-commands-publicationcmds::init_seams`).

use ::utils_error::PgError;
use ::nodes::nodes::Node;

use subscriptioncmds_seams as s;
use utility_out_seams as rt;

pub fn init_seams() {
    s::alter_subscription_owner_oid::set(|subid, new_owner_id| {
        let ctx = mcx::MemoryContext::new("AlterSubscriptionOwner_oid");
        crate::AlterSubscriptionOwner_oid(ctx.mcx(), subid, new_owner_id)
    });

    s::AlterSubscriptionOwner::set(|name, new_owner_id| {
        let ctx = mcx::MemoryContext::new("AlterSubscriptionOwner");
        crate::AlterSubscriptionOwner(ctx.mcx(), name, new_owner_id)
    });

    // utility.c `ProcessUtilitySlow` dispatches CREATE/ALTER/DROP SUBSCRIPTION
    // through tcop-utility-out-seams.
    rt::create_subscription::set(create_subscription_seam);
    rt::alter_subscription::set(alter_subscription_seam);
    rt::drop_subscription::set(drop_subscription_seam);
}

/// Outward-seam adapter for `CreateSubscription` (utility.c,
/// `T_CreateSubscriptionStmt`).
fn create_subscription_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstate: &mut ::nodes::parsestmt::ParseState<'mcx>,
    stmt: &Node<'mcx>,
    is_top_level: bool,
) -> ::utils_error::PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    let cs = stmt
        .as_createsubscriptionstmt()
        .ok_or_else(|| PgError::error("create_subscription_seam: statement is not a CreateSubscriptionStmt"))?;
    crate::CreateSubscription(mcx, pstate, cs, is_top_level)
}

/// Outward-seam adapter for `AlterSubscription` (utility.c,
/// `T_AlterSubscriptionStmt`).
fn alter_subscription_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstate: &mut ::nodes::parsestmt::ParseState<'mcx>,
    stmt: &Node<'mcx>,
    is_top_level: bool,
) -> ::utils_error::PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    let as_ = stmt
        .as_altersubscriptionstmt()
        .ok_or_else(|| PgError::error("alter_subscription_seam: statement is not an AlterSubscriptionStmt"))?;
    crate::AlterSubscription(mcx, pstate, as_, is_top_level)
}

/// Outward-seam adapter for `DropSubscription` (utility.c,
/// `T_DropSubscriptionStmt`).
fn drop_subscription_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    stmt: &Node<'mcx>,
    is_top_level: bool,
) -> ::utils_error::PgResult<()> {
    let ds = stmt
        .as_dropsubscriptionstmt()
        .ok_or_else(|| PgError::error("drop_subscription_seam: statement is not a DropSubscriptionStmt"))?;
    crate::DropSubscription(mcx, ds, is_top_level)
}
