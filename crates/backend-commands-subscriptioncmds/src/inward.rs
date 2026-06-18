//! Installation of this unit's two inward seams.
//!
//! The seam contracts (`backend-commands-subscriptioncmds-seams`) are
//! `Mcx`-free: `AlterSubscriptionOwner` is reached from the ALTER ... OWNER TO
//! utility dispatch (`backend-commands-alter`) and `alter_subscription_owner_oid`
//! from `backend-catalog-pg-shdepend`'s REASSIGN OWNED, neither of which has an
//! `Mcx` in scope. Each installer wrapper spins up a fresh `MemoryContext` to
//! obtain an `Mcx` for the ported body (the established bridging idiom, cf.
//! `backend-commands-publicationcmds::init_seams`).

use backend_commands_subscriptioncmds_seams as s;

pub fn init_seams() {
    s::alter_subscription_owner_oid::set(|subid, new_owner_id| {
        let ctx = mcx::MemoryContext::new("AlterSubscriptionOwner_oid");
        crate::AlterSubscriptionOwner_oid(ctx.mcx(), subid, new_owner_id)
    });

    s::AlterSubscriptionOwner::set(|name, new_owner_id| {
        let ctx = mcx::MemoryContext::new("AlterSubscriptionOwner");
        crate::AlterSubscriptionOwner(ctx.mcx(), name, new_owner_id)
    });
}
