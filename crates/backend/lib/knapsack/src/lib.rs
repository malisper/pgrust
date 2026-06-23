//! Port of `src/backend/lib/knapsack.c` (PostgreSQL 18.3).
//!
//! Knapsack problem solver.
//!
//! Given input vectors of integral item weights (must be >= 0) and values
//! (double >= 0), compute the set of items which produces the greatest total
//! value without exceeding a specified total weight; each item is included at
//! most once (this is the 0/1 knapsack problem).  Weight 0 items will always be
//! included.
//!
//! The performance of this algorithm is pseudo-polynomial, O(nW) where W is the
//! weight limit.  To use with non-integral weights or approximate solutions,
//! the caller should pre-scale the input weights to a suitable range.  This
//! allows approximate solutions in polynomial time (the general case of the
//! exact problem is NP-hard).
//!
//! # Memory model
//!
//! The C original creates a private `AllocSetContext` ("Knapsack") as a child of
//! `CurrentMemoryContext`, switches to it, `palloc()`s the two scratch arrays
//! (`values`, `sets`) plus all the working `Bitmapset`s inside it, and at the
//! end `MemoryContextDelete()`s the whole context — after `bms_copy()`ing the
//! result out into the original (caller's) context.
//!
//! Here `DiscreteKnapsack` takes the caller's context `mcx` (the analogue of
//! C's `CurrentMemoryContext` at entry, where the result must end up) and
//! creates a private child [`::mcx::MemoryContext`] (`local_ctx`) for all scratch
//! allocations. The scratch arrays and every working bitmapset are allocated in
//! `local_ctx.mcx()`; the result is `bms_copy`'d into the caller's `mcx`, and
//! `local_ctx` is dropped at function exit, reclaiming everything — exactly
//! mirroring `MemoryContextDelete(local_ctx)`.

use ::nodes_core::bitmapset::{
    bms_add_member, bms_copy, bms_del_member, bms_make_singleton, bms_replace_members,
};
use ::mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_error::PgResult;
use ::nodes::bitmapset::Bitmapset;

/// DiscreteKnapsack
///
/// The `item_values` input is optional; if omitted, all the items are assumed
/// to have value 1.
///
/// Returns a `Bitmapset` of the `0..(num_items-1)` indexes of the items chosen
/// for inclusion in the solution (the C NULL/empty set is `None`).
///
/// This uses the usual dynamic-programming algorithm, adapted to reuse the
/// memory on each pass (by working from larger weights to smaller).  At the
/// start of pass number `i`, the `values[w]` array contains the largest value
/// computed with total weight <= w, using only items with indices < i; and
/// `sets[w]` contains the bitmap of items actually used for that value.  (The
/// bitmapsets are all pre-initialized with an unused high bit so that memory
/// allocation is done only once.)
#[allow(non_snake_case)]
pub fn DiscreteKnapsack<'mcx>(
    mcx: Mcx<'mcx>,
    max_weight: i32,
    num_items: i32,
    item_weights: &[i32],
    item_values: Option<&[f64]>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    // MemoryContext local_ctx = AllocSetContextCreate(CurrentMemoryContext,
    //                                                 "Knapsack",
    //                                                 ALLOCSET_SMALL_SIZES);
    // MemoryContext oldctx = MemoryContextSwitchTo(local_ctx);
    //
    // The pilot context is a usage tracker; "switching to" it means routing the
    // scratch allocations through `local_ctx.mcx()`. ALLOCSET_SMALL_SIZES sizes
    // the AllocSet blocks; the pilot context is pure accounting, so block sizing
    // is moot and only the name is carried.
    let local_ctx = mcx.context().new_child("Knapsack");
    let lcx = local_ctx.mcx();

    // Assert(max_weight >= 0);
    debug_assert!(max_weight >= 0);
    // Assert(num_items > 0 && item_weights);
    debug_assert!(num_items > 0 && item_weights.len() >= num_items as usize);
    // C's item_values is a trusted `double *` for num_items entries when given.
    debug_assert!(item_values.is_none_or(|v| v.len() >= num_items as usize));

    let len = 1usize + max_weight as usize;

    // values = palloc((1 + max_weight) * sizeof(double));
    let mut values: PgVec<f64> = vec_with_capacity_in(lcx, len)?;
    // sets   = palloc((1 + max_weight) * sizeof(Bitmapset *));
    let mut sets: PgVec<Option<PgBox<Bitmapset>>> = vec_with_capacity_in(lcx, len)?;

    // for (i = 0; i <= max_weight; ++i)
    // {
    //     values[i] = 0;
    //     sets[i] = bms_make_singleton(num_items);
    // }
    for _i in 0..=max_weight {
        values.push(0.0);
        sets.push(Some(bms_make_singleton(lcx, num_items)?));
    }

    // for (i = 0; i < num_items; ++i)
    for i in 0..num_items {
        // int    iw = item_weights[i];
        let iw = item_weights[i as usize];
        // double iv = item_values ? item_values[i] : 1;
        let iv = match item_values {
            Some(item_values) => item_values[i as usize],
            None => 1.0,
        };

        // for (j = max_weight; j >= iw; --j)
        let mut j = max_weight;
        while j >= iw {
            // int ow = j - iw;
            let ow = j - iw;

            // if (values[j] <= values[ow] + iv)
            if values[j as usize] <= values[ow as usize] + iv {
                // copy sets[ow] to sets[j] without realloc
                // if (j != ow)
                //     sets[j] = bms_replace_members(sets[j], sets[ow]);
                if j != ow {
                    // Take both endpoints out of the array so sets[ow] can be
                    // borrowed immutably while sets[j] is consumed.
                    // bms_replace_members overwrites sets[j]'s contents with
                    // sets[ow]'s (resizing only if needed), exactly like C.
                    let owned_j = sets[j as usize].take();
                    let owned_ow = sets[ow as usize].take();
                    let replaced = bms_replace_members(lcx, owned_j, owned_ow.as_deref())?;
                    sets[j as usize] = replaced;
                    sets[ow as usize] = owned_ow;
                }

                // sets[j] = bms_add_member(sets[j], i);
                let owned_j = sets[j as usize].take();
                sets[j as usize] = Some(bms_add_member(lcx, owned_j, i)?);

                // values[j] = values[ow] + iv;
                values[j as usize] = values[ow as usize] + iv;
            }

            j -= 1;
        }
    }

    // MemoryContextSwitchTo(oldctx);
    // result = bms_del_member(bms_copy(sets[max_weight]), num_items);
    //
    // bms_copy copies the winning scratch set into the caller's context `mcx`
    // (the analogue of copying into `oldctx`), independent of `local_ctx` which
    // is reclaimed when it drops at function exit.
    let copied = bms_copy(mcx, sets[max_weight as usize].as_deref())?;
    let result = bms_del_member(copied, num_items);

    // MemoryContextDelete(local_ctx): at function exit the scratch arrays
    // (`sets`, `values`) drop first, then `local_ctx`, freeing the two scratch
    // arrays and every working bitmapset in one shot. `result` already lives in
    // the caller's `mcx`, so it survives independently.
    Ok(result)
}

#[cfg(test)]
mod tests;
