use super::*;
use ::nodes_core::bitmapset::bms_next_member;

/// Collect every member of a set in ascending order via `bms_next_member`,
/// the analogue of iterating with `bms_next_member` in C.
fn members(set: Option<&Bitmapset>) -> Vec<i32> {
    let mut result = Vec::new();
    let mut member = -1;
    loop {
        member = bms_next_member(set, member);
        if member < 0 {
            return result;
        }
        result.push(member);
    }
}

#[test]
fn chooses_best_value_under_weight() {
    let ctx = mcx::MemoryContext::new("t");
    let weights = [2, 3, 4, 5];
    let values = [3.0, 4.0, 5.0, 8.0];
    let result = DiscreteKnapsack(ctx.mcx(), 5, 4, &weights, Some(&values)).unwrap();
    assert_eq!(members(result.as_deref()), vec![3]);
}

#[test]
fn includes_zero_weight_items() {
    let ctx = mcx::MemoryContext::new("t");
    let weights = [0, 2, 3];
    let result = DiscreteKnapsack(ctx.mcx(), 2, 3, &weights, None).unwrap();
    assert_eq!(members(result.as_deref()), vec![0, 1]);
}

#[test]
fn default_value_one_each() {
    // With unit values, the solver maximizes the number of items packed.
    let ctx = mcx::MemoryContext::new("t");
    let weights = [1, 1, 1, 5];
    let result = DiscreteKnapsack(ctx.mcx(), 3, 4, &weights, None).unwrap();
    assert_eq!(members(result.as_deref()), vec![0, 1, 2]);
}

#[test]
fn zero_max_weight_keeps_only_zero_weight_items() {
    let ctx = mcx::MemoryContext::new("t");
    let weights = [0, 1, 0];
    let result = DiscreteKnapsack(ctx.mcx(), 0, 3, &weights, None).unwrap();
    assert_eq!(members(result.as_deref()), vec![0, 2]);
}

#[test]
fn empty_solution_when_nothing_fits() {
    // Every item is heavier than the budget and none is weight-0, so the
    // optimal pack is empty (the C original returns an empty Bitmapset/NULL).
    let ctx = mcx::MemoryContext::new("t");
    let weights = [3, 4, 5];
    let result = DiscreteKnapsack(ctx.mcx(), 2, 3, &weights, None).unwrap();
    assert_eq!(members(result.as_deref()), vec![]);
}
