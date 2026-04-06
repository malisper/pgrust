# Compiled query execution (push-based / fused loops)

## Idea
Instead of the Volcano pull model where each tuple returns up through the
executor tree, "compile" the plan into a fused loop at plan time. Similar
to how CompiledTupleDecoder precomputes decode steps, a compiled plan would
fuse scan + filter + projection + decode into a single tight loop with no
per-tuple function calls, TupleSlot allocation, or return/re-enter overhead.

## Motivation
Profiling (April 2026) shows ~25-30% of seq scan time in pure executor
overhead:
- execute_plan loop: ~10%
- function pointer dispatch: ~15%
- TupleSlot wrapping/unwrapping: ~8.5%

The actual tuple decoding and page access is a minority of the total cost.

## What it would look like

For a bare `SELECT * FROM t`, the compiled form would be:
```
loop over pages:
    prepare_page (batch visibility)
    lock page
    loop over visible tuples:
        decode bytes directly into output buffer
    unlock page
```

For `SELECT col FROM t WHERE pred`, fuse filter + projection into the inner
loop -- never allocate intermediate TupleSlots:
```
loop over visible tuples:
    decode only needed columns
    evaluate predicate inline
    if passes: emit projected row
```

## Background
This is the "data-centric compilation" approach from HyPer/Umbra. The key
insight: tuples never "return" up the tree; the producer pushes data directly
into the consumer's logic. Pipeline breakers (sorts, aggregates, hash joins)
are the natural boundaries between compiled fragments.

## Approach: flattened step array (like CompiledTupleDecoder)

The current function pointer dispatch only avoids the match at the top node.
Nested nodes (e.g. `Projection(Filter(SeqScan))`) still go through per-tuple
function pointer calls at each layer via `exec_next(&mut state.input, ctx)`.

At plan init time, recursively walk the plan tree and flatten pipelineable
nodes into a linear `Vec<CompiledStep>`, similar to how CompiledTupleDecoder
turns column descriptors into a flat `Vec<DecodeStep>`:

```rust
enum CompiledStep {
    SeqScanNext { rel, decoder },
    Filter { predicate },
    Project { targets },
}

struct CompiledPlan {
    steps: Vec<CompiledStep>,
}
```

Execution becomes a single loop: for each tuple, walk the steps array
linearly. No recursion, no function pointer calls per layer. Pipeline
breakers (ORDER BY, aggregates, hash joins) would be boundaries between
compiled fragments.

## Prerequisites
- Current function-pointer dispatch is already in place
- Page-mode visibility batching is done
- Would need a way to represent compiled plans (enum of specialized structs,
  or trait objects, or closures)
