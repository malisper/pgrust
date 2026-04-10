# Precompute cast functions at bind time

## Summary

`::` casts currently bind to a generic `Expr::Cast` node and decide the exact
conversion at execution time. That is correct for the small set of supported
casts, but it means every row pays for runtime dispatch on the source/target
types.

This should be tightened so cast resolution happens during binding or planning.
If the input type and target type are known, the binder should select a
concrete cast implementation once and hand the executor a specialized
expression node or function pointer.

Examples:

- `int4 -> text`
- `text -> int4`
- `bool -> text`

## Why deferred

The parser and binder do not yet have broad expression type inference. Without
that, many casts cannot be resolved fully at bind time because the engine does
not always know the input type of an arbitrary expression.

A clean implementation wants:

- expression type inference for bound expressions
- cast lookup/resolution during binding
- constant-folding for casts on literals where possible
- specialized executor nodes or preselected cast callbacks

Until that exists, the generic runtime `Expr::Cast` path is a pragmatic bridge
for correctness, but not the final execution model.
