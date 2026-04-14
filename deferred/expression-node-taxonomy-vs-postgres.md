## Expression Node Taxonomy vs PostgreSQL

`pgrust` now has some PostgreSQL-shaped semantic expression nodes in
[src/include/nodes/primnodes.rs](src/include/nodes/primnodes.rs),
such as `Var`, `OpExpr`, `BoolExpr`, `FuncExpr`, `Aggref`, `SubLink`, and
`SubPlan`.

But the overall expression taxonomy still does not line up with PostgreSQL's
[primnodes.h](~/postgres/src/include/nodes/primnodes.h).

Examples of current differences:

- `pgrust` still uses project-local `Expr` variants like `Cast`, `Coalesce`,
  `Like`, `Similar`, `IsNull`, `IsNotNull`, `Random`, `CurrentDate`,
  `CurrentTime`, `CurrentTimestamp`, `LocalTime`, and `LocalTimestamp`.
- PostgreSQL splits those concepts across a richer set of semantic node types,
  including `RelabelType`, `CoerceViaIO`, `ArrayCoerceExpr`,
  `ConvertRowtypeExpr`, `CollateExpr`, `CoalesceExpr`, `SQLValueFunction`,
  `NullTest`, `BooleanTest`, `CaseExpr`, `RowCompareExpr`, `XmlExpr`,
  `JsonValueExpr`, `JsonConstructorExpr`, and `JsonExpr`.
- `pgrust` currently models `Coalesce` as a binary node, while PostgreSQL uses
  n-ary `CoalesceExpr.args`.
- `pgrust` currently models all casts through a single generic `Expr::Cast`,
  while PostgreSQL uses multiple coercion node families depending on semantics.

Why this matters:

- PostgreSQL's expression node types are semantic categories, not just syntax
  spellings.
- Planner transforms, type coercion, collation handling, null semantics, and
  executor behavior often depend on those distinctions.
- Keeping a flatter project-local `Expr` enum forces more special-case matches
  in binder/planner/executor rewrites and makes the tree shape diverge from
  PostgreSQL.

Deferred direction:

- Keep ordinary functions as `FuncExpr` and ordinary operators as `OpExpr`.
- Preserve dedicated node types for semantic forms that PostgreSQL also keeps
  distinct, instead of flattening everything into generic function calls.
- Gradually replace coarse project-local `Expr` variants with closer
  PostgreSQL-shaped semantic nodes where planner/executor behavior depends on
  the distinction.
