/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef NODEFUNCS_H
#define NODEFUNCS_H
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
/* exprType: planner infrastructure outside the crate-under-test's scope.
 * The oracle driver models PASSING variables exactly like the shipped Rust
 * API (vars: &[(name, Oid)]): each varexprs entry is a PgDiffVarExpr
 * carrying the type oid directly, and exprType reads it back. Environment
 * model, documented in pg_jsonpath_env.c. */
typedef struct PgDiffVarExpr
{
	NodeTag		type;			/* T_Invalid; never IsA-tested by the walker */
	Oid			typeoid;
} PgDiffVarExpr;
extern Oid	exprType(const Node *expr);
extern int32 exprTypmod(const Node *expr);
#endif
