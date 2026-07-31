/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * Minimal executor state shapes so the (unreachable-by-construction)
 * JSON_TABLE machinery in the verbatim jsonpath_exec.c COMPILES; only the
 * fields that TU reads exist. Nothing in this harness ever constructs
 * these — ExecEvalExpr / init_MultiFuncCall etc. are LOUD ABORT stubs in
 * pg_jsonpath_exec_env.c. */
#ifndef EXECNODES_H
#define EXECNODES_H
#include "nodes/primnodes.h"

typedef struct ExprContext
{
	NodeTag		type;
	Datum		caseValue_datum;
	bool		caseValue_isNull;
} ExprContext;

typedef struct ExprState
{
	NodeTag		type;
	Expr	   *expr;
} ExprState;

typedef struct Plan
{
	NodeTag		type;
} Plan;

typedef struct TableFuncScan
{
	Plan		scan;
	TableFunc  *tablefunc;
} TableFuncScan;

typedef struct PlanState
{
	NodeTag		type;
	Plan	   *plan;
	ExprContext *ps_ExprContext;
} PlanState;

typedef struct ScanState
{
	PlanState	ps;
} ScanState;

typedef struct TableFuncScanState
{
	ScanState	ss;
	List	   *passingvalexprs;
	List	   *colvalexprs;
	void	   *opaque;
} TableFuncScanState;

extern Datum ExecEvalExpr(ExprState *state, ExprContext *econtext,
						  bool *isNull);
#endif
