/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code
 * (compile surface only). The SQL/JSON node definitions are VERBATIM from
 * src/include/nodes/primnodes.h @ 18.3 where marked; the executor never
 * runs over them in this harness (JSON_TABLE and the JsonExpr executor
 * entries are unreachable-by-construction — see pg_jsonpath_exec_env.c's
 * loud abort stubs). */
#ifndef PRIMNODES_H
#define PRIMNODES_H
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"

typedef int ParseLoc;
#define pg_node_attr(...)

/* Expr supertype (shape from primnodes.h @ 18.3) */
typedef struct Expr
{
	NodeTag		type;
} Expr;

/* Const: only the fields the vendored TU reads (constvalue/constisnull) */
typedef struct Const
{
	Expr		xpr;
	Oid			consttype;
	Datum		constvalue;
	bool		constisnull;
} Const;

/* opaque here: referenced by JsonExpr pointers only */
typedef struct JsonFormat JsonFormat;
typedef struct JsonReturning JsonReturning;

/* ---- VERBATIM from nodes/primnodes.h @ 18.3 (comments elided) ---- */
typedef enum JsonWrapper
{
	JSW_UNSPEC,
	JSW_NONE,
	JSW_CONDITIONAL,
	JSW_UNCONDITIONAL,
} JsonWrapper;

typedef enum JsonBehaviorType
{
	JSON_BEHAVIOR_NULL = 0,
	JSON_BEHAVIOR_ERROR,
	JSON_BEHAVIOR_EMPTY,
	JSON_BEHAVIOR_TRUE,
	JSON_BEHAVIOR_FALSE,
	JSON_BEHAVIOR_UNKNOWN,
	JSON_BEHAVIOR_EMPTY_ARRAY,
	JSON_BEHAVIOR_EMPTY_OBJECT,
	JSON_BEHAVIOR_DEFAULT,
} JsonBehaviorType;

typedef struct JsonBehavior
{
	NodeTag		type;

	JsonBehaviorType btype;
	Node	   *expr;
	bool		coerce;
	ParseLoc	location;
} JsonBehavior;

typedef enum JsonExprOp
{
	JSON_EXISTS_OP,
	JSON_QUERY_OP,
	JSON_VALUE_OP,
	JSON_TABLE_OP,
} JsonExprOp;

typedef struct JsonExpr
{
	Expr		xpr;

	JsonExprOp	op;

	char	   *column_name;

	Node	   *formatted_expr;

	JsonFormat *format;

	Node	   *path_spec;

	JsonReturning *returning;

	List	   *passing_names;
	List	   *passing_values;

	JsonBehavior *on_empty;
	JsonBehavior *on_error;

	bool		use_io_coercion;
	bool		use_json_coercion;

	JsonWrapper wrapper;

	bool		omit_quotes;

	Oid			collation;

	ParseLoc	location;
} JsonExpr;

typedef struct JsonTablePath
{
	NodeTag		type;

	Const	   *value;
	char	   *name;
} JsonTablePath;

typedef struct JsonTablePlan
{
	pg_node_attr(abstract)

	NodeTag		type;
} JsonTablePlan;

typedef struct JsonTablePathScan
{
	JsonTablePlan plan;

	JsonTablePath *path;

	bool		errorOnError;

	JsonTablePlan *child;

	int			colMin;
	int			colMax;
} JsonTablePathScan;

typedef struct JsonTableSiblingJoin
{
	JsonTablePlan plan;

	JsonTablePlan *lplan;
	JsonTablePlan *rplan;
} JsonTableSiblingJoin;
/* ---- end VERBATIM ---- */

/* TableFunc: only the fields the vendored TU reads */
typedef struct TableFunc
{
	NodeTag		type;
	Node	   *docexpr;
	Node	   *plan;
	List	   *colvalexprs;
	List	   *passingvalexprs;
} TableFunc;
#endif
