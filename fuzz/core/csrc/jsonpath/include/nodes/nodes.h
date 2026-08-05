/*
 * SHIM nodes/nodes.h for the jsonpath_diff oracle — NOT PostgreSQL code.
 *
 * Just enough of the node framework for the vendored TUs: the NodeTag
 * values actually used (List family, String, ErrorSaveContext), the Node
 * supertype, and the IsA/castNode accessors with their real semantics
 * (castNode's assertion is compiled out exactly like a production build).
 * Shapes follow src/include/nodes/nodes.h VERBATIM where copied.
 */
#ifndef NODES_H
#define NODES_H

#include "postgres.h"

typedef enum NodeTag
{
	T_Invalid = 0,
	T_List = 1,
	T_IntList = 2,
	T_OidList = 3,
	T_XidList = 4,
	T_String = 5,
	T_ErrorSaveContext = 6,
	/* jsonpathexec_diff additions (shim model tags; values arbitrary —
	 * tags never cross the FFI boundary) */
	T_Const = 7,
	T_ExprState = 8,
	T_TableFuncScan = 9,
	T_TableFuncScanState = 10,
	T_JsonExpr = 11,
	T_JsonTablePath = 12,
	T_JsonTablePathScan = 13,
	T_JsonTableSiblingJoin = 14,
} NodeTag;

/* postgres.h forward-declares "typedef struct Node Node"; define it here */
struct Node
{
	NodeTag		type;
};

typedef struct ErrorData ErrorData;		/* opaque here */

#define nodeTag(nodeptr)		(((const Node*)(nodeptr))->type)
#define IsA(nodeptr,_type_)		(nodeTag(nodeptr) == T_##_type_)

#define newNode(size, tag) \
({	Node   *_result; \
	_result = (Node *) palloc0(size); \
	_result->type = (tag); \
	_result; \
})
#define makeNode(_type_)		((_type_ *) newNode(sizeof(_type_),T_##_type_))

static inline Node *
castNodeImpl(NodeTag type, void *ptr)
{
	Assert(ptr == NULL || nodeTag(ptr) == type);
	return (Node *) ptr;
}
#define castNode(_type_, nodeptr) ((_type_ *) castNodeImpl(T_##_type_, nodeptr))

/* TransactionId lives in c.h/postgres_ext.h; pg_list.h's xid arm needs it */
typedef uint32 TransactionId;

#endif							/* NODES_H */
