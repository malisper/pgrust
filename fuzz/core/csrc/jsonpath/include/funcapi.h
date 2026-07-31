/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * jsonb_path_query is a set-returning function; the SRF/MultiFuncCall
 * machinery is OUT OF SCOPE for this lane (documented carve in
 * fuzz/core/src/jsonpathexec_diff.rs). The wrapper must still COMPILE and
 * LINK; init_MultiFuncCall / per_MultiFuncCall are LOUD ABORT stubs in
 * pg_jsonpath_exec_env.c, so any accidental call dies loudly instead of
 * fabricating SRF behavior. Struct/macro shapes follow funcapi.h @ 18.3. */
#ifndef FUNCAPI_H
#define FUNCAPI_H
#include "postgres.h"
#include "fmgr.h"

typedef struct FuncCallContext
{
	uint64		call_cntr;
	uint64		max_calls;
	void	   *user_fctx;
	MemoryContext multi_call_memory_ctx;
} FuncCallContext;

extern FuncCallContext *init_MultiFuncCall(FunctionCallInfo fcinfo);
extern FuncCallContext *per_MultiFuncCall(FunctionCallInfo fcinfo);

#define SRF_IS_FIRSTCALL() (fcinfo->flinfo == NULL || fcinfo->flinfo->fn_extra == NULL)
#define SRF_FIRSTCALL_INIT() init_MultiFuncCall(fcinfo)
#define SRF_PERCALL_SETUP() per_MultiFuncCall(fcinfo)
#define SRF_RETURN_NEXT(_funcctx, _result) \
	do { \
		(_funcctx)->call_cntr++; \
		PG_RETURN_DATUM(_result); \
	} while (0)
#define SRF_RETURN_DONE(_funcctx) \
	do { \
		PG_RETURN_NULL(); \
	} while (0)
#endif
