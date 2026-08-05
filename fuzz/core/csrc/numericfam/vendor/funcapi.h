#ifndef NV_FUNCAPI_H
#define NV_FUNCAPI_H
typedef struct FuncCallContext
{
	uint64		call_cntr;
	uint64		max_calls;
	void	   *user_fctx;
	MemoryContext multi_call_memory_ctx;
} FuncCallContext;

extern FuncCallContext *nv_srf_firstcall_init(FunctionCallInfo fcinfo);
extern FuncCallContext *nv_srf_percall_setup(FunctionCallInfo fcinfo);
extern bool nv_srf_is_firstcall(FunctionCallInfo fcinfo);
extern Datum nv_srf_return_next(FunctionCallInfo fcinfo, Datum result);
extern Datum nv_srf_return_done(FunctionCallInfo fcinfo);

#define SRF_IS_FIRSTCALL() nv_srf_is_firstcall(fcinfo)
#define SRF_FIRSTCALL_INIT() nv_srf_firstcall_init(fcinfo)
#define SRF_PERCALL_SETUP() nv_srf_percall_setup(fcinfo)
#define SRF_RETURN_NEXT(funcctx, result) return nv_srf_return_next(fcinfo, result)
#define SRF_RETURN_DONE(funcctx) return nv_srf_return_done(fcinfo)
#endif
