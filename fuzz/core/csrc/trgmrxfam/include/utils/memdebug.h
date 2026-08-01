/* SHIM (trgmrxfam): valgrind client-request macros -> no-ops (identical to
 * a real build without USE_VALGRIND). */
#define VALGRIND_CHECK_MEM_IS_DEFINED(addr, size) ((void) 0)
#define VALGRIND_CREATE_MEMPOOL(context, redzones, zeroed) ((void) 0)
#define VALGRIND_DESTROY_MEMPOOL(context) ((void) 0)
#define VALGRIND_MAKE_MEM_DEFINED(addr, size) ((void) 0)
#define VALGRIND_MAKE_MEM_NOACCESS(addr, size) ((void) 0)
#define VALGRIND_MAKE_MEM_UNDEFINED(addr, size) ((void) 0)
#define VALGRIND_MEMPOOL_ALLOC(context, addr, size) ((void) 0)
#define VALGRIND_MEMPOOL_FREE(context, addr) ((void) 0)
#define VALGRIND_MEMPOOL_CHANGE(context, optr, nptr, size) ((void) 0)
