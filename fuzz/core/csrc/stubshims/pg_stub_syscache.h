/*
 * pg_stub_syscache.h — extern declarations for the stub:syscache-row
 * facility (store + interception + verbatim consumers defined in
 * ../pg_stub_syscache.c; Rust half in fuzz/core/src/stub_syscache.rs;
 * usage contract in fuzz/STUBS.md).
 *
 * A NEW oracle TU whose verbatim paste calls the SearchSysCacheN /
 * GetSysCacheOidN family maps those names onto the store BEFORE the paste
 * (the same pattern pg_stub_state.h documents for pinned globals):
 *
 *   #define SearchSysCache1(id, k1) pg_stub_syscache_search(id, k1, 0, 0, 0)
 *   #define SearchSysCache4(id, k1, k2, k3, k4) \
 *       pg_stub_syscache_search(id, k1, k2, k3, k4)
 *   #define GetSysCacheOid2(id, oidcol, k1, k2) \
 *       pg_stub_syscache_getoid(id, k1, k2, 0, 0)
 *   #define ReleaseSysCache(t) pg_stub_syscache_release(t)
 *   #define HeapTupleIsValid(t) ((t) != NULL)
 *   #define GETSTRUCT(t) (*(void **) (t))
 *
 * plus the cache-id dispatch tags below for the names its bodies use, and
 * its own FormData_pg_* typedefs (each oracle TU stays self-contained; the
 * canonical vendored layouts live in pg_stub_syscache.c).  The rows are
 * loaded by the Rust driver through stub_syscache::set_rows, which derives
 * them ONCE from the fuzz input and loads BOTH sides.
 *
 * Covered caches / row fields, clamps, and the unreachable-state hazard:
 * see fuzz/STUBS.md ("stub:syscache-row").
 */
#ifndef PG_STUB_SYSCACHE_H
#define PG_STUB_SYSCACHE_H

#include <stdint.h>

/* shim-internal cache-id dispatch tags (18.3 generates its enum values at
 * build time; the NAMES are what verbatim bodies reference) */
#define PG_STUB_SYSCACHE_AMOPSTRATEGY 1
#define PG_STUB_SYSCACHE_AMOPOPID 2
#define PG_STUB_SYSCACHE_AMPROCNUM 3
#define PG_STUB_SYSCACHE_OPEROID 4
#define PG_STUB_SYSCACHE_CLAOID 5
#define PG_STUB_SYSCACHE_TYPEOID 6
#define PG_STUB_SYSCACHE_ATTNUM 7
#define PG_STUB_SYSCACHE_PROCOID 8

/* store lifecycle (wire format: fuzz/core/src/stub_syscache.rs) */
int			pg_stub_syscache_load(const uint8_t *wire, int wirelen);
void		pg_stub_syscache_reset(void);
int			pg_stub_syscache_plane(uint8_t *out, int outcap, int *outlen);

/* interception entry points (Datum-typed as uintptr_t at this boundary) */
void	   *pg_stub_syscache_search(int cacheId, uintptr_t k1, uintptr_t k2,
									uintptr_t k3, uintptr_t k4);
uint32_t	pg_stub_syscache_getoid(int cacheId, uintptr_t k1, uintptr_t k2,
									uintptr_t k3, uintptr_t k4);
void		pg_stub_syscache_release(void *tuple);

/* verbatim 18.3 lsyscache consumers compiled over the store (renamed
 * exports; the try_ variants report the miss-path elog as status 1) */
uint32_t	pg_stub_syscache_get_opfamily_proc(uint32_t opfamily, uint32_t lefttype,
											   uint32_t righttype, int16_t procnum);
uint32_t	pg_stub_syscache_get_opfamily_member(uint32_t opfamily, uint32_t lefttype,
												 uint32_t righttype, int16_t strategy);
uint32_t	pg_stub_syscache_get_opcode(uint32_t opno);
uint32_t	pg_stub_syscache_get_atttype(uint32_t relid, int16_t attnum);
int			pg_stub_syscache_try_get_opclass_family(uint32_t opclass, uint32_t *out);
int			pg_stub_syscache_try_get_typlenbyval(uint32_t typid, int16_t *typlen,
												 uint8_t *typbyval);
int			pg_stub_syscache_try_get_func_rettype(uint32_t funcid, uint32_t *out);

#endif							/* PG_STUB_SYSCACHE_H */
