/* SHIM common/hashfn.h — decls only; hash arms are the ops-target's
 * charter, abort-stubbed in the driver TU (unreachable from io arms). */
#ifndef PG_JSONBFAM_SHIM_HASHFN_H
#define PG_JSONBFAM_SHIM_HASHFN_H
#include "postgres.h"
extern uint64 hash_any(const unsigned char *k, int keylen);
extern uint64 hash_any_extended(const unsigned char *k, int keylen, uint64 seed);
/* verbatim from src/include/common/hashfn.h @ 62d6c7d3df */
#define ROTATE_HIGH_AND_LOW_32BITS(v) \
	((((v) << 1) & UINT64CONST(0xfffffffe00000000)) | \
	(((v) >> 31) & (UINT64CONST(1) << 32)) | \
	((((v) & UINT64CONST(0x00000000ffffffff)) << 1) & \
	UINT64CONST(0x00000000fffffffe)) | \
	(((v) >> 31) & UINT64CONST(1)))
#endif
