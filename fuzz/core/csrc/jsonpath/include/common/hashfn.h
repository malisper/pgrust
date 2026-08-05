/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * jsonb_util.c's only hashfn.h consumers are JsonbHashScalarValue(Extended),
 * which are GIN/hash-opclass entry points unreachable from jsonpath
 * execution. The hash entries are LOUD ABORT stubs in
 * pg_jsonpath_exec_env.c (carved-unreachable, never a silent wrong hash). */
#ifndef HASHFN_H
#define HASHFN_H
#include "postgres.h"
/* ROTATE_HIGH_AND_LOW_32BITS VERBATIM from common/hashfn.h @ 18.3 */
#define ROT(x, k) (((x) << (k)) | ((x) >> (32 - (k))))
#define ROTATE_HIGH_AND_LOW_32BITS(v) \
	((((v) << 1) & UINT64CONST(0xfffffffe00000000)) | \
	 (((v) >> 31) & UINT64CONST(0x1)) | \
	 ((((v) & UINT64CONST(0xffffffff)) << 32) >> 31))

extern Datum hash_any(const unsigned char *k, int keylen);
extern Datum hash_any_extended(const unsigned char *k, int keylen, uint64 seed);
#endif
