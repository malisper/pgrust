/* Standalone shim for the vendored REL_18_3 regex engine. MALLOC here is
 * palloc_extended(NO_OOM) = NULL-on-failure, so malloc is shape-identical;
 * Assert compiles out (production build); CHECK_FOR_INTERRUPTS keeps its
 * real global-load+branch; ereport arms are unreachable under the C
 * collation and abort if taken. */
#ifndef CREF_REGEX_POSTGRES_H
#define CREF_REGEX_POSTGRES_H

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef unsigned int Oid;

typedef struct varlena text;

#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId) ((bool) ((objectId) != InvalidOid))
#define Assert(condition) ((void) 0)
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))

extern volatile int cref_InterruptPending;
#define CHECK_FOR_INTERRUPTS() \
	do { if (cref_InterruptPending) abort(); } while (0)

#define MCXT_ALLOC_NO_OOM 0x0002
#define palloc_extended(sz, flags) malloc(sz)
#define repalloc_extended(p, sz, flags) realloc((p), (sz))
static inline void *palloc(Size sz) { return malloc(sz); }
static inline void pfree(void *p) { free(p); }

#define ereport(elevel, rest) abort()
#define ERROR 21

/* pgstrcasecmp.c (verbatim bodies; upstream outlines them in libpgport) */
static inline unsigned char
pg_ascii_toupper(unsigned char ch)
{
	if (ch >= 'a' && ch <= 'z')
		ch += 'A' - 'a';
	return ch;
}

static inline unsigned char
pg_ascii_tolower(unsigned char ch)
{
	if (ch >= 'A' && ch <= 'Z')
		ch += 'a' - 'A';
	return ch;
}

#endif
