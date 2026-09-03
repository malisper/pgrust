/*
 * pg_statext_io.c: vendored PostgreSQL C oracle for the statsblob_diff
 * differential fuzz target (100%-coverage / attacker-surface campaign;
 * crate crates/backend/statistics/statistics). Target functions are the
 * on-disk-bytea DESERIALIZERS for extended statistics, reachable with an
 * attacker-influenceable bytea via PG18 statistics restore
 * (pg_restore_*_stats / stats_import). The matching *_recv input functions
 * are feature-not-supported stubs (vacuous); the deserialize path below is
 * the genuine length-field parser.
 *
 * Provenance (all three deserialize bodies VERBATIM), from the upstream
 * tree at REL_18_6 (724edf9bde9d356724ad384a2e196edc3c9f80f7, "Stamp 18.6";
 * re-vendored 2026-09-02 — the one 18.3→18.6 change in the copied sections
 * is b5fd5723a6 "Fix size check in statext_dependencies_deserialize()",
 * SizeOfItem(ndeps) → MinSizeOfItems(ndeps), applied below):
 *   - src/backend/statistics/mvdistinct.c: statext_ndistinct_deserialize
 *     (250..343) + its file macros SizeOfHeader/SizeOfItem/MinSizeOfItem/
 *     MinSizeOfItems (44..57).
 *   - src/backend/statistics/dependencies.c: statext_dependencies_deserialize
 *     (498..587) + its file macros SizeOfHeader/SizeOfItem/MinSizeOfItem/
 *     MinSizeOfItems (37..49).
 *   - src/backend/statistics/mcv.c: statext_mcv_deserialize (996..1327) + its
 *     file macros ITEM_SIZE/MinSizeOfMCVList/SizeOfMCVList (53..71).
 *   - src/include/statistics/statistics.h: STATS_MAX_DIMENSIONS (19),
 *     STATS_NDISTINCT_MAGIC/TYPE (22..23), STATS_DEPS_MAGIC/TYPE (43..44),
 *     STATS_MCV_MAGIC/TYPE (66..67), STATS_MCVLIST_MAX_ITEMS,
 *     MVNDistinct(Item), MVDependency/MVDependencies, MCVItem/MCVList.
 *   - src/include/statistics/extended_stats_internal.h: DimensionInfo
 *     (34..39).
 *   - src/include/varatt.h: varattrib structs + macros (18..325) — VERBATIM
 *     block below, byte-identical with pg_arrayfuncs_io.c.
 *   - src/include/access/tupmacs.h: fetch_att (49..76) — VERBATIM.
 *
 * SHIMS (plumbing only, never logic — Michael's rule: mock the ENVIRONMENT,
 * never the COMPUTATION); every symbol carries a pg_stx_ prefix so this TU
 * can never cross-bind with other lanes' oracles:
 *   - Assert/AssertMacro -> no-op (NDEBUG parity: production Postgres ships
 *     with asserts OFF; this bar must be RELEASE-effective — debug-assert
 *     masking law). CONSEQUENCE, documented in the driver header: the three
 *     deserialize bodies guard their per-item nattributes / per-dependency k
 *     / per-dimension nvalues/nbytes / MCV item index ONLY with Assert. With
 *     asserts off (as in every shipped Postgres) a blob that violates those
 *     invariants but survives the byte-SIZE gates is undefined behaviour in
 *     C. The Rust crate converts every one of those asserts into a runtime
 *     rejection, so it is strictly safer. The Rust driver therefore runs the
 *     verbatim C oracle ONLY on blobs that are memory-safe for it (cleanly
 *     size/magic/type/count-rejectable, or fully well-formed); the
 *     invariant-violating band is exercised against the Rust side alone as a
 *     no-crash / clean-reject assertion (statext_diff.rs, "C-unsafe band").
 *   - elog(ERROR,...): records ERRCODE_INTERNAL_ERROR (class 9 = XX000,
 *     elog.c's default sqlstate when no errcode() is supplied) into the
 *     shared _Thread_local pg_diff_errcode and longjmps back to the driver
 *     entry, which setjmps and reports the class. Every message-formatting
 *     argument is discarded. All the deserialize error sites use elog(ERROR)
 *     (never ereport), so class 9 is the single reject class.
 *   - palloc/palloc0/repalloc/pfree: growable TLS pointer arena; every
 *     pg_diff_* entry calls pg_stx_arena_reset() first so an error-path
 *     longjmp cannot leak (the LSan incident class). CRUCIALLY the arena
 *     enforces MaxAllocSize (utils/memutils.h: 1GB-1) exactly as real
 *     mcxt.c palloc does — a length field driving an oversize request is a
 *     clean elog(ERROR) reject in production, not an abort, and the oracle
 *     must reproduce that reject rather than malloc-abort.
 *   - PG_ORACLE_GUARD_CHECK at every entry (holder-thread check;
 *     csrc/pg_oracle_guard.[ch]).
 *   - VARSIZE_ANY / VARDATA_ANY etc. operate on the plain 4-byte-header
 *     uncompressed varlena the driver builds; the short/compressed/external
 *     arms are dead under the driver precondition but present so the bodies
 *     stay verbatim.
 */

#include <assert.h>
#include <limits.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "pg_oracle_guard.h"

/* ---- shared error plane (defined in pg_float_io.c) ---- */
extern _Thread_local int pg_diff_errcode;

/* ---- symbol isolation: pg_stx_ prefix for every pasted global ---- */
#define statext_ndistinct_deserialize pg_stx_ndistinct_deserialize
#define statext_dependencies_deserialize pg_stx_dependencies_deserialize
#define statext_mcv_deserialize pg_stx_mcv_deserialize
#define fetch_att pg_stx_fetch_att

/* ---- c.h base types (LP64, exactly what configure produces) ---- */
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef double float8;
typedef size_t Size;
typedef uint32 Oid;
typedef uintptr_t Datum;
typedef char *Pointer;
typedef int16 AttrNumber;
#define SIZEOF_DATUM 8
#define InvalidOid ((Oid) 0)
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define Assert(x) ((void) 0)
#define AssertMacro(x) ((void) 0)
#define PG_USED_FOR_ASSERTS_ONLY

/* c.h alignment macros (LP64 values) */
#define MAXIMUM_ALIGNOF 8
#define TYPEALIGN(ALIGNVAL,LEN)  \
	(((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))
#define MAXALIGN(LEN) TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

/* utils/memutils.h */
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */
#define AllocSizeIsValid(size)	((Size) (size) <= MaxAllocSize)

/* postgres.h converters (LP64 arms) */
#define DatumGetPointer(X) ((Pointer) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define CharGetDatum(X) ((Datum) (X))
#define Int16GetDatum(X) ((Datum) (X))
#define Int32GetDatum(X) ((Datum) (X))
#define VARHDRSZ ((int32) sizeof(int32))
struct Node;					/* opaque; never dereferenced here */
typedef struct Node Node;

/* c.h: struct varlena / bytea */
struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[FLEXIBLE_ARRAY_MEMBER];	/* Data content is here */
};
typedef struct varlena bytea;

/* ---- error plane: elog(ERROR,...) -> record class + longjmp ---- */
#define ERRCODE_INTERNAL_ERROR 9

static _Thread_local jmp_buf pg_stx_jmp;

static void
pg_stx_raise(void)
{
	longjmp(pg_stx_jmp, 1);
}

/* elog(ERROR, ...) -> class 9 (XX000), the deserialize reject class. */
#define elog(level, ...) \
	do { \
		if (getenv("PG_STX_DEBUG")) \
			fprintf(stderr, "elog fired at pg_statext_io.c:%d\n", __LINE__); \
		pg_diff_errcode = ERRCODE_INTERNAL_ERROR; \
		pg_stx_raise(); \
	} while (0)

/* ---- palloc arena (growable; MaxAllocSize-checked, mcxt.c parity) ---- */
static _Thread_local void **pg_stx_arena;
static _Thread_local int pg_stx_arena_n;
static _Thread_local int pg_stx_arena_cap;

static void
pg_stx_arena_reset(void)
{
	int			i;

	for (i = 0; i < pg_stx_arena_n; i++)
		free(pg_stx_arena[i]);
	pg_stx_arena_n = 0;
}

static void
pg_stx_arena_track(void *p)
{
	if (pg_stx_arena_n == pg_stx_arena_cap)
	{
		pg_stx_arena_cap = pg_stx_arena_cap ? pg_stx_arena_cap * 2 : 64;
		pg_stx_arena = realloc(pg_stx_arena,
							   pg_stx_arena_cap * sizeof(void *));
		if (!pg_stx_arena)
			abort();
	}
	pg_stx_arena[pg_stx_arena_n++] = p;
}

/* mcxt.c: palloc rejects an out-of-range size with elog(ERROR), it does not
 * abort. That reject is exactly the production behaviour a length-field
 * attacker triggers, so the oracle reproduces it. */
static void *
pg_stx_palloc(size_t n)
{
	void	   *p;

	if (!AllocSizeIsValid(n))
		elog(ERROR, "invalid memory alloc request size %zu", n);
	p = malloc(n ? n : 1);
	if (!p)
		abort();
	pg_stx_arena_track(p);
	return p;
}

static void *
pg_stx_palloc0(size_t n)
{
	void	   *p;

	if (!AllocSizeIsValid(n))
		elog(ERROR, "invalid memory alloc request size %zu", n);
	p = calloc(1, n ? n : 1);
	if (!p)
		abort();
	pg_stx_arena_track(p);
	return p;
}

static void *
pg_stx_repalloc(void *old, size_t n)
{
	int			i;

	if (!AllocSizeIsValid(n))
		elog(ERROR, "invalid memory alloc request size %zu", n);
	for (i = pg_stx_arena_n - 1; i >= 0; i--)
	{
		if (pg_stx_arena[i] == old)
		{
			void	   *p = realloc(old, n ? n : 1);

			if (!p)
				abort();
			pg_stx_arena[i] = p;
			return p;
		}
	}
	abort();					/* repalloc of a pointer the arena never issued */
}

static void
pg_stx_pfree(void *p)
{
	int			i;

	for (i = pg_stx_arena_n - 1; i >= 0; i--)
	{
		if (pg_stx_arena[i] == p)
		{
			free(p);
			pg_stx_arena[i] = pg_stx_arena[--pg_stx_arena_n];
			return;
		}
	}
	abort();					/* pfree of a pointer the arena never issued */
}

#define palloc(n) pg_stx_palloc(n)
#define palloc0(n) pg_stx_palloc0(n)
#define repalloc(p, n) pg_stx_repalloc((p), (n))
#define pfree(p) pg_stx_pfree(p)

/* ==== VERBATIM: varatt structs + macros (varatt.h lines 18..325 @ REL_18_6 724edf9bde) ==== */
/*
 * struct varatt_external is a traditional "TOAST pointer", that is, the
 * information needed to fetch a Datum stored out-of-line in a TOAST table.
 * The data is compressed if and only if the external size stored in
 * va_extinfo is less than va_rawsize - VARHDRSZ.
 *
 * This struct must not contain any padding, because we sometimes compare
 * these pointers using memcmp.
 *
 * Note that this information is stored unaligned within actual tuples, so
 * you need to memcpy from the tuple into a local struct variable before
 * you can look at these fields!  (The reason we use memcmp is to avoid
 * having to do that just to detect equality of two TOAST pointers...)
 */
typedef struct varatt_external
{
	int32		va_rawsize;		/* Original data size (includes header) */
	uint32		va_extinfo;		/* External saved size (without header) and
								 * compression method */
	Oid			va_valueid;		/* Unique ID of value within TOAST table */
	Oid			va_toastrelid;	/* RelID of TOAST table containing it */
}			varatt_external;

/*
 * These macros define the "saved size" portion of va_extinfo.  Its remaining
 * two high-order bits identify the compression method.
 */
#define VARLENA_EXTSIZE_BITS	30
#define VARLENA_EXTSIZE_MASK	((1U << VARLENA_EXTSIZE_BITS) - 1)

/*
 * struct varatt_indirect is a "TOAST pointer" representing an out-of-line
 * Datum that's stored in memory, not in an external toast relation.
 * The creator of such a Datum is entirely responsible that the referenced
 * storage survives for as long as referencing pointer Datums can exist.
 *
 * Note that just as for struct varatt_external, this struct is stored
 * unaligned within any containing tuple.
 */
typedef struct varatt_indirect
{
	struct varlena *pointer;	/* Pointer to in-memory varlena */
}			varatt_indirect;

/*
 * struct varatt_expanded is a "TOAST pointer" representing an out-of-line
 * Datum that is stored in memory, in some type-specific, not necessarily
 * physically contiguous format that is convenient for computation not
 * storage.  APIs for this, in particular the definition of struct
 * ExpandedObjectHeader, are in src/include/utils/expandeddatum.h.
 *
 * Note that just as for struct varatt_external, this struct is stored
 * unaligned within any containing tuple.
 */
typedef struct ExpandedObjectHeader ExpandedObjectHeader;

typedef struct varatt_expanded
{
	ExpandedObjectHeader *eohptr;
} varatt_expanded;

/*
 * Type tag for the various sorts of "TOAST pointer" datums.  The peculiar
 * value for VARTAG_ONDISK comes from a requirement for on-disk compatibility
 * with a previous notion that the tag field was the pointer datum's length.
 */
typedef enum vartag_external
{
	VARTAG_INDIRECT = 1,
	VARTAG_EXPANDED_RO = 2,
	VARTAG_EXPANDED_RW = 3,
	VARTAG_ONDISK = 18
} vartag_external;

/* this test relies on the specific tag values above */
#define VARTAG_IS_EXPANDED(tag) \
	(((tag) & ~1) == VARTAG_EXPANDED_RO)

#define VARTAG_SIZE(tag) \
	((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
	 VARTAG_IS_EXPANDED(tag) ? sizeof(varatt_expanded) : \
	 (tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
	 (AssertMacro(false), 0))

/*
 * These structs describe the header of a varlena object that may have been
 * TOASTed.  Generally, don't reference these structs directly, but use the
 * macros below.
 *
 * We use separate structs for the aligned and unaligned cases because the
 * compiler might otherwise think it could generate code that assumes
 * alignment while touching fields of a 1-byte-header varlena.
 */
typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
	struct						/* Compressed-in-line format */
	{
		uint32		va_header;
		uint32		va_tcinfo;	/* Original data size (excludes header) and
								 * compression method; see va_extinfo */
		char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	}			va_compressed;
} varattrib_4b;

typedef struct
{
	uint8		va_header;
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

/* TOAST pointers are a subset of varattrib_1b with an identifying tag byte */
typedef struct
{
	uint8		va_header;		/* Always 0x80 or 0x01 */
	uint8		va_tag;			/* Type of datum */
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;

/*
 * Endian-dependent macros.  These are considered internal --- use the
 * external macros below instead of using these directly.
 */

#ifdef WORDS_BIGENDIAN

#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0xC0) == 0x40)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x80)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x80)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	(((varattrib_1b *) (PTR))->va_header & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (len) & 0x3FFFFFFF)
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = ((len) & 0x3FFFFFFF) | 0x40000000)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (len) | 0x80)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x80, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#else							/* !WORDS_BIGENDIAN */

#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x01)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)

/* VARSIZE_4B() should only be used on known-aligned data */
#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2) | 0x02)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (((uint8) (len)) << 1) | 0x01)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x01, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#endif							/* WORDS_BIGENDIAN */

#define VARDATA_4B(PTR)		(((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARDATA_4B_C(PTR)	(((varattrib_4b *) (PTR))->va_compressed.va_data)
#define VARDATA_1B(PTR)		(((varattrib_1b *) (PTR))->va_data)
#define VARDATA_1B_E(PTR)	(((varattrib_1b_e *) (PTR))->va_data)

/*
 * Externally visible TOAST macros begin here.
 */

#define VARHDRSZ_EXTERNAL		offsetof(varattrib_1b_e, va_data)
#define VARHDRSZ_COMPRESSED		offsetof(varattrib_4b, va_compressed.va_data)
#define VARHDRSZ_SHORT			offsetof(varattrib_1b, va_data)

#define VARATT_SHORT_MAX		0x7F
#define VARATT_CAN_MAKE_SHORT(PTR) \
	(VARATT_IS_4B_U(PTR) && \
	 (VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX)
#define VARATT_CONVERTED_SHORT_SIZE(PTR) \
	(VARSIZE(PTR) - VARHDRSZ + VARHDRSZ_SHORT)

#define VARDATA(PTR)						VARDATA_4B(PTR)
#define VARSIZE(PTR)						VARSIZE_4B(PTR)

#define VARSIZE_SHORT(PTR)					VARSIZE_1B(PTR)
#define VARDATA_SHORT(PTR)					VARDATA_1B(PTR)

#define VARTAG_EXTERNAL(PTR)				VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR)				(VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))
#define VARDATA_EXTERNAL(PTR)				VARDATA_1B_E(PTR)

#define VARATT_IS_COMPRESSED(PTR)			VARATT_IS_4B_C(PTR)
#define VARATT_IS_EXTERNAL(PTR)				VARATT_IS_1B_E(PTR)
#define VARATT_IS_SHORT(PTR)				VARATT_IS_1B(PTR)
#define VARATT_IS_EXTENDED(PTR)				(!VARATT_IS_4B_U(PTR))

#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B(PTR, len)
#define SET_VARSIZE_SHORT(PTR, len)			SET_VARSIZE_1B(PTR, len)
#define SET_VARSIZE_COMPRESSED(PTR, len)	SET_VARSIZE_4B_C(PTR, len)

#define SET_VARTAG_EXTERNAL(PTR, tag)		SET_VARTAG_1B_E(PTR, tag)

#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
	  VARSIZE_4B(PTR)))

/* Size of a varlena data, excluding header */
#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR)-VARHDRSZ_EXTERNAL : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT : \
	  VARSIZE_4B(PTR)-VARHDRSZ))

/* caution: this will not work on an external or compressed-in-line Datum */
/* caution: this will return a possibly unaligned pointer */
#define VARDATA_ANY(PTR) \
	 (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))
/* ==== end VERBATIM varatt ==== */

/* ==== VERBATIM: fetch_att (tupmacs.h lines 49..76 @ REL_18_6 724edf9bde) ==== */
static inline Datum
fetch_att(const void *T, bool attbyval, int attlen)
{
	if (attbyval)
	{
		switch (attlen)
		{
			case sizeof(char):
				return CharGetDatum(*((const char *) T));
			case sizeof(int16):
				return Int16GetDatum(*((const int16 *) T));
			case sizeof(int32):
				return Int32GetDatum(*((const int32 *) T));
#if SIZEOF_DATUM == 8
			case sizeof(Datum):
				return *((const Datum *) T);
#endif
			default:
				elog(ERROR, "unsupported byval length: %d", attlen);
				return 0;
		}
	}
	else
		return PointerGetDatum(T);
}
/* ==== end VERBATIM fetch_att ==== */

/* ==== VERBATIM: statistics.h constants + structs (REL_18_6 724edf9bde) ==== */
#define STATS_MAX_DIMENSIONS	8	/* max number of attributes */

#define STATS_NDISTINCT_MAGIC		0xA352BFA4	/* struct identifier */
#define STATS_NDISTINCT_TYPE_BASIC	1	/* struct version */

typedef struct MVNDistinctItem
{
	double		ndistinct;		/* ndistinct value for this combination */
	int			nattributes;	/* number of attributes */
	AttrNumber *attributes;		/* attribute numbers */
} MVNDistinctItem;

typedef struct MVNDistinct
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of ndistinct (BASIC) */
	uint32		nitems;			/* number of items in the statistic */
	MVNDistinctItem items[FLEXIBLE_ARRAY_MEMBER];	/* items */
} MVNDistinct;

#define STATS_DEPS_MAGIC		0xB4549A2C	/* marks serialized bytea */
#define STATS_DEPS_TYPE_BASIC	1	/* basic dependencies type */

typedef struct MVDependency
{
	double		degree;			/* degree of validity (0-1) */
	AttrNumber	nattributes;	/* number of attributes */
	AttrNumber	attributes[FLEXIBLE_ARRAY_MEMBER];	/* attribute numbers */
} MVDependency;

typedef struct MVDependencies
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MV Dependencies (BASIC) */
	uint32		ndeps;			/* number of dependencies */
	MVDependency *deps[FLEXIBLE_ARRAY_MEMBER];	/* dependencies */
} MVDependencies;

#define STATS_MCV_MAGIC			0xE1A651C2	/* marks serialized bytea */
#define STATS_MCV_TYPE_BASIC	1	/* basic MCV list type */
#define STATS_MCVLIST_MAX_ITEMS 10000	/* max items in MCV list */

typedef struct MCVItem
{
	double		frequency;		/* frequency of this combination */
	double		base_frequency; /* frequency if independent */
	bool	   *isnull;			/* NULL flags */
	Datum	   *values;			/* item values */
} MCVItem;

typedef struct MCVList
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MCV list (BASIC) */
	uint32		nitems;			/* number of MCV items in the array */
	AttrNumber	ndimensions;	/* number of dimensions */
	Oid			types[STATS_MAX_DIMENSIONS];		/* OIDs of data types */
	MCVItem		items[FLEXIBLE_ARRAY_MEMBER];		/* array of MCV items */
} MCVList;

/* extended_stats_internal.h: DimensionInfo (34..39) */
typedef struct DimensionInfo
{
	int			nvalues;		/* number of deduplicated values */
	int			nbytes;			/* number of bytes (serialized) */
	int			nbytes_aligned; /* size of deserialized data with alignment */
	int			typlen;			/* type length (pass-by-value/reference) */
	bool		typbyval;		/* is type pass-by-value? */
} DimensionInfo;
/* ==== end VERBATIM statistics structs ==== */

/* ==== VERBATIM: mvdistinct.c file macros (44..57 @ REL_18_6 724edf9bde) ==== */
/* size of the struct header fields (magic, type, nitems) */
#define ND_SizeOfHeader		(3 * sizeof(uint32))

/* size of a serialized ndistinct item (coefficient, natts, atts) */
#define ND_SizeOfItem(natts) \
	(sizeof(double) + sizeof(int) + (natts) * sizeof(AttrNumber))

/* minimal size of a ndistinct item (with two attributes) */
#define ND_MinSizeOfItem	ND_SizeOfItem(2)

/* minimal size of mvndistinct, when all items are minimal */
#define ND_MinSizeOfItems(nitems)	\
	(ND_SizeOfHeader + (nitems) * ND_MinSizeOfItem)

/* ==== VERBATIM: statext_ndistinct_deserialize (mvdistinct.c 250..343) ==== */
/*
 * The body is byte-identical with the vendored source; only the file-local
 * macro names SizeOfHeader / MinSizeOfItems are spelled ND_* here because
 * this TU also carries the dependencies.c and mcv.c macros of the same
 * name (they differ). No logic is changed.
 */
MVNDistinct *
statext_ndistinct_deserialize(bytea *data)
{
	int			i;
	Size		minimum_size;
	MVNDistinct ndist;
	MVNDistinct *ndistinct;
	char	   *tmp;

	if (data == NULL)
		return NULL;

	/* we expect at least the basic fields of MVNDistinct struct */
	if (VARSIZE_ANY_EXHDR(data) < ND_SizeOfHeader)
		elog(ERROR, "invalid MVNDistinct size %zu (expected at least %zu)",
			 VARSIZE_ANY_EXHDR(data), ND_SizeOfHeader);

	/* initialize pointer to the data part (skip the varlena header) */
	tmp = VARDATA_ANY(data);

	/* read the header fields and perform basic sanity checks */
	memcpy(&ndist.magic, tmp, sizeof(uint32));
	tmp += sizeof(uint32);
	memcpy(&ndist.type, tmp, sizeof(uint32));
	tmp += sizeof(uint32);
	memcpy(&ndist.nitems, tmp, sizeof(uint32));
	tmp += sizeof(uint32);

	if (ndist.magic != STATS_NDISTINCT_MAGIC)
		elog(ERROR, "invalid ndistinct magic %08x (expected %08x)",
			 ndist.magic, STATS_NDISTINCT_MAGIC);
	if (ndist.type != STATS_NDISTINCT_TYPE_BASIC)
		elog(ERROR, "invalid ndistinct type %d (expected %d)",
			 ndist.type, STATS_NDISTINCT_TYPE_BASIC);
	if (ndist.nitems == 0)
		elog(ERROR, "invalid zero-length item array in MVNDistinct");

	/* what minimum bytea size do we expect for those parameters */
	minimum_size = ND_MinSizeOfItems(ndist.nitems);
	if (VARSIZE_ANY_EXHDR(data) < minimum_size)
		elog(ERROR, "invalid MVNDistinct size %zu (expected at least %zu)",
			 VARSIZE_ANY_EXHDR(data), minimum_size);

	/*
	 * Allocate space for the ndistinct items (no space for each item's
	 * attnos: those live in bitmapsets allocated separately)
	 */
	ndistinct = palloc0(MAXALIGN(offsetof(MVNDistinct, items)) +
						(ndist.nitems * sizeof(MVNDistinctItem)));
	ndistinct->magic = ndist.magic;
	ndistinct->type = ndist.type;
	ndistinct->nitems = ndist.nitems;

	for (i = 0; i < ndistinct->nitems; i++)
	{
		MVNDistinctItem *item = &ndistinct->items[i];

		/* ndistinct value */
		memcpy(&item->ndistinct, tmp, sizeof(double));
		tmp += sizeof(double);

		/* number of attributes */
		memcpy(&item->nattributes, tmp, sizeof(int));
		tmp += sizeof(int);
		Assert((item->nattributes >= 2) && (item->nattributes <= STATS_MAX_DIMENSIONS));

		item->attributes
			= (AttrNumber *) palloc(item->nattributes * sizeof(AttrNumber));

		memcpy(item->attributes, tmp, sizeof(AttrNumber) * item->nattributes);
		tmp += sizeof(AttrNumber) * item->nattributes;

		/* still within the bytea */
		Assert(tmp <= ((char *) data + VARSIZE_ANY(data)));
	}

	/* we should have consumed the whole bytea exactly */
	Assert(tmp == ((char *) data + VARSIZE_ANY(data)));

	return ndistinct;
}

/* ==== VERBATIM: dependencies.c file macros (37..49 @ REL_18_6 724edf9bde) ==== */
/* size of the struct header fields (magic, type, ndeps) */
#define DEP_SizeOfHeader		(3 * sizeof(uint32))

/* size of a serialized dependency (degree, natts, atts) */
#define DEP_SizeOfItem(natts) \
	(sizeof(double) + sizeof(AttrNumber) * (1 + (natts)))

/* minimal size of a dependency (with two attributes) */
#define DEP_MinSizeOfItem	DEP_SizeOfItem(2)

/* minimal size of dependencies, when all deps are minimal */
#define DEP_MinSizeOfItems(ndeps) \
	(DEP_SizeOfHeader + (ndeps) * DEP_MinSizeOfItem)

/* ==== VERBATIM: statext_dependencies_deserialize (dependencies.c 498..587) ==== */
MVDependencies *
statext_dependencies_deserialize(bytea *data)
{
	int			i;
	Size		min_expected_size;
	MVDependencies *dependencies;
	char	   *tmp;

	if (data == NULL)
		return NULL;

	if (VARSIZE_ANY_EXHDR(data) < DEP_SizeOfHeader)
		elog(ERROR, "invalid MVDependencies size %zu (expected at least %zu)",
			 VARSIZE_ANY_EXHDR(data), DEP_SizeOfHeader);

	/* read the MVDependencies header */
	dependencies = (MVDependencies *) palloc0(sizeof(MVDependencies));

	/* initialize pointer to the data part (skip the varlena header) */
	tmp = VARDATA_ANY(data);

	/* read the header fields and perform basic sanity checks */
	memcpy(&dependencies->magic, tmp, sizeof(uint32));
	tmp += sizeof(uint32);
	memcpy(&dependencies->type, tmp, sizeof(uint32));
	tmp += sizeof(uint32);
	memcpy(&dependencies->ndeps, tmp, sizeof(uint32));
	tmp += sizeof(uint32);

	if (dependencies->magic != STATS_DEPS_MAGIC)
		elog(ERROR, "invalid dependency magic %d (expected %d)",
			 dependencies->magic, STATS_DEPS_MAGIC);

	if (dependencies->type != STATS_DEPS_TYPE_BASIC)
		elog(ERROR, "invalid dependency type %d (expected %d)",
			 dependencies->type, STATS_DEPS_TYPE_BASIC);

	if (dependencies->ndeps == 0)
		elog(ERROR, "invalid zero-length item array in MVDependencies");

	/* what minimum bytea size do we expect for those parameters */
	min_expected_size = DEP_MinSizeOfItems(dependencies->ndeps);

	if (VARSIZE_ANY_EXHDR(data) < min_expected_size)
		elog(ERROR, "invalid dependencies size %zu (expected at least %zu)",
			 VARSIZE_ANY_EXHDR(data), min_expected_size);

	/* allocate space for the MCV items */
	dependencies = repalloc(dependencies, offsetof(MVDependencies, deps)
							+ (dependencies->ndeps * sizeof(MVDependency *)));

	for (i = 0; i < dependencies->ndeps; i++)
	{
		double		degree;
		AttrNumber	k;
		MVDependency *d;

		/* degree of validity */
		memcpy(&degree, tmp, sizeof(double));
		tmp += sizeof(double);

		/* number of attributes */
		memcpy(&k, tmp, sizeof(AttrNumber));
		tmp += sizeof(AttrNumber);

		/* is the number of attributes valid? */
		Assert((k >= 2) && (k <= STATS_MAX_DIMENSIONS));

		/* now that we know the number of attributes, allocate the dependency */
		d = (MVDependency *) palloc0(offsetof(MVDependency, attributes)
									 + (k * sizeof(AttrNumber)));

		d->degree = degree;
		d->nattributes = k;

		/* copy attribute numbers */
		memcpy(d->attributes, tmp, sizeof(AttrNumber) * d->nattributes);
		tmp += sizeof(AttrNumber) * d->nattributes;

		dependencies->deps[i] = d;

		/* still within the bytea */
		Assert(tmp <= ((char *) data + VARSIZE_ANY(data)));
	}

	/* we should have consumed the whole bytea exactly */
	Assert(tmp == ((char *) data + VARSIZE_ANY(data)));

	return dependencies;
}

/* ==== VERBATIM: mcv.c file macros (53..71 @ REL_18_6 724edf9bde) ==== */
#define MCV_ITEM_SIZE(ndims)	\
	((ndims) * (sizeof(uint16) + sizeof(bool)) + 2 * sizeof(double))

#define MCV_MinSizeOfMCVList		\
	(VARHDRSZ + sizeof(uint32) * 3 + sizeof(AttrNumber))

#define MCV_SizeOfMCVList(ndims,nitems)	\
	((MCV_MinSizeOfMCVList + sizeof(Oid) * (ndims)) + \
	 ((ndims) * sizeof(DimensionInfo)) + \
	 ((nitems) * MCV_ITEM_SIZE(ndims)))

/* ==== VERBATIM: statext_mcv_deserialize (mcv.c 996..1327) ==== */
MCVList *
statext_mcv_deserialize(bytea *data)
{
	int			dim,
				i;
	Size		expected_size;
	MCVList    *mcvlist;
	char	   *raw;
	char	   *ptr;
	char	   *endptr PG_USED_FOR_ASSERTS_ONLY;

	int			ndims,
				nitems;
	DimensionInfo *info = NULL;

	/* local allocation buffer (used only for deserialization) */
	Datum	  **map = NULL;

	/* MCV list */
	Size		mcvlen;

	/* buffer used for the result */
	Size		datalen;
	char	   *dataptr;
	char	   *valuesptr;
	char	   *isnullptr;

	if (data == NULL)
		return NULL;

	/*
	 * We can't possibly deserialize a MCV list if there's not even a complete
	 * header. We need an explicit formula here, because we serialize the
	 * header fields one by one, so we need to ignore struct alignment.
	 */
	if (VARSIZE_ANY(data) < MCV_MinSizeOfMCVList)
		elog(ERROR, "invalid MCV size %zu (expected at least %zu)",
			 VARSIZE_ANY(data), MCV_MinSizeOfMCVList);

	/* read the MCV list header */
	mcvlist = (MCVList *) palloc0(offsetof(MCVList, items));

	/* pointer to the data part (skip the varlena header) */
	raw = (char *) data;
	ptr = VARDATA_ANY(raw);
	endptr = (char *) raw + VARSIZE_ANY(data);

	/* get the header and perform further sanity checks */
	memcpy(&mcvlist->magic, ptr, sizeof(uint32));
	ptr += sizeof(uint32);

	memcpy(&mcvlist->type, ptr, sizeof(uint32));
	ptr += sizeof(uint32);

	memcpy(&mcvlist->nitems, ptr, sizeof(uint32));
	ptr += sizeof(uint32);

	memcpy(&mcvlist->ndimensions, ptr, sizeof(AttrNumber));
	ptr += sizeof(AttrNumber);

	if (mcvlist->magic != STATS_MCV_MAGIC)
		elog(ERROR, "invalid MCV magic %u (expected %u)",
			 mcvlist->magic, STATS_MCV_MAGIC);

	if (mcvlist->type != STATS_MCV_TYPE_BASIC)
		elog(ERROR, "invalid MCV type %u (expected %u)",
			 mcvlist->type, STATS_MCV_TYPE_BASIC);

	if (mcvlist->ndimensions == 0)
		elog(ERROR, "invalid zero-length dimension array in MCVList");
	else if ((mcvlist->ndimensions > STATS_MAX_DIMENSIONS) ||
			 (mcvlist->ndimensions < 0))
		elog(ERROR, "invalid length (%d) dimension array in MCVList",
			 mcvlist->ndimensions);

	if (mcvlist->nitems == 0)
		elog(ERROR, "invalid zero-length item array in MCVList");
	else if (mcvlist->nitems > STATS_MCVLIST_MAX_ITEMS)
		elog(ERROR, "invalid length (%u) item array in MCVList",
			 mcvlist->nitems);

	nitems = mcvlist->nitems;
	ndims = mcvlist->ndimensions;

	/*
	 * Check amount of data including DimensionInfo for all dimensions and
	 * also the serialized items (including uint16 indexes). Also, walk
	 * through the dimension information and add it to the sum.
	 */
	expected_size = MCV_SizeOfMCVList(ndims, nitems);

	/*
	 * Check that we have at least the dimension and info records, along with
	 * the items. We don't know the size of the serialized values yet. We need
	 * to do this check first, before accessing the dimension info.
	 */
	if (VARSIZE_ANY(data) < expected_size)
		elog(ERROR, "invalid MCV size %zu (expected %zu)",
			 VARSIZE_ANY(data), expected_size);

	/* Now copy the array of type Oids. */
	memcpy(mcvlist->types, ptr, sizeof(Oid) * ndims);
	ptr += (sizeof(Oid) * ndims);

	/* Now it's safe to access the dimension info. */
	info = palloc(ndims * sizeof(DimensionInfo));

	memcpy(info, ptr, ndims * sizeof(DimensionInfo));
	ptr += (ndims * sizeof(DimensionInfo));

	/* account for the value arrays */
	for (dim = 0; dim < ndims; dim++)
	{
		/*
		 * XXX I wonder if we can/should rely on asserts here. Maybe those
		 * checks should be done every time?
		 */
		Assert(info[dim].nvalues >= 0);
		Assert(info[dim].nbytes >= 0);

		expected_size += info[dim].nbytes;
	}

	/*
	 * Now we know the total expected MCV size, including all the pieces
	 * (header, dimension info. items and deduplicated data). So do the final
	 * check on size.
	 */
	if (VARSIZE_ANY(data) != expected_size)
		elog(ERROR, "invalid MCV size %zu (expected %zu)",
			 VARSIZE_ANY(data), expected_size);

	/*
	 * We need an array of Datum values for each dimension, so that we can
	 * easily translate the uint16 indexes later. We also need a top-level
	 * array of pointers to those per-dimension arrays.
	 *
	 * While allocating the arrays for dimensions, compute how much space we
	 * need for a copy of the by-ref data, as we can't simply point to the
	 * original values (it might go away).
	 */
	datalen = 0;				/* space for by-ref data */
	map = (Datum **) palloc(ndims * sizeof(Datum *));

	for (dim = 0; dim < ndims; dim++)
	{
		map[dim] = (Datum *) palloc(sizeof(Datum) * info[dim].nvalues);

		/* space needed for a copy of data for by-ref types */
		datalen += info[dim].nbytes_aligned;
	}

	/*
	 * Now resize the MCV list so that the allocation includes all the data.
	 *
	 * Allocate space for a copy of the data, as we can't simply reference the
	 * serialized data - it's not aligned properly, and it may disappear while
	 * we're still using the MCV list, e.g. due to catcache release.
	 *
	 * We do care about alignment here, because we will allocate all the
	 * pieces at once, but then use pointers to different parts.
	 */
	mcvlen = MAXALIGN(offsetof(MCVList, items) + (sizeof(MCVItem) * nitems));

	/* arrays of values and isnull flags for all MCV items */
	mcvlen += nitems * MAXALIGN(sizeof(Datum) * ndims);
	mcvlen += nitems * MAXALIGN(sizeof(bool) * ndims);

	/* we don't quite need to align this, but it makes some asserts easier */
	mcvlen += MAXALIGN(datalen);

	/* now resize the deserialized MCV list, and compute pointers to parts */
	mcvlist = repalloc(mcvlist, mcvlen);

	/* pointer to the beginning of values/isnull arrays */
	valuesptr = (char *) mcvlist
		+ MAXALIGN(offsetof(MCVList, items) + (sizeof(MCVItem) * nitems));

	isnullptr = valuesptr + (nitems * MAXALIGN(sizeof(Datum) * ndims));

	dataptr = isnullptr + (nitems * MAXALIGN(sizeof(bool) * ndims));

	/*
	 * Build mapping (index => value) for translating the serialized data into
	 * the in-memory representation.
	 */
	for (dim = 0; dim < ndims; dim++)
	{
		/* remember start position in the input array */
		char	   *start PG_USED_FOR_ASSERTS_ONLY = ptr;

		if (info[dim].typbyval)
		{
			/* for by-val types we simply copy data into the mapping */
			for (i = 0; i < info[dim].nvalues; i++)
			{
				Datum		v = 0;

				memcpy(&v, ptr, info[dim].typlen);
				ptr += info[dim].typlen;

				map[dim][i] = fetch_att(&v, true, info[dim].typlen);

				/* no under/overflow of input array */
				Assert(ptr <= (start + info[dim].nbytes));
			}
		}
		else
		{
			/* for by-ref types we need to also make a copy of the data */

			/* passed by reference, but fixed length (name, tid, ...) */
			if (info[dim].typlen > 0)
			{
				for (i = 0; i < info[dim].nvalues; i++)
				{
					memcpy(dataptr, ptr, info[dim].typlen);
					ptr += info[dim].typlen;

					/* just point into the array */
					map[dim][i] = PointerGetDatum(dataptr);
					dataptr += MAXALIGN(info[dim].typlen);
				}
			}
			else if (info[dim].typlen == -1)
			{
				/* varlena */
				for (i = 0; i < info[dim].nvalues; i++)
				{
					uint32		len;

					/* read the uint32 length */
					memcpy(&len, ptr, sizeof(uint32));
					ptr += sizeof(uint32);

					/* the length is data-only */
					SET_VARSIZE(dataptr, len + VARHDRSZ);
					memcpy(VARDATA(dataptr), ptr, len);
					ptr += len;

					/* just point into the array */
					map[dim][i] = PointerGetDatum(dataptr);

					/* skip to place of the next deserialized value */
					dataptr += MAXALIGN(len + VARHDRSZ);
				}
			}
			else if (info[dim].typlen == -2)
			{
				/* cstring */
				for (i = 0; i < info[dim].nvalues; i++)
				{
					uint32		len;

					memcpy(&len, ptr, sizeof(uint32));
					ptr += sizeof(uint32);

					memcpy(dataptr, ptr, len);
					ptr += len;

					/* just point into the array */
					map[dim][i] = PointerGetDatum(dataptr);
					dataptr += MAXALIGN(len);
				}
			}

			/* no under/overflow of input array */
			Assert(ptr <= (start + info[dim].nbytes));

			/* no overflow of the output mcv value */
			Assert(dataptr <= ((char *) mcvlist + mcvlen));
		}

		/* check we consumed input data for this dimension exactly */
		Assert(ptr == (start + info[dim].nbytes));
	}

	/* we should have also filled the MCV list exactly */
	Assert(dataptr == ((char *) mcvlist + mcvlen));

	/* deserialize the MCV items and translate the indexes to Datums */
	for (i = 0; i < nitems; i++)
	{
		MCVItem    *item = &mcvlist->items[i];

		item->values = (Datum *) valuesptr;
		valuesptr += MAXALIGN(sizeof(Datum) * ndims);

		item->isnull = (bool *) isnullptr;
		isnullptr += MAXALIGN(sizeof(bool) * ndims);

		memcpy(item->isnull, ptr, sizeof(bool) * ndims);
		ptr += sizeof(bool) * ndims;

		memcpy(&item->frequency, ptr, sizeof(double));
		ptr += sizeof(double);

		memcpy(&item->base_frequency, ptr, sizeof(double));
		ptr += sizeof(double);

		/* finally translate the indexes (for non-NULL only) */
		for (dim = 0; dim < ndims; dim++)
		{
			uint16		index;

			memcpy(&index, ptr, sizeof(uint16));
			ptr += sizeof(uint16);

			if (item->isnull[dim])
				continue;

			item->values[dim] = map[dim][index];
		}

		/* check we're not overflowing the input */
		Assert(ptr <= endptr);
	}

	/* check that we processed all the data */
	Assert(ptr == endptr);

	/* release the buffers used for mapping */
	for (dim = 0; dim < ndims; dim++)
		pfree(map[dim]);

	pfree(map);

	return mcvlist;
}

/* =====================================================================
 * Differential driver entries.
 *
 * Each takes the varlena BODY (header already stripped by the Rust caller),
 * rebuilds a plain 4-byte-header uncompressed varlena inside a red-zoned
 * arena buffer, runs the verbatim deserializer under setjmp, and — on
 * success — writes a canonical structural DIGEST into `out` (cap `outcap`,
 * actual length via `*outlen`). The digest is byte-identical to the one the
 * Rust driver builds from its own parse (statext_diff.rs).
 *
 * Return: 0 = accepted (digest in out); >0 = errclass (9 for every
 * deserialize elog reject, including the MaxAllocSize reject). The driver
 * only ever calls these on blobs it has classified C-memory-safe.
 *
 * RED-ZONE: the input varlena is placed at the front of a buffer whose tail
 * is zero-padded (PG_STX_REDZONE bytes) so that a *bounded* over-read from an
 * assert-guarded field that the driver did not fully pre-screen lands in
 * mapped, deterministic zero memory rather than faulting. This never changes
 * a verbatim body; it only hardens the harness against its own
 * classification being imperfect. It does NOT make an unbounded index OOB
 * safe — those blobs are Rust-only by construction.
 * ===================================================================== */

#define PG_STX_REDZONE 4096

/* Build a 4B-header varlena {header, body...} in a fresh red-zoned buffer. */
static bytea *
pg_stx_make_varlena(const uint8 *body, uint32 bodylen)
{
	Size		total = (Size) VARHDRSZ + bodylen;
	char	   *buf = palloc0(total + PG_STX_REDZONE);

	SET_VARSIZE(buf, total);
	if (bodylen)
		memcpy(buf + VARHDRSZ, body, bodylen);
	return (bytea *) buf;
}

/* ---- digest writer (little-endian, self-describing) ---- */
typedef struct
{
	uint8	   *p;
	uint8	   *end;
	int			ok;
} pg_stx_dw;

static void
dw_bytes(pg_stx_dw *w, const void *src, size_t n)
{
	if (!w->ok || (size_t) (w->end - w->p) < n)
	{
		w->ok = 0;
		return;
	}
	memcpy(w->p, src, n);
	w->p += n;
}

static void
dw_u8(pg_stx_dw *w, uint8 v)
{
	dw_bytes(w, &v, 1);
}
static void
dw_u16(pg_stx_dw *w, uint16 v)
{
	dw_bytes(w, &v, 2);
}
static void
dw_u32(pg_stx_dw *w, uint32 v)
{
	dw_bytes(w, &v, 4);
}
static void
dw_u64(pg_stx_dw *w, uint64 v)
{
	dw_bytes(w, &v, 8);
}

int
pg_diff_statext_ndistinct(const uint8 *body, uint32 bodylen,
						  uint8 *out, int outcap, int *outlen)
{
	MVNDistinct *nd;
	bytea	   *data;
	pg_stx_dw	w;
	int			i;

	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stx_arena_reset();
	pg_diff_errcode = 0;
	*outlen = 0;

	if (setjmp(pg_stx_jmp) != 0)
	{
		pg_stx_arena_reset();
		return pg_diff_errcode;
	}

	data = pg_stx_make_varlena(body, bodylen);
	nd = statext_ndistinct_deserialize(data);

	w.p = out;
	w.end = out + outcap;
	w.ok = 1;
	dw_u32(&w, nd->nitems);
	for (i = 0; i < (int) nd->nitems; i++)
	{
		MVNDistinctItem *it = &nd->items[i];
		uint64		bits;
		int			j;

		memcpy(&bits, &it->ndistinct, 8);
		dw_u64(&w, bits);
		dw_u32(&w, (uint32) it->nattributes);
		for (j = 0; j < it->nattributes; j++)
			dw_u16(&w, (uint16) it->attributes[j]);
	}
	if (!w.ok)
		abort();				/* out buffer too small: harness sizing bug */
	*outlen = (int) (w.p - out);
	pg_stx_arena_reset();
	return 0;
}

int
pg_diff_statext_deps(const uint8 *body, uint32 bodylen,
					 uint8 *out, int outcap, int *outlen)
{
	MVDependencies *deps;
	bytea	   *data;
	pg_stx_dw	w;
	int			i;

	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stx_arena_reset();
	pg_diff_errcode = 0;
	*outlen = 0;

	if (setjmp(pg_stx_jmp) != 0)
	{
		pg_stx_arena_reset();
		return pg_diff_errcode;
	}

	data = pg_stx_make_varlena(body, bodylen);
	deps = statext_dependencies_deserialize(data);

	w.p = out;
	w.end = out + outcap;
	w.ok = 1;
	dw_u32(&w, deps->ndeps);
	for (i = 0; i < (int) deps->ndeps; i++)
	{
		MVDependency *d = deps->deps[i];
		uint64		bits;
		int			j;

		memcpy(&bits, &d->degree, 8);
		dw_u64(&w, bits);
		dw_u32(&w, (uint32) d->nattributes);
		for (j = 0; j < d->nattributes; j++)
			dw_u16(&w, (uint16) d->attributes[j]);
	}
	if (!w.ok)
		abort();
	*outlen = (int) (w.p - out);
	pg_stx_arena_reset();
	return 0;
}

/*
 * MCV digest. The driver guarantees (for blobs sent here) that every
 * dimension is either by-val (typlen 1/2/4/8), by-ref fixed (typlen>0),
 * varlena (typlen -1) or cstring (typlen -2), and that every item index is
 * < that dimension's nvalues. The digest records structure + resolved value
 * bytes so pointer identity never enters the comparison.
 */
int
pg_diff_statext_mcv(const uint8 *body, uint32 bodylen,
					uint8 *out, int outcap, int *outlen)
{
	MCVList    *m;
	bytea	   *data;
	pg_stx_dw	w;
	int			i,
				dim;
	DimensionInfo dinfo[STATS_MAX_DIMENSIONS];

	PG_ORACLE_GUARD_CHECK(__func__);
	pg_stx_arena_reset();
	pg_diff_errcode = 0;
	*outlen = 0;

	if (setjmp(pg_stx_jmp) != 0)
	{
		pg_stx_arena_reset();
		return pg_diff_errcode;
	}

	data = pg_stx_make_varlena(body, bodylen);
	m = statext_mcv_deserialize(data);

	/*
	 * Re-read the DimensionInfo directly from the (validated) blob so the
	 * digest can describe each value; the deserialized MCVList does not
	 * retain typlen/typbyval. Layout matches the deserializer exactly.
	 */
	{
		const uint8 *dp = body + (sizeof(uint32) * 3 + sizeof(AttrNumber))
			+ sizeof(Oid) * m->ndimensions;

		memcpy(dinfo, dp, m->ndimensions * sizeof(DimensionInfo));
	}

	w.p = out;
	w.end = out + outcap;
	w.ok = 1;
	dw_u32(&w, m->nitems);
	dw_u16(&w, (uint16) m->ndimensions);
	for (dim = 0; dim < m->ndimensions; dim++)
	{
		dw_u32(&w, m->types[dim]);
		dw_u32(&w, (uint32) dinfo[dim].typlen);
		dw_u8(&w, dinfo[dim].typbyval ? 1 : 0);
	}
	for (i = 0; i < (int) m->nitems; i++)
	{
		MCVItem    *it = &m->items[i];
		uint64		fb,
					bb;

		for (dim = 0; dim < m->ndimensions; dim++)
			dw_u8(&w, it->isnull[dim] ? 1 : 0);
		memcpy(&fb, &it->frequency, 8);
		memcpy(&bb, &it->base_frequency, 8);
		dw_u64(&w, fb);
		dw_u64(&w, bb);
		for (dim = 0; dim < m->ndimensions; dim++)
		{
			Datum		v = it->values[dim];

			if (it->isnull[dim])
			{
				dw_u8(&w, 0);	/* null marker */
				continue;
			}
			dw_u8(&w, 1);
			if (dinfo[dim].typbyval)
			{
				dw_u64(&w, (uint64) v);
			}
			else if (dinfo[dim].typlen > 0)
			{
				dw_u32(&w, (uint32) dinfo[dim].typlen);
				dw_bytes(&w, DatumGetPointer(v), dinfo[dim].typlen);
			}
			else if (dinfo[dim].typlen == -1)
			{
				char	   *vp = DatumGetPointer(v);
				uint32		len = VARSIZE_ANY_EXHDR(vp);

				dw_u32(&w, len);
				dw_bytes(&w, VARDATA_ANY(vp), len);
			}
			else			/* cstring (-2): NUL-terminated copy */
			{
				char	   *vp = DatumGetPointer(v);
				uint32		len = (uint32) strlen(vp);

				dw_u32(&w, len);
				dw_bytes(&w, vp, len);
			}
		}
	}
	if (!w.ok)
		abort();
	*outlen = (int) (w.p - out);
	pg_stx_arena_reset();
	return 0;
}
