/*
 * pg_scalarxid_datum.c: vendored PostgreSQL datum.c oracle for the
 * scalarxid_diff differential fuzz target, round 3 (datum_ops.rs arms).
 *
 * The scalar-datum Kani family WALLED on serialize/copy/restore (fleet x2,
 * 24GB/600s — CBMC alloc+memcpy blowup), flipping these lines to the
 * differential-fuzz route. The verbatim C + shim census below is COPIED from
 * proofs/scalar-datum/c/pg_datum.c (same worktree, fetched/eyeballed against
 * REL_18_STABLE == Stamp-18.3 62d6c7d3df on 2026-07-30) rather than
 * re-transcribed; see that file's header for the full provenance notes.
 *
 * Function bodies VERBATIM: datumGetSize, datumCopy, datumTransfer,
 * datumIsEqual, datumEstimateSpace, datumSerialize, datumRestore
 * (src/backend/utils/adt/datum.c) + varatt.h structs/macros (little-endian
 * arms) + postgres.h Datum plumbing.
 *
 * Shims (plumbing only, never logic) — deltas vs the proofs copy:
 *   1. ereport/elog -> err out-param flags, SAME program points as the
 *      proofs copy: *err = 1 for ERRCODE_DATA_EXCEPTION ("invalid Datum
 *      pointer"), *err = 2 for the elog internal-error class ("invalid
 *      typLen"). PROOF_EREPORT_FLAG is defined locally to that convention.
 *   2. palloc -> fixed static bump buffer, RESET AT EVERY DRIVER ENTRY
 *      (fuzz iterations reuse the heap; drivers refuse inputs that could
 *      overflow the cap before calling in). pfree -> no-op.
 *   3. Expanded-object machinery -> trapping stubs setting
 *      pg_dx_eoh_reached; the Rust driver FENCES expanded headers out of
 *      generated inputs and asserts the trap never fires per exec
 *      (vacuity insurance). Expanded-object arms are out of scope.
 *   4. Renames pg_datum* kept; types local to this translation unit.
 *   5. Assert/AssertMacro compile out (production NDEBUG parity). NOTE the
 *      documented deviation: datumRestore's Assert(header > 0) is a no-op
 *      in C, while the Rust port release-asserts there — the driver only
 *      feeds well-formed images (roundtrip shapes), so the arm never fires
 *      on either side; the hardening delta stays recorded in
 *      proofs/scalar-datum/README.md.
 */

#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef uint8_t uint8;
typedef uint32_t uint32;
typedef size_t Size;
#define Assert(x) ((void) 0)
#define AssertMacro(x) ((void) 0)
#define PROOF_EREPORT_FLAG(err) (*(err) = 1)

/* Shared TLS errcode channel (defined in csrc/pg_float_io.c). */
extern _Thread_local int pg_diff_errcode;

/* ---- postgres.h / c.h plumbing (verbatim from the proofs copy) ---- */
typedef uintptr_t Datum;
typedef char *Pointer;
#define DatumGetPointer(X) ((Pointer) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define PointerIsValid(pointer) ((const void *) (pointer) != NULL)

/* ---- varatt.h, verbatim structs + little-endian macro arms ---- */
typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[1];	/* FLEXIBLE_ARRAY_MEMBER spelled [1] */
	}			va_4byte;
	struct						/* Compressed-in-line format */
	{
		uint32		va_header;
		uint32		va_tcinfo;
		char		va_data[1];
	}			va_compressed;
} varattrib_4b;

typedef struct
{
	uint8		va_header;
	char		va_data[1];
} varattrib_1b;

typedef struct
{
	uint8		va_header;		/* Always 0x01 */
	uint8		va_tag;			/* Type of datum */
	char		va_data[1];
} varattrib_1b_e;

typedef enum vartag_external
{
	VARTAG_INDIRECT = 1,
	VARTAG_EXPANDED_RO = 2,
	VARTAG_EXPANDED_RW = 3,
	VARTAG_ONDISK = 18
} vartag_external;

typedef struct
{
	char		bytes[8];
} varatt_indirect;
typedef struct
{
	char		bytes[8];
} varatt_expanded;
typedef struct
{
	char		bytes[16];
} varatt_external;

struct varlena
{
	char		vl_len_[4];
	char		vl_dat[1];
};

#define VARTAG_IS_EXPANDED(tag) \
	(((tag) & ~1) == VARTAG_EXPANDED_RO)

#define VARTAG_SIZE(tag) \
	((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
	 VARTAG_IS_EXPANDED(tag) ? sizeof(varatt_expanded) : \
	 (tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
	 (AssertMacro(false), 0))

#define VARHDRSZ_EXTERNAL		offsetof(varattrib_1b_e, va_data)

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

#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define VARSIZE(PTR)						VARSIZE_4B(PTR)
#define VARTAG_EXTERNAL(PTR)				VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR)				(VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))

#define VARATT_IS_EXTERNAL(PTR)				VARATT_IS_1B_E(PTR)
#define VARATT_IS_EXTERNAL_EXPANDED_RW(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_EXPANDED_RW)
#define VARATT_IS_EXTERNAL_EXPANDED(PTR) \
	(VARATT_IS_EXTERNAL(PTR) && VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(PTR)))

#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
	  VARSIZE_4B(PTR)))

/* ---- shim 2: palloc -> static bump buffer, reset per driver entry ---- */
#define PG_DX_HEAP_CAP 4096
static _Thread_local char pg_dx_heap[PG_DX_HEAP_CAP] __attribute__((aligned(8)));
static _Thread_local Size pg_dx_heap_next = 0;

static void *
pg_dx_palloc(Size n)
{
	Size		start = (pg_dx_heap_next + 7) & ~((Size) 7);

	/* drivers refuse inputs that could overflow the cap */
	pg_dx_heap_next = start + n;
	return pg_dx_heap + start;
}

#define palloc(n) pg_dx_palloc(n)
#define pfree(p) ((void) 0)

/* ---- shim 3: expanded-object machinery -> trapping stubs ---- */
static _Thread_local int pg_dx_eoh_reached_flag = 0;

int
pg_dx_eoh_reached(void)
{
	return pg_dx_eoh_reached_flag;
}

typedef struct
{
	int			dummy;
} ExpandedObjectHeader;
static ExpandedObjectHeader pg_dx_eoh_dummy;

static ExpandedObjectHeader *
DatumGetEOHP(Datum d)
{
	(void) d;
	pg_dx_eoh_reached_flag = 1;
	return &pg_dx_eoh_dummy;
}

static Size
EOH_get_flat_size(ExpandedObjectHeader *eohptr)
{
	(void) eohptr;
	pg_dx_eoh_reached_flag = 1;
	return 0;
}

static void
EOH_flatten_into(ExpandedObjectHeader *eohptr, void *result, Size allocated_size)
{
	(void) eohptr;
	(void) result;
	(void) allocated_size;
	pg_dx_eoh_reached_flag = 1;
}

typedef struct
{
	int			dummy;
} MemoryContextData;
typedef MemoryContextData *MemoryContext;
static MemoryContextData pg_dx_cur_mcx;
#define CurrentMemoryContext (&pg_dx_cur_mcx)

static Datum
TransferExpandedObject(Datum d, MemoryContext cxt)
{
	(void) cxt;
	pg_dx_eoh_reached_flag = 1;
	return d;
}

/*-------------------------------------------------------------------------
 * datumGetSize — body VERBATIM (see header; err: 1 = data exception,
 * 2 = invalid-typLen internal class).
 *-------------------------------------------------------------------------
 */
static Size
pg_datumGetSize(Datum value, int typByVal, int typLen, int *err)
{
	Size		size;

	if (typByVal)
	{
		/* Pass-by-value types are always fixed-length */
		Assert(typLen > 0 && typLen <= sizeof(Datum));
		size = (Size) typLen;
	}
	else
	{
		if (typLen > 0)
		{
			/* Fixed-length pass-by-ref type */
			size = (Size) typLen;
		}
		else if (typLen == -1)
		{
			/* It is a varlena datatype */
			struct varlena *s = (struct varlena *) DatumGetPointer(value);

			if (!PointerIsValid(s))
			{
				PROOF_EREPORT_FLAG(err);	/* errcode(ERRCODE_DATA_EXCEPTION),
											 * errmsg("invalid Datum pointer") */
				return 0;
			}

			size = (Size) VARSIZE_ANY(s);
		}
		else if (typLen == -2)
		{
			/* It is a cstring datatype */
			char	   *s = (char *) DatumGetPointer(value);

			if (!PointerIsValid(s))
			{
				PROOF_EREPORT_FLAG(err);	/* errcode(ERRCODE_DATA_EXCEPTION),
											 * errmsg("invalid Datum pointer") */
				return 0;
			}

			size = (Size) (strlen(s) + 1);
		}
		else
		{
			*err = 2;			/* elog(ERROR, "invalid typLen: %d", typLen) */
			size = 0;			/* keep compiler quiet */
			return 0;
		}
	}

	return size;
}

/*-------------------------------------------------------------------------
 * datumCopy — body VERBATIM (palloc per shim 2, EOH per shim 3).
 *-------------------------------------------------------------------------
 */
static Datum
pg_datumCopy(Datum value, int typByVal, int typLen, int *err)
{
	Datum		res;

	if (typByVal)
		res = value;
	else if (typLen == -1)
	{
		/* It is a varlena datatype */
		struct varlena *vl = (struct varlena *) DatumGetPointer(value);

		if (VARATT_IS_EXTERNAL_EXPANDED(vl))
		{
			/* Flatten into the caller's memory context */
			ExpandedObjectHeader *eoh = DatumGetEOHP(value);
			Size		resultsize;
			char	   *resultptr;

			resultsize = EOH_get_flat_size(eoh);
			resultptr = (char *) palloc(resultsize);
			EOH_flatten_into(eoh, resultptr, resultsize);
			res = PointerGetDatum(resultptr);
		}
		else
		{
			/* Otherwise, just copy the varlena datum verbatim */
			Size		realSize;
			char	   *resultptr;

			realSize = (Size) VARSIZE_ANY(vl);
			resultptr = (char *) palloc(realSize);
			memcpy(resultptr, vl, realSize);
			res = PointerGetDatum(resultptr);
		}
	}
	else
	{
		/* Pass by reference, but not varlena, so not toasted */
		Size		realSize;
		char	   *resultptr;

		realSize = pg_datumGetSize(value, typByVal, typLen, err);
		if (*err)
			return 0;

		resultptr = (char *) palloc(realSize);
		memcpy(resultptr, DatumGetPointer(value), realSize);
		res = PointerGetDatum(resultptr);
	}
	return res;
}

/*-------------------------------------------------------------------------
 * datumTransfer — body VERBATIM (TransferExpandedObject per shim 3).
 *-------------------------------------------------------------------------
 */
static Datum
pg_datumTransfer(Datum value, int typByVal, int typLen, int *err)
{
	if (!typByVal && typLen == -1 &&
		VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(value)))
		value = TransferExpandedObject(value, CurrentMemoryContext);
	else
		value = pg_datumCopy(value, typByVal, typLen, err);
	return value;
}

/*-------------------------------------------------------------------------
 * datumIsEqual — body VERBATIM (bool return rides as int).
 *-------------------------------------------------------------------------
 */
static int
pg_datumIsEqual(Datum value1, Datum value2, int typByVal, int typLen, int *err)
{
	int			res;

	if (typByVal)
	{
		/*
		 * just compare the two datums. NOTE: just comparing "len" bytes will
		 * not do the work, because we do not know how these bytes are aligned
		 * inside the "Datum".  We assume instead that any given datatype is
		 * consistent about how it fills extraneous bits in the Datum.
		 */
		res = (value1 == value2);
	}
	else
	{
		Size		size1,
					size2;
		char	   *s1,
				   *s2;

		/*
		 * Compare the bytes pointed by the pointers stored in the datums.
		 */
		size1 = pg_datumGetSize(value1, typByVal, typLen, err);
		if (*err)
			return 0;
		size2 = pg_datumGetSize(value2, typByVal, typLen, err);
		if (*err)
			return 0;
		if (size1 != size2)
			return 0;
		s1 = (char *) DatumGetPointer(value1);
		s2 = (char *) DatumGetPointer(value2);
		res = (memcmp(s1, s2, size1) == 0);
	}
	return res;
}

/*-------------------------------------------------------------------------
 * datumEstimateSpace — body VERBATIM (EOH per shim 3).
 *-------------------------------------------------------------------------
 */
static Size
pg_datumEstimateSpace(Datum value, int isnull, int typByVal, int typLen, int *err)
{
	Size		sz = sizeof(int);

	if (!isnull)
	{
		/* no need to use add_size, can't overflow */
		if (typByVal)
			sz += sizeof(Datum);
		else if (typLen == -1 &&
				 VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value)))
		{
			/* Expanded objects need to be flattened, see comment below */
			sz += EOH_get_flat_size(DatumGetEOHP(value));
		}
		else
		{
			sz += pg_datumGetSize(value, typByVal, typLen, err);
			if (*err)
				return 0;
		}
	}

	return sz;
}

/*-------------------------------------------------------------------------
 * datumSerialize — body VERBATIM (palloc/pfree per shim 2, EOH per shim 3).
 *-------------------------------------------------------------------------
 */
static int
pg_datumSerialize(Datum value, int isnull, int typByVal, int typLen,
				  char **start_address, int *err)
{
	ExpandedObjectHeader *eoh = NULL;
	int			header;

	/* Write header word. */
	if (isnull)
		header = -2;
	else if (typByVal)
		header = -1;
	else if (typLen == -1 &&
			 VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value)))
	{
		eoh = DatumGetEOHP(value);
		header = EOH_get_flat_size(eoh);
	}
	else
	{
		header = pg_datumGetSize(value, typByVal, typLen, err);
		if (*err)
			return 0;
	}
	memcpy(*start_address, &header, sizeof(int));
	*start_address += sizeof(int);

	/* If not null, write payload bytes. */
	if (!isnull)
	{
		if (typByVal)
		{
			memcpy(*start_address, &value, sizeof(Datum));
			*start_address += sizeof(Datum);
		}
		else if (eoh)
		{
			char	   *tmp;

			/*
			 * EOH_flatten_into expects the target address to be maxaligned,
			 * so we can't store directly to *start_address.
			 */
			tmp = (char *) palloc(header);
			EOH_flatten_into(eoh, tmp, header);
			memcpy(*start_address, tmp, header);
			*start_address += header;

			/* be tidy. */
			pfree(tmp);
		}
		else
		{
			memcpy(*start_address, DatumGetPointer(value), header);
			*start_address += header;
		}
	}
	return 0;
}

/*-------------------------------------------------------------------------
 * datumRestore — body VERBATIM (palloc per shim 2; Assert(header > 0)
 * compiles out, production parity — see the deviation note in the header).
 *-------------------------------------------------------------------------
 */
static Datum
pg_datumRestore(char **start_address, int *isnull)
{
	int			header;
	void	   *d;

	/* Read header word. */
	memcpy(&header, *start_address, sizeof(int));
	*start_address += sizeof(int);

	/* If this datum is NULL, we can stop here. */
	if (header == -2)
	{
		*isnull = 1;
		return (Datum) 0;
	}

	/* OK, datum is not null. */
	*isnull = 0;

	/* If this datum is pass-by-value, sizeof(Datum) bytes follow. */
	if (header == -1)
	{
		Datum		val;

		memcpy(&val, *start_address, sizeof(Datum));
		*start_address += sizeof(Datum);
		return val;
	}

	/* Pass-by-reference case; copy indicated number of bytes. */
	Assert(header > 0);
	d = palloc(header);
	memcpy(d, *start_address, header);
	*start_address += header;
	return PointerGetDatum(d);
}

/* ========== fuzz-facing driver entries (NOT Postgres code) ========== */

static void
pg_dx_reset(void)
{
	pg_dx_heap_next = 0;
	pg_dx_eoh_reached_flag = 0;
	pg_diff_errcode = 0;
}

/* get_size: 0 ok (*out written), else the err class (1 data-exception,
 * 2 invalid-typLen). */
int
pg_dx_get_size(uintptr_t value, int byval, int typlen, size_t *out)
{
	int			err = 0;
	Size		sz;

	pg_dx_reset();
	sz = pg_datumGetSize((Datum) value, byval, typlen, &err);
	if (err)
		return err;
	*out = sz;
	return 0;
}

/* copy/transfer: 0 ok — byval result in *outval; byref result bytes copied
 * into out (cap bytes, *outlen set); else the err class. */
static int
pg_dx_copy_common(int transfer, uintptr_t value, int byval, int typlen,
				  uintptr_t *outval, unsigned char *out, size_t cap,
				  size_t *outlen)
{
	int			err = 0;
	Datum		res;

	pg_dx_reset();
	res = transfer ? pg_datumTransfer((Datum) value, byval, typlen, &err)
		: pg_datumCopy((Datum) value, byval, typlen, &err);
	if (err)
		return err;
	if (byval)
	{
		*outval = (uintptr_t) res;
		*outlen = 0;
		return 0;
	}
	{
		Size		sz = pg_datumGetSize(res, byval, typlen, &err);

		if (err)
			return err;
		if (sz > cap)
			return 100;			/* driver refusal; Rust length-caps first */
		memcpy(out, DatumGetPointer(res), sz);
		*outlen = sz;
		*outval = 0;
	}
	return 0;
}

int
pg_dx_copy(uintptr_t value, int byval, int typlen, uintptr_t *outval,
		   unsigned char *out, size_t cap, size_t *outlen)
{
	return pg_dx_copy_common(0, value, byval, typlen, outval, out, cap, outlen);
}

int
pg_dx_transfer(uintptr_t value, int byval, int typlen, uintptr_t *outval,
			   unsigned char *out, size_t cap, size_t *outlen)
{
	return pg_dx_copy_common(1, value, byval, typlen, outval, out, cap, outlen);
}

/* is_equal: 0/1 verdict in *res; returns the err class or 0. */
int
pg_dx_is_equal(uintptr_t v1, uintptr_t v2, int byval, int typlen, int *res)
{
	int			err = 0;
	int			r;

	pg_dx_reset();
	r = pg_datumIsEqual((Datum) v1, (Datum) v2, byval, typlen, &err);
	if (err)
		return err;
	*res = r;
	return 0;
}

/* estimate_space: 0 ok (*out written), else the err class. */
int
pg_dx_estimate_space(uintptr_t value, int isnull, int byval, int typlen,
					 size_t *out)
{
	int			err = 0;
	Size		sz;

	pg_dx_reset();
	sz = pg_datumEstimateSpace((Datum) value, isnull, byval, typlen, &err);
	if (err)
		return err;
	*out = sz;
	return 0;
}

/* serialize: 0 ok (image written to out, *outlen set), else the err class.
 * Caller sizes out >= estimate (Rust drives estimate first). */
int
pg_dx_serialize(uintptr_t value, int isnull, int byval, int typlen,
				unsigned char *out, size_t cap, size_t *outlen)
{
	int			err = 0;
	char	   *cursor = (char *) out;
	Size		est;

	pg_dx_reset();
	est = pg_datumEstimateSpace((Datum) value, isnull, byval, typlen, &err);
	if (err)
		return err;
	if (est > cap)
		return 100;				/* driver refusal; Rust length-caps first */
	pg_datumSerialize((Datum) value, isnull, byval, typlen, &cursor, &err);
	if (err)
		return err;
	*outlen = (size_t) (cursor - (char *) out);
	return 0;
}

/* restore: parses image at in; *isnull set; byval value in *outval; byref
 * payload copied to out (*outlen set). Returns bytes consumed. */
size_t
pg_dx_restore(const unsigned char *in, int *isnull, uintptr_t *outval,
			  unsigned char *out, size_t cap, size_t *outlen)
{
	char	   *cursor = (char *) in;
	Datum		d;
	int			header;

	pg_dx_reset();
	d = pg_datumRestore(&cursor, isnull);
	*outval = 0;
	*outlen = 0;
	if (!*isnull)
	{
		memcpy(&header, in, sizeof(int));
		if (header == -1)
			*outval = (uintptr_t) d;
		else
		{
			if ((size_t) header > cap)
				return 0;		/* driver refusal; Rust length-caps first */
			memcpy(out, DatumGetPointer(d), (size_t) header);
			*outlen = (size_t) header;
		}
	}
	return (size_t) (cursor - (char *) in);
}
