/*
 * Verbatim PostgreSQL C for the datum.c copy/serialize kernel family
 * (proofs/scalar-datum):
 *   datumGetSize, datumCopy, datumTransfer, datumIsEqual,
 *   datumEstimateSpace, datumSerialize, datumRestore
 *
 * Provenance (fetched 2026-07-30, postgres/postgres REL_18_STABLE):
 *   src/backend/utils/adt/datum.c      (all seven function bodies, verbatim)
 *   src/include/varatt.h               (varattrib structs + VARATT_/VARSIZE_/
 *                                       VARTAG_ macro definitions, verbatim
 *                                       little-endian arms)
 *   src/include/postgres.h / c.h       (DatumGetPointer/PointerGetDatum/
 *                                       PointerIsValid, Datum typedef)
 * Campaign oracle pin is PostgreSQL 18.3 (Stamp-18.3, upstream 62d6c7d3df);
 * REL_18_STABLE datum.c is byte-identical for these functions (file is
 * unchanged since PG 16 except comments — eyeballed at fetch time).
 *
 * Shims (plumbing only, never logic) — every one listed:
 *   1. ereport(ERROR, errcode(ERRCODE_DATA_EXCEPTION), ...) -> the suite's
 *      PROOF_EREPORT_FLAG out-param convention (pg_proof_shim.h): the shimmed
 *      function takes `int *err`, sets *err = 1 (= ERRCODE_DATA_EXCEPTION)
 *      at the exact ereport program point and returns a 0 sentinel.
 *      elog(ERROR, "invalid typLen"/"unexpected typLen") sets *err = 2
 *      (internal error class). Message text never crosses the seam.
 *   2. palloc -> fixed static-buffer bump allocator (pg_proof_palloc below);
 *      pfree -> no-op. Allocation strategy is not part of any claim (mirrors
 *      the Rust side's static-buffer mcx stub).
 *   3. Expanded-object machinery (DatumGetEOHP / EOH_get_flat_size /
 *      EOH_flatten_into / TransferExpandedObject) -> trapping stubs that set
 *      pg_proof_eoh_reached. Harnesses FENCE the expanded arm out of domain
 *      and assert the trap never fired (reachability guard, vacuity
 *      insurance). The expanded-object arm is session machinery, out of
 *      scope for this family.
 *   4. Function renames pg_datum_* wrapping VERBATIM bodies; the bool return
 *      of datumIsEqual rides as int (Kani lowers Rust bool FFI awkwardly);
 *      datumSerialize's void return rides as int 0 (Kani lowers Rust () as
 *      `struct Unit`, which goto-cc rejects against C void).
 *   5. memcpy/memcmp from <string.h> (CBMC-native models — cash precedent);
 *      strlen -> pg_proof_strlen (exact total replacement; no libc model).
 *   6. VARTAG_SIZE's unreachable-tag arm: PG spells it
 *      (AssertMacro(false), 0) — Assert compiles out in production, so C
 *      yields size 0 for an unrecognized tag; the Rust port PANICS there
 *      (types_tuple varatt::vartag_size). Harnesses fence tags to the four
 *      defined values {1,2,3,18}; the out-of-fence behavior difference is a
 *      documented pgrust hardening, recorded in the family README.
 */

#include "../../support/c/pg_proof_shim.h"
#include <string.h>				/* memcpy/memcmp: CBMC-native models */

/* ---- postgres.h / c.h plumbing ---- */
typedef uintptr_t Datum;
typedef char *Pointer;
#define DatumGetPointer(X) ((Pointer) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define PointerIsValid(pointer) ((const void *) (pointer) != NULL)

/* no libc model for strlen (pg_proof_shim / cash precedent) */
static Size
pg_proof_strlen(const char *s)
{
	Size		n = 0;

	while (s[n] != '\0')
		n++;
	return n;
}

#define strlen(s) pg_proof_strlen(s)

/* ---- varatt.h, verbatim structs + little-endian macro arms ---- */
typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[1];	/* FLEXIBLE_ARRAY_MEMBER spelled [1] (shim:
								 * goto-cc friendliness; never indexed past
								 * what the harness allocates) */
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

/* sizeof stand-ins for the external-pointer payload structs (varatt.h:
 * varatt_indirect = 1 pointer, varatt_expanded = 1 pointer,
 * varatt_external = 4 x int32) */
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

/* ---- shim 2: palloc -> static bump buffer; pfree -> no-op ---- */
#define PG_PROOF_C_HEAP_CAP 2048
static char pg_proof_c_heap[PG_PROOF_C_HEAP_CAP]
__attribute__((aligned(8)));
static Size pg_proof_c_heap_next = 0;

static void *
pg_proof_palloc(Size n)
{
	Size		start = (pg_proof_c_heap_next + 7) & ~((Size) 7);

	/* harnesses keep total allocation under the cap */
	pg_proof_c_heap_next = start + n;
	return pg_proof_c_heap + start;
}

#define palloc(n) pg_proof_palloc(n)
#define pfree(p) ((void) 0)

/* ---- shim 3: expanded-object machinery -> trapping stubs ---- */
int			pg_proof_eoh_reached = 0;

typedef struct
{
	int			dummy;
} ExpandedObjectHeader;
static ExpandedObjectHeader pg_proof_eoh_dummy;

static ExpandedObjectHeader *
DatumGetEOHP(Datum d)
{
	pg_proof_eoh_reached = 1;
	return &pg_proof_eoh_dummy;
}

static Size
EOH_get_flat_size(ExpandedObjectHeader *eohptr)
{
	pg_proof_eoh_reached = 1;
	return 0;
}

static void
EOH_flatten_into(ExpandedObjectHeader *eohptr, void *result, Size allocated_size)
{
	pg_proof_eoh_reached = 1;
}

typedef struct
{
	int			dummy;
} MemoryContextData;
typedef MemoryContextData *MemoryContext;
static MemoryContextData pg_proof_cur_mcx;
#define CurrentMemoryContext (&pg_proof_cur_mcx)

static Datum
TransferExpandedObject(Datum d, MemoryContext cxt)
{
	pg_proof_eoh_reached = 1;
	return d;
}

/*-------------------------------------------------------------------------
 * datumGetSize — body VERBATIM from REL_18_STABLE datum.c, except the two
 * ereport/elog sites rewired per shim 1 (int *err out-param; *err = 1 for
 * ERRCODE_DATA_EXCEPTION, *err = 2 for the elog internal-error class; each
 * followed by `return 0;` at the exact abort point).
 *-------------------------------------------------------------------------
 */
Size
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
 * datumCopy — body VERBATIM (palloc per shim 2, EOH per shim 3; the inner
 * datumGetSize call routes through the shimmed signature, propagating err).
 *-------------------------------------------------------------------------
 */
Datum
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
Datum
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
 * datumIsEqual — body VERBATIM (bool return rides as int; inner
 * datumGetSize per shim 1).
 *-------------------------------------------------------------------------
 */
int
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
			return false;
		s1 = (char *) DatumGetPointer(value1);
		s2 = (char *) DatumGetPointer(value2);
		res = (memcmp(s1, s2, size1) == 0);
	}
	return res;
}

/*-------------------------------------------------------------------------
 * datumEstimateSpace — body VERBATIM (EOH per shim 3, inner datumGetSize
 * per shim 1).
 *-------------------------------------------------------------------------
 */
Size
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
 * datumSerialize — body VERBATIM (palloc/pfree per shim 2, EOH per shim 3,
 * inner datumGetSize per shim 1). Caller provides the output buffer via
 * start_address exactly as in C.
 *-------------------------------------------------------------------------
 */
int
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
 * datumRestore — body VERBATIM (palloc per shim 2). The Assert(header > 0)
 * compiles out exactly as in a production postgres build; the harness fences
 * the domain to well-formed images (the Rust port hardens this arm with a
 * release assert — documented deviation, see README).
 *-------------------------------------------------------------------------
 */
Datum
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
		*isnull = true;
		return (Datum) 0;
	}

	/* OK, datum is not null. */
	*isnull = false;

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

/* harness helpers: reset the C-side proof heap + trap flag between nothing —
 * each Kani harness runs in a fresh state, but exposed for completeness */
void
pg_proof_c_reset(void)
{
	pg_proof_c_heap_next = 0;
	pg_proof_eoh_reached = 0;
}

/* ---------------------------------------------------------------------------
 * p1-lanep addition (2026-07-31): FullTransactionIdFromAllowableAt,
 * VERBATIM from src/include/access/transam.h (REL_18_STABLE, byte-identical
 * to the Stamp-18.3 pin) with the two Asserts compiled out exactly as a
 * production NDEBUG build does (Assert is a no-op in the shim). The struct
 * FullTransactionId wrapper is flattened to uint64 (the value member) —
 * representation-only, the arithmetic is untouched.
 * C oracle for adt/xid8funcs::full_xid_from_allowable_at.
 * ------------------------------------------------------------------------ */
#define PG_FirstNormalTransactionId ((uint32) 3)
#define PG_TransactionIdIsNormal(xid) ((xid) >= PG_FirstNormalTransactionId)

uint64
pg_fxid_from_allowable_at(uint64 nextFullXid, uint32 xid)
{
	uint32		epoch;

	/* Special transaction ID. */
	if (!PG_TransactionIdIsNormal(xid))
		return (uint64) xid;   /* FullTransactionIdFromEpochAndXid(0, xid) */

	epoch = (uint32) (nextFullXid >> 32);   /* EpochFromFullTransactionId */
	if (xid > (uint32) nextFullXid)         /* XidFromFullTransactionId */
	{
		/* Assert(epoch != 0): compiled out (NDEBUG parity) */
		epoch--;
	}

	return (((uint64) epoch) << 32) | (uint64) xid;  /* FromEpochAndXid */
}
