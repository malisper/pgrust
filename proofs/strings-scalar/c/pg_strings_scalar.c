/*
 * Vendored from postgres REL_18_STABLE (fetched 2026-07-29 via
 * raw.githubusercontent.com/postgres/postgres/REL_18_STABLE):
 *
 *   SECTION A — src/backend/utils/adt/varlena.c
 *       byteain (escaped-style scan + decode; the "traditional escaped
 *       style" two-pass core), VAL/DIG macros verbatim.
 *   SECTION B — src/backend/access/common/detoast.c   toast_datum_size
 *               src/backend/access/common/toast_compression.c
 *                                                     toast_get_compression_id
 *               src/backend/utils/adt/varlena.c       pg_column_size,
 *                   pg_column_compression, pg_column_toast_chunk_id
 *               src/include/access/toast_compression.h ToastCompressionId
 *   SECTION C — src/backend/utils/adt/varlena.c       btvarstrequalimage,
 *                   check_collation_set
 *
 * varatt.h is vendored VERBATIM alongside as varatt_rel18.h (WORDS_BIGENDIAN
 * undefined => the little-endian macro arm, matching pgrust's LE targets).
 *
 * SHIMS (everything else is verbatim; bodies keep C's names/structure):
 *  - fmgr unwrapping: PG_FUNCTION_ARGS -> plain typed parameters; PG_RETURN_*
 *    -> plain returns; PG_RETURN_NULL -> *out_isnull = 1 sentinel.
 *  - byteain: PG_GETARG_CSTRING -> const char * (harness passes a
 *    NUL-terminated buffer with no interior NUL, the cstring contract);
 *    palloc(bc + VARHDRSZ) -> caller-provided payload buffer `out` (header
 *    lives on the Rust side only; the theorem compares payload + count);
 *    ereturn(invalid syntax) -> `return 0` reject verdict; the "\x" hex arm
 *    -> `return -2` TRAP (it delegates to hex_decode_safe, owned by
 *    proofs/hex; every harness fences it out and asserts the trap is
 *    unreached, so the fence failing is LOUD, not vacuous).
 *  - pg_column_*: the fn_extra-memoized get_fn_expr_argtype+get_typlen
 *    lookup -> the PROOF_TYPLEN static seam (set via pg_set_typlen; the Rust
 *    side stubs varlena::builtins::cached_arg_typlen to the same value —
 *    identical seam both sides, catalog walk out of the theorem);
 *    elog(ERROR ...) -> PROOF_EREPORT_FLAG-style err out-params;
 *    cstring_to_text(result) -> copy the name into caller buffer (compared
 *    byte-wise by the harness); DatumGetPointer/PG_GETARG_DATUM -> the
 *    attr pointer parameter directly.
 *  - toast_datum_size: the VARATT_IS_EXTERNAL_INDIRECT and
 *    VARATT_IS_EXTERNAL_EXPANDED arms dereference embedded live pointers
 *    (unmodelable here) -> both rewired to a TRAP sentinel (0xDEAD). The
 *    Rust rows carry the same fence ("indirect/expanded arms out of
 *    theorem"); harnesses assume the input is not one of those forms AND
 *    assert the trap value is never produced.
 *  - btvarstrequalimage: pg_newlocale_from_collation(collid)->deterministic
 *    -> the PROOF_LOCALE_DET static seam (set via pg_set_locale_det; Rust
 *    installs a pg_locale_seams::collation_is_deterministic impl reading
 *    the same value). C's builtin C/POSIX locales are deterministic=true;
 *    the harness fences the seam accordingly (documented in src/lib.rs).
 *    check_collation_set's ereport(ERRCODE_INDETERMINATE_COLLATION) ->
 *    err-flag return.
 * No logic is shimmed beyond the listed rewires: every scan condition,
 * octal-decode expression, header bit-test and switch arm is byte-for-byte
 * the postgres body.
 */

#include "../../support/c/pg_proof_shim.h"

/* struct varlena (postgres c.h), needed by varatt_rel18.h */
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
struct varlena
{
	char		vl_len_[4];		/* Do not touch this field directly! */
	char		vl_dat[4];		/* PROOF: fixed stand-in for FLEXIBLE_ARRAY_MEMBER
								 * (never indexed beyond header macros here) */
};
typedef struct varlena bytea;
typedef struct varlena text;

#include "varatt_rel18.h"

#include <string.h>

/* ======================================================================
 * SECTION A: byteain — escaped-style scan + decode (varlena.c)
 * ====================================================================== */

#define VAL(CH)			((CH) - '0')
#define DIG(VAL)		((VAL) + '0')

/*
 * byteain core, escaped style. Returns 1 = accept, 0 = reject (C: ereturn
 * invalid input syntax), -2 = hex-arm TRAP (fenced out of every theorem).
 * out receives the decoded PAYLOAD bytes; *out_bc the decoded byte count
 * (C's first-pass bc, before the += VARHDRSZ header accounting).
 */
int
pg_byteain(const char *inputText, unsigned char *out, int *out_bc)
{
	const char *tp;
	unsigned char *rp;
	int			bc;

	/* Recognize hex input */
	if (inputText[0] == '\\' && inputText[1] == 'x')
		return -2;				/* PROOF TRAP: hex arm out of theorem */

	/* Else, it's the traditional escaped style */
	for (bc = 0, tp = inputText; *tp != '\0'; bc++)
	{
		if (tp[0] != '\\')
			tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
			tp += 4;
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
			tp += 2;
		else
		{
			/*
			 * one backslash, not followed by another or ### valid octal
			 */
			return 0;			/* PROOF SHIM: ereturn(22P02) -> reject */
		}
	}

	*out_bc = bc;				/* PROOF: saved before pass two reuses bc */

	tp = inputText;
	rp = out;					/* PROOF SHIM: palloc -> caller buffer */
	while (*tp != '\0')
	{
		if (tp[0] != '\\')
			*rp++ = *tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
		{
			bc = VAL(tp[1]);
			bc <<= 3;
			bc += VAL(tp[2]);
			bc <<= 3;
			*rp++ = bc + VAL(tp[3]);

			tp += 4;
		}
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
		{
			*rp++ = '\\';
			tp += 2;
		}
		else
		{
			/*
			 * We should never get here. The first pass should not allow it.
			 */
			return 0;			/* PROOF SHIM: ereturn(22P02) -> reject */
		}
	}

	return 1;
}

/* ======================================================================
 * SECTION B: toast header accessors
 *   toast_datum_size (detoast.c), toast_get_compression_id
 *   (toast_compression.c), pg_column_size / pg_column_compression /
 *   pg_column_toast_chunk_id (varlena.c)
 * ====================================================================== */

/* toast_compression.h, verbatim */
typedef enum ToastCompressionId
{
	TOAST_PGLZ_COMPRESSION_ID = 0,
	TOAST_LZ4_COMPRESSION_ID = 1,
	TOAST_INVALID_COMPRESSION_ID = 2,
} ToastCompressionId;

#define PG_TOAST_TRAP 0xDEADDEADull	/* indirect/expanded arm sentinel */

/*
 * toast_datum_size (detoast.c), verbatim except the indirect/expanded arms
 * (see header): those return the PG_TOAST_TRAP sentinel instead of chasing
 * the embedded pointer.
 */
uint64
pg_toast_datum_size(const unsigned char *attr_bytes)
{
	const struct varlena *attr = (const struct varlena *) attr_bytes;
	Size		result;

	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		struct varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
		result = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);
	}
	else if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
		return PG_TOAST_TRAP;	/* PROOF TRAP: arm fenced out of theorem */
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(attr))
	{
		return PG_TOAST_TRAP;	/* PROOF TRAP: arm fenced out of theorem */
	}
	else if (VARATT_IS_SHORT(attr))
	{
		result = VARSIZE_SHORT(attr);
	}
	else
	{
		result = VARSIZE(attr);
	}
	return result;
}

/* toast_get_compression_id (toast_compression.c), verbatim */
int
pg_toast_get_compression_id(const unsigned char *attr_bytes)
{
	const struct varlena *attr = (const struct varlena *) attr_bytes;
	ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;

	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		struct varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
			cmid = VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer);
	}
	else if (VARATT_IS_COMPRESSED(attr))
		cmid = VARDATA_COMPRESSED_GET_COMPRESS_METHOD(attr);

	return (int) cmid;
}

/* ---- PROOF_TYPLEN seam: replaces the fn_extra-memoized
 * get_fn_expr_argtype + get_typlen lookup (both sides read the same
 * harness-set value; the catalog walk leaves the theorem). ---- */
static int	pg_proof_typlen = 0;

int
pg_set_typlen(int typlen)
{
	pg_proof_typlen = typlen;
	return 0;					/* int return: Kani lowers Rust () as a
								 * struct goto-cc rejects vs C void */
}

/*
 * pg_column_size (varlena.c), verbatim body with the fn_extra memoization
 * replaced by the typlen seam (see header). Returns the int32 result;
 * cstring arm takes strlen over the caller's pointer exactly as C does.
 */
int32
pg_column_size_c(const unsigned char *value, const char *value_as_cstring)
{
	int32		result;
	int			typlen;

	typlen = pg_proof_typlen;	/* PROOF SHIM: fn_extra seam */

	if (typlen == -1)
	{
		/* varlena type, possibly toasted */
		result = pg_toast_datum_size(value);
	}
	else if (typlen == -2)
	{
		/* cstring */
		result = strlen(value_as_cstring) + 1;
	}
	else
	{
		/* ordinary fixed-width type */
		result = typlen;
	}

	return result;
}

/*
 * pg_column_compression (varlena.c). Out-parameters instead of Datum:
 *   return 0  -> SQL NULL (PG_RETURN_NULL)
 *   return 1  -> text result; name copied into out_name (caller buffer,
 *                >= 4 bytes), *out_name_len set (cstring_to_text shim)
 *   return -1 -> elog(ERROR, "invalid compression method id %d")
 */
int
pg_column_compression_c(const unsigned char *attr, char *out_name, int *out_name_len)
{
	int			typlen;
	const char *result;
	ToastCompressionId cmid;

	typlen = pg_proof_typlen;	/* PROOF SHIM: fn_extra seam */

	if (typlen != -1)
		return 0;				/* PG_RETURN_NULL() */

	/* get the compression method id stored in the compressed varlena */
	cmid = pg_toast_get_compression_id(attr);
	if (cmid == TOAST_INVALID_COMPRESSION_ID)
		return 0;				/* PG_RETURN_NULL() */

	/* convert compression method id to compression method name */
	switch (cmid)
	{
		case TOAST_PGLZ_COMPRESSION_ID:
			result = "pglz";
			break;
		case TOAST_LZ4_COMPRESSION_ID:
			result = "lz4";
			break;
		default:
			return -1;			/* PROOF SHIM: elog(ERROR, invalid cmid) */
	}

	{
		/* PROOF SHIM: cstring_to_text -> caller buffer */
		int			i = 0;

		while (result[i] != '\0')
		{
			out_name[i] = result[i];
			i++;
		}
		*out_name_len = i;
	}
	return 1;
}

/*
 * pg_column_toast_chunk_id (varlena.c).
 *   return 0 -> SQL NULL; return 1 -> *out_valueid = toast_pointer.va_valueid
 */
int
pg_column_toast_chunk_id_c(const unsigned char *attr_bytes, uint32 *out_valueid)
{
	int			typlen;
	const struct varlena *attr;
	struct varatt_external toast_pointer;

	typlen = pg_proof_typlen;	/* PROOF SHIM: fn_extra seam */

	if (typlen != -1)
		return 0;				/* PG_RETURN_NULL() */

	attr = (const struct varlena *) attr_bytes;

	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
		return 0;				/* PG_RETURN_NULL() */

	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

	*out_valueid = toast_pointer.va_valueid;
	return 1;
}

/* ======================================================================
 * SECTION C: btvarstrequalimage + check_collation_set (varlena.c)
 * ====================================================================== */

#define InvalidOid ((Oid) 0)
#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))

/* ---- PROOF_LOCALE_DET seam: pg_newlocale_from_collation(collid)->
 * deterministic. Both sides read the same harness-set value; locale
 * resolution leaves the theorem. The harness fences C/POSIX collations to
 * deterministic=true (C's builtin locales), see src/lib.rs. ---- */
static int	pg_proof_locale_det = 1;

int
pg_set_locale_det(int det)
{
	pg_proof_locale_det = det;
	return 0;					/* int return: void/Unit FFI trap */
}

/*
 * check_collation_set (varlena.c), verbatim condition; ereport(ERROR,
 * ERRCODE_INDETERMINATE_COLLATION ...) -> return 0 err verdict.
 */
static int
pg_check_collation_set(Oid collid)
{
	if (!OidIsValid(collid))
	{
		return 0;				/* PROOF SHIM: ereport(42P22) -> err */
	}
	return 1;
}

/*
 * btvarstrequalimage (varlena.c).
 *   return -1 -> check_collation_set error (42P22 class)
 *   return 0/1 -> PG_RETURN_BOOL(locale->deterministic)
 */
int
pg_btvarstrequalimage(Oid collid)
{
	if (!pg_check_collation_set(collid))
		return -1;

	/* PROOF SHIM: locale = pg_newlocale_from_collation(collid) -> seam */
	return pg_proof_locale_det ? 1 : 0;
}
