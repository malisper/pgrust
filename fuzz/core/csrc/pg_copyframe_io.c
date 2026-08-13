/*
 * Vendored PostgreSQL C: COPY field-framing parsers — differential-fuzz
 * oracle for the Q8-F1 memory-safety surface (VENDOR-COPY lane, covdiff
 * campaign). Targets: the text field split/de-escape and the binary
 * per-field length framing, exactly the two paths EDGE2 flagged as having
 * NO verbatim-C oracle in csrc/ (findings-edge2.md "Coverage gaps").
 *
 * Provenance (bodies VERBATIM unless a shim is listed below), from the
 * repo's vendored ground-truth checkout
 * ../pgrust-reference/vendor/postgres-src @
 * 62d6c7d3df6287f1bd83199c1a746e50d31571a0 ("Stamp 18.3", REL_18):
 *   - src/backend/commands/copyfromparse.c
 *       361..393  CopyGetInt32 / CopyGetInt16      — VERBATIM
 *       700..742  CopyReadBinaryData               — VERBATIM
 *       1534..1542 GetDecimalFromHex               — VERBATIM
 *       1563..1806 CopyReadAttributesText          — VERBATIM
 *   - src/common/stringinfo.c enlargeStringInfo MaxAllocSize guard
 *       — the >= (MaxAllocSize - len) check is reproduced VERBATIM in the
 *         shim enlargeStringInfo() below (it is the verdict-determining
 *         branch for the huge-field-length Q8 case); the physical
 *         allocation is capped (plumbing, see below).
 *
 * WHY THIS TARGET (the Q8-F1 bug class): both paths are hand-rolled parsers
 * over an attacker-controlled buffer whose framing is a length/prefix word.
 * The historical bug (Q8-F1) was an empty-slice reach on the COPY path; the
 * ST3 class was wrapping arithmetic on a length word. The COPY_FIELD_LEN
 * bank in fuzz/core/src/edge.rs (`-1` legal NULL sentinel vs `-2/-3/INT_MIN`
 * illegal negatives and INT_MAX oversize) was built by EDGE2 and had NO C to
 * fire against until this lane. The bar: pgrust's shipped
 * copy_cmd::fromparse field parse must ACCEPT-or-REJECT each malformed /
 * empty input IDENTICALLY to this verbatim C — same field image, same
 * accept/reject, same sqlstate CLASS. A pgrust PANIC / OOB / assert where C
 * cleanly rejects (a framing error) is HIGH (the Q8 class).
 *
 * Shims (PLUMBING ONLY, never logic — same conventions as pg_varbit_io.c):
 *   - CopyFromStateData: reduced to the fields the vendored bodies touch —
 *     opts.{delim,null_print,null_print_len,default_print,default_print_len},
 *     max_fields, line_buf/attribute_buf (StringInfoData), raw_fields, and
 *     the binary raw_buf/raw_buf_index/raw_buf_len/raw_reached_eof. The
 *     tuple/relation/fmgr/defexpr members the text default-marker branch
 *     names are present ONLY so that (verbatim) branch type-checks; it is
 *     DEAD (default_print is always NULL and attnumlist length is 0 in this
 *     oracle, so neither guard is ever taken) — this is how the relation
 *     entanglement is avoided without editing the body.
 *   - StringInfoData/resetStringInfo/enlargeStringInfo: the c.h/stringinfo.h
 *     definitions, reduced. enlargeStringInfo performs the VERBATIM
 *     MaxAllocSize guard (the branch that decides the huge-length verdict:
 *     ereport ERRCODE_PROGRAM_LIMIT_EXCEEDED) but only ever PHYSICALLY
 *     allocates up to CPF_PHYS_CAP bytes: CopyReadBinaryData can never write
 *     more than the (bounded) input length into the buffer, so the cap is
 *     unobservable on the value/verdict planes and avoids a 1 GB malloc per
 *     oversize case. Driver inputs are bounded < CPF_PHYS_CAP.
 *   - ereport(ERROR,(...)) -> record errcode class, longjmp (errfinish's
 *     control flow); errmsg/errdetail evaluate their args then discard (the
 *     comparator checks the errcode CLASS, not text).
 *   - CopyLoadRawBuf -> sets raw_reached_eof (the whole COPY stream is
 *     preloaded into raw_buf by the entry points; the buffer never refills).
 *   - pg_verifymbstr -> no-op returning true: the shipped Rust side runs
 *     under the default server encoding PG_SQL_ASCII, whose pg_verify_mbstr
 *     is itself a no-op (every byte valid). Encoding verification is a
 *     separate mbstr surface with its own oracle; it is carved here so the
 *     de-escape/delimiter/null-marker framing surface is isolated. Both
 *     sides therefore agree trivially on this plane.
 *   - pg_ntoh16/32 -> byteswap (host is little-endian on laptop + CI cluster).
 *   - ReceiveFunctionCall (binary path): CARVED — the binary driver compares
 *     FRAMING (field count, per-field length sentinel handling, byte image,
 *     count-match / EOF verdict), not typreceive. pgrust's
 *     read_binary_attr_data does the same length-bounded byte load; the
 *     typreceive call sits strictly after the framing decisions and belongs
 *     to the per-type recv oracles (already swept by EDGE2, 0 findings).
 */

#include "postgres.h"			/* shared shim: int types, palloc, Assert */

#include <ctype.h>
#include <limits.h>
#include <setjmp.h>
#include <string.h>
#include "pg_oracle_guard.h"	/* oracle-serialization holder check */

/* FAMILY SYMBOL ISOLATION (pg_int_io.c/pg_varbit_io.c precedent): this TU
 * shares the pg_difffuzz_oracle cc::Build. Every helper below is file-static
 * (so no duplicate-symbol link error on ld.lld); the only exported symbols
 * are the cpf_* entry points at the bottom. */

typedef unsigned char cpf_uchar;
typedef size_t Size;

#define MaxAllocSize ((Size) 0x3fffffff)	/* 1 gigabyte - 1 (memutils.h) */
#define CPF_PHYS_CAP (1 << 20)				/* physical alloc bound; see header */

#define Min(a, b) ((a) < (b) ? (a) : (b))

/* ---- error plane (pg_varbit_io.c convention) ---- */
#define PG_DIFF_ERR_NONE 0
#define PG_DIFF_ERR_BAD_COPY 5			/* 22P04 bad_copy_file_format */
#define PG_DIFF_ERR_PROGRAM_LIMIT 3		/* 54000 program_limit_exceeded */
#define PG_DIFF_ERR_ENCODING 7			/* 22021 character_not_in_repertoire */

#define ERRCODE_BAD_COPY_FILE_FORMAT PG_DIFF_ERR_BAD_COPY
#define ERRCODE_PROGRAM_LIMIT_EXCEEDED PG_DIFF_ERR_PROGRAM_LIMIT

static _Thread_local int cpf_errcode_val;
static _Thread_local jmp_buf cpf_jb;

static int
errcode(int code)
{
	cpf_errcode_val = code;
	return 0;
}

static int
errmsg(const char *fmt, ...)
{
	(void) fmt;
	return 0;
}

static int
errdetail(const char *fmt, ...)
{
	(void) fmt;
	return 0;
}

/* ereport(ERROR, (errcode(x), errmsg(y), ...)) : evaluate the arg list (so
 * errcode records the class), then longjmp — errfinish's non-return path. */
static int
cpf_collect(int first, ...)
{
	(void) first;
	return 0;
}

#define ERROR 20
#define ereport(elevel, rest) \
	do { \
		(void) (elevel); \
		cpf_collect rest; \
		longjmp(cpf_jb, 1); \
	} while (0)

/* ---- StringInfo shim (stringinfo.h reduced) ---- */
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
}			StringInfoData;

typedef StringInfoData *StringInfo;

static void
resetStringInfo(StringInfo str)
{
	str->len = 0;
	str->cursor = 0;
	if (str->data && str->maxlen > 0)
		str->data[0] = '\0';
}

/* VERBATIM MaxAllocSize guard (stringinfo.c enlargeStringInfo); physical
 * allocation capped per file header. */
static void
enlargeStringInfo(StringInfo str, int needed)
{
	Size		want;

	if (needed < 0)				/* should not happen */
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("invalid string enlargement request size: %d", needed)));
	if (((Size) needed) >= (MaxAllocSize - (Size) str->len))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("string buffer exceeds maximum allowed length (%zu bytes)",
						(Size) MaxAllocSize)));

	want = (Size) str->len + (Size) needed + 1;
	if (want > (Size) CPF_PHYS_CAP)
		want = (Size) CPF_PHYS_CAP;
	if ((int) want > str->maxlen)
	{
		str->data = (char *) realloc(str->data, want);
		str->maxlen = (int) want;
	}
}

/* ---- COPY state shim (copyfrom_internal.h reduced) ---- */
typedef struct CopyFormatOptionsShim
{
	char		delim[2];
	char		quote[2];		/* CSV quote char (VENDOR-COPYROW line reader) */
	char		escape[2];		/* CSV escape char (VENDOR-COPYROW line reader) */
	const char *null_print;
	int			null_print_len;
	const char *default_print;	/* always NULL here -> default branch dead */
	int			default_print_len;
}			CopyFormatOptionsShim;

typedef struct CopyFromStateData
{
	CopyFormatOptionsShim opts;
	int			max_fields;
	StringInfoData line_buf;
	StringInfoData attribute_buf;
	char	  **raw_fields;

	/* binary framing */
	char	   *raw_buf;
	int			raw_buf_index;
	int			raw_buf_len;
	bool		raw_reached_eof;

	/* line/row framing (VENDOR-COPYROW: CopyReadLine/CopyReadLineText) */
	char	   *input_buf;		/* aliases raw_buf (no-transcode); NUL-padded */
	int			input_buf_index;
	int			input_buf_len;	/* bytes "verified"/visible (grows to raw_buf_len) */
	bool		input_reached_eof;
	bool		input_reached_error;
	int			srclen;			/* total preloaded stream length */
	int			eol_type;
	int			cur_lineno;
	int			copy_src;		/* COPY_FILE here -> frontend-drain branch dead */
	bool		line_buf_valid;

	/* dead-branch shims (text default-marker path; never executed) */
	int			attnumlist;		/* list_length(attnumlist) -> this int (0) */
	void	  **defexprs;
	bool	   *defaults;
	void	   *rel;
}			CopyFromStateData;

typedef CopyFromStateData *CopyFromState;

/* macros / helpers referenced verbatim */
#define RAW_BUF_BYTES(cstate) ((cstate)->raw_buf_len - (cstate)->raw_buf_index)
#define OCTVALUE(c) ((c) - '0')
#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define IS_HIGHBIT_SET(ch) ((cpf_uchar) (ch) & 0x80)
#define repalloc(ptr, sz) realloc((ptr), (sz))

/* dead-branch type shims (default-marker path only) */
typedef void *TupleDesc;
typedef struct FormData_pg_attribute_shim
{
	int			unused;
}		   *Form_pg_attribute;
#define RelationGetDescr(rel) ((TupleDesc) 0)
#define TupleDescAttr(td, i) ((Form_pg_attribute) 0)
#define NameStr(n) ""
#define list_length(l) (l)
#define list_nth_int(l, n) 0

/* pg_verifymbstr under the default server encoding PG_SQL_ASCII, reproduced
 * faithfully: the SQL_ASCII verifier is pg_ascii_verifystr == nul_pos (wchar.c
 * / pgrust common/wchar), i.e. high-bit bytes are VALID single bytes and the
 * only rejected byte is an embedded NUL, which pg_verify_mbstr reports as
 * ERRCODE_CHARACTER_NOT_IN_REPERTOIRE (22021). CopyReadAttributesText always
 * calls this with noError = false, so it throws. Reproducing this (instead of
 * a no-op) keeps the de-escape surface's `\NNN`/`\xHH` embedded-NUL cases a
 * live differential rather than a false accept. */
static bool
pg_verifymbstr(const char *mbstr, int len, bool noError)
{
	int			i;

	for (i = 0; i < len; i++)
	{
		if (mbstr[i] == '\0')
		{
			if (noError)
				return false;
			ereport(ERROR,
					(errcode(PG_DIFF_ERR_ENCODING),
					 errmsg("invalid byte sequence for encoding")));
		}
	}
	return true;
}

static uint32
pg_ntoh32(uint32 x)
{
	return __builtin_bswap32(x);
}

static uint16
pg_ntoh16(uint16 x)
{
	return __builtin_bswap16(x);
}

/* CopyLoadRawBuf: the whole stream is preloaded; never refill. */
static void
CopyLoadRawBuf(CopyFromState cstate)
{
	cstate->raw_reached_eof = true;
}

/* errhint shim (line-reader ereport arg lists reference it). */
static int
errhint(const char *fmt, ...)
{
	(void) fmt;
	return 0;
}

/* ---- line-reader plumbing (VENDOR-COPYROW) ---- */
/* eol_type values (copyfrom_internal.h EolType, VERBATIM order). */
#define EOL_UNKNOWN 0
#define EOL_NL 1
#define EOL_CR 2
#define EOL_CRNL 3
/* copy_src values (copy.h CopySource); only COPY_FILE/COPY_FRONTEND matter. */
#define COPY_FILE 0
#define COPY_FRONTEND 1
#define INPUT_BUF_SIZE 65536
#define INPUT_BUF_BYTES(cstate) ((cstate)->input_buf_len - (cstate)->input_buf_index)

/*
 * appendBinaryStringInfo (stringinfo.c) — REFILL_LINEBUF's transfer into
 * line_buf. Bodies of the MaxAllocSize/physical-cap policy live in the shared
 * enlargeStringInfo above; driver inputs are bounded < CPF_PHYS_CAP so the
 * physical cap is never observable on the line-image plane.
 */
static void
appendBinaryStringInfo(StringInfo str, const char *data, int datalen)
{
	enlargeStringInfo(str, datalen);
	memcpy(str->data + str->len, data, (size_t) datalen);
	str->len += datalen;
	str->data[str->len] = '\0';
}

/*
 * SQL_ASCII streaming verify (wchar.c pg_ascii_verifystr, VERBATIM: memchr for
 * NUL): the count of valid bytes before the first NUL, or the whole length.
 * SQL_ASCII treats every non-NUL byte (incl. high-bit) as a valid 1-byte char;
 * an embedded NUL is the ONLY rejected byte. Mirrors the shipped side exactly
 * (crates/common/wchar pg_ascii_verifystr / pg_verify_mbstr_len max_len<=1).
 */
static int
cpf_ascii_verifystr(const char *s, int len)
{
	int			i;

	for (i = 0; i < len; i++)
		if (s[i] == '\0')
			return i;
	return len;
}

/*
 * CopyConvertBuf (no-transcoding arm), reproducing the shipped
 * copy_convert_buf: reveal more verified bytes; on a NUL at the front of the
 * unverified region flag input_reached_error; on exhaustion at raw EOF flag
 * input_reached_eof.
 */
static void
CopyConvertBuf(CopyFromState cstate)
{
	int			preverified = cstate->input_buf_len;
	int			unverified = cstate->raw_buf_len - cstate->input_buf_len;
	int			nverified;

	if (unverified == 0)
	{
		if (cstate->raw_reached_eof)
			cstate->input_reached_eof = true;
		return;
	}
	nverified = cpf_ascii_verifystr(cstate->raw_buf + preverified, unverified);
	if (nverified == 0)
	{
		/* SQL_ASCII max length is 1, so a leading NUL is always an error. */
		if (cstate->raw_reached_eof || unverified >= 1)
			cstate->input_reached_error = true;
		return;
	}
	cstate->input_buf_len += nverified;
}

/*
 * CopyConversionError (no-transcoding arm): pg_verify_mbstr(noError=false)
 * over the unverified tail throws ENCODING (22021) on the offending NUL —
 * exactly what the shipped copy_conversion_error does.
 */
static void
CopyConversionError(CopyFromState cstate)
{
	pg_verifymbstr(cstate->raw_buf + cstate->input_buf_len,
				   cstate->raw_buf_len - cstate->input_buf_len, false);
	/* not reached (pg_verifymbstr ereports on the NUL) */
}

/*
 * CopyLoadInputBuf: PLUMBING mirroring the SHIPPED no-transcoding SQL_ASCII
 * path (copy/src/fromparse.rs copy_load_input_buf). input_buf aliases raw_buf
 * (no transcoding); the whole stream is preloaded into raw_buf with the
 * guaranteed NUL pad at [raw_buf_len]. On the FIRST call the SQL_ASCII verify
 * reveals every byte up to the first embedded NUL (or all of them) with EOF
 * still false; only a later call (once everything verified is consumed) sets
 * EOF. This two-phase hit_eof timing is load-bearing — it gates
 * IF_NEED_REFILL_AND_{NOT_EOF_CONTINUE,EOF_BREAK}, so it must match the shipped
 * side on trailing-backslash / trailing-CR-at-EOF cases. An embedded NUL is
 * rejected here (encoding error), exactly as the shipped line reader rejects
 * it before field parsing — keeping the embedded-NUL case a live differential
 * rather than a plumbing artifact.
 */
static void
CopyLoadInputBuf(CopyFromState cstate)
{
	int			nbytes = INPUT_BUF_BYTES(cstate);

	cstate->raw_buf_index = cstate->input_buf_index;	/* no-transcode alias */
	for (;;)
	{
		CopyConvertBuf(cstate);
		if (INPUT_BUF_BYTES(cstate) > nbytes)
			return;
		if (cstate->input_reached_error)
			CopyConversionError(cstate);	/* throws ENCODING */
		if (cstate->input_reached_eof)
			return;
		/* CopyLoadRawBuf would refill, but the whole stream is preloaded and
		 * raw_reached_eof is true, so this point is never reached. */
		cstate->raw_reached_eof = true;
	}
}

/* CopyGetData: referenced only by CopyReadLine's COPY_FRONTEND drain branch,
 * which is DEAD here (copy_src is always COPY_FILE). Present so that verbatim
 * branch links. */
static int
CopyGetData(CopyFromState cstate, void *databuf, int minread, int maxread)
{
	(void) cstate;
	(void) databuf;
	(void) minread;
	(void) maxread;
	return 0;
}

/* ============================================================= */
/* VERBATIM vendored bodies (copyfromparse.c @ 62d6c7d3df) below. */
/* ============================================================= */

/* --- copyfromparse.c 700..742 CopyReadBinaryData (VERBATIM) --- */
static int
CopyReadBinaryData(CopyFromState cstate, char *dest, int nbytes)
{
	int			copied_bytes = 0;

	if (RAW_BUF_BYTES(cstate) >= nbytes)
	{
		/* Enough bytes are present in the buffer. */
		memcpy(dest, cstate->raw_buf + cstate->raw_buf_index, nbytes);
		cstate->raw_buf_index += nbytes;
		copied_bytes = nbytes;
	}
	else
	{
		/*
		 * Not enough bytes in the buffer, so must read from the file.  Need
		 * to loop since 'nbytes' could be larger than the buffer size.
		 */
		do
		{
			int			copy_bytes;

			/* Load more data if buffer is empty. */
			if (RAW_BUF_BYTES(cstate) == 0)
			{
				CopyLoadRawBuf(cstate);
				if (cstate->raw_reached_eof)
					break;		/* EOF */
			}

			/* Transfer some bytes. */
			copy_bytes = Min(nbytes - copied_bytes, RAW_BUF_BYTES(cstate));
			memcpy(dest, cstate->raw_buf + cstate->raw_buf_index, copy_bytes);
			cstate->raw_buf_index += copy_bytes;
			dest += copy_bytes;
			copied_bytes += copy_bytes;
		} while (copied_bytes < nbytes);
	}

	return copied_bytes;
}

/* --- copyfromparse.c 361..393 CopyGetInt32/CopyGetInt16 (VERBATIM) --- */
static inline bool
CopyGetInt32(CopyFromState cstate, int32 *val)
{
	uint32		buf;

	if (CopyReadBinaryData(cstate, (char *) &buf, sizeof(buf)) != sizeof(buf))
	{
		*val = 0;				/* suppress compiler warning */
		return false;
	}
	*val = (int32) pg_ntoh32(buf);
	return true;
}

/*
 * CopyGetInt16 reads an int16 that appears in network byte order
 */
static inline bool
CopyGetInt16(CopyFromState cstate, int16 *val)
{
	uint16		buf;

	if (CopyReadBinaryData(cstate, (char *) &buf, sizeof(buf)) != sizeof(buf))
	{
		*val = 0;				/* suppress compiler warning */
		return false;
	}
	*val = (int16) pg_ntoh16(buf);
	return true;
}

/* --- copyfromparse.c 1535..1543 GetDecimalFromHex (VERBATIM) --- */
static int
GetDecimalFromHex(char hex)
{
	if (isdigit((unsigned char) hex))
		return hex - '0';
	else
		return tolower((unsigned char) hex) - 'a' + 10;
}

/* --- copyfromparse.c 1563..1806 CopyReadAttributesText (VERBATIM) --- */
static int
CopyReadAttributesText(CopyFromState cstate)
{
	char		delimc = cstate->opts.delim[0];
	int			fieldno;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool		found_delim = false;
		char	   *start_ptr;
		char	   *end_ptr;
		int			input_len;
		bool		saw_non_ascii = false;

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field.
		 *
		 * Note that in this loop, we are scanning to locate the end of field
		 * and also speculatively performing de-escaping.  Once we find the
		 * end-of-field, we can match the raw field contents against the null
		 * marker string.  Only after that comparison fails do we know that
		 * de-escaping is actually the right thing to do; therefore we *must
		 * not* throw any syntax errors before we've done the null-marker
		 * check.
		 */
		for (;;)
		{
			char		c;

			end_ptr = cur_ptr;
			if (cur_ptr >= line_end_ptr)
				break;
			c = *cur_ptr++;
			if (c == delimc)
			{
				found_delim = true;
				break;
			}
			if (c == '\\')
			{
				if (cur_ptr >= line_end_ptr)
					break;
				c = *cur_ptr++;
				switch (c)
				{
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
						{
							/* handle \013 */
							int			val;

							val = OCTVALUE(c);
							if (cur_ptr < line_end_ptr)
							{
								c = *cur_ptr;
								if (ISOCTAL(c))
								{
									cur_ptr++;
									val = (val << 3) + OCTVALUE(c);
									if (cur_ptr < line_end_ptr)
									{
										c = *cur_ptr;
										if (ISOCTAL(c))
										{
											cur_ptr++;
											val = (val << 3) + OCTVALUE(c);
										}
									}
								}
							}
							c = val & 0377;
							if (c == '\0' || IS_HIGHBIT_SET(c))
								saw_non_ascii = true;
						}
						break;
					case 'x':
						/* Handle \x3F */
						if (cur_ptr < line_end_ptr)
						{
							char		hexchar = *cur_ptr;

							if (isxdigit((unsigned char) hexchar))
							{
								int			val = GetDecimalFromHex(hexchar);

								cur_ptr++;
								if (cur_ptr < line_end_ptr)
								{
									hexchar = *cur_ptr;
									if (isxdigit((unsigned char) hexchar))
									{
										cur_ptr++;
										val = (val << 4) + GetDecimalFromHex(hexchar);
									}
								}
								c = val & 0xff;
								if (c == '\0' || IS_HIGHBIT_SET(c))
									saw_non_ascii = true;
							}
						}
						break;
					case 'b':
						c = '\b';
						break;
					case 'f':
						c = '\f';
						break;
					case 'n':
						c = '\n';
						break;
					case 'r':
						c = '\r';
						break;
					case 't':
						c = '\t';
						break;
					case 'v':
						c = '\v';
						break;

						/*
						 * in all other cases, take the char after '\'
						 * literally
						 */
				}
			}

			/* Add c to output string */
			*output_ptr++ = c;
		}

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (input_len == cstate->opts.null_print_len &&
			strncmp(start_ptr, cstate->opts.null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;
		/* Check whether raw input matched default marker */
		else if (fieldno < list_length(cstate->attnumlist) &&
				 cstate->opts.default_print &&
				 input_len == cstate->opts.default_print_len &&
				 strncmp(start_ptr, cstate->opts.default_print, input_len) == 0)
		{
			/* fieldno is 0-indexed and attnum is 1-indexed */
			int			m = list_nth_int(cstate->attnumlist, fieldno) - 1;

			if (cstate->defexprs[m] != NULL)
			{
				/* defaults contain entries for all physical attributes */
				cstate->defaults[m] = true;
			}
			else
			{
				TupleDesc	tupDesc = RelationGetDescr(cstate->rel);
				Form_pg_attribute att = TupleDescAttr(tupDesc, m);

				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("unexpected default marker in COPY data"),
						 errdetail("Column \"%s\" has no default value.",
								   NameStr(att->attname))));
			}
		}
		else
		{
			/*
			 * At this point we know the field is supposed to contain data.
			 *
			 * If we de-escaped any non-7-bit-ASCII chars, make sure the
			 * resulting string is valid data for the db encoding.
			 */
			if (saw_non_ascii)
			{
				char	   *fld = cstate->raw_fields[fieldno];

				pg_verifymbstr(fld, output_ptr - fld, false);
			}
		}

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
			break;
	}

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}

/* ============================================================= */
/* VERBATIM line/row framing (copyfromparse.c @ 62d6c7d3df) below */
/* — CopyReadLine + CopyReadLineText, the raw-line reader that    */
/* splits COPY input into rows before field parsing (VENDOR-COPYROW). */
/* ============================================================= */

/* --- copyfromparse.c 97..136 loop macros (VERBATIM) --- */
#define IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(extralen) \
if (1) \
{ \
	if (input_buf_ptr + (extralen) >= copy_buf_len && !hit_eof) \
	{ \
		input_buf_ptr = prev_raw_ptr; /* undo fetch */ \
		need_data = true; \
		continue; \
	} \
} else ((void) 0)

#define IF_NEED_REFILL_AND_EOF_BREAK(extralen) \
if (1) \
{ \
	if (input_buf_ptr + (extralen) >= copy_buf_len && hit_eof) \
	{ \
		if (extralen) \
			input_buf_ptr = copy_buf_len; /* consume the partial character */ \
		/* backslash just before EOF, treat as data char */ \
		result = true; \
		break; \
	} \
} else ((void) 0)

#define REFILL_LINEBUF \
if (1) \
{ \
	if (input_buf_ptr > cstate->input_buf_index) \
	{ \
		appendBinaryStringInfo(&cstate->line_buf, \
							 cstate->input_buf + cstate->input_buf_index, \
							   input_buf_ptr - cstate->input_buf_index); \
		cstate->input_buf_index = input_buf_ptr; \
	} \
} else ((void) 0)

static bool CopyReadLineText(CopyFromState cstate, bool is_csv);

/* --- copyfromparse.c 1157..1228 CopyReadLine (VERBATIM) --- */
static bool
CopyReadLine(CopyFromState cstate, bool is_csv)
{
	bool		result;

	resetStringInfo(&cstate->line_buf);
	cstate->line_buf_valid = false;

	/* Parse data and transfer into line_buf */
	result = CopyReadLineText(cstate, is_csv);

	if (result)
	{
		/*
		 * Reached EOF.  In protocol version 3, we should ignore anything
		 * after \. up to the protocol end of copy data.  (XXX maybe better
		 * not to treat \. as special?)
		 */
		if (cstate->copy_src == COPY_FRONTEND)
		{
			int			inbytes;

			do
			{
				inbytes = CopyGetData(cstate, cstate->input_buf,
									  1, INPUT_BUF_SIZE);
			} while (inbytes > 0);
			cstate->input_buf_index = 0;
			cstate->input_buf_len = 0;
			cstate->raw_buf_index = 0;
			cstate->raw_buf_len = 0;
		}
	}
	else
	{
		/*
		 * If we didn't hit EOF, then we must have transferred the EOL marker
		 * to line_buf along with the data.  Get rid of it.
		 */
		switch (cstate->eol_type)
		{
			case EOL_NL:
				Assert(cstate->line_buf.len >= 1);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
				cstate->line_buf.len--;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case EOL_CR:
				Assert(cstate->line_buf.len >= 1);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\r');
				cstate->line_buf.len--;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case EOL_CRNL:
				Assert(cstate->line_buf.len >= 2);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 2] == '\r');
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
				cstate->line_buf.len -= 2;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case EOL_UNKNOWN:
				/* shouldn't get here */
				Assert(false);
				break;
		}
	}

	/* Now it's safe to use the buffer in error messages */
	cstate->line_buf_valid = true;

	return result;
}

/* --- copyfromparse.c 1233..1530 CopyReadLineText (VERBATIM) --- */
static bool
CopyReadLineText(CopyFromState cstate, bool is_csv)
{
	char	   *copy_input_buf;
	int			input_buf_ptr;
	int			copy_buf_len;
	bool		need_data = false;
	bool		hit_eof = false;
	bool		result = false;

	/* CSV variables */
	bool		in_quote = false,
				last_was_esc = false;
	char		quotec = '\0';
	char		escapec = '\0';

	if (is_csv)
	{
		quotec = cstate->opts.quote[0];
		escapec = cstate->opts.escape[0];
		/* ignore special escape processing if it's the same as quotec */
		if (quotec == escapec)
			escapec = '\0';
	}

	/*
	 * The objective of this loop is to transfer the entire next input line
	 * into line_buf.  Hence, we only care for detecting newlines (\r and/or
	 * \n) and the end-of-copy marker (\.).
	 *
	 * In CSV mode, \r and \n inside a quoted field are just part of the data
	 * value and are put in line_buf.  We keep just enough state to know if we
	 * are currently in a quoted field or not.
	 *
	 * The input has already been converted to the database encoding.  All
	 * supported server encodings have the property that all bytes in a
	 * multi-byte sequence have the high bit set, so a multibyte character
	 * cannot contain any newline or escape characters embedded in the
	 * multibyte sequence.  Therefore, we can process the input byte-by-byte,
	 * regardless of the encoding.
	 *
	 * For speed, we try to move data from input_buf to line_buf in chunks
	 * rather than one character at a time.  input_buf_ptr points to the next
	 * character to examine; any characters from input_buf_index to
	 * input_buf_ptr have been determined to be part of the line, but not yet
	 * transferred to line_buf.
	 *
	 * For a little extra speed within the loop, we copy input_buf and
	 * input_buf_len into local variables.
	 */
	copy_input_buf = cstate->input_buf;
	input_buf_ptr = cstate->input_buf_index;
	copy_buf_len = cstate->input_buf_len;

	for (;;)
	{
		int			prev_raw_ptr;
		char		c;

		/*
		 * Load more data if needed.
		 *
		 * TODO: We could just force four bytes of read-ahead and avoid the
		 * many calls to IF_NEED_REFILL_AND_NOT_EOF_CONTINUE().  That was
		 * unsafe with the old v2 COPY protocol, but we don't support that
		 * anymore.
		 */
		if (input_buf_ptr >= copy_buf_len || need_data)
		{
			REFILL_LINEBUF;

			CopyLoadInputBuf(cstate);
			/* update our local variables */
			hit_eof = cstate->input_reached_eof;
			input_buf_ptr = cstate->input_buf_index;
			copy_buf_len = cstate->input_buf_len;

			/*
			 * If we are completely out of data, break out of the loop,
			 * reporting EOF.
			 */
			if (INPUT_BUF_BYTES(cstate) <= 0)
			{
				result = true;
				break;
			}
			need_data = false;
		}

		/* OK to fetch a character */
		prev_raw_ptr = input_buf_ptr;
		c = copy_input_buf[input_buf_ptr++];

		if (is_csv)
		{
			/*
			 * If character is '\r', we may need to look ahead below.  Force
			 * fetch of the next character if we don't already have it.  We
			 * need to do this before changing CSV state, in case '\r' is also
			 * the quote or escape character.
			 */
			if (c == '\r')
			{
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			}

			/*
			 * Dealing with quotes and escapes here is mildly tricky. If the
			 * quote char is also the escape char, there's no problem - we
			 * just use the char as a toggle. If they are different, we need
			 * to ensure that we only take account of an escape inside a
			 * quoted field and immediately preceding a quote char, and not
			 * the second in an escape-escape sequence.
			 */
			if (in_quote && c == escapec)
				last_was_esc = !last_was_esc;
			if (c == quotec && !last_was_esc)
				in_quote = !in_quote;
			if (c != escapec)
				last_was_esc = false;

			/*
			 * Updating the line count for embedded CR and/or LF chars is
			 * necessarily a little fragile - this test is probably about the
			 * best we can do.  (XXX it's arguable whether we should do this
			 * at all --- is cur_lineno a physical or logical count?)
			 */
			if (in_quote && c == (cstate->eol_type == EOL_NL ? '\n' : '\r'))
				cstate->cur_lineno++;
		}

		/* Process \r */
		if (c == '\r' && (!is_csv || !in_quote))
		{
			/* Check for \r\n on first line, _and_ handle \r\n. */
			if (cstate->eol_type == EOL_UNKNOWN ||
				cstate->eol_type == EOL_CRNL)
			{
				/*
				 * If need more data, go back to loop top to load it.
				 *
				 * Note that if we are at EOF, c will wind up as '\0' because
				 * of the guaranteed pad of input_buf.
				 */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);

				/* get next char */
				c = copy_input_buf[input_buf_ptr];

				if (c == '\n')
				{
					input_buf_ptr++;	/* eat newline */
					cstate->eol_type = EOL_CRNL;	/* in case not set yet */
				}
				else
				{
					/* found \r, but no \n */
					if (cstate->eol_type == EOL_CRNL)
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 !is_csv ?
								 errmsg("literal carriage return found in data") :
								 errmsg("unquoted carriage return found in data"),
								 !is_csv ?
								 errhint("Use \"\\r\" to represent carriage return.") :
								 errhint("Use quoted CSV field to represent carriage return.")));

					/*
					 * if we got here, it is the first line and we didn't find
					 * \n, so don't consume the peeked character
					 */
					cstate->eol_type = EOL_CR;
				}
			}
			else if (cstate->eol_type == EOL_NL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !is_csv ?
						 errmsg("literal carriage return found in data") :
						 errmsg("unquoted carriage return found in data"),
						 !is_csv ?
						 errhint("Use \"\\r\" to represent carriage return.") :
						 errhint("Use quoted CSV field to represent carriage return.")));
			/* If reach here, we have found the line terminator */
			break;
		}

		/* Process \n */
		if (c == '\n' && (!is_csv || !in_quote))
		{
			if (cstate->eol_type == EOL_CR || cstate->eol_type == EOL_CRNL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !is_csv ?
						 errmsg("literal newline found in data") :
						 errmsg("unquoted newline found in data"),
						 !is_csv ?
						 errhint("Use \"\\n\" to represent newline.") :
						 errhint("Use quoted CSV field to represent newline.")));
			cstate->eol_type = EOL_NL;	/* in case not set yet */
			/* If reach here, we have found the line terminator */
			break;
		}

		/*
		 * Process backslash, except in CSV mode where backslash is a normal
		 * character.
		 */
		if (c == '\\' && !is_csv)
		{
			char		c2;

			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			IF_NEED_REFILL_AND_EOF_BREAK(0);

			/* -----
			 * get next character
			 * Note: we do not change c so if it isn't \., we can fall
			 * through and continue processing.
			 * -----
			 */
			c2 = copy_input_buf[input_buf_ptr];

			if (c2 == '.')
			{
				input_buf_ptr++;	/* consume the '.' */
				if (cstate->eol_type == EOL_CRNL)
				{
					/* Get the next character */
					IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
					/* if hit_eof, c2 will become '\0' */
					c2 = copy_input_buf[input_buf_ptr++];

					if (c2 == '\n')
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 errmsg("end-of-copy marker does not match previous newline style")));
					else if (c2 != '\r')
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 errmsg("end-of-copy marker is not alone on its line")));
				}

				/* Get the next character */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
				/* if hit_eof, c2 will become '\0' */
				c2 = copy_input_buf[input_buf_ptr++];

				if (c2 != '\r' && c2 != '\n')
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("end-of-copy marker is not alone on its line")));

				if ((cstate->eol_type == EOL_NL && c2 != '\n') ||
					(cstate->eol_type == EOL_CRNL && c2 != '\n') ||
					(cstate->eol_type == EOL_CR && c2 != '\r'))
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("end-of-copy marker does not match previous newline style")));

				/*
				 * If there is any data on this line before the \., complain.
				 */
				if (cstate->line_buf.len > 0 ||
					prev_raw_ptr > cstate->input_buf_index)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("end-of-copy marker is not alone on its line")));

				/*
				 * Discard the \. and newline, then report EOF.
				 */
				cstate->input_buf_index = input_buf_ptr;
				result = true;	/* report EOF */
				break;
			}
			else
			{
				/*
				 * If we are here, it means we found a backslash followed by
				 * something other than a period.  In non-CSV mode, anything
				 * after a backslash is special, so we skip over that second
				 * character too.  If we didn't do that \\. would be
				 * considered an eof-of copy, while in non-CSV mode it is a
				 * literal backslash followed by a period.
				 */
				input_buf_ptr++;
			}
		}
	}							/* end of outer loop */

	/*
	 * Transfer any still-uncopied data to line_buf.
	 */
	REFILL_LINEBUF;

	return result;
}

/* ============================================================= */
/* Differential-fuzz entry points (cpf_*) — the ONLY exports.    */
/* ============================================================= */

/*
 * TEXT field split. Runs verbatim CopyReadAttributesText over `line`
 * (line_len bytes) with the given single-byte delimiter and null marker.
 *
 * Output contract mirrors the shipped Rust `fields_of` (copy/src/tests.rs):
 * per field k -> raw_fields[k]==NULL is a SQL NULL (out_isnull[k]=1); else
 * the field is the cstring at raw_fields[k], i.e. bytes up to its first NUL.
 * Field bytes are packed into out_bytes; out_off[k]/out_len[k] index them.
 *
 * Returns the field count on success, or -1 on a rejected input; in both
 * cases *out_errclass is the recorded errcode class (0 == accepted).
 */
int
cpf_copy_read_attrs_text(const char *line, int line_len,
						 char delim, const char *null_print, int null_print_len,
						 int max_fields,
						 int *out_isnull, unsigned char *out_bytes,
						 int out_bytes_cap, int *out_off, int *out_len,
						 int out_fields_cap, int *out_errclass)
{
	CopyFromStateData st;
	char	   *linebuf;
	int			nfields;

	cpf_errcode_val = PG_DIFF_ERR_NONE;
	memset(&st, 0, sizeof(st));

	/* CopyReadAttributesText scans line_buf.data in place; give it a private
	 * NUL-terminated copy (the fmgr line_buf is likewise mutable). */
	linebuf = (char *) malloc((size_t) line_len + 1);
	if (line_len > 0)
		memcpy(linebuf, line, (size_t) line_len);
	linebuf[line_len] = '\0';
	st.line_buf.data = linebuf;
	st.line_buf.len = line_len;
	st.line_buf.maxlen = line_len + 1;

	st.attribute_buf.data = NULL;
	st.attribute_buf.len = 0;
	st.attribute_buf.maxlen = 0;

	st.opts.delim[0] = delim;
	st.opts.delim[1] = '\0';
	st.opts.null_print = null_print;
	st.opts.null_print_len = null_print_len;
	st.opts.default_print = NULL;	/* default-marker branch dead */
	st.opts.default_print_len = 0;
	st.max_fields = max_fields > 0 ? max_fields : 1;
	st.raw_fields = (char **) malloc(sizeof(char *) * (size_t) st.max_fields);
	st.attnumlist = 0;				/* list_length -> 0 : default branch dead */

	if (setjmp(cpf_jb) != 0)
	{
		*out_errclass = cpf_errcode_val;
		free(linebuf);
		free(st.raw_fields);
		free(st.attribute_buf.data);
		return -1;
	}

	nfields = CopyReadAttributesText(&st);
	*out_errclass = PG_DIFF_ERR_NONE;

	{
		int			k;
		int			pos = 0;

		for (k = 0; k < nfields && k < out_fields_cap; k++)
		{
			char	   *f = st.raw_fields[k];

			if (f == NULL)
			{
				out_isnull[k] = 1;
				out_off[k] = 0;
				out_len[k] = 0;
				continue;
			}
			out_isnull[k] = 0;
			{
				int			flen = (int) strlen(f);

				if (pos + flen > out_bytes_cap)
					flen = out_bytes_cap - pos;
				if (flen < 0)
					flen = 0;
				memcpy(out_bytes + pos, f, (size_t) flen);
				out_off[k] = pos;
				out_len[k] = flen;
				pos += flen;
			}
		}
	}

	free(linebuf);
	free(st.raw_fields);
	free(st.attribute_buf.data);
	return nfields;
}

/*
 * BINARY field framing. Preloads the whole `data` stream (len bytes) into
 * raw_buf, then runs the verbatim CopyReadBinaryData / CopyGetInt16 /
 * CopyGetInt32 primitives and the verbatim CopyReadBinaryAttribute fld_size
 * logic (copyfromparse.c 2013..2064: ==-1 NULL sentinel, <0 "invalid field
 * size", enlargeStringInfo(fld_size), CopyReadBinaryData count-match). The
 * ReceiveFunctionCall is CARVED (see file header): we record the raw field
 * image and its NULL flag, exactly the plane the shipped
 * read_binary_attr_data reaches before typreceive.
 *
 * nattrs is the column count the reader expects (mirrors attr_count): the
 * i16 field-count header must equal it (else 22P04), which is how the shipped
 * copy_from_binary_one_row is driven.
 *
 * Returns the field count on success (== nattrs), 0 on the -1 EOF marker, or
 * -1 on a rejected input; *out_errclass carries the errcode class.
 */
int
cpf_copy_read_binary_fields(const char *data, int len, int nattrs,
							int *out_isnull, unsigned char *out_bytes,
							int out_bytes_cap, int *out_off, int *out_len,
							int out_fields_cap, int *out_errclass)
{
	CopyFromStateData st;
	StringInfoData attrbuf;
	int16		fld_count;
	int			i;
	int			pos = 0;

	cpf_errcode_val = PG_DIFF_ERR_NONE;
	memset(&st, 0, sizeof(st));
	memset(&attrbuf, 0, sizeof(attrbuf));

	st.raw_buf = (char *) data;		/* CopyReadBinaryData only reads it */
	st.raw_buf_index = 0;
	st.raw_buf_len = len;
	st.raw_reached_eof = false;
	st.attribute_buf = attrbuf;

	if (setjmp(cpf_jb) != 0)
	{
		*out_errclass = cpf_errcode_val;
		free(st.attribute_buf.data);
		return -1;
	}

	/* CopyFromBinaryOneRow: field-count header (i16, network order). */
	if (!CopyGetInt16(&st, &fld_count))
	{
		/* EOF before any field-count word: the shipped side returns "no
		 * more rows" (Ok(false)); model as accepted, 0 fields. */
		*out_errclass = PG_DIFF_ERR_NONE;
		free(st.attribute_buf.data);
		return 0;
	}
	if (fld_count == -1)
	{
		/* binary EOF marker: nothing may follow. */
		char		dummy;

		if (CopyReadBinaryData(&st, &dummy, 1) > 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("received copy data after EOF marker")));
		*out_errclass = PG_DIFF_ERR_NONE;
		free(st.attribute_buf.data);
		return 0;
	}
	if ((int) fld_count != nattrs)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("row field count is %d, expected %d", fld_count, nattrs)));

	for (i = 0; i < nattrs; i++)
	{
		int32		fld_size;

		/* CopyReadBinaryAttribute (copyfromparse.c 2013..): */
		if (!CopyGetInt32(&st, &fld_size))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unexpected EOF in COPY data")));
		if (fld_size == -1)
		{
			if (i < out_fields_cap)
			{
				out_isnull[i] = 1;
				out_off[i] = 0;
				out_len[i] = 0;
			}
			continue;
		}
		if (fld_size < 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid field size")));

		resetStringInfo(&st.attribute_buf);
		enlargeStringInfo(&st.attribute_buf, fld_size);
		if (CopyReadBinaryData(&st, st.attribute_buf.data, fld_size) != fld_size)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unexpected EOF in COPY data")));
		st.attribute_buf.len = fld_size;
		/* ReceiveFunctionCall CARVED here (see header). */

		if (i < out_fields_cap)
		{
			int			flen = fld_size;

			if (pos + flen > out_bytes_cap)
				flen = out_bytes_cap - pos;
			if (flen < 0)
				flen = 0;
			memcpy(out_bytes + pos, st.attribute_buf.data, (size_t) flen);
			out_isnull[i] = 0;
			out_off[i] = pos;
			out_len[i] = flen;
			pos += flen;
		}
	}

	*out_errclass = PG_DIFF_ERR_NONE;
	free(st.attribute_buf.data);
	return nattrs;
}

/*
 * LINE/ROW framing (VENDOR-COPYROW). Preloads the whole `data` stream (len
 * bytes) into input_buf and runs the verbatim CopyReadLine loop the shipped
 * COPY-from loop drives (NextCopyFromRawFields): repeatedly read a line until
 * EOF, splitting rows on \r / \n / \r\n (state-tracked in eol_type), honoring
 * the `\.` end-of-copy marker and the non-CSV backslash-escape skip. Encoding
 * is SQL_ASCII (every byte a valid 1-byte char), mirrored by the shipped
 * no-transcoding path (see CopyLoadInputBuf header).
 *
 * Output contract (mirrors the shipped `parse_lines`): each emitted line's
 * content (EOL stripped, exactly line_buf after CopyReadLine) is packed into
 * out_bytes; out_off[k]/out_len[k] index it. The COPY loop model:
 *   done = CopyReadLine(); if (done && line_buf.len == 0) stop (clean EOF);
 *   emit line; if (done) stop (line just before EOF / after \.).
 * *out_saw_eof records whether the terminating CopyReadLine returned EOF
 * (true for a clean end, a `\.` marker, or a trailing unterminated line;
 * a rejected input never reaches here). Returns the emitted line count, or
 * -1 on a rejected input (with *out_errclass set to the errcode class).
 */
int
cpf_copy_read_lines(const char *data, int len, int is_csv,
					char delim, char quote, char escape,
					unsigned char *out_bytes, int out_bytes_cap,
					int *out_off, int *out_len, int out_lines_cap,
					int *out_errclass, int *out_saw_eof)
{
	CopyFromStateData st;
	char	   *inbuf;
	int			nlines = 0;
	int			pos = 0;

	cpf_errcode_val = PG_DIFF_ERR_NONE;
	memset(&st, 0, sizeof(st));
	*out_saw_eof = 0;

	/* Preload whole stream into one buffer that serves as BOTH raw_buf and
	 * input_buf (no-transcoding alias), with the guaranteed NUL pad at
	 * [raw_buf_len] (copyfrom_internal.h: input_buf palloc'd INPUT_BUF_SIZE+1,
	 * NUL-padded). CopyLoadInputBuf reveals bytes up to the first embedded NUL
	 * (SQL_ASCII verify), then EOF; an embedded NUL is rejected (encoding). */
	inbuf = (char *) malloc((size_t) len + 1);
	if (len > 0)
		memcpy(inbuf, data, (size_t) len);
	inbuf[len] = '\0';
	st.input_buf = inbuf;
	st.raw_buf = inbuf;				/* alias (no transcoding) */
	st.raw_buf_index = 0;
	st.raw_buf_len = len;
	st.raw_reached_eof = true;		/* whole stream preloaded */
	st.input_buf_index = 0;
	st.input_buf_len = 0;			/* nothing "verified" yet */
	st.input_reached_eof = false;
	st.input_reached_error = false;
	st.srclen = len;
	st.eol_type = EOL_UNKNOWN;
	st.cur_lineno = 0;
	st.copy_src = COPY_FILE;		/* frontend-drain branch dead */

	/* line_buf grows via appendBinaryStringInfo/enlargeStringInfo. */
	st.line_buf.data = NULL;
	st.line_buf.len = 0;
	st.line_buf.maxlen = 0;

	st.opts.delim[0] = delim;
	st.opts.delim[1] = '\0';
	st.opts.quote[0] = quote;
	st.opts.quote[1] = '\0';
	st.opts.escape[0] = escape;
	st.opts.escape[1] = '\0';

	if (setjmp(cpf_jb) != 0)
	{
		*out_errclass = cpf_errcode_val;
		free(st.line_buf.data);
		free(inbuf);
		return -1;
	}

	for (;;)
	{
		bool		done = CopyReadLine(&st, is_csv != 0);

		if (done && st.line_buf.len == 0)
		{
			/* clean end of input (or a bare `\.` marker on its own line). */
			*out_saw_eof = 1;
			break;
		}

		/* emit this line's content (EOL already stripped by CopyReadLine). */
		if (nlines < out_lines_cap)
		{
			int			llen = st.line_buf.len;

			if (pos + llen > out_bytes_cap)
				llen = out_bytes_cap - pos;
			if (llen < 0)
				llen = 0;
			memcpy(out_bytes + pos, st.line_buf.data, (size_t) llen);
			out_off[nlines] = pos;
			out_len[nlines] = llen;
			pos += llen;
		}
		nlines++;

		if (done)
		{
			/* the emitted line was the last (unterminated tail / pre-\. ). */
			*out_saw_eof = 1;
			break;
		}
	}

	*out_errclass = PG_DIFF_ERR_NONE;
	free(st.line_buf.data);
	free(inbuf);
	return nlines;
}
