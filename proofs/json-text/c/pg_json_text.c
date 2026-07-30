/*
 * pg_json_text.c — vendored PostgreSQL text-json (json.c, NOT jsonb)
 * scalar builtins for the Kani C≡Rust equivalence suite (proofs/json-text).
 *
 * Provenance (fetched 2026-07-30 from raw.githubusercontent.com):
 *   - src/backend/utils/adt/json.c   @ REL_18_STABLE:
 *       json_out, json_send, json_build_object_noargs, json_build_array_noargs
 *   - src/backend/utils/adt/varlena.c @ REL_18_STABLE:
 *       text_to_cstring, cstring_to_text_with_len
 *   - src/backend/libpq/pqformat.c   @ REL_18_STABLE:
 *       pq_begintypsend, pq_sendtext, pq_endtypsend
 *
 * Function bodies are verbatim. Shims (plumbing only), each documented:
 *   1. fmgr unwrapping: PG_FUNCTION_ARGS -> plain C signatures. Text
 *      arguments ride as (const char *data, int len) pairs — the
 *      post-PG_GETARG_TEXT_PP / VARDATA_ANY+VARSIZE_ANY_EXHDR caller
 *      contract (pre-detoasted plain payloads; toast plane out of proof,
 *      same fence as the bytea-cmp family).
 *   2. palloc / StringInfo -> caller-provided fixed buffers. StringInfoData
 *      is modeled as (char *data, int len) over a caller buffer; the
 *      enlarge path is out of scope because every harness output fits the
 *      fixed cap (a reached overflow would be an OOB failure = harness
 *      defect, never silent).
 *   3. pg_server_to_client -> identity (no conversion). This is verbatim
 *      C behavior when client_encoding == server_encoding (the function
 *      returns its input pointer); the conversion seam is out of proof on
 *      both sides (the Rust harness installs the mbutils seam as None =
 *      same identity).
 *   4. pg_detoast_datum_packed (inside text_to_cstring) -> identity: the
 *      input is already a plain payload per shim 1, so the detoast is the
 *      no-op branch; `tunpacked != t` is then false and the pfree arm is
 *      dead, exactly as in C for a plain input.
 *
 * No logic edits.
 */

#include "../../support/c/pg_proof_shim.h"

/* ---- varlena.c: text_to_cstring tail, verbatim modulo shims 1/2/4 ----
 *
 * C:  tunpacked = pg_detoast_datum_packed(t)  [identity, shim 4]
 *     len = VARSIZE_ANY_EXHDR(tunpacked)      [the `len` parameter, shim 1]
 *     result = palloc(len + 1)                [caller buffer, shim 2]
 *     memcpy(result, VARDATA_ANY(tunpacked), len);
 *     result[len] = '\0';
 *     (tunpacked != t) pfree arm dead under shim 4.
 * Returns len (the produced cstring length, excl NUL).
 */
int
pg_text_to_cstring(const char *vardata, int len, char *result)
{
	int			i;

	for (i = 0; i < len; i++)	/* memcpy(result, VARDATA_ANY(...), len) */
		result[i] = vardata[i];
	result[len] = '\0';
	return len;
}

/*
 * json.c json_out, verbatim modulo shim 1:
 *   PG_RETURN_CSTRING(TextDatumGetCString(txt))
 * TextDatumGetCString == text_to_cstring on the detoasted text.
 */
int
pg_json_out(const char *vardata, int len, char *result)
{
	return pg_text_to_cstring(vardata, len, result);
}

/* ---- varlena.c: cstring_to_text_with_len, verbatim modulo shims 1/2 ----
 * palloc(len + VARHDRSZ) -> caller buffer `out`; SET_VARSIZE writes the
 * 4-byte little-endian varlena header word (len+VARHDRSZ) << 2, matching
 * the target's (aarch64-le, no big-endian arm here) SET_VARSIZE.
 * Returns total image size (VARHDRSZ + len).
 */
static int
pg_cstring_to_text_with_len(const char *s, int len, unsigned char *out)
{
	unsigned int hdr = ((unsigned int) (len + (int) VARHDRSZ)) << 2;
	int			i;

	out[0] = (unsigned char) (hdr & 0xff);
	out[1] = (unsigned char) ((hdr >> 8) & 0xff);
	out[2] = (unsigned char) ((hdr >> 16) & 0xff);
	out[3] = (unsigned char) ((hdr >> 24) & 0xff);
	for (i = 0; i < len; i++)
		out[VARHDRSZ + i] = (unsigned char) s[i];
	return (int) VARHDRSZ + len;
}

/* json.c json_build_object_noargs: PG_RETURN_TEXT_P(cstring_to_text_with_len("{}", 2)) */
int
pg_json_build_object_noargs(unsigned char *out)
{
	return pg_cstring_to_text_with_len("{}", 2, out);
}

/* json.c json_build_array_noargs: PG_RETURN_TEXT_P(cstring_to_text_with_len("[]", 2)) */
int
pg_json_build_array_noargs(unsigned char *out)
{
	return pg_cstring_to_text_with_len("[]", 2, out);
}

/* ---- pqformat.c: pq_begintypsend / pq_sendtext / pq_endtypsend,
 * verbatim modulo shims 2/3 (StringInfo = out buffer + len cursor) ---- */

/*
 * json.c json_send, composed exactly as the C body does:
 *   pq_begintypsend(&buf);                       -> 4 zero bytes reserved
 *   pq_sendtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
 *       pg_server_to_client identity (shim 3) -> appendBinaryStringInfo
 *   PG_RETURN_BYTEA_P(pq_endtypsend(&buf));      -> SET_VARSIZE(buf.len)
 * Returns total bytea image size (VARHDRSZ + len); image in `out`.
 */
int
pg_json_send(const char *vardata, int len, unsigned char *out)
{
	int			buflen = 0;
	int			i;
	unsigned int hdr;

	/* pq_begintypsend: reserve four bytes for the bytea length word */
	out[buflen++] = '\0';
	out[buflen++] = '\0';
	out[buflen++] = '\0';
	out[buflen++] = '\0';

	/* pq_sendtext: p = pg_server_to_client(str, slen) == str (shim 3);
	 * appendBinaryStringInfo(buf, str, slen) */
	for (i = 0; i < len; i++)
		out[buflen++] = (unsigned char) vardata[i];

	/* pq_endtypsend: SET_VARSIZE(result, buf->len) — LE header word */
	hdr = ((unsigned int) buflen) << 2;
	out[0] = (unsigned char) (hdr & 0xff);
	out[1] = (unsigned char) ((hdr >> 8) & 0xff);
	out[2] = (unsigned char) ((hdr >> 16) & 0xff);
	out[3] = (unsigned char) ((hdr >> 24) & 0xff);
	return buflen;
}
