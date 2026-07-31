/*
 * Glue (NOT PostgreSQL code): thin C-linkage exports over the VERBATIM
 * vendored oracle TUs in this directory (see shim/c.h for provenance).
 * Every function here only marshals buffers into the verbatim entry points
 * — no logic. Return convention: 0 = ok, negative = the vendored code's own
 * error return, matching each C API's contract.
 */

#include "postgres.h"

#include "common/base64.h"
#include "common/cryptohash.h"
#include "common/hmac.h"
#include "common/md5.h"
#include "common/scram-common.h"
#include "common/sha1.h"
#include "common/sha2.h"
#include "mb/pg_wchar.h"
#include "port/pg_crc32c.h"

/* errcode capture for the ascii oracle (see pg_hashenc_ascii.c). */
_Thread_local int pg_hashenc_errcode;

int
pg_hashenc_errcode_get(void)
{
	return pg_hashenc_errcode;
}

void
pg_hashenc_errcode_reset(void)
{
	pg_hashenc_errcode = 0;
}

/* ---- base64.c (verbatim TU) ---- */

int
pg_hashenc_b64_encode(const uint8 *src, int len, char *dst, int dstlen)
{
	return pg_b64_encode(src, len, dst, dstlen);
}

int
pg_hashenc_b64_decode(const char *src, int len, uint8 *dst, int dstlen)
{
	return pg_b64_decode(src, len, dst, dstlen);
}

int
pg_hashenc_b64_enc_len(int srclen)
{
	return pg_b64_enc_len(srclen);
}

int
pg_hashenc_b64_dec_len(int srclen)
{
	return pg_b64_dec_len(srclen);
}

/* ---- md5.c + md5_common.c (verbatim TUs; fallback engine, no OpenSSL) ---- */

int
pg_hashenc_md5_hash(const void *buff, size_t len, char *hexsum33)
{
	const char *errstr = NULL;

	return pg_md5_hash(buff, len, hexsum33, &errstr) ? 0 : -1;
}

int
pg_hashenc_md5_binary(const void *buff, size_t len, uint8 *out16)
{
	const char *errstr = NULL;

	return pg_md5_binary(buff, len, out16, &errstr) ? 0 : -1;
}

int
pg_hashenc_md5_encrypt(const char *passwd, const uint8 *salt, size_t salt_len,
					   char *buf36)
{
	const char *errstr = NULL;

	return pg_md5_encrypt(passwd, salt, salt_len, buf36, &errstr) ? 0 : -1;
}

/* ---- cryptohash.c dispatch over md5.c/sha1.c/sha2.c (verbatim TUs) ---- */

int
pg_hashenc_digest(int type, const uint8 *data, size_t len, uint8 *dest,
				  size_t destlen)
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create((pg_cryptohash_type) type);
	int			rc;

	if (ctx == NULL)
		return -1;
	rc = pg_cryptohash_init(ctx);
	if (rc == 0)
		rc = pg_cryptohash_update(ctx, data, len);
	if (rc == 0)
		rc = pg_cryptohash_final(ctx, dest, destlen);
	pg_cryptohash_free(ctx);
	return rc;
}

/*
 * Split-update plane: same digest, data fed in two chunks (exercises the
 * verbatim incremental buffering arms on both sides).
 */
int
pg_hashenc_digest_split(int type, const uint8 *data, size_t len, size_t split,
						uint8 *dest, size_t destlen)
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create((pg_cryptohash_type) type);
	int			rc;

	if (ctx == NULL)
		return -1;
	if (split > len)
		split = len;
	rc = pg_cryptohash_init(ctx);
	if (rc == 0)
		rc = pg_cryptohash_update(ctx, data, split);
	if (rc == 0)
		rc = pg_cryptohash_update(ctx, data + split, len - split);
	if (rc == 0)
		rc = pg_cryptohash_final(ctx, dest, destlen);
	pg_cryptohash_free(ctx);
	return rc;
}

/* ---- hmac.c (verbatim TU) ---- */

int
pg_hashenc_hmac(int type, const uint8 *key, size_t keylen,
				const uint8 *data, size_t datalen, uint8 *dest, size_t destlen)
{
	pg_hmac_ctx *ctx = pg_hmac_create((pg_cryptohash_type) type);
	int			rc;

	if (ctx == NULL)
		return -1;
	rc = pg_hmac_init(ctx, key, keylen);
	if (rc == 0)
		rc = pg_hmac_update(ctx, data, datalen);
	if (rc == 0)
		rc = pg_hmac_final(ctx, dest, destlen);
	pg_hmac_free(ctx);
	return rc;
}

/* ---- scram-common.c (verbatim TU, FRONTEND build) ---- */

int
pg_hashenc_scram_salted_password(const char *password, const uint8 *salt,
								 int saltlen, int iterations, uint8 *out32)
{
	const char *errstr = NULL;

	return scram_SaltedPassword(password, PG_SHA256, PG_SHA256_DIGEST_LENGTH,
								salt, saltlen, iterations, out32, &errstr);
}

int
pg_hashenc_scram_h(const uint8 *input, uint8 *out32)
{
	const char *errstr = NULL;

	return scram_H(input, PG_SHA256, PG_SHA256_DIGEST_LENGTH, out32, &errstr);
}

int
pg_hashenc_scram_client_key(const uint8 *salted, uint8 *out32)
{
	const char *errstr = NULL;

	return scram_ClientKey(salted, PG_SHA256, PG_SHA256_DIGEST_LENGTH, out32,
						   &errstr);
}

int
pg_hashenc_scram_server_key(const uint8 *salted, uint8 *out32)
{
	const char *errstr = NULL;

	return scram_ServerKey(salted, PG_SHA256, PG_SHA256_DIGEST_LENGTH, out32,
						   &errstr);
}

/* Result is malloc'd (FRONTEND arm); caller frees via pg_hashenc_free. */
char *
pg_hashenc_scram_build_secret(const uint8 *salt, int saltlen, int iterations,
							  const char *password)
{
	const char *errstr = NULL;

	return scram_build_secret(PG_SHA256, PG_SHA256_DIGEST_LENGTH, salt,
							  saltlen, iterations, password, &errstr);
}

void
pg_hashenc_free(void *p)
{
	free(p);
}

/* ---- ascii.c cores (verbatim, pg_hashenc_ascii.c) ---- */

extern void pg_to_ascii(unsigned char *src, unsigned char *src_end,
						unsigned char *dest, int enc);
extern void ascii_safe_strlcpy(char *dest, const char *src, size_t destsiz);

/* Returns 0 ok; 1 = ERRCODE_FEATURE_NOT_SUPPORTED (unsupported encoding). */
int
pg_hashenc_to_ascii(const unsigned char *src, size_t len, unsigned char *dest,
					int enc)
{
	pg_hashenc_errcode = 0;
	pg_to_ascii((unsigned char *) src, (unsigned char *) src + len, dest, enc);
	return pg_hashenc_errcode;
}

int
pg_hashenc_valid_encoding(int enc)
{
	return PG_VALID_ENCODING(enc) ? 1 : 0;
}

void
pg_hashenc_ascii_safe_strlcpy(char *dest, const char *src, size_t destsiz)
{
	ascii_safe_strlcpy(dest, src, destsiz);
}

/* ---- pg_crc.c SQL wrappers (verbatim TU) over pg_crc32c_sb8.c ---- */

extern Datum crc32_bytea(PG_FUNCTION_ARGS);
extern Datum crc32c_bytea(PG_FUNCTION_ARGS);

/*
 * Build a 4-byte-header bytea around the payload and call the verbatim SQL
 * wrapper. varatt.h 4-byte header on little-endian: len<<2, low bits 00.
 */
int64_t
pg_hashenc_crc32_bytea(const uint8 *data, size_t len)
{
	size_t		total = len + 4;
	bytea	   *b = malloc(total);
	pg_shim_FunctionCallInfoBaseData fc;
	Datum		d;
	uint32		hdr = (uint32) (total << 2);

	memcpy(b->vl_len_, &hdr, 4);
	memcpy(b->vl_dat, data, len);
	fc.arg0 = b;
	d = crc32_bytea(&fc);
	free(b);
	return (int64_t) d;
}

int64_t
pg_hashenc_crc32c_bytea(const uint8 *data, size_t len)
{
	size_t		total = len + 4;
	bytea	   *b = malloc(total);
	pg_shim_FunctionCallInfoBaseData fc;
	Datum		d;
	uint32		hdr = (uint32) (total << 2);

	memcpy(b->vl_len_, &hdr, 4);
	memcpy(b->vl_dat, data, len);
	fc.arg0 = b;
	d = crc32c_bytea(&fc);
	free(b);
	return (int64_t) d;
}
