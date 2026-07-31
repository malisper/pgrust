/*
 * pg_diff shims over the VERBATIM vendored PostgreSQL 18.3 crypto/hash
 * family (provenance: postgres-src @ 62d6c7d3df6287f1bd83199c1a746e50d31571a0,
 * REL_18; see shim_fe/postgres_fe.h). NOT PostgreSQL code — plumbing only:
 * every function below marshals fuzz-provided bytes into the unmodified
 * vendored entry points and copies results out. No computation happens here.
 *
 * Error convention: 0 = ok, -1 = the vendored function reported failure
 * (frontend arms report failure only on OOM/engine error, which the
 * comparator treats as a fatal harness error, not a divergence).
 */

#include "postgres_fe.h"

#include "common/base64.h"
#include "common/cryptohash.h"
#include "common/hmac.h"
#include "common/md5.h"
#include "common/scram-common.h"
#include "common/sha1.h"
#include "common/sha2.h"

/* pg_crc.c's SQL bodies, compiled over the shim call frame. */
#include "pg_diff_fmgr_shim.h"
extern Datum crc32_bytea(pg_diff_bytea_frame *fcinfo);
extern Datum crc32c_bytea(pg_diff_bytea_frame *fcinfo);

int
pg_diff_md5_hash(const void *data, size_t len, char *hexsum33)
{
	const char *errstr = NULL;

	return pg_md5_hash(data, len, hexsum33, &errstr) ? 0 : -1;
}

int
pg_diff_md5_binary(const void *data, size_t len, uint8 *out16)
{
	const char *errstr = NULL;

	return pg_md5_binary(data, len, out16, &errstr) ? 0 : -1;
}

int
pg_diff_md5_encrypt(const char *passwd, const uint8 *salt, size_t salt_len,
					char *buf36)
{
	const char *errstr = NULL;

	return pg_md5_encrypt(passwd, salt, salt_len, buf36, &errstr) ? 0 : -1;
}

/*
 * which: 0=SHA1, 1=SHA224, 2=SHA256, 3=SHA384, 4=SHA512 (md5 handled above).
 * The input is fed through pg_cryptohash_update in nchunks pieces whose
 * lengths are chunk_lens[] (summing to len), mirroring the exact update
 * sequence the Rust side performs, so both incremental engines see the
 * same call schedule.
 */
int
pg_diff_sha(int which, const uint8 *data, size_t len,
			const size_t *chunk_lens, int nchunks,
			uint8 *out, size_t *outlen)
{
	pg_cryptohash_type t;
	pg_cryptohash_ctx *ctx;
	size_t		digest_len;
	size_t		off = 0;
	int			i;

	switch (which)
	{
		case 0:
			t = PG_SHA1;
			digest_len = SHA1_DIGEST_LENGTH;
			break;
		case 1:
			t = PG_SHA224;
			digest_len = PG_SHA224_DIGEST_LENGTH;
			break;
		case 2:
			t = PG_SHA256;
			digest_len = PG_SHA256_DIGEST_LENGTH;
			break;
		case 3:
			t = PG_SHA384;
			digest_len = PG_SHA384_DIGEST_LENGTH;
			break;
		case 4:
			t = PG_SHA512;
			digest_len = PG_SHA512_DIGEST_LENGTH;
			break;
		default:
			return -1;
	}
	*outlen = digest_len;

	ctx = pg_cryptohash_create(t);
	if (ctx == NULL)
		return -1;
	if (pg_cryptohash_init(ctx) < 0)
	{
		pg_cryptohash_free(ctx);
		return -1;
	}
	for (i = 0; i < nchunks; i++)
	{
		if (pg_cryptohash_update(ctx, data + off, chunk_lens[i]) < 0)
		{
			pg_cryptohash_free(ctx);
			return -1;
		}
		off += chunk_lens[i];
	}
	if (off != len || pg_cryptohash_final(ctx, out, digest_len) < 0)
	{
		pg_cryptohash_free(ctx);
		return -1;
	}
	pg_cryptohash_free(ctx);
	return 0;
}

/* which: 0=SHA224, 1=SHA256, 2=SHA384, 3=SHA512. Chunking as pg_diff_sha. */
int
pg_diff_hmac(int which, const uint8 *key, size_t keylen,
			 const uint8 *msg, size_t msglen,
			 const size_t *chunk_lens, int nchunks,
			 uint8 *out, size_t *outlen)
{
	static const pg_cryptohash_type types[4] = {PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512};
	static const size_t lens[4] = {PG_SHA224_DIGEST_LENGTH, PG_SHA256_DIGEST_LENGTH,
	PG_SHA384_DIGEST_LENGTH, PG_SHA512_DIGEST_LENGTH};
	pg_hmac_ctx *ctx;
	size_t		off = 0;
	int			i;

	if (which < 0 || which > 3)
		return -1;
	*outlen = lens[which];

	ctx = pg_hmac_create(types[which]);
	if (ctx == NULL)
		return -1;
	if (pg_hmac_init(ctx, key, keylen) < 0)
	{
		pg_hmac_free(ctx);
		return -1;
	}
	for (i = 0; i < nchunks; i++)
	{
		if (pg_hmac_update(ctx, msg + off, chunk_lens[i]) < 0)
		{
			pg_hmac_free(ctx);
			return -1;
		}
		off += chunk_lens[i];
	}
	if (off != msglen || pg_hmac_final(ctx, out, lens[which]) < 0)
	{
		pg_hmac_free(ctx);
		return -1;
	}
	pg_hmac_free(ctx);
	return 0;
}

int
pg_diff_scram_salted_password(const char *password, const uint8 *salt,
							  int saltlen, int iterations, uint8 *out32)
{
	const char *errstr = NULL;

	return scram_SaltedPassword(password, PG_SHA256, SCRAM_SHA_256_KEY_LEN,
								salt, saltlen, iterations, out32, &errstr);
}

int
pg_diff_scram_h(const uint8 *input, uint8 *out32)
{
	const char *errstr = NULL;

	return scram_H(input, PG_SHA256, SCRAM_SHA_256_KEY_LEN, out32, &errstr);
}

int
pg_diff_scram_client_key(const uint8 *salted_password, uint8 *out32)
{
	const char *errstr = NULL;

	return scram_ClientKey(salted_password, PG_SHA256, SCRAM_SHA_256_KEY_LEN,
						   out32, &errstr);
}

int
pg_diff_scram_server_key(const uint8 *salted_password, uint8 *out32)
{
	const char *errstr = NULL;

	return scram_ServerKey(salted_password, PG_SHA256, SCRAM_SHA_256_KEY_LEN,
						   out32, &errstr);
}

/* malloc'd secret string or NULL on vendored failure. */
char *
pg_diff_scram_build_secret(const uint8 *salt, int saltlen, int iterations,
						   const char *password)
{
	const char *errstr = NULL;

	return scram_build_secret(PG_SHA256, SCRAM_SHA_256_KEY_LEN,
							  salt, saltlen, iterations, password, &errstr);
}

long long
pg_diff_crc32_bytea(const void *data, size_t len)
{
	pg_diff_bytea_frame f = {data, len};

	return (long long) crc32_bytea(&f);
}

long long
pg_diff_crc32c_bytea(const void *data, size_t len)
{
	pg_diff_bytea_frame f = {data, len};

	return (long long) crc32c_bytea(&f);
}
