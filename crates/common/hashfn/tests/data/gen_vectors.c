/* Emits Rust parity vectors by running the real vendored hashfn.c.
 * Regenerate: clang -O2 -I tests/data/cref -o gen gen_vectors.c \
 *   ../pgrust-fabled/vendor/postgres-src/src/common/hashfn.c && ./gen > c_vectors.rs */
#include <stdio.h>
#include <stdint.h>
#include "postgres.h"
#include "common/hashfn.h"

static uint64_t prng = 0x243f6a8885a308d3ULL;
static unsigned char next_byte(void)
{
	prng = prng * 6364136223846793005ULL + 1442695040888963407ULL;
	return (unsigned char) (prng >> 56);
}

int main(void)
{
	static unsigned char buf[8192] __attribute__((aligned(16)));
	static const uint64 seeds[] = {0, 1, 0x00000000ffffffffULL,
		0xffffffff00000000ULL, 0x0123456789abcdefULL, 0xdeadbeefcafef00dULL};
	static const int lens[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
		37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56,
		57, 58, 59, 60, 61, 62, 63, 64, 100, 255, 256, 1000, 4093, 8000};
	int nlens = sizeof(lens) / sizeof(lens[0]);
	int nseeds = sizeof(seeds) / sizeof(seeds[0]);

	for (size_t i = 0; i < sizeof(buf); i++)
		buf[i] = next_byte();

	printf("// Generated from vendored postgres hashfn.c (little-endian, clang). Do not edit.\n");
	printf("// Generator: scratchpad gen_vectors.c shim-compiles src/common/hashfn.c unmodified.\n");

	printf("pub static HASH_BYTES: &[(usize, usize, u32)] = &[\n");
	for (int li = 0; li < nlens; li++)
		for (int off = 0; off < 8; off++)
			printf("    (%d, %d, 0x%08x),\n", lens[li], off,
				   hash_bytes(buf + off, lens[li]));
	printf("];\n");

	printf("pub static HASH_BYTES_EXTENDED: &[(usize, usize, u64, u64)] = &[\n");
	for (int li = 0; li < nlens; li++)
		for (int off = 0; off < 8; off++)
			for (int si = 0; si < nseeds; si++)
				printf("    (%d, %d, 0x%016llx, 0x%016llx),\n", lens[li], off,
					   (unsigned long long) seeds[si],
					   (unsigned long long) hash_bytes_extended(buf + off, lens[li], seeds[si]));
	printf("];\n");

	static const uint32 u32s[] = {0, 1, 2, 42, 0x7fffffff, 0x80000000, 0xffffffff,
		16384, 2614, 0x01020304, 0x9e3779b9};
	int nu = sizeof(u32s) / sizeof(u32s[0]);
	printf("pub static HASH_BYTES_UINT32: &[(u32, u32)] = &[\n");
	for (int i = 0; i < nu; i++)
		printf("    (0x%08x, 0x%08x),\n", u32s[i], hash_bytes_uint32(u32s[i]));
	printf("];\n");

	printf("pub static HASH_BYTES_UINT32_EXTENDED: &[(u32, u64, u64)] = &[\n");
	for (int i = 0; i < nu; i++)
		for (int si = 0; si < nseeds; si++)
			printf("    (0x%08x, 0x%016llx, 0x%016llx),\n", u32s[i],
				   (unsigned long long) seeds[si],
				   (unsigned long long) hash_bytes_uint32_extended(u32s[i], seeds[si]));
	printf("];\n");

	static const uint64 u64s[] = {0, 1, 0xffffffffffffffffULL, 0x0123456789abcdefULL,
		0x8000000100000001ULL, 0xdeadbeefcafef00dULL, 0x00000000ffffffffULL};
	int n64 = sizeof(u64s) / sizeof(u64s[0]);
	printf("pub static HASH_COMBINE: &[(u32, u32, u32)] = &[\n");
	for (int i = 0; i < nu; i++)
		for (int j = 0; j < nu; j++)
			printf("    (0x%08x, 0x%08x, 0x%08x),\n", u32s[i], u32s[j],
				   hash_combine(u32s[i], u32s[j]));
	printf("];\n");

	printf("pub static HASH_COMBINE64: &[(u64, u64, u64)] = &[\n");
	for (int i = 0; i < n64; i++)
		for (int j = 0; j < n64; j++)
			printf("    (0x%016llx, 0x%016llx, 0x%016llx),\n",
				   (unsigned long long) u64s[i], (unsigned long long) u64s[j],
				   (unsigned long long) hash_combine64(u64s[i], u64s[j]));
	printf("];\n");

	printf("pub static MURMURHASH32: &[(u32, u32)] = &[\n");
	for (int i = 0; i < nu; i++)
		printf("    (0x%08x, 0x%08x),\n", u32s[i], murmurhash32(u32s[i]));
	printf("];\n");

	printf("pub static MURMURHASH64: &[(u64, u64)] = &[\n");
	for (int i = 0; i < n64; i++)
		printf("    (0x%016llx, 0x%016llx),\n", (unsigned long long) u64s[i],
			   (unsigned long long) murmurhash64(u64s[i]));
	printf("];\n");

	printf("pub static ROTATE_HL32: &[(u64, u64)] = &[\n");
	for (int i = 0; i < n64; i++)
		printf("    (0x%016llx, 0x%016llx),\n", (unsigned long long) u64s[i],
			   (unsigned long long) ROTATE_HIGH_AND_LOW_32BITS(u64s[i]));
	printf("];\n");

	/* string_hash / tag_hash over NUL-free prefixes of buf plus embedded NULs */
	static unsigned char sbuf[64];
	for (int i = 0; i < 63; i++)
		sbuf[i] = (unsigned char) (33 + (i * 7) % 90);
	sbuf[20] = '\0';
	sbuf[63] = '\0';
	printf("pub static STRING_HASH: &[(usize, u32)] = &[\n");
	{
		Size ks[] = {1, 2, 4, 8, 16, 21, 32, 64, 128};
		for (size_t i = 0; i < sizeof(ks) / sizeof(ks[0]); i++)
			printf("    (%zu, 0x%08x),\n", ks[i], string_hash(sbuf, ks[i]));
	}
	printf("];\n");
	printf("pub static STRING_HASH_KEY: &[u8; 64] = b\"");
	for (int i = 0; i < 64; i++)
		printf("\\x%02x", sbuf[i]);
	printf("\";\n");

	printf("pub static TAG_HASH: &[(usize, usize, u32)] = &[\n");
	for (int off = 0; off < 8; off++)
		for (int ks = 0; ks <= 24; ks++)
			printf("    (%d, %d, 0x%08x),\n", ks, off, tag_hash(buf + off, (Size) ks));
	printf("];\n");
	return 0;
}
