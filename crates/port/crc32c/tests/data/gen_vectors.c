/* Emits Rust parity vectors by running the real vendored C, both paths.
 * Regenerate (ARM64 host with FEAT_CRC32):
 *   clang -O2 -I tests/data/cref -o gen tests/data/gen_vectors.c \
 *     ../../../../pgrust-fabled/vendor/postgres-src/src/port/pg_crc32c_sb8.c \
 *     ../../../../pgrust-fabled/vendor/postgres-src/src/port/pg_crc32c_armv8.c \
 *   && ./gen > tests/data/c_vectors.rs
 * Exits nonzero if the C sb8 and armv8 paths ever disagree. */
#include <stdio.h>
#include <stdlib.h>
#include "c.h"
#include "port/pg_crc32c.h"

static uint64_t prng = 0x243f6a8885a308d3ULL;
static unsigned char next_byte(void)
{
	prng = prng * 6364136223846793005ULL + 1442695040888963407ULL;
	return (unsigned char) (prng >> 56);
}

int main(void)
{
	static unsigned char buf[8256] __attribute__((aligned(16)));
	static const int lens[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
		14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
		31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
		48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
		73, 90, 106, 122, 250, 256, 511, 512, 1024, 4095, 4096, 8192};
	static const uint32 seeds[] = {0xFFFFFFFF, 0xDEADBEEF};
	int nlens = sizeof(lens) / sizeof(lens[0]);

	for (size_t i = 0; i < sizeof(buf); i++)
		buf[i] = next_byte();

	printf("// Generated from vendored pg_crc32c_sb8.c + pg_crc32c_armv8.c. Do not edit.\n");
	printf("// Generator: tests/data/gen_vectors.c (both C paths asserted equal).\n");
	printf("pub static COMP_CRC32C: &[(usize, usize, u32, u32)] = &[\n");
	for (int li = 0; li < nlens; li++)
		for (int off = 0; off < 8; off++)
			for (int si = 0; si < 2; si++)
			{
				uint32 s = pg_comp_crc32c_sb8(seeds[si], buf + off, lens[li]);
				uint32 h = pg_comp_crc32c_armv8(seeds[si], buf + off, lens[li]);

				if (s != h)
				{
					fprintf(stderr, "sb8/armv8 mismatch len=%d off=%d seed=%08x\n",
							lens[li], off, seeds[si]);
					return 1;
				}
				printf("    (%d, %d, 0x%08x, 0x%08x),\n", lens[li], off, seeds[si], s);
			}
	printf("];\n");
	return 0;
}
