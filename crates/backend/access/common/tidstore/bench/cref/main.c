/*
 * Paired with ../rig/src/main.rs — identical lane shapes, LCG, offsets,
 * barrier. Vendored verbatim tidstore.c over real PG headers + verbatim
 * mcxt/aset/slab/bump.
 */
#include "postgres.h"

#include "access/tidstore.h"
#include "storage/itemptr.h"
#include "utils/memutils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define NBLOCKS 65536
#define NOFFS_BITMAP 20
#define NOFFS_INLINE 2

static OffsetNumber bitmap_offs[NOFFS_BITMAP];
static const OffsetNumber inline_offs[NOFFS_INLINE] = {4, 200};

static inline uint64 lcg(uint64 *s)
{
	*s = *s * 6364136223846793005ULL + 1442695040888963407ULL;
	return *s;
}

#define BAR(v) __asm__ __volatile__("" : "+r"(v))

static double now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1e9 + ts.tv_nsec;
}

static double bench_set(uint64 iters, const OffsetNumber *offs, int noffs)
{
	TidStore   *ts = NULL;
	uint64		acc = 0;
	double		t0 = now_ns();

	for (uint64 i = 0; i < iters; i++)
	{
		uint64		b = i & (NBLOCKS - 1);

		if (b == 0)
		{
			if (ts)
				TidStoreDestroy(ts);
			ts = TidStoreCreateLocal((size_t) 256 * 1024 * 1024, true);
		}
		TidStoreSetBlockOffsets(ts, (BlockNumber) b, (OffsetNumber *) offs, noffs);
		acc += b;
		BAR(acc);
	}
	double		t1 = now_ns();

	if (ts)
		TidStoreDestroy(ts);
	return (t1 - t0) / (double) iters;
}

static TidStore *build_store(void)
{
	TidStore   *ts = TidStoreCreateLocal((size_t) 256 * 1024 * 1024, true);

	for (uint32 b = 0; b < NBLOCKS; b++)
		TidStoreSetBlockOffsets(ts, b, bitmap_offs, NOFFS_BITMAP);
	return ts;
}

static double bench_member(uint64 iters, bool hit)
{
	TidStore   *ts = build_store();
	uint64		s = 0x243F6A8885A308D3ULL;
	uint64		acc = 0;
	double		t0 = now_ns();

	for (uint64 i = 0; i < iters; i++)
	{
		uint64		r = lcg(&s);
		BlockNumber blk = (r >> 33) & (NBLOCKS - 1);
		OffsetNumber off = (OffsetNumber) (3 + 14 * ((r >> 13) % NOFFS_BITMAP) + (hit ? 0 : 1));
		ItemPointerData tid;

		ItemPointerSet(&tid, blk, off);
		acc ^= (uint64) TidStoreIsMember(ts, &tid);
		BAR(acc);
	}
	double		t1 = now_ns();

	TidStoreDestroy(ts);
	return (t1 - t0) / (double) iters;
}

static double bench_iterate(uint64 iters)
{
	TidStore   *ts = build_store();
	uint64		rounds = iters / NBLOCKS;
	uint64		acc = 0;
	OffsetNumber buf[512];

	if (rounds == 0)
		rounds = 1;
	double		t0 = now_ns();

	for (uint64 r = 0; r < rounds; r++)
	{
		TidStoreIter *it = TidStoreBeginIterate(ts);
		TidStoreIterResult *res;

		while ((res = TidStoreIterateNext(it)) != NULL)
		{
			int			n = TidStoreGetBlockOffsets(res, buf, lengthof(buf));

			acc ^= (uint64) res->blkno + (uint64) n;
			BAR(acc);
		}
		TidStoreEndIterate(it);
	}
	double		t1 = now_ns();

	TidStoreDestroy(ts);
	return (t1 - t0) / (double) (rounds * NBLOCKS);
}

int
main(int argc, char **argv)
{
	const char *name = argc > 1 ? argv[1] : "tidstore_set_dense";
	uint64		iters = argc > 2 ? strtoull(argv[2], NULL, 10) : 3000000;
	int			reps = argc > 3 ? atoi(argv[3]) : 5;
	double		best = 1e300;

	MemoryContextInit();

	for (int j = 0; j < NOFFS_BITMAP; j++)
		bitmap_offs[j] = (OffsetNumber) (3 + 14 * j);

	double		(*f) (uint64) = NULL;
	double		(*fs) (uint64, const OffsetNumber *, int) = NULL;
	const OffsetNumber *offs = NULL;
	int			noffs = 0;
	bool		hit = false;
	double		(*fm) (uint64, bool) = NULL;

	if (strcmp(name, "tidstore_set_dense") == 0)
	{
		fs = bench_set;
		offs = bitmap_offs;
		noffs = NOFFS_BITMAP;
	}
	else if (strcmp(name, "tidstore_set_inline") == 0)
	{
		fs = bench_set;
		offs = inline_offs;
		noffs = NOFFS_INLINE;
	}
	else if (strcmp(name, "tidstore_member_hit") == 0)
	{
		fm = bench_member;
		hit = true;
	}
	else if (strcmp(name, "tidstore_member_miss") == 0)
	{
		fm = bench_member;
		hit = false;
	}
	else if (strcmp(name, "tidstore_iterate") == 0)
		f = bench_iterate;
	else
	{
		fprintf(stderr, "unknown bench %s\n", name);
		return 1;
	}

#define RUN(n) (fs ? fs((n), offs, noffs) : fm ? fm((n), hit) : f(n))
	if (reps > 1)
		RUN(iters / 10);
	for (int r = 0; r < reps; r++)
	{
		double		v = RUN(iters);

		if (v < best)
			best = v;
	}
	printf("%s\t%.4f\n", name, best);
	return 0;
}
