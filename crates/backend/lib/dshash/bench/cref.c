/*
 * C reference for the dshash benches: PostgreSQL 18.3
 * src/backend/lib/dshash.c lifted verbatim in structure, with (a) the lwlock
 * fast path from bench/cref/lwlock_ref.c (LWLockAttemptLock / Acquire /
 * Release, generic-gcc atomics, wait path aborts — benches are uncontended)
 * and (b) a dsa identity shim: dsa_pointer = the address, dsa_get_address =
 * cast, dsa_allocate = malloc. The shim REMOVES dsa's real translation/
 * allocation overhead from the C side, so the comparison is conservative for
 * the Rust port (whose thread-native design is exactly this identity).
 * Parameters stay fn pointers (C's real dispatch shape).
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef int32_t int32;

#define BAR(v) __asm__ __volatile__("" : "+r"(v))

/* ---- port/atomics.h (generic-gcc) + lwlock fast path (bench/cref/lwlock_ref.c) ---- */
#define MAX_BACKENDS ((1U << 18) - 1)

#define LW_FLAG_HAS_WAITERS ((uint32) 1 << 31)
#define LW_FLAG_RELEASE_OK ((uint32) 1 << 30)

#define LW_VAL_EXCLUSIVE (MAX_BACKENDS + 1)
#define LW_VAL_SHARED 1

#define LW_LOCK_MASK (MAX_BACKENDS | LW_VAL_EXCLUSIVE)

#define MAX_SIMUL_LWLOCKS 200

typedef struct pg_atomic_uint32
{
	volatile uint32 value;
} pg_atomic_uint32;

static inline uint32
pg_atomic_read_u32(volatile pg_atomic_uint32 *ptr)
{
	return ptr->value;
}

static inline bool
pg_atomic_compare_exchange_u32(volatile pg_atomic_uint32 *ptr,
							   uint32 *expected, uint32 newval)
{
	return __atomic_compare_exchange_n(&ptr->value, expected, newval, false,
									   __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

static inline uint32
pg_atomic_sub_fetch_u32(volatile pg_atomic_uint32 *ptr, int32 sub_)
{
	return __atomic_sub_fetch(&ptr->value, sub_, __ATOMIC_SEQ_CST);
}

typedef struct proclist_head
{
	int			head;
	int			tail;
} proclist_head;

typedef struct LWLock
{
	uint16		tranche;
	pg_atomic_uint32 state;
	proclist_head waiters;
} LWLock;

typedef enum LWLockMode
{
	LW_EXCLUSIVE,
	LW_SHARED,
	LW_WAIT_UNTIL_FREE,
} LWLockMode;

typedef struct LWLockHandle
{
	LWLock	   *lock;
	LWLockMode	mode;
} LWLockHandle;

static int	num_held_lwlocks = 0;
static LWLockHandle held_lwlocks[MAX_SIMUL_LWLOCKS];

volatile uint32 InterruptHoldoffCount = 0;

#define HOLD_INTERRUPTS() (InterruptHoldoffCount++)
#define RESUME_INTERRUPTS() (InterruptHoldoffCount--)

static void
LWLockInitialize(LWLock *lock, int tranche_id)
{
	lock->tranche = (uint16) tranche_id;
	lock->state.value = LW_FLAG_RELEASE_OK;
	lock->waiters.head = -1;
	lock->waiters.tail = -1;
}

static bool
LWLockAttemptLock(LWLock *lock, LWLockMode mode)
{
	uint32		old_state;

	old_state = pg_atomic_read_u32(&lock->state);

	while (true)
	{
		uint32		desired_state;
		bool		lock_free;

		desired_state = old_state;

		if (mode == LW_EXCLUSIVE)
		{
			lock_free = (old_state & LW_LOCK_MASK) == 0;
			if (lock_free)
				desired_state += LW_VAL_EXCLUSIVE;
		}
		else
		{
			lock_free = (old_state & LW_VAL_EXCLUSIVE) == 0;
			if (lock_free)
				desired_state += LW_VAL_SHARED;
		}

		if (pg_atomic_compare_exchange_u32(&lock->state,
										   &old_state, desired_state))
		{
			if (lock_free)
				return false;
			else
				return true;
		}
	}
}

static bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (num_held_lwlocks >= MAX_SIMUL_LWLOCKS)
		abort();

	HOLD_INTERRUPTS();

	for (;;)
	{
		bool		mustwait;

		mustwait = LWLockAttemptLock(lock, mode);

		if (!mustwait)
			break;

		abort();				/* wait path unreachable uncontended */
	}

	held_lwlocks[num_held_lwlocks].lock = lock;
	held_lwlocks[num_held_lwlocks++].mode = mode;

	return true;
}

static void
LWLockWakeup(LWLock *lock)
{
	(void) lock;
	abort();
}

static LWLockMode
LWLockDisownInternal(LWLock *lock)
{
	LWLockMode	mode;
	int			i;

	for (i = num_held_lwlocks; --i >= 0;)
		if (lock == held_lwlocks[i].lock)
			break;

	if (i < 0)
		abort();

	mode = held_lwlocks[i].mode;

	num_held_lwlocks--;
	for (; i < num_held_lwlocks; i++)
		held_lwlocks[i] = held_lwlocks[i + 1];

	return mode;
}

static void
LWLockRelease(LWLock *lock)
{
	LWLockMode	mode = LWLockDisownInternal(lock);
	uint32		oldstate;
	bool		check_waiters;

	if (mode == LW_EXCLUSIVE)
		oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_EXCLUSIVE);
	else
		oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_SHARED);

	check_waiters =
		((oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK))
		 == (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK))
		&& ((oldstate & LW_LOCK_MASK) == 0);

	if (check_waiters)
		LWLockWakeup(lock);

	RESUME_INTERRUPTS();
}

/* ---- dsa identity shim ---- */
typedef uintptr_t dsa_pointer;
typedef struct dsa_area dsa_area;

#define InvalidDsaPointer ((dsa_pointer) 0)
#define DsaPointerIsValid(x) ((x) != 0)

static inline void *
dsa_get_address(dsa_area *area, dsa_pointer p)
{
	(void) area;
	return (void *) p;
}

static inline dsa_pointer
dsa_allocate(dsa_area *area, size_t size)
{
	void	   *p = malloc(size);

	(void) area;
	if (!p)
		abort();
	return (dsa_pointer) p;
}

static inline dsa_pointer
dsa_allocate_zero(dsa_area *area, size_t size)
{
	void	   *p = calloc(1, size);

	(void) area;
	if (!p)
		abort();
	return (dsa_pointer) p;
}

static inline void
dsa_free(dsa_area *area, dsa_pointer p)
{
	(void) area;
	free((void *) p);
}

/* ---- lib/dshash.h + dshash.c (REL_18_3), dsa calls substituted ---- */
typedef uint32 dshash_hash;
typedef dsa_pointer dshash_table_handle;

typedef int (*dshash_compare_function) (const void *a, const void *b,
										size_t size, void *arg);
typedef dshash_hash (*dshash_hash_function) (const void *v, size_t size,
											 void *arg);
typedef void (*dshash_copy_function) (void *dest, const void *src,
									  size_t size, void *arg);

typedef struct dshash_parameters
{
	size_t		key_size;
	size_t		entry_size;
	dshash_compare_function compare_function;
	dshash_hash_function hash_function;
	dshash_copy_function copy_function;
	int			tranche_id;
} dshash_parameters;

typedef struct dshash_table_item
{
	dsa_pointer next;
	dshash_hash hash;
} dshash_table_item;

#define DSHASH_NUM_PARTITIONS_LOG2 7
#define DSHASH_NUM_PARTITIONS (1 << DSHASH_NUM_PARTITIONS_LOG2)
#define DSHASH_MAGIC 0x75ff6a20

typedef struct dshash_partition
{
	LWLock		lock;
	size_t		count;
} dshash_partition;

typedef struct dshash_table_control
{
	dshash_table_handle handle;
	uint32		magic;
	dshash_partition partitions[DSHASH_NUM_PARTITIONS];
	int			lwlock_tranche_id;
	size_t		size_log2;
	dsa_pointer buckets;
} dshash_table_control;

typedef struct dshash_table
{
	dsa_area   *area;
	dshash_parameters params;
	void	   *arg;
	dshash_table_control *control;
	dsa_pointer *buckets;
	size_t		size_log2;
} dshash_table;

#define MAXALIGN(LEN) (((uintptr_t) (LEN) + 7) & ~(uintptr_t) 7)

#define ENTRY_FROM_ITEM(item) \
	((char *)(item) + MAXALIGN(sizeof(dshash_table_item)))
#define ITEM_FROM_ENTRY(entry) \
	((dshash_table_item *)((char *)(entry) - MAXALIGN(sizeof(dshash_table_item))))

#define NUM_SPLITS(size_log2) (size_log2 - DSHASH_NUM_PARTITIONS_LOG2)
#define NUM_BUCKETS(size_log2) (((size_t) 1) << (size_log2))
#define BUCKETS_PER_PARTITION(size_log2) (((size_t) 1) << NUM_SPLITS(size_log2))
#define MAX_COUNT_PER_PARTITION(hash_table)				\
	(BUCKETS_PER_PARTITION(hash_table->size_log2) / 2 + \
	 BUCKETS_PER_PARTITION(hash_table->size_log2) / 4)
#define PARTITION_FOR_HASH(hash) \
	(hash >> ((sizeof(dshash_hash) * 8) - DSHASH_NUM_PARTITIONS_LOG2))
#define BUCKET_INDEX_FOR_HASH_AND_SIZE(hash, size_log2) \
	(hash >> ((sizeof(dshash_hash) * 8) - (size_log2)))
#define PARTITION_FOR_BUCKET_INDEX(bucket_idx, size_log2) \
	((bucket_idx) >> NUM_SPLITS(size_log2))
#define BUCKET_FOR_HASH(hash_table, hash)								\
	(hash_table->buckets[												\
		BUCKET_INDEX_FOR_HASH_AND_SIZE(hash, hash_table->size_log2)])
#define PARTITION_LOCK(hash_table, i) \
	(&(hash_table)->control->partitions[(i)].lock)

static inline dshash_hash
hash_key(dshash_table *hash_table, const void *key)
{
	return hash_table->params.hash_function(key,
											hash_table->params.key_size,
											hash_table->arg);
}

static inline bool
equal_keys(dshash_table *hash_table, const void *a, const void *b)
{
	return hash_table->params.compare_function(a, b,
											   hash_table->params.key_size,
											   hash_table->arg) == 0;
}

static inline void
copy_key(dshash_table *hash_table, void *dest, const void *src)
{
	hash_table->params.copy_function(dest, src,
									 hash_table->params.key_size,
									 hash_table->arg);
}

static inline void
ensure_valid_bucket_pointers(dshash_table *hash_table)
{
	if (hash_table->size_log2 != hash_table->control->size_log2)
	{
		hash_table->buckets = dsa_get_address(hash_table->area,
											  hash_table->control->buckets);
		hash_table->size_log2 = hash_table->control->size_log2;
	}
}

static inline dshash_table_item *
find_in_bucket(dshash_table *hash_table, const void *key,
			   dsa_pointer item_pointer)
{
	while (DsaPointerIsValid(item_pointer))
	{
		dshash_table_item *item;

		item = dsa_get_address(hash_table->area, item_pointer);
		if (equal_keys(hash_table, key, ENTRY_FROM_ITEM(item)))
			return item;
		item_pointer = item->next;
	}
	return NULL;
}

static void
insert_item_into_bucket(dshash_table *hash_table,
						dsa_pointer item_pointer,
						dshash_table_item *item,
						dsa_pointer *bucket)
{
	item->next = *bucket;
	*bucket = item_pointer;
}

static dshash_table_item *
insert_into_bucket(dshash_table *hash_table,
				   const void *key,
				   dsa_pointer *bucket)
{
	dsa_pointer item_pointer;
	dshash_table_item *item;

	item_pointer = dsa_allocate(hash_table->area,
								hash_table->params.entry_size +
								MAXALIGN(sizeof(dshash_table_item)));
	item = dsa_get_address(hash_table->area, item_pointer);
	copy_key(hash_table, ENTRY_FROM_ITEM(item), key);
	insert_item_into_bucket(hash_table, item_pointer, item, bucket);
	return item;
}

static bool
delete_key_from_bucket(dshash_table *hash_table,
					   const void *key,
					   dsa_pointer *bucket_head)
{
	while (DsaPointerIsValid(*bucket_head))
	{
		dshash_table_item *item;

		item = dsa_get_address(hash_table->area, *bucket_head);

		if (equal_keys(hash_table, key, ENTRY_FROM_ITEM(item)))
		{
			dsa_pointer next;

			next = item->next;
			dsa_free(hash_table->area, *bucket_head);
			*bucket_head = next;

			return true;
		}
		bucket_head = &item->next;
	}
	return false;
}

static void resize(dshash_table *hash_table, size_t new_size_log2);

static dshash_table *
dshash_create(dsa_area *area, const dshash_parameters *params, void *arg)
{
	dshash_table *hash_table;
	dsa_pointer control;

	hash_table = malloc(sizeof(dshash_table));

	control = dsa_allocate(area, sizeof(dshash_table_control));

	hash_table->area = area;
	hash_table->params = *params;
	hash_table->arg = arg;
	hash_table->control = dsa_get_address(area, control);
	hash_table->control->handle = control;
	hash_table->control->magic = DSHASH_MAGIC;
	hash_table->control->lwlock_tranche_id = params->tranche_id;

	{
		dshash_partition *partitions = hash_table->control->partitions;
		int			tranche_id = hash_table->control->lwlock_tranche_id;
		int			i;

		for (i = 0; i < DSHASH_NUM_PARTITIONS; ++i)
		{
			LWLockInitialize(&partitions[i].lock, tranche_id);
			partitions[i].count = 0;
		}
	}

	hash_table->control->size_log2 = DSHASH_NUM_PARTITIONS_LOG2;
	hash_table->control->buckets =
		dsa_allocate_zero(area, sizeof(dsa_pointer) * DSHASH_NUM_PARTITIONS);
	hash_table->buckets = dsa_get_address(area, hash_table->control->buckets);
	hash_table->size_log2 = hash_table->control->size_log2;

	return hash_table;
}

static void *
dshash_find(dshash_table *hash_table, const void *key, bool exclusive)
{
	dshash_hash hash;
	size_t		partition;
	dshash_table_item *item;

	hash = hash_key(hash_table, key);
	partition = PARTITION_FOR_HASH(hash);

	LWLockAcquire(PARTITION_LOCK(hash_table, partition),
				  exclusive ? LW_EXCLUSIVE : LW_SHARED);
	ensure_valid_bucket_pointers(hash_table);

	item = find_in_bucket(hash_table, key, BUCKET_FOR_HASH(hash_table, hash));

	if (!item)
	{
		LWLockRelease(PARTITION_LOCK(hash_table, partition));
		return NULL;
	}
	else
		return ENTRY_FROM_ITEM(item);
}

static void *
dshash_find_or_insert(dshash_table *hash_table,
					  const void *key,
					  bool *found)
{
	dshash_hash hash;
	size_t		partition_index;
	dshash_partition *partition;
	dshash_table_item *item;

	hash = hash_key(hash_table, key);
	partition_index = PARTITION_FOR_HASH(hash);
	partition = &hash_table->control->partitions[partition_index];

restart:
	LWLockAcquire(PARTITION_LOCK(hash_table, partition_index),
				  LW_EXCLUSIVE);
	ensure_valid_bucket_pointers(hash_table);

	item = find_in_bucket(hash_table, key, BUCKET_FOR_HASH(hash_table, hash));

	if (item)
		*found = true;
	else
	{
		*found = false;

		if (partition->count > MAX_COUNT_PER_PARTITION(hash_table))
		{
			LWLockRelease(PARTITION_LOCK(hash_table, partition_index));
			resize(hash_table, hash_table->size_log2 + 1);

			goto restart;
		}

		item = insert_into_bucket(hash_table, key,
								  &BUCKET_FOR_HASH(hash_table, hash));
		item->hash = hash;
		++partition->count;
	}

	return ENTRY_FROM_ITEM(item);
}

static bool
dshash_delete_key(dshash_table *hash_table, const void *key)
{
	dshash_hash hash;
	size_t		partition;
	bool		found;

	hash = hash_key(hash_table, key);
	partition = PARTITION_FOR_HASH(hash);

	LWLockAcquire(PARTITION_LOCK(hash_table, partition), LW_EXCLUSIVE);
	ensure_valid_bucket_pointers(hash_table);

	if (delete_key_from_bucket(hash_table, key,
							   &BUCKET_FOR_HASH(hash_table, hash)))
	{
		found = true;
		--hash_table->control->partitions[partition].count;
	}
	else
		found = false;

	LWLockRelease(PARTITION_LOCK(hash_table, partition));

	return found;
}

static void
dshash_release_lock(dshash_table *hash_table, void *entry)
{
	dshash_table_item *item = ITEM_FROM_ENTRY(entry);
	size_t		partition_index = PARTITION_FOR_HASH(item->hash);

	LWLockRelease(PARTITION_LOCK(hash_table, partition_index));
}

static void
resize(dshash_table *hash_table, size_t new_size_log2)
{
	dsa_pointer old_buckets;
	dsa_pointer new_buckets_shared;
	dsa_pointer *new_buckets;
	size_t		size;
	size_t		new_size = ((size_t) 1) << new_size_log2;
	size_t		i;

	for (i = 0; i < DSHASH_NUM_PARTITIONS; ++i)
	{
		LWLockAcquire(PARTITION_LOCK(hash_table, i), LW_EXCLUSIVE);
		if (i == 0 && hash_table->control->size_log2 >= new_size_log2)
		{
			LWLockRelease(PARTITION_LOCK(hash_table, 0));
			return;
		}
	}

	new_buckets_shared =
		dsa_allocate_zero(hash_table->area, sizeof(dsa_pointer) * new_size);
	new_buckets = dsa_get_address(hash_table->area, new_buckets_shared);

	size = ((size_t) 1) << hash_table->control->size_log2;
	for (i = 0; i < size; ++i)
	{
		dsa_pointer item_pointer = hash_table->buckets[i];

		while (DsaPointerIsValid(item_pointer))
		{
			dshash_table_item *item;
			dsa_pointer next_item_pointer;

			item = dsa_get_address(hash_table->area, item_pointer);
			next_item_pointer = item->next;
			insert_item_into_bucket(hash_table, item_pointer, item,
									&new_buckets[BUCKET_INDEX_FOR_HASH_AND_SIZE(item->hash,
																				new_size_log2)]);
			item_pointer = next_item_pointer;
		}
	}

	old_buckets = hash_table->control->buckets;
	hash_table->control->buckets = new_buckets_shared;
	hash_table->control->size_log2 = new_size_log2;
	hash_table->buckets = new_buckets;
	dsa_free(hash_table->area, old_buckets);

	for (i = 0; i < DSHASH_NUM_PARTITIONS; ++i)
		LWLockRelease(PARTITION_LOCK(hash_table, i));
}

/* ---- bench params + lanes (mirror rig/src/main.rs exactly) ---- */
typedef struct bench_entry
{
	uint64		key;
	uint64		value;
} bench_entry;

static dshash_hash
bench_hash(const void *v, size_t size, void *arg)
{
	uint64		k;

	(void) size;
	(void) arg;
	memcpy(&k, v, 8);
	return (dshash_hash) ((k * 0x9E3779B97F4A7C15ull) >> 32);
}

static int
bench_cmp(const void *a, const void *b, size_t size, void *arg)
{
	(void) arg;
	return memcmp(a, b, size);
}

static void
bench_copy(void *dest, const void *src, size_t size, void *arg)
{
	(void) arg;
	memcpy(dest, src, size);
}

static const dshash_parameters bench_params = {
	.key_size = sizeof(uint64),
	.entry_size = sizeof(bench_entry),
	.compare_function = bench_cmp,
	.hash_function = bench_hash,
	.copy_function = bench_copy,
	.tranche_id = 1,
};

#define NKEYS 4096

static dshash_table *
bench_table(void)
{
	dshash_table *t = dshash_create(NULL, &bench_params, NULL);

	for (uint64 k = 0; k < NKEYS; k++)
	{
		bool		found;
		bench_entry *e = dshash_find_or_insert(t, &k, &found);

		e->value = k;
		dshash_release_lock(t, e);
	}
	return t;
}

static double
now_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (double) ts.tv_sec * 1e9 + (double) ts.tv_nsec;
}

#define LCG(s) ((s) = (s) * 6364136223846793005ull + 1442695040888963407ull)

static double
bench_find_hit(uint64 iters, bool exclusive)
{
	dshash_table *t = bench_table();
	uint64		s = 0x243F6A8885A308D3ull;
	uint64		acc = 0;
	double		t0 = now_ns();

	for (uint64 i = 0; i < iters; i++)
	{
		uint64		key = (LCG(s) >> 33) % NKEYS;
		bench_entry *e = dshash_find(t, &key, exclusive);

		acc ^= e->value;
		dshash_release_lock(t, e);
		BAR(acc);
	}
	return (now_ns() - t0) / (double) iters;
}

static double
bench_find_miss(uint64 iters)
{
	dshash_table *t = bench_table();
	uint64		s = 0x243F6A8885A308D3ull;
	uintptr_t	acc = 0;
	double		t0 = now_ns();

	for (uint64 i = 0; i < iters; i++)
	{
		uint64		key = NKEYS + (LCG(s) >> 33) % NKEYS;
		void	   *e = dshash_find(t, &key, false);

		acc ^= (uintptr_t) e;
		BAR(acc);
	}
	return (now_ns() - t0) / (double) iters;
}

static double
bench_fii_hit(uint64 iters)
{
	dshash_table *t = bench_table();
	uint64		s = 0x243F6A8885A308D3ull;
	uint64		acc = 0;
	double		t0 = now_ns();

	for (uint64 i = 0; i < iters; i++)
	{
		uint64		key = (LCG(s) >> 33) % NKEYS;
		bool		found;
		bench_entry *e = dshash_find_or_insert(t, &key, &found);

		acc ^= e->value + found;
		dshash_release_lock(t, e);
		BAR(acc);
	}
	return (now_ns() - t0) / (double) iters;
}

static double
bench_insert_delete(uint64 iters)
{
	dshash_table *t = bench_table();
	uint64		acc = 0;
	double		t0 = now_ns();

	for (uint64 i = 0; i < iters; i++)
	{
		uint64		key = NKEYS + (i & 1023);
		bool		found;
		bench_entry *e = dshash_find_or_insert(t, &key, &found);

		e->value = key;
		acc ^= found;
		dshash_release_lock(t, e);
		acc ^= dshash_delete_key(t, &key);
		BAR(acc);
	}
	return (now_ns() - t0) / (double) iters;
}

int
main(int argc, char **argv)
{
	const char *name = argc > 1 ? argv[1] : "dshash_find_shared_hit";
	uint64		iters = argc > 2 ? strtoull(argv[2], NULL, 10) : 10000000ull;
	int			reps = argc > 3 ? atoi(argv[3]) : 5;
	double		best = 1e300;
	double		(*f1) (uint64) = NULL;

	if (strcmp(name, "dshash_find_shared_hit") == 0)
	{
		if (reps > 1)
			bench_find_hit(iters / 10, false);
		for (int r = 0; r < reps; r++)
		{
			double		v = bench_find_hit(iters, false);

			if (v < best)
				best = v;
		}
	}
	else if (strcmp(name, "dshash_find_excl_hit") == 0)
	{
		if (reps > 1)
			bench_find_hit(iters / 10, true);
		for (int r = 0; r < reps; r++)
		{
			double		v = bench_find_hit(iters, true);

			if (v < best)
				best = v;
		}
	}
	else
	{
		if (strcmp(name, "dshash_find_miss") == 0)
			f1 = bench_find_miss;
		else if (strcmp(name, "dshash_fii_hit") == 0)
			f1 = bench_fii_hit;
		else if (strcmp(name, "dshash_insert_delete") == 0)
			f1 = bench_insert_delete;
		else
		{
			fprintf(stderr, "unknown bench %s\n", name);
			return 1;
		}
		if (reps > 1)
			f1(iters / 10);
		for (int r = 0; r < reps; r++)
		{
			double		v = f1(iters);

			if (v < best)
				best = v;
		}
	}

	printf("%s\t%.4f\n", name, best);
	return 0;
}
