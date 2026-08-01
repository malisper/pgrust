/* SHIM (trgmrxfam): spinlocks appear only in dynahash's partitioned
 * (shared-memory) paths, never requested here. */
#ifndef TRGMRX_SPIN_H
#define TRGMRX_SPIN_H
typedef int slock_t;
#define SpinLockInit(lock) (*(lock) = 0)
#define SpinLockAcquire(lock) (*(lock) = 1)
#define SpinLockRelease(lock) (*(lock) = 0)
#endif
