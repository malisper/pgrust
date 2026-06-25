# Audit: backend-access-transam-subtrans

C source: `src/backend/access/transam/subtrans.c` (PostgreSQL 18.3)
Port: `crates/backend-access-transam-subtrans/src/lib.rs`
Auditor re-derived from C + c2rust-runs/backend-access-transam-subtrans/src/subtrans.rs.

## Function inventory

| C function (line) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `TransactionIdToPage` (61) | `TransactionIdToPage` (103) | MATCH | `xid / (int64) SUBTRANS_XACTS_PER_PAGE`; cast widths identical. |
| `TransactionIdToEntry` macro (66) | `TransactionIdToEntry` (109) | MATCH | `xid % SUBTRANS_XACTS_PER_PAGE`. |
| `SubTransSetParent` (85) | `SubTransSetParent` (165) | MATCH | Bank lock LW_EXCLUSIVE; SimpleLruReadPage(write_ok=true); set-if-changed with `Assert(*ptr==Invalid)`; page_dirty=true; release. Asserts→debug_assert. |
| `SubTransGetParent` (122) | `SubTransGetParent` (208) | MATCH | Xmin Assert (debug); !IsNormal→Invalid early return; ReadPage_ReadOnly (acquires lock); read entry; release bank lock. |
| `SubTransGetTopmostTransaction` (163) | `SubTransGetTopmostTransaction` (241) | MATCH | parentXid/previousXid loop, break on `Precedes(parent, Xmin)`, corruption elog(ERROR) under `!Precedes(parent, previous)`; same message/format. |
| `SUBTRANSShmemBuffers` (201) | `SUBTRANSShmemBuffers` (284) | MATCH | `buffers==0 → AutotuneBuffers(512,1024)` else `Min(Max(16,buffers),MAX_ALLOWED)` = `.max(16).min(SLRU_MAX_ALLOWED_BUFFERS)`. |
| `SUBTRANSShmemSize` (214) | `SUBTRANSShmemSize` (297) | MATCH | `SimpleLruShmemSize(buffers, 0)`. |
| `SUBTRANSShmemInit` (220) | `SUBTRANSShmemInit` (303) | MATCH | autotune publish via SetConfigOption (DYNAMIC_DEFAULT then OVERRIDE retry); SimpleLruInit("subtransaction",buffers,0,"pg_subtrans",SUBTRANS_BUFFER,SUBTRANS_SLRU,NONE,false); PagePrecedes set; SlruPagePrecedesUnitTests. Installs ctl into per-backend thread_local (C file-static). |
| `check_subtrans_buffers` (254) | `check_subtrans_buffers` (351) | MATCH | delegates to `check_slru_buffers("subtransaction_buffers", newval)`; (false,detail)→Err carries GUC_check_errdetail per repo hook contract. |
| `BootStrapSUBTRANS` (270) | `BootStrapSUBTRANS` (374) | MATCH | bank-lock(0); ZeroSUBTRANSPage(0); SimpleLruWritePage; Assert !page_dirty; release. |
| `ZeroSUBTRANSPage` (296) | `ZeroSUBTRANSPage` (400) | MATCH | `SimpleLruZeroPage(ctl, pageno)`. |
| `StartupSUBTRANS` (309) | `StartupSUBTRANS` (410) | MATCH | startPage=ToPage(oldestActive); endPage=ToPage(XidFromFull(nextXid)); for(;;) lock-change release/reacquire (pointer-eq on bank lock), Zero, break at endPage, wraparound `>ToPage(MaxTransactionId)→0`; final release. |
| `CheckPointSUBTRANS` (355) | `CheckPointSUBTRANS` (476) | MATCH | `SimpleLruWriteAll(ctl,true)`. TRACE macros are no-ops (DTrace), correctly omitted. |
| `ExtendSUBTRANS` (379) | `ExtendSUBTRANS` (492) | MATCH | early return unless `Entry==0 || ==FirstNormal`; bank lock; Zero; release. |
| `TruncateSUBTRANS` (411) | `TruncateSUBTRANS` (520) | MATCH | TransactionIdRetreat(oldestXact); cutoff=ToPage; SimpleLruTruncate. |
| `SubTransPagePrecedes` (435) | `SubTransPagePrecedes` (538) | MATCH | xid = page*PER_PAGE + FirstNormal+1 (both operands, wrapping); `Precedes(x1,x2) && Precedes(x1, x2+PER_PAGE-1)`. |
| `TransactionIdRetreat` (transam.h, used by Truncate) | `TransactionIdRetreat` (116) | MATCH | do/while wraparound to skip < FirstNormal. |

All 17 C functions present. No MISSING/PARTIAL/DIVERGES.

## Constants verified against headers

- `SUBTRANS_XACTS_PER_PAGE = BLCKSZ / sizeof(TransactionId)` — matches subtrans.c.
- `LWTRANCHE_SUBTRANS_BUFFER`, `LWTRANCHE_SUBTRANS_SLRU` — sourced from types-storage (verified vs storage/lwlock.h ordering).
- `FirstNormalTransactionId=3`, `MaxTransactionId=0xFFFFFFFF`, `InvalidTransactionId=0` — transam.h.
- `SYNC_HANDLER_NONE` — slru.h; `SimpleLruInit(... SYNC_HANDLER_NONE, false)` matches C.
- `PGC_POSTMASTER`, `PGC_S_DYNAMIC_DEFAULT`, `PGC_S_OVERRIDE` — guc.h.
- `XidFromFullTransactionId(x) = (uint32) x.value` — `nextXid.xid()` matches.
- `SimpleLruAutotuneBuffers(512, 1024)` — args match C call.

## Seam / wiring audit

Owned seam crate (C-source coverage = subtrans.c): `backend-access-transam-subtrans-seams`.
All five declarations installed by `init_seams()`:
- `sub_trans_get_parent`, `sub_trans_set_parent`, `sub_trans_get_topmost_transaction` — thin delegate via `with_ctl` to the per-backend control state (the C file-static `SubTransCtlData`). No branching/computation in the seam path.
- `sub_trans_shmem_size` → `SUBTRANSShmemSize()`; `sub_trans_shmem_init` → `SUBTRANSShmemInit`.
- Also installs GUC slots `vars::subtransaction_buffers` (accessors over the thread_local) and `hooks::check_subtrans_buffers` (the C GUC variable + check hook home).
`init_seams()` contains only `set()`/`install()` calls; wired into `seams-init::init_all()`.

Outward calls (all real cross-subsystem reads, would cycle if direct):
- `snapmgr_pc_seams::transaction_xmin` — `TransactionXmin` (snapmgr backend-global). Used in GetTopmost break condition (load-bearing) + debug asserts. Existing canonical getter seam, not invented.
- `varsup_seams::read_next_full_transaction_id` — `TransamVariables->nextXid` (StartupSUBTRANS).
- `guc_seams::set_config_option` — `SetConfigOption` publish.
SLRU (`SimpleLru*`, bank-lock `LWLockAcquire/Release`) and transam predicates are **direct deps** (acyclic, ported), not seamed — correct.

## Design conformance

- Per-backend C file-statics (`SubTransCtlData`, GUC `int subtransaction_buffers`) → `thread_local` (AGENTS.md backend-global rule). Not shared statics. PASS.
- No ambient-global getter seam introduced for TransactionXmin: reused the pre-existing snapmgr-pc-seams getter (the only canonical home). PASS.
- No invented opacity: `SlruCtlData`/`SubTransState` are real types from the ported SLRU crate. PASS.
- Locks: bank locks acquired/released with bare `LWLockAcquire/LWLockRelease` mirroring slru.c's release/re-acquire protocol (the C contract enters/exits with the lock held and unwinds with locks held to be released by transaction-abort `LWLockReleaseAll`); same DESIGN_DEBT note as the SLRU crate. Acceptable per the inherited contract.
- No allocation on a palloc path: the only `format!` is the GUC buffer text (C builds it with snprintf into a stack buffer — non-palloc). `.unwrap()` is on a 4-byte slice `try_into` (infallible, mirrors the C pointer deref, not an error path). PASS.
- Zero `todo!`/`unimplemented!`. PASS.

## Verdict: PASS

Every function MATCH; seams thin and fully installed; design rules satisfied.
4/4 in-crate arithmetic unit tests pass; `cargo check --workspace` clean.
