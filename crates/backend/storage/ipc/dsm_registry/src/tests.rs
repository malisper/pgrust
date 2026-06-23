//! Tests for the `dsm_registry.c` port.
//!
//! Seams are global slots and the backend statics are `thread_local`, so these
//! run with `--test-threads=1`. The layout and the three input-validation
//! paths need no seams (they return before any seam call). The
//! registry-orchestration test installs toy shmem/lwlock/dsa/dshash backings and
//! drives the size-mismatch path, which exercises connect + find_or_insert +
//! lock discipline without reaching the real `dsm_create` (whose own substrate
//! seams are out of this unit's scope).

use super::*;

use mcx::MemoryContext;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Once;

/// A leaked `TopMemoryContext`-equivalent handle for tests that need to pass
/// `top_mcx` into `GetNamedDSMSegment`. None of these tests actually allocate
/// through it (they error before `dsm_create`).
fn top_mcx() -> Mcx<'static> {
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("test-top")));
    ctx.mcx()
}

// ------------------------- layout / validation -------------------------

#[test]
fn c_layout_matches_postgres_shapes() {
    // offsetof(DSMRegistryEntry, handle) == 64 (name[64], then handle).
    assert_eq!(core::mem::offset_of!(DSMRegistryEntry, handle), 64);
    // DSMRegistryCtxStruct = dsa_handle(u32) + dshash_table_handle(u64),
    // repr(C) padded to 16.
    assert_eq!(core::mem::size_of::<DSMRegistryCtxStruct>(), 16);
    assert_eq!(DSMRegistryShmemSize(), 16);
    // sizeof(DSMRegistryEntry) = 64 + 4(handle) + pad(4) + 8(size) = 80.
    assert_eq!(core::mem::size_of::<DSMRegistryEntry>(), 80);
}

#[test]
fn validates_name_and_size_like_postgres() {
    let mut found = false;

    let err = GetNamedDSMSegment("", 16, None, &mut found, top_mcx()).unwrap_err();
    assert_eq!(err.message(), "DSM segment name cannot be empty");

    let long = "x".repeat(64);
    let err = GetNamedDSMSegment(&long, 16, None, &mut found, top_mcx()).unwrap_err();
    assert_eq!(err.message(), "DSM segment name too long");

    let err = GetNamedDSMSegment("zero_size", 0, None, &mut found, top_mcx()).unwrap_err();
    assert_eq!(err.message(), "DSM segment size must be nonzero");
}

// ------------------- toy shmem/lwlock/dsa/dshash backing -------------------

#[derive(Clone, Copy, Default)]
struct Entry {
    handle: types_storage::dsm_handle,
    size: usize,
}

#[derive(Default)]
struct World {
    table: HashMap<String, Entry>,
    /// The name currently held under its partition lock.
    held: Option<String>,
    main_lock_depth: i32,
}

thread_local! {
    static WORLD: RefCell<World> = RefCell::new(World::default());
    /// A fabricated shmem control block for the registry ctx.
    static CTX_BLOCK: RefCell<DSMRegistryCtxStruct> =
        const { RefCell::new(DSMRegistryCtxStruct { dsah: 0, dshh: 0 }) };
    /// A fabricated entry slot the dshash "returns" a pointer into.
    static ENTRY_SLOT: RefCell<DSMRegistryEntry> = RefCell::new(DSMRegistryEntry {
        name: [0; DSM_REGISTRY_ENTRY_NAME_LEN],
        handle: 0,
        size: 0,
    });
}

static INSTALL: Once = Once::new();

fn install_seams() {
    INSTALL.call_once(|| {
        shmem::shmem_init_struct::set(|_name, _size| {
            CTX_BLOCK.with(|b| {
                let ptr = b.as_ptr() as *mut u8;
                Ok((ptr, false))
            })
        });

        lwlock::lwlock_acquire_main::set(|offset, _mode| {
            WORLD.with(|w| w.borrow_mut().main_lock_depth += 1);
            Ok(lwlock::MainLWLockGuard::new(offset, true))
        });
        lwlock::lwlock_release_main::set(|_offset| {
            WORLD.with(|w| w.borrow_mut().main_lock_depth -= 1);
            Ok(())
        });

        dsa::dsa_create::set(|_tranche| Ok(0xA000 as *mut types_storage::DsaArea));
        dsa::dsa_attach::set(|_h| Ok(0xA000 as *mut types_storage::DsaArea));
        dsa::dsa_pin::set(|_a| Ok(()));
        dsa::dsa_pin_mapping::set(|_a| Ok(()));
        dsa::dsa_get_handle::set(|_a| 0xA000);

        dshash::dshash_create::set(|_a, _p| Ok(0xB000 as *mut types_storage::DshashTable));
        dshash::dshash_attach::set(|_a, _p, _h| Ok(0xB000 as *mut types_storage::DshashTable));
        dshash::dshash_get_hash_table_handle::set(|_t| 0xB000);

        dshash::dshash_find_or_insert::set(|table, key| {
            // The seam now carries the raw `const void *key` bytes; the registry
            // uses UTF-8 string names, so recover the &str for the test map.
            let key = std::str::from_utf8(key).expect("registry key is UTF-8");
            WORLD.with(|w| {
                let mut w = w.borrow_mut();
                let existed = w.table.contains_key(key);
                if !existed {
                    w.table.insert(key.to_string(), Entry::default());
                }
                w.held = Some(key.to_string());
                // Mirror the stored entry into the fabricated slot and hand back
                // a pointer to it; the registry reads/writes it through the ptr.
                let e = w.table[key];
                ENTRY_SLOT.with(|slot| {
                    let mut slot = slot.borrow_mut();
                    slot.handle = e.handle;
                    slot.size = e.size;
                });
                let ptr = ENTRY_SLOT.with(|slot| slot.as_ptr() as *mut u8);
                Ok(dshash::DshashEntryGuard::new(table, ptr, existed))
            })
        });
        dshash::dshash_release_lock::set(|_t, _e| {
            // Write the slot back into the table (a real dshash mutates in
            // place; our slot is a copy), then drop the held marker.
            WORLD.with(|w| {
                let mut w = w.borrow_mut();
                if let Some(name) = w.held.take() {
                    let slot = ENTRY_SLOT.with(|s| *s.borrow());
                    if let Some(e) = w.table.get_mut(&name) {
                        e.handle = slot.handle;
                        e.size = slot.size;
                    }
                }
            });
        });
    });
}

fn reset_world() {
    WORLD.with(|w| *w.borrow_mut() = World::default());
    CTX_BLOCK.with(|b| *b.borrow_mut() = DSMRegistryCtxStruct { dsah: 0, dshh: 0 });
    DSM_REGISTRY_TABLE.with(|c| c.set(core::ptr::null_mut()));
    DSM_REGISTRY_DSA.with(|c| c.set(core::ptr::null_mut()));
    DSM_REGISTRY_CTX.with(|c| c.set(core::ptr::null_mut()));
}

fn setup() {
    install_seams();
    reset_world();
}

// ------------------------------- tests -------------------------------

#[test]
fn shmem_init_resets_ctx_when_freshly_created() {
    setup();
    CTX_BLOCK.with(|b| {
        let mut b = b.borrow_mut();
        b.dsah = 0xDEAD;
        b.dshh = 0xBEEF;
    });
    DSMRegistryShmemInit().unwrap();
    CTX_BLOCK.with(|b| {
        let b = b.borrow();
        assert_eq!(b.dsah, DSA_HANDLE_INVALID);
        assert_eq!(b.dshh, DSHASH_HANDLE_INVALID);
    });
}

#[test]
fn size_mismatch_errors_and_releases_lock() {
    setup();
    DSMRegistryShmemInit().unwrap();
    // Seed an existing entry with a known size by inserting via find_or_insert,
    // writing the size, then releasing the partition lock.
    WORLD.with(|w| {
        w.borrow_mut().table.insert(
            "seg".to_string(),
            Entry {
                handle: DSM_HANDLE_INVALID,
                size: 4096,
            },
        );
    });

    // Ask with a mismatching size -> error, and the partition + init locks
    // must both be released.
    let mut found = false;
    let err = GetNamedDSMSegment("seg", 8192, None, &mut found, top_mcx()).unwrap_err();
    assert_eq!(
        err.message(),
        "requested DSM segment size does not match size of existing segment"
    );
    WORLD.with(|w| {
        let w = w.borrow();
        assert!(w.held.is_none(), "partition lock leaked");
        assert_eq!(w.main_lock_depth, 0, "init lock leaked");
    });
}

#[test]
fn connect_publishes_handles_and_balances_init_lock() {
    setup();
    DSMRegistryShmemInit().unwrap();
    // A first lookup of a brand-new name: connect (publish dsah/dshh) then
    // insert the entry. We stop before the real dsm_create by checking the
    // post-find_or_insert state through a mismatching second call would error;
    // instead we verify connect happened by re-reading the ctx block.
    WORLD.with(|w| {
        // pre-seed so find_or_insert reports a freshly inserted entry with an
        // invalid handle, which would otherwise drive dsm_create — so we just
        // assert connect side effects after manually invoking init via a
        // size-mismatch on a pre-sized entry.
        w.borrow_mut().table.insert(
            "x".to_string(),
            Entry {
                handle: DSM_HANDLE_INVALID,
                size: 16,
            },
        );
    });
    let mut found = false;
    let _ = GetNamedDSMSegment("x", 32, None, &mut found, top_mcx()).unwrap_err();
    // init_dsm_registry ran and published the handles into the ctx block.
    CTX_BLOCK.with(|b| {
        let b = b.borrow();
        assert_eq!(b.dsah, 0xA000);
        assert_eq!(b.dshh, 0xB000);
    });
    WORLD.with(|w| assert_eq!(w.borrow().main_lock_depth, 0));
}
