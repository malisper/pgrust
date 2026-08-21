//! Unit slice of the M3-H exit gates (the real-backend legs live in
//! `scripts/pgrc2-ddl-e2e.sh` / `scripts/pgrc2-crash-durability-e2e.sh`):
//! refusal identity pins, the pendingDirDeletes partition/reparent laws,
//! the visibility decision, schema derivation (byval hint law), directory
//! path shape, and the ino-reuse invalidation TWO-TEETH test over the
//! shared part registry.

use crate::session::{partition_pending, reparent_pending, PendingDirDelete};

// ---------------------------------------------------------------------------
// Refusal identity (exactness-sweep C12 posture: 0A000 + stable message)
// ---------------------------------------------------------------------------

#[test]
fn refusal_identity_is_pinned() {
    let e = crate::unsupported("TID scans");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(e.message(), "pgrcolumnar2 does not support TID scans");
}

#[test]
fn trickle_refusal_identity_is_pinned() {
    // The restored M3-H ModifyTable gate's error identity (the M5e scout's
    // F-1; wording frozen to the v3 gate @ dc3c67c56214 so error-parity
    // suites see ONE message shape across the lanes): 0A000 always, and the
    // INSERT arm carries the bulk-load hint verbatim.
    use crate::dml::{trickle_unsupported, TrickleOp};
    for (op, msg) in [
        (
            TrickleOp::Insert,
            "pgrcolumnar2 does not support INSERT (bulk-load with COPY; \
             trickle DML arrives with the M5 delta store)",
        ),
        (TrickleOp::Update, "pgrcolumnar2 does not support UPDATE"),
        (TrickleOp::Delete, "pgrcolumnar2 does not support DELETE"),
        (TrickleOp::Merge, "pgrcolumnar2 does not support MERGE"),
    ] {
        let e = trickle_unsupported(op);
        assert_eq!(e.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(e.message(), msg);
    }
}

#[test]
fn write_error_sqlstates_are_class_honest() {
    use pgrc2_write::WriteError as W;
    assert_eq!(
        crate::write_error(W::Refused { what: "x" }).sqlstate(),
        types_error::ERRCODE_FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        crate::write_error(W::ManifestChain { at: "x" }).sqlstate(),
        types_error::ERRCODE_DATA_CORRUPTED
    );
    assert_eq!(
        crate::write_error(W::Io {
            op: "open",
            path: "p".into(),
            errno: 5
        })
        .sqlstate(),
        types_error::ERRCODE_IO_ERROR
    );
}

// ---------------------------------------------------------------------------
// pendingDirDeletes laws (smgrDoPendingDeletes transcription)
// ---------------------------------------------------------------------------

fn e(path: &str, at_commit: bool, nest_level: i32) -> PendingDirDelete {
    PendingDirDelete {
        path: path.to_string(),
        at_commit,
        nest_level,
    }
}

#[test]
fn commit_deletes_dropped_dirs_and_cancels_creations() {
    let pending = vec![e("created", false, 1), e("dropped", true, 1)];
    let (del, keep) = partition_pending(pending, 1, true);
    assert_eq!(del, vec!["dropped".to_string()]);
    assert!(keep.is_empty());
}

#[test]
fn abort_deletes_created_dirs_and_cancels_drops() {
    let pending = vec![e("created", false, 1), e("dropped", true, 1)];
    let (del, keep) = partition_pending(pending, 1, false);
    assert_eq!(del, vec!["created".to_string()]);
    assert!(keep.is_empty());
}

#[test]
fn subabort_only_touches_the_aborting_level() {
    // Savepoint (level 2) created a dir and dropped another; outer level 1
    // has its own pending drop.
    let pending = vec![
        e("outer-drop", true, 1),
        e("sp-created", false, 2),
        e("sp-dropped", true, 2),
    ];
    let (del, keep) = partition_pending(pending, 2, false);
    // Rolled-back savepoint: its creation is unlinked NOW, its drop is
    // cancelled; the outer entry survives untouched.
    assert_eq!(del, vec!["sp-created".to_string()]);
    assert_eq!(keep, vec![e("outer-drop", true, 1)]);
}

#[test]
fn subcommit_reparents_to_the_parent_level() {
    let mut pending = vec![e("outer", true, 1), e("sp", true, 3)];
    reparent_pending(&mut pending, 3);
    assert_eq!(pending, vec![e("outer", true, 1), e("sp", true, 2)]);
    // A later top-level commit then executes both.
    let (del, keep) = partition_pending(pending, 1, true);
    assert_eq!(del, vec!["outer".to_string(), "sp".to_string()]);
    assert!(keep.is_empty());
}

// ---------------------------------------------------------------------------
// Visibility decision (the rg_visible law at manifest grain)
// ---------------------------------------------------------------------------

#[test]
fn visibility_law_is_the_old_am_truth_table() {
    use crate::probe::visible;
    // Own transaction: always visible.
    assert!(visible(true, false, false));
    assert!(visible(true, true, false));
    // Committed and not in snapshot: visible.
    assert!(visible(false, false, true));
    // In snapshot (concurrent at snapshot time): invisible even if now
    // committed.
    assert!(!visible(false, true, true));
    // Uncommitted foreign publisher: invisible.
    assert!(!visible(false, false, false));
}

// ---------------------------------------------------------------------------
// Schema derivation (the byval hint law)
// ---------------------------------------------------------------------------

mod schema_tests {
    use types_tuple::tupdesc::{FormData_pg_attribute, NameData};

    fn att(
        atttypid: u32,
        attlen: i16,
        attbyval: bool,
        attalign: i8,
        attnum: i16,
    ) -> FormData_pg_attribute {
        let mut a = FormData_pg_attribute {
            atttypid,
            attlen,
            attnum,
            attbyval,
            attalign,
            ..Default::default()
        };
        a.attname = NameData { data: [0; 64] };
        a.attname.data[..3].copy_from_slice(b"col");
        a.attname.data[3] = b'0' + (attnum as u8 % 10);
        a
    }

    #[test]
    fn supported_shapes_pass_the_create_gate() {
        let atts = [
            att(23, 4, true, b'i' as i8, 1),     // int4 (signed hint)
            att(700, 4, true, b'i' as i8, 2),    // float4
            att(16, 1, true, b'c' as i8, 3),     // bool
            att(26, 4, true, b'i' as i8, 4),     // oid (unsigned hint)
            att(25, -1, false, b'i' as i8, 5),   // text (varlena)
            att(2950, 16, false, b'c' as i8, 6), // uuid (fixed byref)
        ];
        assert!(crate::schema::check_create_supported("t", &atts).is_ok());
    }

    #[test]
    fn unhinted_byval_type_refuses_at_create() {
        // money (790): byval int8-shaped, deliberately NOT in the hint map
        // (a mis-hinted signedness would corrupt datum words silently).
        let atts = [att(790, 8, true, b'd' as i8, 1)];
        let err = crate::schema::check_create_supported("t", &atts).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert!(err.message().contains("type oid 790"), "{}", err.message());
    }

    #[test]
    fn cstring_typlen_refuses() {
        let atts = [att(2275, -2, false, b'c' as i8, 1)];
        assert!(crate::schema::check_create_supported("t", &atts).is_err());
    }
}

// ---------------------------------------------------------------------------
// Directory path shape (O-7: sibling of the main fork)
// ---------------------------------------------------------------------------

#[test]
fn table_dir_is_a_sibling_of_the_main_fork() {
    use types_storage::storage::RelFileLocator;
    // -1 = INVALID_PROC_NUMBER (non-temp relation).
    let dir = crate::dirpath::table_dir_path(
        RelFileLocator {
            spcOid: 1663,
            dbOid: 5,
            relNumber: 16384,
        },
        -1,
    );
    assert_eq!(dir, "base/5/pgrc2_16384");
}

// ---------------------------------------------------------------------------
// The ino-reuse hole: stale hit without invalidation (tooth 1), correct
// bytes after invalidate_relid (tooth 2) — the M3-F→M3-H seam contract.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Startup recovery wiring (the #480 gap): a pure READER must never observe
// an un-recovered crashed directory. Real-FS tempdirs, the production
// reader choke (`scan::resolve_for_scan`) and production `RealVfs`
// recovery — only the txn probes are test doubles.
// ---------------------------------------------------------------------------

mod startup_recovery {
    use pgrc2_format::manifest::CommitPointer;
    use pgrc2_read::{AllCommitted, TableExpect};
    use pgrc2_write::elect::CandidateSource;
    use pgrc2_write::ingest::{NoExternalDetoast, RawDatum};
    use pgrc2_write::publish::{TxnProbe, TxnVerdict};
    use pgrc2_write::seal::ReferenceResolver;
    use pgrc2_write::shred::NoShred;
    use pgrc2_write::writer::{PartCutPolicy, SealEnv, SubxactEvidence, TableWriter, TxnStamp};
    use pgrc2_write::wvfs::{MemVfs, WriteVfs};

    use crate::inval::RecoveryProbe;
    use crate::scan::resolve_for_scan;

    /// All-committed probe: gen 1's publisher (fxid 42) committed; nothing
    /// is in progress (the restart-simulated posture — the crashed
    /// publisher's xid is dead).
    struct CommittedProbe;
    impl TxnProbe for CommittedProbe {
        fn verdict(&self, _fxid: u64) -> TxnVerdict {
            TxnVerdict::Committed
        }
    }
    impl RecoveryProbe for CommittedProbe {
        fn take_recorded_error(&self) -> types_error::PgResult<()> {
            Ok(())
        }
        fn as_txn_probe(&self) -> &dyn TxnProbe {
            self
        }
    }

    fn expect_none() -> TableExpect {
        TableExpect {
            relfilenumber: None,
            spc_db: None,
            schema_fingerprint: None,
        }
    }

    /// One committed single-column int8 publish (gen 1, fxid 42, value 77)
    /// materialized through the real writer over MemVfs; returns every
    /// (name, bytes) in the table directory.
    fn gen1_files() -> Vec<(String, Vec<u8>)> {
        use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
        let schema = vec![ColSchema {
            attno: 1,
            class: StorageClass::ByvalWord {
                width: 8,
                signed: true,
            },
            typlen: 8,
            typbyval: true,
            typalign: b'd',
            collation_class: CollationClass::C,
            semantics: TypeSemantics::SignedInt,
        }];
        let mut vfs = MemVfs::new();
        vfs.mkdir_path("t").expect("mkdir");
        let mut w = TableWriter::open(
            "t".to_string(),
            schema,
            1663,
            5,
            777,
            TxnStamp { fxid: 42, cid: 1 },
            &SubxactEvidence::default(),
            PartCutPolicy::default(),
        )
        .expect("open");
        let sources: [&dyn CandidateSource; 0] = [];
        let resolver = ReferenceResolver;
        let mut shred = NoShred;
        let opts = pgrc2_format::relopt::ShredOptions::default();
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &resolver,
            shred: &mut shred,
            shred_opts: &opts,
        };
        w.append_row(&[RawDatum::Word(77)], &mut NoExternalDetoast, &mut env)
            .expect("append");
        w.finish(&mut env).expect("finish");
        w.publish(&mut vfs, &CommittedProbe).expect("publish");
        vfs.list_dir("t")
            .expect("list")
            .into_iter()
            .map(|n| {
                let bytes = vfs.read_full(&format!("t/{n}")).expect("read");
                (n, bytes)
            })
            .collect()
    }

    /// Materialize the crashed-table shape on the real filesystem: a
    /// committed gen-1 publish, then `CURRENT` repointed to gen 2 whose
    /// manifest never became durable (the publish-window kill: the
    /// `CURRENT` rename persisted, the `manifest-2` link did not — the #462
    /// pre-clean dangling shape, restart-simulated by a fresh process-global
    /// guard entry).
    fn crashed_dir(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "pgrc2_am_recover_{}_{tag}",
            std::process::id()
        ));
        let dir = dir.to_str().expect("utf8 tempdir").to_string();
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create tempdir");
        for (name, bytes) in gen1_files() {
            std::fs::write(format!("{dir}/{name}"), bytes).expect("write");
        }
        let cp = CommitPointer::new(2, 96, 0xDEAD_BEEF);
        std::fs::write(format!("{dir}/CURRENT"), cp.encode()).expect("dangle CURRENT");
        dir
    }

    /// The hole proof (born RED against the pre-wiring reader: it refused
    /// with `manifest generation 2 missing`): a PURE reader open of a
    /// crashed directory gets the table — recovery ran first, repointed
    /// CURRENT durably, and the strict walk found gen 1.
    #[test]
    fn select_after_crash_gets_the_table() {
        let dir = crashed_dir("hole");
        let eff = resolve_for_scan(&dir, 993_001, &CommittedProbe, &AllCommitted, &expect_none())
            .expect("pure reader open of a crashed table directory")
            .expect("gen 1 is effective");
        assert_eq!(eff.manifest.header.gen, 1);
        assert_eq!(eff.manifest.parts.len(), 1);
        assert_eq!(eff.manifest.parts[0].rows, 1);
        // The reader path RECOVERED the directory (exactly one scan)...
        assert_eq!(crate::inval::recovery_scans(&dir), Some(1));
        // ...and the repoint is on disk: CURRENT names gen 1 again.
        let cur = std::fs::read(format!("{dir}/CURRENT")).expect("CURRENT");
        assert_eq!(CommitPointer::decode(&cur).expect("decode").gen, 1);
    }

    /// The tooth: with recovery suppressed (guard pre-marked), the SAME
    /// directory shape still trips the reader walk's strict refusal —
    /// proving the hole is real and recovery-before-readers is what closes
    /// it (if this passes without the wiring the hole test is toothless).
    #[test]
    fn without_recovery_the_reader_refuses() {
        let dir = crashed_dir("tooth");
        crate::inval::mark_dir_recovered(&dir);
        let err = resolve_for_scan(&dir, 993_002, &CommittedProbe, &AllCommitted, &expect_none())
            .expect_err("un-recovered dangling CURRENT must refuse");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_DATA_CORRUPTED);
        assert!(
            err.message().contains("manifest generation 2 missing"),
            "{}",
            err.message()
        );
        assert_eq!(crate::inval::recovery_scans(&dir), Some(0));
    }

    /// The once-per-lifetime guard: the second open runs ZERO recovery
    /// scans (counter witness) and still answers gen 1.
    #[test]
    fn second_open_runs_zero_recovery_scans() {
        let dir = crashed_dir("once");
        for _ in 0..2 {
            let eff =
                resolve_for_scan(&dir, 993_003, &CommittedProbe, &AllCommitted, &expect_none())
                    .expect("open")
                    .expect("effective");
            assert_eq!(eff.manifest.header.gen, 1);
        }
        assert_eq!(crate::inval::recovery_scans(&dir), Some(1));
    }

    /// Concurrent FIRST opens: every racer gets gen 1, exactly one recovery
    /// scan ran (the publish-lock once-gate; recover_and_clean idempotence
    /// is the #480-proven backstop, the lock means it is never needed).
    #[test]
    fn concurrent_first_opens_recover_once() {
        let dir = crashed_dir("race");
        let mut handles = Vec::new();
        for _ in 0..8 {
            let d = dir.clone();
            handles.push(std::thread::spawn(move || {
                let eff = resolve_for_scan(
                    &d,
                    993_004,
                    &CommittedProbe,
                    &AllCommitted,
                    &expect_none(),
                )
                .expect("racing reader open")
                .expect("effective");
                eff.manifest.header.gen
            }));
        }
        for h in handles {
            assert_eq!(h.join().expect("thread"), 1);
        }
        assert_eq!(crate::inval::recovery_scans(&dir), Some(1));
    }

    /// A never-ingested table (directory absent) is the empty posture: no
    /// scan runs, no error, and the guard caches the verdict.
    #[test]
    fn absent_directory_is_empty_without_a_scan() {
        let dir = std::env::temp_dir()
            .join(format!("pgrc2_am_recover_{}_absent", std::process::id()));
        let dir = dir.to_str().expect("utf8").to_string();
        let _ = std::fs::remove_dir_all(&dir);
        let eff = resolve_for_scan(&dir, 993_005, &CommittedProbe, &AllCommitted, &expect_none())
            .expect("open of a never-created table dir");
        assert!(eff.is_none());
        assert_eq!(crate::inval::recovery_scans(&dir), Some(0));
    }
}

mod ino_reuse {
    use pgrc2_format::abi::{ByteArena, DecodeOut};
    use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
    use pgrc2_read::registry::PartKey;
    use pgrc2_read::{reference_binding_leaked, MemPartIo, PartExpect, StreamCursor};
    use pgrc2_write::elect::CandidateSource;
    use pgrc2_write::ingest::{NoExternalDetoast, RawDatum};
    use pgrc2_write::publish::{TxnProbe, TxnVerdict};
    use pgrc2_write::seal::ReferenceResolver;
    use pgrc2_write::shred::NoShred;
    use pgrc2_write::writer::{PartCutPolicy, SealEnv, SubxactEvidence, TableWriter, TxnStamp};
    use pgrc2_write::wvfs::{MemVfs, WriteVfs};
    use std::sync::Arc;

    /// Distinctive fake device id: parallel tests share the process-global
    /// registry, so this test's identities must be collision-free.
    const DEV: u64 = 0x4D33_4801_0000_0001;
    const INO: u64 = 4242;
    const RELID: u32 = 990_001;

    struct AllCommitted;
    impl TxnProbe for AllCommitted {
        fn verdict(&self, _fxid: u64) -> TxnVerdict {
            TxnVerdict::Committed
        }
    }

    /// Seal one single-column int8 part holding exactly `value`; return the
    /// published part-file bytes.
    fn part_bytes(value: i64) -> Vec<u8> {
        let schema = vec![ColSchema {
            attno: 1,
            class: StorageClass::ByvalWord {
                width: 8,
                signed: true,
            },
            typlen: 8,
            typbyval: true,
            typalign: b'd',
            collation_class: CollationClass::C,
            semantics: TypeSemantics::SignedInt,
        }];
        let mut vfs = MemVfs::new();
        vfs.mkdir_path("t").expect("mkdir");
        let mut w = TableWriter::open(
            "t".to_string(),
            schema,
            1663,
            5,
            777,
            TxnStamp { fxid: 42, cid: 1 },
            &SubxactEvidence::default(),
            PartCutPolicy::default(),
        )
        .expect("open");
        let sources: [&dyn CandidateSource; 0] = [];
        let resolver = ReferenceResolver;
        let mut shred = NoShred;
        let opts = pgrc2_format::relopt::ShredOptions::default();
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &resolver,
            shred: &mut shred,
            shred_opts: &opts,
        };
        w.append_row(
            &[RawDatum::Word(value as u64)],
            &mut NoExternalDetoast,
            &mut env,
        )
        .expect("append");
        w.finish(&mut env).expect("finish");
        let out = w.publish(&mut vfs, &AllCommitted).expect("publish");
        vfs.read_full(&format!(
            "t/{}",
            pgrc2_format::dirlayout::part_file_name(out.part_nos[0])
        ))
        .expect("part bytes")
    }

    fn decode_single(part: &Arc<pgrc2_read::OpenPart>) -> i64 {
        let mut cursor =
            StreamCursor::open(Arc::clone(part), reference_binding_leaked(), 1, 0).expect("cursor");
        let mut datums = [0u64; 1];
        let mut arena = [0u64; 16];
        // SAFETY: u64 → u8 reinterpret of a local buffer, alignment 8.
        let arena_bytes = unsafe {
            core::slice::from_raw_parts_mut(arena.as_mut_ptr() as *mut u8, arena.len() * 8)
        };
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(arena_bytes),
        };
        let rows = cursor.decode_full(0, &mut out).expect("decode");
        assert_eq!(rows, 1);
        datums[0] as i64
    }

    #[test]
    fn ino_reuse_stale_hit_then_invalidate() {
        let a = part_bytes(11);
        let b = part_bytes(22);
        assert_eq!(a.len(), b.len(), "equal-length different-content parts");
        assert_ne!(a, b);

        let reg = crate::inval::registry();

        // Open A under (DEV, INO, len); record its key like a scan does.
        let pin_a = reg
            .open_pinned(&PartExpect::none(), || {
                Ok(Box::new(MemPartIo::new(a.clone(), DEV, INO)) as Box<dyn pgrc2_read::PartIo>)
            })
            .expect("open a");
        let ident = pin_a.part().ident();
        let key: PartKey = (ident.dev, ident.ino, ident.len);
        crate::inval::record_part_key(RELID, key);
        assert_eq!(decode_single(pin_a.part()), 11);
        drop(pin_a);

        // TOOTH 1 (the hole is real): the same (dev, ino, len) now carries
        // B's bytes — without invalidation the registry serves A's stale
        // content.
        let pin_stale = reg
            .open_pinned(&PartExpect::none(), || {
                Ok(Box::new(MemPartIo::new(b.clone(), DEV, INO)) as Box<dyn pgrc2_read::PartIo>)
            })
            .expect("open stale");
        assert_eq!(
            decode_single(pin_stale.part()),
            11,
            "expected the STALE hit — if this fails the hole closed underneath \
             us and this test must be rewritten, not deleted"
        );
        drop(pin_stale);

        // TOOTH 2 (the seam closes it): relcache-style invalidation drops
        // the recorded keys; the next open reads B's bytes.
        crate::inval::invalidate_relid(RELID);
        let pin_fresh = reg
            .open_pinned(&PartExpect::none(), || {
                Ok(Box::new(MemPartIo::new(b.clone(), DEV, INO)) as Box<dyn pgrc2_read::PartIo>)
            })
            .expect("open fresh");
        assert_eq!(decode_single(pin_fresh.part()), 22);
    }
}
