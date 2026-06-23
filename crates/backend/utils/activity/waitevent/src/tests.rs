//! Tests for the store-independent logic: the class/type taxonomy, the
//! built-in name lookup and `wait_event_funcs_data` table (with its golden
//! comparison), and the wait-event reporting storage redirect.
//!
//! The custom-wait-event store is backed by real shared-memory dynahash tables
//! and exercised by the shmem/dynahash integration suites; it is not covered
//! here because it needs a live shared-memory segment.

use super::*;
use types_pgstat::wait_event::WAIT_EVENT_SPIN_DELAY;

#[test]
fn wait_event_type_matches_postgres_classes() {
    assert_eq!(pgstat_get_wait_event_type(0), None);
    assert_eq!(
        pgstat_get_wait_event_type(PG_WAIT_TIMEOUT | 6),
        Some("Timeout")
    );
    assert_eq!(pgstat_get_wait_event_type(0x12000000), Some("???"));
}

#[test]
fn builtin_wait_event_names_are_loaded_from_postgres_data() {
    // SpinDelay is the 7th (0-based id 6) member of the case-insensitively
    // sorted Timeout section, so its enum id is PG_WAIT_TIMEOUT | 6.
    assert_eq!(
        pgstat_get_wait_event(WAIT_EVENT_SPIN_DELAY).unwrap(),
        Some(Cow::Borrowed("SpinDelay"))
    );
    // The first (id 0) Activity member, by name.
    let archiver = wait_event_data_by_info(PG_WAIT_ACTIVITY).unwrap();
    assert_eq!(archiver.type_, "Activity");
    assert_eq!(archiver.name, "ArchiverMain");
}

/// The name-lookup ids must be assigned in the *sorted* order of each class,
/// not file order. The IO section is not pre-sorted in the file, so this
/// catches the file-order bug.
#[test]
fn io_name_lookup_ids_follow_sorted_order_not_file_order() {
    assert_eq!(
        wait_event_data_by_info(PG_WAIT_IO | 0).unwrap().name,
        "AioIoCompletion"
    );
    assert_eq!(
        wait_event_data_by_info(PG_WAIT_IO | 1).unwrap().name,
        "AioIoUringExecution"
    );
    assert_eq!(
        wait_event_data_by_info(PG_WAIT_IO | 2).unwrap().name,
        "AioIoUringSubmit"
    );
    assert_eq!(
        wait_event_data_by_info(PG_WAIT_IO | 7).unwrap().name,
        "BuffileTruncate"
    );
    assert_eq!(
        wait_event_data_by_info(PG_WAIT_IO | 8).unwrap().name,
        "BuffileWrite"
    );
}

/// The name lookup only covers the six classes that get a generated
/// `pgstat_get_wait_<class>` function; LWLock/Lock/Extension are excluded.
#[test]
fn name_lookup_excludes_lwlock_lock_extension() {
    for row in wait_event_data() {
        assert!(
            matches!(
                row.type_,
                "Activity" | "BufferPin" | "Client" | "IPC" | "Timeout" | "IO"
            ),
            "unexpected class {} in name lookup",
            row.type_
        );
    }
}

/// `pg_get_wait_events` must reproduce the generated `wait_event_funcs_data.c`
/// table (modulo C-string escaping): every class, sorted, with post-processed
/// descriptions. The golden TSV is generated from PostgreSQL 18.3's
/// `generate-wait_event_types.pl` over the same `wait_event_names.txt`.
#[test]
fn funcs_data_matches_postgres_generated_table() {
    const GOLDEN: &str = include_str!("wait_event_funcs_data.golden.tsv");
    let golden: Vec<(&str, &str, &str)> = GOLDEN
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            let mut it = l.splitn(3, '\t');
            (
                it.next().unwrap(),
                it.next().unwrap(),
                it.next().unwrap_or(""),
            )
        })
        .collect();

    let rows = wait_event_funcs_data();
    assert_eq!(
        rows.len(),
        golden.len(),
        "row count mismatch: got {}, expected {}",
        rows.len(),
        golden.len()
    );
    // Exactly the PG 18.3 count.
    assert_eq!(rows.len(), 273);

    for (i, (row, (gt, gn, gd))) in rows.iter().zip(golden.iter()).enumerate() {
        assert_eq!(&row.type_, gt, "type mismatch at row {i}");
        assert_eq!(&row.name, gn, "name mismatch at row {i} ({})", row.type_);
        assert_eq!(
            &row.description, gd,
            "description mismatch at row {i} ({}/{})",
            row.type_, row.name
        );
    }
}

/// Lock the corrected per-class counts.
#[test]
fn funcs_data_per_class_counts() {
    let rows = wait_event_funcs_data();
    let count = |t: &str| rows.iter().filter(|r| r.type_ == t).count();
    assert_eq!(count("Activity"), 18);
    assert_eq!(count("BufferPin"), 1);
    assert_eq!(count("Client"), 9);
    assert_eq!(count("Extension"), 1);
    assert_eq!(count("IO"), 81);
    assert_eq!(count("IPC"), 57);
    assert_eq!(count("Lock"), 12);
    assert_eq!(count("LWLock"), 84);
    assert_eq!(count("Timeout"), 10);
}

/// Sample-row checks for each tricky description transform.
#[test]
fn funcs_data_description_transforms() {
    let rows = wait_event_funcs_data();
    let find = |t: &str, n: &str| -> &WaitEventRow {
        rows.iter()
            .find(|r| r.type_ == t && r.name == n)
            .unwrap_or_else(|| panic!("missing {t}/{n}"))
    };

    // Trailing period stripped, plain row.
    assert_eq!(
        find("Activity", "ArchiverMain").description,
        "Waiting in main loop of archiver process"
    );
    // GUC <xref> rewritten to underscored guc name.
    assert_eq!(
        find("IPC", "ArchiveCleanupCommand").description,
        "Waiting for archive_cleanup_command to complete"
    );
    // SGML <literal> markup stripped.
    assert_eq!(
        find("IPC", "SafeSnapshot").description,
        "Waiting to obtain a valid snapshot for a READ ONLY DEFERRABLE transaction"
    );
    // <quote> markup turned into real double quotes.
    assert_eq!(
        find("LWLock", "LockManager").description,
        "Waiting to read or update information about \"heavyweight\" locks"
    );
    // "; see ..." trailing clause removed (Lock/virtualxid keeps its verbatim
    // name and strips the xref + trailing reference).
    assert_eq!(
        find("Lock", "virtualxid").description,
        "Waiting to acquire a virtual transaction ID lock"
    );
    // Apostrophe preserved as a literal ' (the C generator escapes it as \'
    // for the C source only).
    assert_eq!(
        find("LWLock", "LockFastPath").description,
        "Waiting to read or update a process' fast-path lock information"
    );
    // LWLock/Lock names stay verbatim (no CamelCasing): WAL/DSM casing kept.
    assert_eq!(find("LWLock", "WALSummarizer").name, "WALSummarizer");
    assert_eq!(find("LWLock", "DSMRegistry").name, "DSMRegistry");
}

#[test]
fn unknown_class_reports_unknown_wait_event() {
    assert_eq!(
        pgstat_get_wait_event(0x12000000 | 3).unwrap(),
        Some(Cow::Borrowed("unknown wait event"))
    );
    assert_eq!(pgstat_get_wait_event(0).unwrap(), None);
}

#[test]
fn wait_event_storage_defaults_to_thread_local_and_can_be_redirected() {
    pgstat_reset_wait_event_storage();
    pgstat_report_wait_start(WAIT_EVENT_SPIN_DELAY);
    assert_eq!(pgstat_current_wait_event_info(), WAIT_EVENT_SPIN_DELAY);

    let storage = WaitEventStorage::new();
    {
        let _guard = pgstat_set_wait_event_storage(storage.clone());
        pgstat_report_wait_start(PG_WAIT_CLIENT);
        assert_eq!(storage.get(), PG_WAIT_CLIENT);
        assert_eq!(pgstat_current_wait_event_info(), PG_WAIT_CLIENT);
        pgstat_report_wait_end();
        assert_eq!(storage.get(), 0);
    }

    assert_eq!(pgstat_current_wait_event_info(), WAIT_EVENT_SPIN_DELAY);
    pgstat_report_wait_end();
    assert_eq!(pgstat_current_wait_event_info(), 0);
}

#[test]
fn name_builder_camelcases_upper_snake() {
    assert_eq!(wait_event_name_from_symbol("AIO_IO_URING_SUBMIT"), "AioIoUringSubmit");
}
