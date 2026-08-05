seam_core::seam!(
    // PostgresMain (tcop/postgres.c); never returns.
    pub fn postgres_main(dbname: &str, username: &str) -> !
);

seam_core::seam!(
    // PostgresSingleUserMain (tcop/postgres.c:4055) + the process-exit frame
    // (main.c DISPATCH_SINGLE). argv is the FULL server argv (argv[1] ==
    // "--single"); username per main.c's get_user_name_or_exit. Exits the
    // process; never returns.
    pub fn postgres_single_user_main(argv: &[String], username: &str) -> !
);

seam_core::seam!(
    // PostgresStdioWireMain (pgrust extension, no C counterpart): the
    // single-user boot ladder + BackendInitialize's connection half, one
    // wire-protocol session over the boot-installed stdio transport
    // provider (§2.4 seam). argv[1] == "--stdio-wire"; username is the
    // fallback identity (the startup packet's user wins). Exits the
    // process; never returns.
    pub fn postgres_stdio_wire_main(argv: &[String], username: &str) -> !
);

seam_core::seam!(
    // ProcessClientReadInterrupt(blocked) (tcop/postgres.c); Err is the
    // ereport(FATAL) "terminating connection" path.
    pub fn process_client_read_interrupt(blocked: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    // ProcessClientWriteInterrupt(blocked) (tcop/postgres.c).
    pub fn process_client_write_interrupt(blocked: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    // process_postgres_switches(argc, argv, ctx, NULL) (postgres.c); the u8 is
    // a types_guc::GucContext discriminant (this decl crate stays types-lean).
    pub fn process_postgres_switches(argv: &[String], gucctx: u8) -> types_error::PgResult<()>
);

seam_core::seam!(
    // CHECK_FOR_INTERRUPTS() (miscadmin.h) -> ProcessInterrupts (postgres.c);
    // a raised cancel/die comes back as the Err.
    pub fn check_for_interrupts() -> types_error::PgResult<()>
);

seam_core::seam!(
    // die (postgres.c) — SIGTERM's thread-model rendering: flag setter run on
    // the target backend's own thread; Err is the single-user immediate
    // ProcessInterrupts leg.
    pub fn die() -> types_error::PgResult<()>
);

seam_core::seam!(
    // StatementCancelHandler (postgres.c) — SIGINT's thread-model rendering;
    // must run on the target backend's own thread (flags are thread-local).
    pub fn statement_cancel_handler()
);

seam_core::seam!(
    // quickdie (postgres.c) — SIGQUIT rendering; exits the process.
    pub fn quickdie() -> !
);

seam_core::seam!(
    // FloatExceptionHandler (postgres.c); always Err (ereport ERROR).
    pub fn float_exception_handler() -> types_error::PgResult<()>
);

seam_core::seam!(
    // HandleRecoveryConflictInterrupt(reason) (postgres.c); u32 is a
    // ProcSignalReason discriminant (this decl crate stays types-lean).
    pub fn handle_recovery_conflict_interrupt(reason: u32)
);

seam_core::seam!(
    // ResetUsage (postgres.c).
    pub fn reset_usage()
);

seam_core::seam!(
    // ShowUsage(title) (postgres.c).
    pub fn show_usage(title: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    // set_debug_options(debug_flag, ctx, source-fixed-ARGV) (postgres.c);
    // ctx u8 = GucContext discriminant.
    pub fn set_debug_options(debug_flag: i32, gucctx: u8) -> types_error::PgResult<()>
);

seam_core::seam!(
    // set_plan_disabling_options(arg, ctx, ARGV) (postgres.c).
    pub fn set_plan_disabling_options(arg: &str, gucctx: u8) -> types_error::PgResult<bool>
);

seam_core::seam!(
    // get_stats_option_name(optarg) (postgres.c); None = invalid.
    pub fn get_stats_option_name(arg: &str) -> Option<&'static str>
);

#[cfg(pgrust_sim)]
seam_core::seam!(
    // PostgresSimNetMain (P4 sim-net; pgrust extension, cfg pgrust_sim
    // only): the wire-session boot ladder over the sim-net transport
    // provider — one deterministic in-process pgwire session driven by the
    // scripted client pump. argv[1] == "--sim-net"; username is the
    // fallback identity. Exits the process; never returns.
    pub fn postgres_sim_net_main(argv: &[String], username: &str) -> !
);

seam_core::tap!(
    // Serial-lease v2 safe-point admission (GL-SLEASE-2; pgrust extension —
    // no C counterpart). ProcessInterrupts calls it after the holdoff /
    // crit-section gates on every drain; installed by seams_init ONLY when
    // the lease is armed (PGRUST_RUNTIME_SERIAL_LEASE=1), so the unarmed
    // posture pays one null-tap check on the already-cold interrupt path.
    pub fn tap_serial_lease_admission()
);

// ---------------------------------------------------------------------------
// GL-STMTTASK-1 — the statement-as-task PROTOCOL ARM (not a seam call: a
// statement-scoped thread-local handshake between the two sides of the
// seam boundary). exec_simple_query (tcop) arms exactly one statement's
// top-level portal run; the executor's statement-task hook consumes the
// arm on its first entry. Lives here because it is the one crate both
// sides production-link (tcop reaches the executor only through seam
// crates; the executor cannot name tcop).
// ---------------------------------------------------------------------------
/// GL-STMTTASK-2 quantum-yield experiment (coordinator/Michael-chartered,
/// DEFAULT OFF): the CHECK_FOR_INTERRUPTS hot path calls [`stmt_yield::tick`]
/// — one thread-local bool load + predictable branch when disarmed (armed
/// only inside a statement-task span with `PGRUST_STMT_TASK_YIELD` on).
/// The actual governor (quantum clock + permit donation) is a registered
/// fn-pointer owned by the executor crate — this seam crate stays
/// runtime-type-free (the stmt_task_arm one-authority pattern).
pub mod stmt_yield {
    thread_local! {
        /// True only for the span of an armed statement-task execution on
        /// this thread (inline session span or dop-1 worker drive span).
        static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    static HOOK: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();

    /// Install the governor (executor side; once, first arm).
    pub fn set_hook(f: fn()) {
        let _ = HOOK.set(f);
    }

    /// Arm/disarm the span (RAII discipline is the caller's — the executor
    /// span guard owns nesting/restore).
    pub fn arm() {
        ARMED.with(|c| c.set(true));
    }
    pub fn disarm() {
        ARMED.with(|c| c.set(false));
    }

    /// The CHECK_FOR_INTERRUPTS-side tick. Disarmed cost: one TLS load +
    /// branch (the statement-task spans are the only setters).
    #[inline(always)]
    pub fn tick() {
        if ARMED.with(|c| c.get()) {
            if let Some(f) = HOOK.get() {
                f();
            }
        }
    }
}

pub mod stmt_task_arm {
    /// The unloaded posture's pure parse (unit-pinned below): DEFAULT OFF;
    /// ON iff exactly `1`/`on` (t35 exact-spelling arming law).
    fn posture(v: Option<&str>) -> bool {
        matches!(v.map(str::trim), Some("1") | Some("on"))
    }

    /// `PGRUST_STMT_TASK` — the GL-STMTTASK-1 master knob (kill knob for
    /// the statement-as-task executor arm). ONE authority: both the tcop
    /// arm site and the executor hook resolve through this cell.
    pub fn stmt_task_enabled() -> bool {
        static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ON.get_or_init(|| posture(std::env::var("PGRUST_STMT_TASK").ok().as_deref()))
    }

    thread_local! {
        /// Set for exactly one simple-protocol statement's portal run on
        /// the session thread; consumed (take) by the FIRST executor-run
        /// hook entry of that statement — the top-level run by
        /// construction. Never set on worker threads.
        static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    /// RAII disarm: the arm can never leak past its statement (error
    /// unwinds included).
    pub struct StmtTaskArm(());

    impl Drop for StmtTaskArm {
        fn drop(&mut self) {
            ARMED.with(|c| c.set(false));
        }
    }

    /// Protocol-side arming. `eligible` carries the protocol-level facts
    /// only the caller knows (simple protocol, single statement, wire
    /// dest, raw SELECT, normal non-subtransaction session state). Arms
    /// only when the knob is ON; the executor hook owns the plan-shape
    /// gates. Knob-OFF cost: one memoized bool read.
    pub fn arm_statement(eligible: bool) -> StmtTaskArm {
        if eligible && stmt_task_enabled() {
            ARMED.with(|c| c.set(true));
        }
        StmtTaskArm(())
    }

    /// Consume the arm (executor side; first hook entry of the statement).
    pub fn take_armed() -> bool {
        ARMED.with(|c| c.replace(false))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// t35 exact-spelling law: DEFAULT OFF; arms on exactly `1`/`on`
        /// (trimmed); every other spelling stays OFF.
        #[test]
        fn knob_default_off_exact_spellings() {
            assert!(!posture(None), "unset = default OFF");
            assert!(posture(Some("1")), "=1 arms");
            assert!(posture(Some("on")), "=on arms");
            assert!(posture(Some(" 1 ")), "trimmed arming spelling");
            assert!(!posture(Some("0")));
            assert!(!posture(Some("off")));
            assert!(!posture(Some("true")), "non-registry spelling stays OFF");
            assert!(!posture(Some("ON")), "case-sensitive: only the exact spellings arm");
            assert!(!posture(Some("")));
        }

        /// Armed-witness companion (the pooldb pattern): the process switch
        /// agrees with the env's spelled posture when the env is set.
        #[test]
        fn knob_env_takes() {
            let Ok(v) = std::env::var("PGRUST_STMT_TASK") else {
                println!("SKIP: PGRUST_STMT_TASK unset (armed-witness leg runs it =0)");
                return;
            };
            let expect = posture(Some(v.as_str()));
            assert_eq!(
                stmt_task_enabled(),
                expect,
                "process switch disagrees with spelled posture"
            );
        }

        /// The arm is statement-scoped and consumed at most once; an
        /// ineligible arm never sets the flag.
        #[test]
        fn arm_scoped_and_consumed() {
            {
                let _g = arm_statement(false);
                assert!(!take_armed(), "ineligible never arms");
            }
            ARMED.with(|c| c.set(true));
            assert!(take_armed(), "armed flag consumed");
            assert!(!take_armed(), "consumed exactly once");
            {
                ARMED.with(|c| c.set(true));
                let _g = StmtTaskArm(());
            }
            assert!(!take_armed(), "guard drop disarms");
        }
    }
}
