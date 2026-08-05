//! Per-session GUC backing storage. C gives every backend a private copy of
//! each GUC variable via fork; our backends are threads, so a bare static
//! atomic backing is process-shared — one session's SET leaks to every
//! session and a child thread's boot-value bring-up writes clobber others'
//! SETs mid-statement (notes/io-method-child-guc-race.md residual).
//!
//! Backing = a const-initialized, no-Drop thread_local Cell: on the fleet
//! target (ELF local-exec TLS) a read is tpidr+ldr — the same two
//! instructions as the old bare static load, no branch. A global-cell +
//! per-session-overlay design was tried first and measured +205 instr/q on
//! select1 (a per-read override check times ~80 hot GUC reads); the pure-TLS
//! shape matches the pre-existing hot-path backings (scan_fgram's parse
//! GUCs) already inside the select1 gate. A thread sees boot defaults until
//! its GUC bring-up (initialize_guc_options[_for_child] + snapshot restore)
//! runs — the same window C children have before read_nondefault_variables.

#[macro_export]
macro_rules! session_guc_bool {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        ::std::thread_local! {
            static $cell: ::core::cell::Cell<bool> = const { ::core::cell::Cell::new($boot) };
        }
        #[inline]
        pub fn $get() -> bool {
            $cell.get()
        }
        pub fn $set(v: bool) {
            $cell.set(v)
        }
    };
}

#[macro_export]
macro_rules! session_guc_int {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        ::std::thread_local! {
            static $cell: ::core::cell::Cell<i32> = const { ::core::cell::Cell::new($boot) };
        }
        #[inline]
        pub fn $get() -> i32 {
            $cell.get()
        }
        pub fn $set(v: i32) {
            $cell.set(v)
        }
    };
}

#[macro_export]
macro_rules! session_guc_real {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        ::std::thread_local! {
            static $cell: ::core::cell::Cell<f64> = const { ::core::cell::Cell::new($boot as f64) };
        }
        #[inline]
        pub fn $get() -> f64 {
            $cell.get()
        }
        pub fn $set(v: f64) {
            $cell.set(v)
        }
    };
}

#[macro_export]
macro_rules! session_guc_string {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        ::std::thread_local! {
            static $cell: ::core::cell::RefCell<Option<String>> =
                const { ::core::cell::RefCell::new(None) };
        }
        pub fn $get() -> Option<String> {
            $cell.with(|c| match &*c.borrow() {
                Some(s) => Some(s.clone()),
                None => {
                    let boot: Option<&'static str> = $boot;
                    boot.map(str::to_owned)
                }
            })
        }
        pub fn $set(v: Option<String>) {
            $cell.with(|c| *c.borrow_mut() = v)
        }
    };
}

// Hot clusters: one thread_local struct per owner so a function reading N
// vars computes the TLS base once (mrs+add) and does N field-offset loads —
// bare per-var TLS statics measured +30 instr/q on select1 (one base
// recomputation per read; dualprof: LocalKey::with dominated the tip delta).
#[macro_export]
macro_rules! session_guc_cluster {
    ($cluster:ident, $tls:ident: $( ($field:ident, $ty:ty, $get:ident, $set:ident, $boot:expr) ),+ $(,)?) => {
        struct $cluster {
            $( $field: ::core::cell::Cell<$ty>, )+
        }
        ::std::thread_local! {
            static $tls: $cluster = const { $cluster {
                $( $field: ::core::cell::Cell::new($boot), )+
            } };
        }
        $(
            #[inline]
            pub fn $get() -> $ty {
                $tls.with(|c| c.$field.get())
            }
            pub fn $set(v: $ty) {
                $tls.with(|c| c.$field.set(v));
            }
        )+
    };
}
