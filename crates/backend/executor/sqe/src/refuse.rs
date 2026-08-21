//! Typed lowering refusals (plan §2: one total lowering function,
//! fail-closed; the refusal enum is the spec of what's missing). P1-1
//! seeds the enum with the causes the current lowering vocabulary can
//! hit — P1-4 builds the census/EXPLAIN surface and CI allowlist over it.
//! The enum is exhaustive by construction: no wildcard admission, no
//! `None`/`Err(String)` refusals anywhere in the engine (risks.md §6).

/// Why a plan (or plan fragment) refused to lower. Every variant is a
/// NAMED, censusable cause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refuse {
    /// No plan of record for this query id (rig RON table miss).
    NoPlanForQuery { q: u32 },
    /// A predicate leaf outside the lowering vocabulary (e.g. NOT over a
    /// non-Contains leg).
    PredUnsupported { what: &'static str },
    /// IN list with other than two members (the 43 carry only In2; longer
    /// lists lower to a disjunction when one appears — P4-1d).
    InListWidth { n: usize },
    /// A literal that does not parse as the column's type.
    ConstUnparsable { col: u32, text: String },
    /// An aggregate shape outside the vocabulary.
    AggUnsupported { what: &'static str },
    /// HAVING other than COUNT(*) > k (P4-1e).
    HavingUnsupported,
    /// MetadataAnswer with a predicate that is not stats-answerable.
    MetadataPredNotStatsAnswerable,
    /// The elected family has no registered stencil.
    FamilyUnregistered { family: crate::ir::Family },
    /// No family-election rule fired for the shape (the coverage gap the
    /// census orders — plan §2).
    NoFamilyRule { what: &'static str },
    /// Non-C collation on a text operation (plan §6: typed refusal until
    /// census heat argues for sidecars).
    CollationUnsupported { attno: u32, collation: u32 },
    /// A column type oid outside the P1-1 vocabulary.
    TypeUnsupported { attno: u32, oid: u32 },
    /// A nullable varlena column whose parts publish a dict: the dict
    /// lanes carry a zero-null proof until nullable-dict lands (engine
    /// currency ruling — typed refusal, never a wrong answer).
    NullableDict { attno: u32 },
    /// A nullable column reached a family/shape whose body is not yet
    /// 3VL-threaded (null-blind fold or packed key without a nullmask).
    /// The null-threaded shapes (metadata answer, fused filter-agg, the
    /// hash-plane byval-key foundation) admit; everything else refuses
    /// until its fold routes through the single fold law.
    NullableUnsupported { attno: u32, family: crate::ir::Family },
    /// A face outside an operation's admitted set (e.g. SUM/AVG over a
    /// float/unsigned/fixed face, width-8 unsigned words — no
    /// order-preserving i64 embed / no exact i128 sum law yet).
    FaceUnsupported { attno: u32, what: &'static str },
    /// SQL front-end (rig) parse/shape failures, carried as one cause
    /// with the reason text (the rig's Err(String) tolerance ends at the
    /// engine boundary).
    SqlUnsupported { what: String },
    /// Grouped shape outside the server-proven grouped vocabulary
    /// (`planner::check_server_grouped`).
    GroupServeUnsupported { what: &'static str },
    /// Elected family not yet proven on the server path.
    FamilyUnservedServer { family: crate::ir::Family },
    /// No sound stats witness bounds the grouped answer under the
    /// stencils' emit cap — serving would risk silent truncation.
    GroupCountUnwitnessed { what: &'static str },
    /// Unbounded row-returning scan without a stats witness bounding the
    /// surviving rows under the answer cap (the scan-answer law: a bare
    /// scan's answer is O(survivors) — never a surprise full-table
    /// materialization through the DestReceiver).
    ScanRowsUnwitnessed { what: &'static str },
    /// [sortgrp v1] Order-sensitive aggregate shape outside the SortGrouped
    /// v1 admission (mixed per-agg sort specs, ORDER-BY-less
    /// string_agg/array_agg, DISTINCT/FILTER qualifiers, variable or
    /// out-of-range percentile fractions, expression group keys, agg
    /// mixes beyond the tier-2 set).
    SortAggUnsupported { what: &'static str },
    /// [sortgrp v1] No stats witness bounds the family's sort/append
    /// payload bytes (byte_len/nonnull faces absent) — serving would risk
    /// unbounded memory, so the shape refuses typed (never an OOM).
    SortAggBytesUnwitnessed { what: &'static str },
    /// [sortgrp v1] The witnessed payload-byte estimate exceeds the
    /// family's admission budget (spill is P6-1 scope — typed refusal,
    /// never a spill fork or an OOM).
    SortAggOverBudget { est: u64, budget: u64 },
    /// [winserve v1] Window shape outside the WindowServe v1 admission
    /// (non-default frames, unhandled function/argument shapes, spec
    /// drift between the seam and the plan).
    WinUnsupported { what: &'static str },
    /// [winserve v1] No stats witness bounds the family's scatter/sort/
    /// answer payload bytes (a window answer is O(rows)) — serving would
    /// risk unbounded memory, so the shape refuses typed (never an OOM).
    WinBytesUnwitnessed { what: &'static str },
    /// [winserve v1] The witnessed payload-byte estimate exceeds the
    /// family's admission budget (spill is P6-1 scope — typed refusal,
    /// never a spill fork or an OOM).
    WinOverBudget { est: u64, budget: u64 },
    /// [P6-1 spill] The grouped statement's witnessed byte estimate
    /// exceeds the E18 budget and the elected shape has no spill arm
    /// (spill-design.md §4) — typed refusal, never an OOM. Shapes the
    /// spill arm serves never mint this.
    GroupedSpillUnavailable { what: &'static str, est: u64, budget: u64 },
    /// [cap-retire] The finalize answer-bytes law (spill-design.md §3.4,
    /// RULED Michael 2026-08-19): the grouped answer plane — the TRUE
    /// (counted, witness-grade, never estimated) group set the stencil is
    /// about to materialize — exceeds the answer budget. Minted at
    /// FINALIZE, after the bounded-memory fold, with the exact byte
    /// account in hand; 53400 (raising the budget is a legitimate
    /// remedy). This is the retirement replacement for the 2^20
    /// group-count witness cap on spill-served shapes: they now serve
    /// the correct answer or refuse HERE — never truncate, never OOM.
    GroupAnswerOverBudget { got: u64, budget: u64 },
    /// [spill-2] A spill substrate I/O event failed mid-statement
    /// (create/append/read on a temp spill file). v1 PANICKED here; the
    /// typed seam raises it as a RUNTIME error through the same
    /// `RunRefusal` unwind the finalize answer-bytes law rides — the
    /// statement fails loudly and typed (I/O class, not a capability
    /// refusal), the server never dies, and the store's drop still
    /// deletes the tree. `op` is the failed event (`create`/`append`/
    /// `read`); `detail` carries the OS error text.
    SpillIo { op: &'static str, detail: String },
}

/// [cap-retire] Typed panic payload carrying a RUNTIME refusal (the
/// finalize answer-bytes law) out of the backend-free engine: the shell
/// wraps `Engine::run` in a catch, downcasts THIS type back to the
/// refusal lattice, and re-raises anything else (cancel payloads, real
/// panics) untouched.
pub struct RunRefusal(pub Refuse);

/// Raise a typed runtime refusal from inside a stencil. The unwind rides
/// the same statement-scoped path as cooperative cancellation (workers
/// are catch_unwind'd at the pool seam; the armer re-raises), so the
/// payload surfaces from `Engine::run` on the dispatching thread.
pub fn raise_runtime(r: Refuse) -> ! {
    std::panic::panic_any(RunRefusal(r))
}

impl std::fmt::Display for Refuse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refuse::NoPlanForQuery { q } => write!(f, "no-plan-for-query:q={q}"),
            Refuse::PredUnsupported { what } => write!(f, "pred-unsupported:{what}"),
            Refuse::InListWidth { n } => write!(f, "in-list-width:{n}"),
            Refuse::ConstUnparsable { col, text } => {
                write!(f, "const-unparsable:c{col}:{text}")
            }
            Refuse::AggUnsupported { what } => write!(f, "agg-unsupported:{what}"),
            Refuse::HavingUnsupported => write!(f, "having-unsupported"),
            Refuse::MetadataPredNotStatsAnswerable => {
                write!(f, "metadata-pred-not-stats-answerable")
            }
            Refuse::FamilyUnregistered { family } => {
                write!(f, "family-unregistered:{family:?}")
            }
            Refuse::NoFamilyRule { what } => write!(f, "no-family-rule:{what}"),
            Refuse::CollationUnsupported { attno, collation } => {
                write!(f, "collation-unsupported:c{attno}:coll={collation}")
            }
            Refuse::TypeUnsupported { attno, oid } => {
                write!(f, "type-unsupported:c{attno}:oid={oid}")
            }
            Refuse::NullableDict { attno } => write!(f, "nullable-dict:c{attno}"),
            Refuse::NullableUnsupported { attno, family } => {
                write!(f, "nullable-unsupported:c{attno}:family={family:?}")
            }
            Refuse::FaceUnsupported { attno, what } => {
                write!(f, "face-unsupported:c{attno}:{what}")
            }
            Refuse::SqlUnsupported { what } => write!(f, "sql-unsupported:{what}"),
            Refuse::GroupServeUnsupported { what } => {
                write!(f, "group-serve-unsupported:{what}")
            }
            Refuse::FamilyUnservedServer { family } => {
                write!(f, "family-unserved-server:{family:?}")
            }
            Refuse::GroupCountUnwitnessed { what } => {
                write!(f, "group-count-unwitnessed:{what}")
            }
            Refuse::ScanRowsUnwitnessed { what } => {
                write!(f, "scan-rows-unwitnessed:{what}")
            }
            Refuse::SortAggUnsupported { what } => write!(f, "sortagg-unsupported:{what}"),
            Refuse::SortAggBytesUnwitnessed { what } => {
                write!(f, "sortagg-bytes-unwitnessed:{what}")
            }
            Refuse::SortAggOverBudget { est, budget } => {
                write!(f, "sortagg-over-budget:est={est}:budget={budget}")
            }
            Refuse::WinUnsupported { what } => write!(f, "winagg-unsupported:{what}"),
            Refuse::WinBytesUnwitnessed { what } => {
                write!(f, "winagg-bytes-unwitnessed:{what}")
            }
            Refuse::WinOverBudget { est, budget } => {
                write!(f, "winagg-over-budget:est={est}:budget={budget}")
            }
            Refuse::GroupedSpillUnavailable { what, est, budget } => {
                write!(f, "grouped-spill-unavailable:{what}:est={est}:budget={budget}")
            }
            Refuse::GroupAnswerOverBudget { got, budget } => {
                write!(f, "group-answer-over-budget:got={got}:budget={budget}")
            }
            Refuse::SpillIo { op, detail } => write!(f, "spill-io:{op}:{detail}"),
        }
    }
}
