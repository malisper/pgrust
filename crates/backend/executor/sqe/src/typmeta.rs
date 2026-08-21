//! TypMeta — the typed-currency identity of a column or answer column
//! (production-plan §P1-1; port-study/currency-insertion.md §2).
//!
//! Replaces the PoC's three schema shims: `col_width` (stencils/mod.rs),
//! the `sx` all-byval-are-signed sign-extend, and the `is_date_col` /
//! `kkind` column-NAME sniffs. Type facts come from the schema provider
//! (the PG catalog at P2-1; the rig's cols.tsv stand-in until then) and
//! ride the plan — nothing in the engine re-derives type by name.

/// PG type oids the engine currently distinguishes. The engine never
/// interprets an unknown oid — unknown shapes refuse at lowering.
pub mod oids {
    pub const BOOL: u32 = 16;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const INT8: u32 = 20;
    pub const OID: u32 = 26;
    pub const DATE: u32 = 1082;
    pub const TIMESTAMP: u32 = 1114;
    /// timestamptz: the SAME stored word class as timestamp (µs since
    /// PG epoch) — a vocabulary entry, not new machinery (type census
    /// 2026-08-17 item 2).
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const TEXT: u32 = 25;
    /// varchar: rides the text word class verbatim (varlena, collatable).
    pub const VARCHAR: u32 = 1043;
    /// bpchar (1042): admitted under the PAD-AWARE LAW ONLY (Michael's
    /// ruling 2026-08-19, ADJUDICATION-20260818 §bpchar). char(n) columns
    /// store exactly-n-CHARACTER blank-padded images (bpcharin/bpcharrecv
    /// enforce the typmod at every ingest path — the night/tpch-bpchar
    /// tie-law corpus), so within one declared width, byte equality IS
    /// bpchareq. The seam pads query literals to the column's declared
    /// width at lowering (eq/ne/IN); LIKE sees the RAW stored bytes
    /// (PG's bpcharlike matches the PADDED value — oracle trap T4).
    /// ORDER/range comparison stays REFUSED: bpcharcmp trims trailing
    /// blanks first, and padded-memcmp diverges from it whenever a byte
    /// < 0x20 appears ('a' < E'a\x01' per PG; padded bytes order the
    /// other way — oracle trap T5b). Bare bpchar (typmod −1) stores
    /// UNPADDED and refuses everywhere but verbatim render/LIKE.
    pub const BPCHAR: u32 = 1042;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const NUMERIC: u32 = 1700;
    /// Unsigned-word family (census 2026-08-17: byval words whose datum
    /// must NEVER sign-extend — the SIGNED-flag law's oid class).
    pub const XID: u32 = 28;
    pub const CID: u32 = 29;
    /// "char" (oid 18): width-1 unsigned word.
    pub const CHAR: u32 = 18;
    /// uuid: Fixed{16} by-ref face, memcmp order.
    pub const UUID: u32 = 2950;
    /// name: Fixed{64} by-ref face — a NUL-padded 64-byte buffer whose
    /// memcmp order IS PG's namecmp order under C collation (strncmp of
    /// NUL-padded buffers == memcmp of the full 64 bytes). name is the
    /// system-catalog identifier type; its comparisons always resolve C
    /// collation (typcollation 950) — non-C shapes refuse at lowering.
    pub const NAME: u32 = 19;
    /// jsonb ([json-rung1]): the stored column is the byte-exact IMAGE
    /// lane (VarlenaVerbatim, render-only — the engine never interprets
    /// the container bytes). Typed serving rides the SHRED-LANE virtual
    /// columns witnessed at bank open (`bank::witness_jsonb_shred`),
    /// never this oid's own face.
    pub const JSONB: u32 = 3802;
}

/// The unsigned-word oid class (byval faces that zero-extend). Width-8
/// unsigned (xid8/pg_lsn) stays refused-unaudited (census D-item) — it
/// has no order-preserving i64 embed.
pub fn is_unsigned_word(oid: u32) -> bool {
    matches!(oid, oids::OID | oids::XID | oids::CID | oids::CHAR)
}

/// C collation: the only collation the engine's byte-ordered text paths
/// implement (TextReg gid order, dict rank walks, KElem byte compares).
/// Any other collation is a typed refusal at lowering (plan §6) — which
/// is exactly why collation rides TypMeta and every predicate fingerprint.
pub const COLLATION_C: u32 = 950;

/// [packednum] NUMERIC(P,S) typmod → the declared scale S (PG's
/// `numeric_typmod_scale`: low 11 bits of typmod−VARHDRSZ, sign-extended
/// from bit 10 — negative scales are valid since PG 15). `None` for the
/// unconstrained typmod (−1) — no DDL scale exists. A negative declared
/// scale can never witness a PackedNumeric lane (stored dscale >= 0), so
/// callers gating the zero-part witness get `None` for those too.
pub fn numeric_typmod_scale(typmod: i32) -> Option<i32> {
    if typmod < 4 {
        return None;
    }
    let s = ((((typmod - 4) & 0x7ff) ^ 1024) - 1024) as i32;
    (s >= 0).then_some(s)
}

/// [sqe-bpchar] bpchar(n) typmod → the declared CHARACTER width n
/// (atttypmod = n + VARHDRSZ, n >= 1). `None` for the unconstrained
/// typmod (−1): bare bpchar stores UNPADDED, so the uniform-padding
/// witness the pad-aware law rides does not exist there.
pub fn bpchar_declared_chars(typmod: i32) -> Option<i32> {
    (typmod >= 5).then(|| typmod - 4)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TypMeta {
    pub oid: u32,
    /// typlen: 1/2/4/8 for byval words, -1 for varlena.
    pub width: i8,
    pub byval: bool,
    /// 0 for non-collatable types.
    pub collation: u32,
    /// atttypmod (-1 = none). Carried for render/coercion consumers
    /// (interval(p), bit(n) — P4-0); the P1-1 vocabulary ignores it
    /// (type census 2026-08-17 item 4).
    pub typmod: i32,
}

impl TypMeta {
    pub const fn byval(oid: u32, width: i8) -> TypMeta {
        TypMeta { oid, width, byval: true, collation: 0, typmod: -1 }
    }
    pub const fn varlena(oid: u32, collation: u32) -> TypMeta {
        TypMeta { oid, width: -1, byval: false, collation, typmod: -1 }
    }
    /// Fixed-length by-reference (uuid: len 16). width carries typlen.
    pub const fn fixed(oid: u32, len: i8) -> TypMeta {
        TypMeta { oid, width: len, byval: false, collation: 0, typmod: -1 }
    }
    pub const INT2: TypMeta = TypMeta::byval(oids::INT2, 2);
    pub const INT4: TypMeta = TypMeta::byval(oids::INT4, 4);
    pub const INT8: TypMeta = TypMeta::byval(oids::INT8, 8);
    pub const DATE: TypMeta = TypMeta::byval(oids::DATE, 4);
    pub const BOOL: TypMeta = TypMeta::byval(oids::BOOL, 1);
    pub const FLOAT4: TypMeta = TypMeta::byval(oids::FLOAT4, 4);
    pub const FLOAT8: TypMeta = TypMeta::byval(oids::FLOAT8, 8);
    /// Unsigned width-4 word (oid).
    pub const OID: TypMeta = TypMeta::byval(oids::OID, 4);
    pub const UUID: TypMeta = TypMeta::fixed(oids::UUID, 16);
    /// numeric is varlena in PG; width -1, not collatable.
    pub const NUMERIC: TypMeta = TypMeta::varlena(oids::NUMERIC, 0);
    pub const TEXT_C: TypMeta = TypMeta::varlena(oids::TEXT, COLLATION_C);

    #[inline(always)]
    pub fn is_varlena(&self) -> bool {
        self.width < 0
    }
    /// Fixed-length by-reference face (typlen > 0, not byval): uuid class.
    #[inline(always)]
    pub fn is_fixed(&self) -> bool {
        !self.byval && self.width > 0
    }
    /// Byval word width in bytes (panics on varlena — callers gate).
    #[inline(always)]
    pub fn byval_width(&self) -> u32 {
        debug_assert!(self.width > 0);
        self.width as u32
    }
}
