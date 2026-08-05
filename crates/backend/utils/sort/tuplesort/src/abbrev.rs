//! Per-sort abbreviation state: C hangs it off `ssup_extra`; here it lives in
//! TuplesortData so SortSupport stays a Copy word bundle.

use ::datum::Datum;

use crate::ssup::{with_varlena_payload, AbbrevArm, AbbrevKind, SortComparator};

enum ConverterState {
    VarStr(varlena::abbrev::VarStrAbbrevState),
    VarStrXfrm(VarStrXfrmState),
    Uuid(::adt_uuid::abbrev::UuidAbbrevState),
    Network(::adt_network::abbrev::NetworkAbbrevState),
    Numeric(::adt_numeric::sortsupport::NumericAbbrevState),
}

/// varstr_abbrev_convert non-C arm (varlena.c): abbreviated keys are
/// pg_strnxfrm(_prefix) sort-key prefixes; buf1/buf2 mirror C's blob cache
/// (repeated equal inputs skip the transform and the HLL adds).
struct VarStrXfrmState {
    locale: &'static pg_locale::PgLocale,
    inner: varlena::abbrev::VarStrAbbrevState,
    buf1: Vec<u8>,
    buf2: Vec<u8>,
    last_valid: bool,
    last_len2: usize,
}

const MAX_PREFIX_BYTES: usize = 8;

impl VarStrXfrmState {
    fn new(locale: &'static pg_locale::PgLocale, bpchar: bool) -> VarStrXfrmState {
        VarStrXfrmState {
            locale,
            inner: varlena::abbrev::VarStrAbbrevState::new(bpchar),
            buf1: Vec::new(),
            buf2: vec![0; 1024],
            last_valid: false,
            last_len2: 0,
        }
    }

    fn convert(&mut self, payload: &[u8]) -> u64 {
        let data = self.inner.trimmed(payload);
        let mut prefix = [0u8; MAX_PREFIX_BYTES];
        if self.last_valid && self.buf1 == data {
            let n = MAX_PREFIX_BYTES.min(self.last_len2);
            prefix[..n].copy_from_slice(&self.buf2[..n]);
            // C `goto done`: cache hits skip the HLL adds.
            return u64::from_be_bytes(prefix);
        }
        self.buf1.clear();
        self.buf1.extend_from_slice(data);
        self.last_valid = true;

        let bsize = if self.locale.pg_strnxfrm_prefix_enabled() {
            self.locale.pg_strnxfrm_prefix(&mut self.buf2[..MAX_PREFIX_BYTES], data)
        } else {
            loop {
                let bsize = self.locale.pg_strnxfrm(&mut self.buf2[..], data);
                if bsize < self.buf2.len() {
                    break bsize;
                }
                self.buf2.resize(bsize + 1, 0);
            }
        };
        self.last_len2 = bsize;
        let n = MAX_PREFIX_BYTES.min(bsize);
        prefix[..n].copy_from_slice(&self.buf2[..n]);
        self.inner.record(data, u64::from_ne_bytes(prefix));
        u64::from_be_bytes(prefix)
    }
}

pub struct AbbrevState {
    pub full_comparator: SortComparator,
    conv: ConverterState,
}

impl AbbrevState {
    pub fn new(arm: AbbrevArm) -> AbbrevState {
        let conv = match arm.kind {
            AbbrevKind::VarStrC => {
                ConverterState::VarStr(varlena::abbrev::VarStrAbbrevState::new(false))
            }
            AbbrevKind::BpcharC => {
                ConverterState::VarStr(varlena::abbrev::VarStrAbbrevState::new(true))
            }
            AbbrevKind::VarStrXfrm { locale, bpchar } => {
                ConverterState::VarStrXfrm(VarStrXfrmState::new(locale, bpchar))
            }
            AbbrevKind::Uuid => ConverterState::Uuid(::adt_uuid::abbrev::UuidAbbrevState::new()),
            AbbrevKind::Network => {
                ConverterState::Network(::adt_network::abbrev::NetworkAbbrevState::new())
            }
            AbbrevKind::Numeric => {
                ConverterState::Numeric(::adt_numeric::sortsupport::NumericAbbrevState::new())
            }
        };
        AbbrevState { full_comparator: arm.full_comparator, conv }
    }

    /// # Safety
    /// `original` is a live non-null datum of the arm's type: a varlena of
    /// any form (VarStrC/BpcharC/Network/Numeric) or a 16-byte uuid (Uuid).
    #[inline]
    pub unsafe fn convert(&mut self, original: Datum) -> Datum {
        let word = match &mut self.conv {
            ConverterState::VarStr(s) => {
                with_varlena_payload(original, |b| s.convert_slim(b))
            }
            ConverterState::VarStrXfrm(s) => with_varlena_payload(original, |b| s.convert(b)),
            ConverterState::Uuid(s) => {
                s.convert(&*(original.as_usize() as *const ::adt_uuid::PgUuid))
            }
            ConverterState::Network(s) => with_varlena_payload(original, |b| {
                s.convert(::adt_network::InetRef::from_payload(b))
            }),
            ConverterState::Numeric(s) => with_varlena_payload(original, |b| s.convert(b)),
        };
        Datum::from_u64(word)
    }

    pub fn abort(&mut self, memtupcount: i32) -> bool {
        match &mut self.conv {
            ConverterState::VarStr(s) => s.abort_slim(memtupcount),
            ConverterState::VarStrXfrm(s) => s.inner.abort(memtupcount),
            ConverterState::Uuid(s) => s.abort(memtupcount),
            ConverterState::Network(s) => s.abort(memtupcount),
            ConverterState::Numeric(s) => s.abort(memtupcount),
        }
    }
}
