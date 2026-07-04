//! Per-sort abbreviation state: C hangs it off `ssup_extra`; here it lives in
//! TuplesortData so SortSupport stays a Copy word bundle.

use ::datum::Datum;

use crate::ssup::{varlena_payload, AbbrevArm, AbbrevKind, SortComparator};

enum ConverterState {
    VarStr(varlena::abbrev::VarStrAbbrevState),
    Uuid(::adt_uuid::abbrev::UuidAbbrevState),
    Network(::adt_network::abbrev::NetworkAbbrevState),
    Numeric(::adt_numeric::sortsupport::NumericAbbrevState),
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
    /// `original` is a live non-null datum of the arm's type: an untoasted
    /// varlena (VarStrC/BpcharC/Network/Numeric) or a 16-byte uuid (Uuid).
    #[inline]
    pub unsafe fn convert(&mut self, original: Datum) -> Datum {
        let word = match &mut self.conv {
            ConverterState::VarStr(s) => s.convert(varlena_payload(original)),
            ConverterState::Uuid(s) => {
                s.convert(&*(original.as_usize() as *const ::adt_uuid::PgUuid))
            }
            ConverterState::Network(s) => s.convert(::adt_network::InetRef::from_payload(
                varlena_payload(original),
            )),
            ConverterState::Numeric(s) => s.convert(varlena_payload(original)),
        };
        Datum::from_u64(word)
    }

    pub fn abort(&mut self, memtupcount: i32) -> bool {
        match &mut self.conv {
            ConverterState::VarStr(s) => s.abort(memtupcount),
            ConverterState::Uuid(s) => s.abort(memtupcount),
            ConverterState::Network(s) => s.abort(memtupcount),
            ConverterState::Numeric(s) => s.abort(memtupcount),
        }
    }
}
