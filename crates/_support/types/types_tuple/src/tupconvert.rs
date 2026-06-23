//! Tuple-conversion-map vocabulary (`access/tupconvert.h`), trimmed.

use mcx::PgBox;

use crate::attmap::AttrMap;
use crate::heaptuple::TupleDesc;

/// `TupleConversionMap` (`access/tupconvert.h`), trimmed to the fields ports
/// consume: the attribute map plus the input/output row descriptors. The C
/// struct also caches per-conversion working arrays (`invalues`/`outvalues`
/// etc.); those arrive with the owning tupconvert unit, which deforms/forms
/// through the owned tuple model instead.
#[derive(Debug)]
pub struct TupleConversionMap<'mcx> {
    /// `TupleDesc indesc` — tupdesc for the source rowtype.
    pub indesc: TupleDesc<'mcx>,
    /// `TupleDesc outdesc` — tupdesc for the result rowtype.
    pub outdesc: TupleDesc<'mcx>,
    /// `AttrMap *attrMap` — indexes of input fields, or 0 for null.
    pub attrMap: PgBox<'mcx, AttrMap<'mcx>>,
}
