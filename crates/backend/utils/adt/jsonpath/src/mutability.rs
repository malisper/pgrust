//! jspIsMutable / jspIsMutableWalker (jsonpath.c:1280): planner-side
//! mutability of a jsonpath for contain_mutable_functions.

use types_core::catalog::{DATEOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID};
use types_core::Oid;
use types_error::PgResult;

use crate::path::{jsp_init, jsp_init_by_buffer, ItemType, JsonPathItem, JSONPATH_LAX};

// C enum JsonPathDatatypeStatus (jsonpath.c:1253).
#[derive(Clone, Copy, PartialEq, Eq)]
enum DtStatus {
    NonDateTime,
    UnknownDateTime,
    Zoned,
    NonZoned,
}

struct MutCtx<'v> {
    // PASSING (name, exprType) pairs in clause order.
    vars: &'v [(&'v [u8], Oid)],
    current: DtStatus,
    lax: bool,
    mutable: bool,
}

pub fn jsp_is_mutable(image: &[u8], vars: &[(&[u8], Oid)]) -> PgResult<bool> {
    let header = u32::from_ne_bytes([image[4], image[5], image[6], image[7]]);
    let mut cxt = MutCtx {
        vars,
        current: DtStatus::NonDateTime,
        lax: header & JSONPATH_LAX != 0,
        mutable: false,
    };
    walker(jsp_init(image), &mut cxt)?;
    Ok(cxt.mutable)
}

fn walker(item: JsonPathItem<'_>, cxt: &mut MutCtx<'_>) -> PgResult<DtStatus> {
    let mut jpi = item;
    let mut status = DtStatus::NonDateTime;

    while !cxt.mutable {
        match jpi.typ {
            ItemType::Root => {
                debug_assert!(status == DtStatus::NonDateTime);
            }
            ItemType::Current => {
                debug_assert!(status == DtStatus::NonDateTime);
                status = cxt.current;
            }
            ItemType::Filter => {
                let prev = cxt.current;
                cxt.current = status;
                walker(jpi.arg(), cxt)?;
                cxt.current = prev;
            }
            ItemType::Variable => {
                let name = jpi.get_string();
                debug_assert!(status == DtStatus::NonDateTime);
                for (varname, typid) in cxt.vars {
                    // C strncmp(varname->sval, name, len): a prefix match on
                    // the first len bytes accepts a longer varname.
                    if varname.len() < name.len() || &varname[..name.len()] != name {
                        continue;
                    }
                    status = match *typid {
                        DATEOID | TIMEOID | TIMESTAMPOID => DtStatus::NonZoned,
                        TIMETZOID | TIMESTAMPTZOID => DtStatus::Zoned,
                        _ => DtStatus::NonDateTime,
                    };
                    break;
                }
            }
            ItemType::Equal
            | ItemType::NotEqual
            | ItemType::Less
            | ItemType::Greater
            | ItemType::LessOrEqual
            | ItemType::GreaterOrEqual => {
                debug_assert!(status == DtStatus::NonDateTime);
                let left = walker(jpi.left_arg(), cxt)?;
                let right = walker(jpi.right_arg(), cxt)?;
                // Comparison of datetime types with different timezone status
                // is mutable.
                if left != DtStatus::NonDateTime
                    && right != DtStatus::NonDateTime
                    && (left == DtStatus::UnknownDateTime
                        || right == DtStatus::UnknownDateTime
                        || left != right)
                {
                    cxt.mutable = true;
                }
            }
            ItemType::Not
            | ItemType::IsUnknown
            | ItemType::Exists
            | ItemType::Plus
            | ItemType::Minus => {
                debug_assert!(status == DtStatus::NonDateTime);
                walker(jpi.arg(), cxt)?;
            }
            ItemType::And
            | ItemType::Or
            | ItemType::Add
            | ItemType::Sub
            | ItemType::Mul
            | ItemType::Div
            | ItemType::Mod
            | ItemType::StartsWith => {
                debug_assert!(status == DtStatus::NonDateTime);
                walker(jpi.left_arg(), cxt)?;
                walker(jpi.right_arg(), cxt)?;
            }
            ItemType::IndexArray => {
                for i in 0..jpi.content.array.nelems {
                    let (from, to) = jpi.array_subscript(i);
                    if let Some(to) = to {
                        walker(to, cxt)?;
                    }
                    walker(from, cxt)?;
                }
                // C falls through into jpiAnyArray.
                if !cxt.lax {
                    status = DtStatus::NonDateTime;
                }
            }
            ItemType::AnyArray => {
                if !cxt.lax {
                    status = DtStatus::NonDateTime;
                }
            }
            ItemType::Any => {
                if jpi.content.anybounds.first > 0 {
                    status = DtStatus::NonDateTime;
                }
            }
            ItemType::Datetime => {
                if jpi.content.arg != 0 {
                    let arg = jpi.arg();
                    if arg.typ != ItemType::String {
                        // There will be a runtime error.
                        status = DtStatus::NonDateTime;
                    } else if adt_formatting::datetime_format_has_tz(arg.get_string())? {
                        status = DtStatus::Zoned;
                    } else {
                        status = DtStatus::NonZoned;
                    }
                } else {
                    status = DtStatus::UnknownDateTime;
                }
            }
            ItemType::LikeRegex => {
                debug_assert!(status == DtStatus::NonDateTime);
                let arg = jsp_init_by_buffer(jpi.buffer, jpi.base + jpi.content.like_regex.expr);
                walker(arg, cxt)?;
            }
            ItemType::Null | ItemType::String | ItemType::Numeric | ItemType::Bool => {}
            ItemType::Key
            | ItemType::AnyKey
            | ItemType::Subscript
            | ItemType::Last
            | ItemType::Type
            | ItemType::Size
            | ItemType::Abs
            | ItemType::Floor
            | ItemType::Ceiling
            | ItemType::Double
            | ItemType::KeyValue
            | ItemType::Bigint
            | ItemType::Boolean
            | ItemType::Decimal
            | ItemType::Integer
            | ItemType::Number
            | ItemType::StringFunc => {
                status = DtStatus::NonDateTime;
            }
            ItemType::Time
            | ItemType::Date
            | ItemType::Timestamp
            | ItemType::TimeTz
            | ItemType::TimestampTz => {
                status = DtStatus::NonZoned;
                cxt.mutable = true;
            }
        }

        match jpi.next() {
            Some(next) => jpi = next,
            None => break,
        }
    }

    Ok(status)
}
