//! Local (opclass) reloptions: init_local_reloptions / add_local_*_reloption /
//! build_local_reloptions (reloptions.c). Options support procs fill a
//! LocalRelopts carrier; build produces the C options struct image (4-byte
//! varlena header + fields at C offsetof positions) that rides FmgrInfo
//! opclass-options slots and BRIN column carriers.
use ::datum::Datum;
use ::elog::ereport;
use ::mcx::Mcx;
use ::types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};

use crate::{parse_opt_value, text_array_image, OptData, OptVal};

pub struct LocalOptDef {
    // In-tree options procs register compile-time names (C accepts runtime
    // strings only for extension opclasses, which don't exist here).
    pub name: &'static str,
    pub data: OptData,
    // C offsetof into the options struct, header included (e.g. the first
    // int after the vl_len_ word is offset 4).
    pub offset: usize,
}

#[derive(Default)]
pub struct LocalRelopts {
    struct_size: usize,
    // std Vec: DDL-cold carrier, dies with the call.
    opts: Vec<LocalOptDef>,
    validators: Vec<fn(&mut [u8]) -> Result<(), String>>,
}

impl LocalRelopts {
    pub fn new() -> Self {
        Self::default()
    }

    /// init_local_reloptions.
    pub fn init(&mut self, struct_size: usize) {
        self.struct_size = struct_size;
        self.opts.clear();
        self.validators.clear();
    }

    /// register_reloptions_validator: runs over the built struct image when
    /// validate (CREATE INDEX ... WITH), C's relopts->validators loop.
    pub fn register_validator(&mut self, v: fn(&mut [u8]) -> Result<(), String>) {
        self.validators.push(v);
    }

    /// add_local_int_reloption (desc dropped: only surfaced via extension
    /// catalogs C never reads on this path).
    pub fn add_int(&mut self, name: &'static str, default_val: i32, min: i32, max: i32, offset: usize) {
        self.opts.push(LocalOptDef { name, data: OptData::Int { default_val, min, max }, offset });
    }

    /// add_local_real_reloption.
    pub fn add_real(&mut self, name: &'static str, default_val: f64, min: f64, max: f64, offset: usize) {
        self.opts.push(LocalOptDef { name, data: OptData::Real { default_val, min, max }, offset });
    }

    /// add_local_bool_reloption.
    pub fn add_bool(&mut self, name: &'static str, default_val: bool, offset: usize) {
        self.opts.push(LocalOptDef { name, data: OptData::Bool { default_val }, offset });
    }
}

/// build_local_reloptions: parse `options` (a text[] datum of "name=value";
/// 0 = C NULL) against the registered defs and fill the options struct image.
pub fn build_local_reloptions(
    mcx: Mcx<'_>,
    relopts: &LocalRelopts,
    options: Datum,
    validate: bool,
) -> PgResult<Vec<u8>> {
    let mut vals: Vec<(bool, OptVal)> = relopts
        .opts
        .iter()
        .map(|d| {
            (false, match d.data {
                OptData::Bool { .. } => OptVal::Bool(false),
                OptData::Int { .. } => OptVal::Int(0),
                OptData::Real { .. } => OptVal::Real(0.0),
                OptData::Enum { .. } => OptVal::Enum(0),
            })
        })
        .collect();

    if options != Datum::null() {
        let image = text_array_image(mcx, options)?;
        for text_str in crate::option_text_strs(mcx, &image)?.iter() {
            let mut found = false;
            for (def, (isset, val)) in relopts.opts.iter().zip(vals.iter_mut()) {
                let kw = def.name;
                if text_str.len() > kw.len()
                    && text_str.as_bytes()[kw.len()] == b'='
                    && text_str.as_bytes()[..kw.len()] == *kw.as_bytes()
                {
                    if *isset && validate {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!(
                                "parameter \"{kw}\" specified more than once"
                            ))
                            .into_error()
                            .into());
                    }
                    if let Some(v) =
                        parse_opt_value(kw, &def.data, &text_str[kw.len() + 1..], validate)?
                    {
                        *val = v;
                        *isset = true;
                    }
                    found = true;
                    break;
                }
            }
            if !found && validate {
                let name = match text_str.find('=') {
                    Some(p) => &text_str[..p],
                    None => text_str,
                };
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("unrecognized parameter \"{name}\""))
                    .into_error()
                    .into());
            }
        }
    }

    // allocateReloptStruct + fillRelOptions over the local parse table.
    let size = relopts.struct_size;
    let mut out = vec![0u8; size];
    out[0..4].copy_from_slice(&(((size as u32) << 2).to_ne_bytes()));
    for (def, (isset, val)) in relopts.opts.iter().zip(vals.iter()) {
        match (&def.data, isset, val) {
            (OptData::Bool { default_val }, false, _) | (_, true, OptVal::Bool(default_val)) => {
                out[def.offset] = *default_val as u8;
            }
            (OptData::Int { default_val, .. }, false, _) | (_, true, OptVal::Int(default_val)) => {
                out[def.offset..def.offset + 4].copy_from_slice(&default_val.to_ne_bytes());
            }
            (OptData::Real { default_val, .. }, false, _)
            | (_, true, OptVal::Real(default_val)) => {
                out[def.offset..def.offset + 8].copy_from_slice(&default_val.to_ne_bytes());
            }
            _ => unreachable!("local reloption type confusion for {}", def.name),
        }
    }
    if validate {
        for v in &relopts.validators {
            if let Err(msg) = v(&mut out) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(msg)
                    .into_error()
                    .into());
            }
        }
    }
    Ok(out)
}
