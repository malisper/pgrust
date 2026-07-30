//! Port of `basebackup_target.c`: the base-backup target-type registry
//! (thread_local, single-threaded backend). The 'server' target's sink lives
//! in [`server`] (basebackup_server.c).

#![allow(non_snake_case)]

mod server;

use std::cell::RefCell;

use ::elog::ereport;
use ::mcx::Mcx;
use ::sink::Bbsink;
use ::types_error::{
    ErrorLocation, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR, ERROR,
};

pub enum TargetDetail {
    None,
    Server(String),
    Other(Box<dyn std::any::Any>),
}

pub trait BaseBackupTarget {
    fn check_detail(&self, target: &str, target_detail: Option<&str>) -> PgResult<TargetDetail>;

    fn get_sink<'mcx>(
        &self,
        mcx: Mcx<'mcx>,
        next_sink: Box<Bbsink<'mcx>>,
        detail_arg: TargetDetail,
    ) -> PgResult<Box<Bbsink<'mcx>>>;
}

pub struct BaseBackupTargetType {
    pub name: String,
    pub target: Box<dyn BaseBackupTarget>,
}

pub struct BaseBackupTargetHandle {
    pub type_name: String,
    pub detail_arg: TargetDetail,
}

thread_local! {
    static BASE_BACKUP_TARGET_TYPE_LIST: RefCell<Vec<BaseBackupTargetType>> =
        const { RefCell::new(Vec::new()) };
}

pub fn BaseBackupAddTarget(name: &str, target: Box<dyn BaseBackupTarget>) {
    if BASE_BACKUP_TARGET_TYPE_LIST.with(|l| l.borrow().is_empty()) {
        initialize_target_list();
    }

    BASE_BACKUP_TARGET_TYPE_LIST.with(|l| {
        let mut list = l.borrow_mut();
        // Update in place if the name already exists, else append (C behavior).
        for ttype in list.iter_mut() {
            if ttype.name == name {
                ttype.target = target;
                return;
            }
        }
        list.push(BaseBackupTargetType {
            name: name.to_string(),
            target,
        });
    });
}

pub fn BaseBackupGetTargetHandle(
    target: &str,
    target_detail: Option<&str>,
) -> PgResult<BaseBackupTargetHandle> {
    if BASE_BACKUP_TARGET_TYPE_LIST.with(|l| l.borrow().is_empty()) {
        initialize_target_list();
    }

    let detail = BASE_BACKUP_TARGET_TYPE_LIST.with(|l| -> PgResult<Option<TargetDetail>> {
        let list = l.borrow();
        for ttype in list.iter() {
            if ttype.name == target {
                return Ok(Some(ttype.target.check_detail(target, target_detail)?));
            }
        }
        Ok(None)
    })?;

    match detail {
        Some(detail_arg) => Ok(BaseBackupTargetHandle {
            type_name: target.to_string(),
            detail_arg,
        }),
        None => ereport_unrecognized_target(target),
    }
}

pub fn BaseBackupGetSink<'mcx>(
    mcx: Mcx<'mcx>,
    handle: BaseBackupTargetHandle,
    next_sink: Box<Bbsink<'mcx>>,
) -> PgResult<Box<Bbsink<'mcx>>> {
    let BaseBackupTargetHandle {
        type_name,
        detail_arg,
    } = handle;
    BASE_BACKUP_TARGET_TYPE_LIST.with(|l| {
        let list = l.borrow();
        for ttype in list.iter() {
            if ttype.name == type_name {
                return ttype.target.get_sink(mcx, next_sink, detail_arg);
            }
        }
        ereport_unrecognized_target(&type_name)
    })
}

fn initialize_target_list() {
    BASE_BACKUP_TARGET_TYPE_LIST.with(|l| {
        let mut list = l.borrow_mut();
        list.push(BaseBackupTargetType {
            name: "blackhole".to_string(),
            target: Box::new(BlackholeTarget),
        });
        list.push(BaseBackupTargetType {
            name: "server".to_string(),
            target: Box::new(ServerTarget),
        });
    });
}

struct BlackholeTarget;

impl BaseBackupTarget for BlackholeTarget {
    fn check_detail(&self, target: &str, target_detail: Option<&str>) -> PgResult<TargetDetail> {
        if target_detail.is_some() {
            return ereport_syntax_error(
                format!("target \"{target}\" does not accept a target detail"),
                "reject_target_detail",
            );
        }
        Ok(TargetDetail::None)
    }

    fn get_sink<'mcx>(
        &self,
        _mcx: Mcx<'mcx>,
        next_sink: Box<Bbsink<'mcx>>,
        _detail_arg: TargetDetail,
    ) -> PgResult<Box<Bbsink<'mcx>>> {
        Ok(next_sink)
    }
}

struct ServerTarget;

impl BaseBackupTarget for ServerTarget {
    fn check_detail(&self, target: &str, target_detail: Option<&str>) -> PgResult<TargetDetail> {
        match target_detail {
            None => ereport_syntax_error(
                format!("target \"{target}\" requires a target detail"),
                "server_check_detail",
            ),
            Some(detail) => Ok(TargetDetail::Server(detail.to_string())),
        }
    }

    fn get_sink<'mcx>(
        &self,
        mcx: Mcx<'mcx>,
        next_sink: Box<Bbsink<'mcx>>,
        detail_arg: TargetDetail,
    ) -> PgResult<Box<Bbsink<'mcx>>> {
        let TargetDetail::Server(pathname) = detail_arg else {
            panic!("server target requires a server detail argument");
        };
        server::bbsink_server_new(mcx, next_sink, &pathname)
    }
}

#[track_caller]
pub(crate) fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn ereport_syntax_error<T>(msg: String, func: &'static str) -> PgResult<T> {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(msg)
        .finish(loc(func))?;
    unreachable!("ereport(ERROR) always returns Err")
}

fn ereport_unrecognized_target<T>(target: &str) -> PgResult<T> {
    ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(format!("unrecognized target: \"{target}\""))
        .finish(loc("BaseBackupGetTargetHandle"))?;
    unreachable!("ereport(ERROR) always returns Err")
}

pub fn init_seams() {}

#[cfg(test)]
mod tests;
