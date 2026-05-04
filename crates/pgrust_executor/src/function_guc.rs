use std::collections::{HashMap, HashSet};

use pgrust_catalog_data::PgProcRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SavedFunctionIdentity {
    pub current_user_oid: u32,
    pub active_role_oid: Option<u32>,
}

pub fn parsed_proconfig(config: Option<&[String]>) -> Vec<(String, String)> {
    config
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let (name, value) = entry.split_once('=')?;
            Some((name.to_string(), value.to_string()))
        })
        .collect()
}

pub fn save_function_identity(
    current_user_oid: u32,
    active_role_oid: Option<u32>,
) -> SavedFunctionIdentity {
    SavedFunctionIdentity {
        current_user_oid,
        active_role_oid,
    }
}

pub trait FunctionGucContext {
    type Error;

    fn save_identity(&self) -> SavedFunctionIdentity;
    fn restore_identity(&mut self, saved: SavedFunctionIdentity);
    fn apply_security_definer_identity(&mut self, owner_oid: u32);
    fn gucs(&self) -> &HashMap<String, String>;
    fn gucs_mut(&mut self) -> &mut HashMap<String, String>;
    fn apply_function_guc(
        &mut self,
        name: &str,
        value: Option<&str>,
    ) -> Result<String, Self::Error>;
}

pub fn restore_function_gucs(
    gucs: &mut HashMap<String, String>,
    saved_gucs: &HashMap<String, String>,
    restore_names: impl IntoIterator<Item = String>,
) {
    for name in restore_names {
        if let Some(value) = saved_gucs.get(&name) {
            gucs.insert(name, value.clone());
        } else {
            gucs.remove(&name);
        }
    }
}

pub fn execute_with_sql_function_context<C, T>(
    row: &PgProcRow,
    ctx: &mut C,
    f: impl FnOnce(&mut C) -> Result<T, C::Error>,
) -> Result<T, C::Error>
where
    C: FunctionGucContext,
{
    let entries = parsed_proconfig(row.proconfig.as_deref());
    if entries.is_empty() && !row.prosecdef {
        return f(ctx);
    }

    let saved_identity = ctx.save_identity();
    if row.prosecdef {
        ctx.apply_security_definer_identity(row.proowner);
    }
    let saved_gucs = ctx.gucs().clone();
    let mut restore_names = HashSet::new();
    for (name, value) in entries {
        let normalized = match ctx.apply_function_guc(&name, Some(&value)) {
            Ok(normalized) => normalized,
            Err(err) => {
                *ctx.gucs_mut() = saved_gucs;
                ctx.restore_identity(saved_identity);
                return Err(err);
            }
        };
        restore_names.insert(normalized);
    }

    let result = f(ctx);
    restore_function_gucs(ctx.gucs_mut(), &saved_gucs, restore_names);
    ctx.restore_identity(saved_identity);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_catalog_data::PG_LANGUAGE_SQL_OID;

    #[derive(Debug)]
    struct TestContext {
        gucs: HashMap<String, String>,
        identity: SavedFunctionIdentity,
        fail_guc: Option<String>,
    }

    impl FunctionGucContext for TestContext {
        type Error = String;

        fn save_identity(&self) -> SavedFunctionIdentity {
            self.identity
        }

        fn restore_identity(&mut self, saved: SavedFunctionIdentity) {
            self.identity = saved;
        }

        fn apply_security_definer_identity(&mut self, owner_oid: u32) {
            self.identity.current_user_oid = owner_oid;
        }

        fn gucs(&self) -> &HashMap<String, String> {
            &self.gucs
        }

        fn gucs_mut(&mut self) -> &mut HashMap<String, String> {
            &mut self.gucs
        }

        fn apply_function_guc(
            &mut self,
            name: &str,
            value: Option<&str>,
        ) -> Result<String, Self::Error> {
            if self.fail_guc.as_deref() == Some(name) {
                return Err(format!("bad guc {name}"));
            }
            let normalized = name.to_ascii_lowercase();
            if let Some(value) = value {
                self.gucs.insert(normalized.clone(), value.to_string());
            } else {
                self.gucs.remove(&normalized);
            }
            Ok(normalized)
        }
    }

    #[test]
    fn sql_function_context_restores_identity_and_gucs_after_success() {
        let row = proc_row(true, Some(vec!["work_mem=64kB".into()]));
        let mut ctx = TestContext {
            gucs: HashMap::from([("work_mem".into(), "4MB".into())]),
            identity: SavedFunctionIdentity {
                current_user_oid: 10,
                active_role_oid: Some(11),
            },
            fail_guc: None,
        };

        let observed = execute_with_sql_function_context(&row, &mut ctx, |ctx| {
            Ok::<_, String>((
                ctx.identity.current_user_oid,
                ctx.gucs.get("work_mem").cloned(),
            ))
        })
        .unwrap();

        assert_eq!(observed, (42, Some("64kB".into())));
        assert_eq!(
            ctx.identity,
            SavedFunctionIdentity {
                current_user_oid: 10,
                active_role_oid: Some(11),
            }
        );
        assert_eq!(ctx.gucs.get("work_mem"), Some(&"4MB".into()));
    }

    #[test]
    fn sql_function_context_restores_identity_and_gucs_after_guc_error() {
        let row = proc_row(true, Some(vec!["work_mem=64kB".into()]));
        let mut ctx = TestContext {
            gucs: HashMap::from([("work_mem".into(), "4MB".into())]),
            identity: SavedFunctionIdentity {
                current_user_oid: 10,
                active_role_oid: None,
            },
            fail_guc: Some("work_mem".into()),
        };

        let err = execute_with_sql_function_context(&row, &mut ctx, |_ctx| Ok::<_, String>(()))
            .unwrap_err();

        assert_eq!(err, "bad guc work_mem");
        assert_eq!(
            ctx.identity,
            SavedFunctionIdentity {
                current_user_oid: 10,
                active_role_oid: None,
            }
        );
        assert_eq!(ctx.gucs.get("work_mem"), Some(&"4MB".into()));
    }

    fn proc_row(prosecdef: bool, proconfig: Option<Vec<String>>) -> PgProcRow {
        PgProcRow {
            oid: 1,
            proname: "f".into(),
            pronamespace: 0,
            proowner: 42,
            proacl: None,
            prolang: PG_LANGUAGE_SQL_OID,
            procost: 1.0,
            prorows: 0.0,
            provariadic: 0,
            prosupport: 0,
            prokind: 'f',
            prosecdef,
            proleakproof: false,
            proisstrict: false,
            proretset: false,
            provolatile: 'v',
            proparallel: 'u',
            pronargs: 0,
            pronargdefaults: 0,
            prorettype: 0,
            proargtypes: String::new(),
            proallargtypes: None,
            proargmodes: None,
            proargnames: None,
            proargdefaults: None,
            prosrc: String::new(),
            probin: None,
            prosqlbody: None,
            proconfig,
        }
    }
}
