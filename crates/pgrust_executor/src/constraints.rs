use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use pgrust_access::ItemPointerData;
use pgrust_nodes::primnodes::ColumnDesc;
use pgrust_nodes::{ConstraintTiming, TriggerCallContext, Value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PendingUniqueCheck {
    pub heap_tid: ItemPointerData,
    pub key_values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingParentForeignKeyCheck {
    pub constraint_oid: u32,
    pub relation_name: String,
    pub old_parent_values: Vec<Value>,
    pub replacement_parent_values: Option<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingForeignKeyCheck {
    pub constraint_oid: u32,
    pub relation_name: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct PendingUserConstraintTrigger {
    pub trigger_oid: u32,
    pub proc_oid: u32,
    pub call: TriggerCallContext,
}

#[derive(Debug, Clone, Default)]
struct DeferredConstraintState {
    all_override: Option<ConstraintTiming>,
    named_overrides: BTreeMap<u32, ConstraintTiming>,
    affected_constraint_oids: BTreeSet<u32>,
    pending_foreign_key_checks: Vec<PendingForeignKeyCheck>,
    pending_parent_foreign_key_checks: Vec<PendingParentForeignKeyCheck>,
    pending_unique_checks: HashMap<u32, HashSet<PendingUniqueCheck>>,
    pending_user_constraint_triggers: Vec<PendingUserConstraintTrigger>,
}

#[derive(Debug, Clone, Default)]
pub struct DeferredConstraintSnapshot {
    state: DeferredConstraintState,
}

#[derive(Debug, Clone, Default)]
pub struct DeferredConstraintTracker {
    state: Arc<parking_lot::Mutex<DeferredConstraintState>>,
}

pub type DeferredForeignKeyTracker = DeferredConstraintTracker;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotNullConstraintDescriptor {
    pub column_index: usize,
    pub constraint_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotNullViolation {
    pub column: String,
    pub constraint: String,
}

pub fn find_not_null_violation(
    relation_name: &str,
    columns: &[ColumnDesc],
    not_nulls: &[NotNullConstraintDescriptor],
    values: &[Value],
) -> Option<NotNullViolation> {
    columns.iter().enumerate().find_map(|(index, column)| {
        if column.storage.nullable || !matches!(values.get(index), Some(Value::Null) | None) {
            return None;
        }
        let constraint = not_nulls
            .iter()
            .find(|constraint| constraint.column_index == index)
            .map(|constraint| constraint.constraint_name.clone())
            .or_else(|| column.not_null_constraint_name.clone())
            .unwrap_or_else(|| format!("{relation_name}_{}_not_null", column.name));
        Some(NotNullViolation {
            column: column.name.clone(),
            constraint,
        })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BooleanConstraintResult {
    Pass,
    Fail,
    NonBool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraintFailure {
    pub message: String,
    pub detail: Option<String>,
    pub sqlstate: &'static str,
}

pub fn check_constraint_failure(
    relation_name: &str,
    constraint_name: &str,
    result: BooleanConstraintResult,
) -> Option<CheckConstraintFailure> {
    match result {
        BooleanConstraintResult::Pass => None,
        BooleanConstraintResult::Fail => Some(CheckConstraintFailure {
            message: "check constraint violation".into(),
            detail: None,
            sqlstate: "23514",
        }),
        BooleanConstraintResult::NonBool => Some(CheckConstraintFailure {
            message: "CHECK constraint expression must return boolean".into(),
            detail: Some(format!(
                "constraint \"{constraint_name}\" on relation \"{relation_name}\" produced a non-boolean value"
            )),
            sqlstate: "42804",
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RlsWriteCheckSource {
    Policy,
    ViewCheckOption(String),
    ConflictUpdateVisibility,
    MergeUpdateVisibility,
    MergeDeleteVisibility,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RlsDetailSource {
    BaseRow,
    DisplayExpressions,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RlsWriteCheckFailure {
    pub message: String,
    pub static_detail: Option<String>,
    pub detail: RlsDetailSource,
    pub sqlstate: &'static str,
}

pub fn rls_write_check_failure(
    relation_name: &str,
    policy_name: Option<&str>,
    source: &RlsWriteCheckSource,
    has_display_exprs: bool,
    result: BooleanConstraintResult,
) -> Option<RlsWriteCheckFailure> {
    match result {
        BooleanConstraintResult::Pass => None,
        BooleanConstraintResult::Fail => match source {
            RlsWriteCheckSource::ViewCheckOption(view_name) => Some(RlsWriteCheckFailure {
                message: format!("new row violates check option for view \"{view_name}\""),
                static_detail: None,
                detail: if has_display_exprs {
                    RlsDetailSource::DisplayExpressions
                } else {
                    RlsDetailSource::BaseRow
                },
                sqlstate: "44000",
            }),
            RlsWriteCheckSource::ConflictUpdateVisibility => Some(RlsWriteCheckFailure {
                message: format!(
                    "new row violates row-level security policy (USING expression) for table \"{relation_name}\""
                ),
                static_detail: None,
                detail: RlsDetailSource::None,
                sqlstate: "42501",
            }),
            RlsWriteCheckSource::MergeUpdateVisibility
            | RlsWriteCheckSource::MergeDeleteVisibility => Some(RlsWriteCheckFailure {
                message: format!(
                    "target row violates row-level security policy (USING expression) for table \"{relation_name}\""
                ),
                static_detail: None,
                detail: RlsDetailSource::None,
                sqlstate: "42501",
            }),
            RlsWriteCheckSource::Policy => Some(RlsWriteCheckFailure {
                message: policy_name
                    .map(|policy_name| {
                        format!(
                            "new row violates row-level security policy \"{policy_name}\" for table \"{relation_name}\""
                        )
                    })
                    .unwrap_or_else(|| {
                        format!(
                            "new row violates row-level security policy for table \"{relation_name}\""
                        )
                    }),
                static_detail: None,
                detail: RlsDetailSource::None,
                sqlstate: "42501",
            }),
        },
        BooleanConstraintResult::NonBool => match source {
            RlsWriteCheckSource::ViewCheckOption(view_name) => Some(RlsWriteCheckFailure {
                message: "view CHECK OPTION expression must return boolean".into(),
                static_detail: Some(format!(
                    "check option for view \"{view_name}\" produced a non-boolean value"
                )),
                detail: RlsDetailSource::None,
                sqlstate: "42804",
            }),
            _ => Some(RlsWriteCheckFailure {
                message: "row-level security policy expression must return boolean".into(),
                static_detail: Some(
                    policy_name
                        .map(|policy_name| {
                            format!(
                                "policy \"{policy_name}\" on relation \"{relation_name}\" produced a non-boolean value"
                            )
                        })
                        .unwrap_or_else(|| {
                            format!(
                                "row-level security policy on relation \"{relation_name}\" produced a non-boolean value"
                            )
                        }),
                ),
                detail: RlsDetailSource::None,
                sqlstate: "42804",
            }),
        },
    }
}

impl RlsWriteCheckFailure {
    pub fn split_static_detail(self) -> (String, Option<String>, RlsDetailSource, &'static str) {
        (self.message, self.static_detail, self.detail, self.sqlstate)
    }
}

pub fn row_security_new_row_tid() -> ItemPointerData {
    ItemPointerData {
        block_number: u32::MAX,
        offset_number: 0,
    }
}

impl DeferredConstraintTracker {
    pub fn record(&self, constraint_oid: u32) {
        if constraint_oid == 0 {
            return;
        }
        self.state
            .lock()
            .affected_constraint_oids
            .insert(constraint_oid);
    }

    pub fn record_foreign_key_check(
        &self,
        constraint_oid: u32,
        relation_name: String,
        mut values: Vec<Value>,
    ) {
        if constraint_oid == 0 {
            return;
        }
        Value::materialize_all(&mut values);
        let mut state = self.state.lock();
        state.affected_constraint_oids.insert(constraint_oid);
        state
            .pending_foreign_key_checks
            .push(PendingForeignKeyCheck {
                constraint_oid,
                relation_name,
                values,
            });
    }

    pub fn cancel_foreign_key_check(&self, constraint_oid: u32, values: &[Value]) {
        if constraint_oid == 0 {
            return;
        }
        let mut values = values.iter().map(Value::to_owned_value).collect::<Vec<_>>();
        Value::materialize_all(&mut values);
        self.state
            .lock()
            .pending_foreign_key_checks
            .retain(|check| check.constraint_oid != constraint_oid || check.values != values);
    }

    pub fn record_parent_foreign_key_check(
        &self,
        constraint_oid: u32,
        relation_name: String,
        mut old_parent_values: Vec<Value>,
        mut replacement_parent_values: Option<Vec<Value>>,
    ) {
        if constraint_oid == 0 {
            return;
        }
        Value::materialize_all(&mut old_parent_values);
        if let Some(values) = replacement_parent_values.as_mut() {
            Value::materialize_all(values);
        }
        self.state
            .lock()
            .pending_parent_foreign_key_checks
            .push(PendingParentForeignKeyCheck {
                constraint_oid,
                relation_name,
                old_parent_values,
                replacement_parent_values,
            });
    }

    pub fn record_unique(
        &self,
        constraint_oid: u32,
        heap_tid: ItemPointerData,
        mut key_values: Vec<Value>,
    ) {
        if constraint_oid == 0 {
            return;
        }
        Value::materialize_all(&mut key_values);
        self.state
            .lock()
            .pending_unique_checks
            .entry(constraint_oid)
            .or_default()
            .insert(PendingUniqueCheck {
                heap_tid,
                key_values,
            });
    }

    pub fn record_user_constraint_trigger(
        &self,
        trigger_oid: u32,
        proc_oid: u32,
        mut call: TriggerCallContext,
    ) {
        if trigger_oid == 0 {
            return;
        }
        if let Some(row) = call.old_row.as_mut() {
            Value::materialize_all(row);
        }
        if let Some(row) = call.new_row.as_mut() {
            Value::materialize_all(row);
        }
        for table in &mut call.transition_tables {
            for row in &mut table.rows {
                Value::materialize_all(row);
            }
        }
        self.state
            .lock()
            .pending_user_constraint_triggers
            .push(PendingUserConstraintTrigger {
                trigger_oid,
                proc_oid,
                call,
            });
    }

    pub fn affected_constraint_oids(&self) -> Vec<u32> {
        self.state
            .lock()
            .affected_constraint_oids
            .iter()
            .copied()
            .collect()
    }

    pub fn pending_foreign_key_checks(&self) -> Vec<PendingForeignKeyCheck> {
        self.state.lock().pending_foreign_key_checks.clone()
    }

    pub fn pending_unique_constraint_oids(&self) -> Vec<u32> {
        self.state
            .lock()
            .pending_unique_checks
            .keys()
            .copied()
            .collect()
    }

    pub fn pending_parent_foreign_key_checks(&self) -> Vec<PendingParentForeignKeyCheck> {
        self.state.lock().pending_parent_foreign_key_checks.clone()
    }

    pub fn pending_unique_checks(&self, constraint_oid: u32) -> Vec<PendingUniqueCheck> {
        self.state
            .lock()
            .pending_unique_checks
            .get(&constraint_oid)
            .map(|checks| checks.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn pending_user_constraint_triggers(&self) -> Vec<PendingUserConstraintTrigger> {
        self.state.lock().pending_user_constraint_triggers.clone()
    }

    pub fn clear_foreign_key_constraints(&self, constraint_oids: &BTreeSet<u32>) {
        let mut state = self.state.lock();
        for constraint_oid in constraint_oids {
            state.affected_constraint_oids.remove(constraint_oid);
        }
    }

    pub fn clear_foreign_key_checks(&self, constraint_oids: &BTreeSet<u32>) {
        self.state
            .lock()
            .pending_foreign_key_checks
            .retain(|check| !constraint_oids.contains(&check.constraint_oid));
    }

    pub fn clear_parent_foreign_key_checks(&self, constraint_oids: &BTreeSet<u32>) {
        self.state
            .lock()
            .pending_parent_foreign_key_checks
            .retain(|check| !constraint_oids.contains(&check.constraint_oid));
    }

    pub fn clear_unique_constraints(&self, constraint_oids: &BTreeSet<u32>) {
        let mut state = self.state.lock();
        for constraint_oid in constraint_oids {
            state.pending_unique_checks.remove(constraint_oid);
        }
    }

    pub fn clear_user_constraint_triggers(&self, trigger_oids: &BTreeSet<u32>) {
        self.state
            .lock()
            .pending_user_constraint_triggers
            .retain(|trigger| !trigger_oids.contains(&trigger.trigger_oid));
    }

    pub fn set_all_timing(&self, timing: ConstraintTiming) {
        let mut state = self.state.lock();
        state.named_overrides.clear();
        state.all_override = Some(timing);
    }

    pub fn set_constraint_timing(&self, constraint_oid: u32, timing: ConstraintTiming) {
        if constraint_oid == 0 {
            return;
        }
        self.state
            .lock()
            .named_overrides
            .insert(constraint_oid, timing);
    }

    pub fn effective_timing(
        &self,
        constraint_oid: u32,
        deferrable: bool,
        initially_deferred: bool,
    ) -> ConstraintTiming {
        if !deferrable {
            return ConstraintTiming::Immediate;
        }
        let state = self.state.lock();
        state
            .named_overrides
            .get(&constraint_oid)
            .copied()
            .or(state.all_override)
            .unwrap_or(if initially_deferred {
                ConstraintTiming::Deferred
            } else {
                ConstraintTiming::Immediate
            })
    }

    pub fn snapshot(&self) -> DeferredConstraintSnapshot {
        DeferredConstraintSnapshot {
            state: self.state.lock().clone(),
        }
    }

    pub fn restore(&self, snapshot: DeferredConstraintSnapshot) {
        *self.state.lock() = snapshot.state;
    }

    pub fn is_empty(&self) -> bool {
        let state = self.state.lock();
        state.affected_constraint_oids.is_empty()
            && state.pending_foreign_key_checks.is_empty()
            && state.pending_parent_foreign_key_checks.is_empty()
            && state.pending_unique_checks.is_empty()
            && state.pending_user_constraint_triggers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_constraint_non_bool_reports_constraint_and_relation() {
        let failure = check_constraint_failure(
            "accounts",
            "accounts_balance_check",
            BooleanConstraintResult::NonBool,
        )
        .expect("non-bool result should fail");

        assert_eq!(
            failure.message,
            "CHECK constraint expression must return boolean"
        );
        assert_eq!(failure.sqlstate, "42804");
        assert_eq!(
            failure.detail.as_deref(),
            Some(
                "constraint \"accounts_balance_check\" on relation \"accounts\" produced a non-boolean value"
            )
        );
    }

    #[test]
    fn rls_policy_failure_formats_named_and_unnamed_policies() {
        let named = rls_write_check_failure(
            "orders",
            Some("tenant_policy"),
            &RlsWriteCheckSource::Policy,
            false,
            BooleanConstraintResult::Fail,
        )
        .expect("failing policy should report");
        assert_eq!(
            named.message,
            "new row violates row-level security policy \"tenant_policy\" for table \"orders\""
        );
        assert_eq!(named.detail, RlsDetailSource::None);
        assert_eq!(named.sqlstate, "42501");

        let unnamed = rls_write_check_failure(
            "orders",
            None,
            &RlsWriteCheckSource::Policy,
            false,
            BooleanConstraintResult::Fail,
        )
        .expect("failing policy should report");
        assert_eq!(
            unnamed.message,
            "new row violates row-level security policy for table \"orders\""
        );
    }

    #[test]
    fn view_check_option_chooses_detail_source() {
        let base_row = rls_write_check_failure(
            "orders",
            None,
            &RlsWriteCheckSource::ViewCheckOption("visible_orders".into()),
            false,
            BooleanConstraintResult::Fail,
        )
        .expect("failing check option should report");
        assert_eq!(base_row.detail, RlsDetailSource::BaseRow);

        let display = rls_write_check_failure(
            "orders",
            None,
            &RlsWriteCheckSource::ViewCheckOption("visible_orders".into()),
            true,
            BooleanConstraintResult::Fail,
        )
        .expect("failing check option should report");
        assert_eq!(display.detail, RlsDetailSource::DisplayExpressions);
    }
}
