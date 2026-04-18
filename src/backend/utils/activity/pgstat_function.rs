use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use super::pgstat::{DatabaseStatsStore, SessionStatsState};
use super::pgstat_xact::StatsMutationEffect;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackFunctionsSetting {
    None,
    Pl,
    All,
}

impl TrackFunctionsSetting {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "pl" => Some(Self::Pl),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    pub(crate) const fn tracks_plpgsql(self) -> bool {
        !matches!(self, Self::None)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FunctionStatsEntry {
    pub calls: i64,
    pub total_time_micros: u64,
    pub self_time_micros: u64,
}

impl FunctionStatsEntry {
    pub(crate) fn apply_delta(&mut self, delta: &FunctionStatsDelta) {
        self.calls += delta.calls;
        self.total_time_micros += delta.total_time_micros;
        self.self_time_micros += delta.self_time_micros;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FunctionStatsDelta {
    pub calls: i64,
    pub total_time_micros: u64,
    pub self_time_micros: u64,
}

#[derive(Debug, Clone)]
pub(super) struct FunctionCallFrame {
    pub funcid: u32,
    pub started_at: Instant,
    pub child_micros: u64,
}

impl SessionStatsState {
    pub(crate) fn note_function_drop(
        &mut self,
        oid: u32,
        db_stats: &Arc<RwLock<DatabaseStatsStore>>,
    ) {
        if self.xact_active {
            let saved_xact = self.function_xact.remove(&oid);
            self.stats_effects
                .push(StatsMutationEffect::DropFunction { oid, saved_xact });
            self.dropped_functions_in_xact.insert(oid);
        } else {
            self.pending_flush.functions.remove(&oid);
            db_stats.write().remove_function(oid);
        }
        self.clear_snapshot();
    }

    pub(crate) fn begin_function_call(&mut self, funcid: u32) {
        self.call_stack.push(FunctionCallFrame {
            funcid,
            started_at: Instant::now(),
            child_micros: 0,
        });
    }

    pub(crate) fn finish_function_call(&mut self, funcid: u32) {
        let Some(frame) = self.call_stack.pop() else {
            return;
        };
        if frame.funcid != funcid {
            self.call_stack.clear();
            return;
        }
        let elapsed_micros = frame.started_at.elapsed().as_micros() as u64;
        let self_micros = elapsed_micros.saturating_sub(frame.child_micros);
        if self.xact_active {
            let xact = self.function_xact.entry(funcid).or_default();
            xact.calls += 1;
            xact.total_time_micros += elapsed_micros;
            xact.self_time_micros += self_micros;
        } else {
            let pending = self.pending_flush.functions.entry(funcid).or_default();
            pending.calls += 1;
            pending.total_time_micros += elapsed_micros;
            pending.self_time_micros += self_micros;
        }
        if let Some(parent) = self.call_stack.last_mut() {
            parent.child_micros += elapsed_micros;
        }
        self.clear_snapshot();
    }
}
