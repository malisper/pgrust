//! Bug base (contract §4.1.3 / spec §1.5): directory per seed —
//! `bugbase/<seed>/{seed,plan.plan,shrunk.plan,runs.json}`. Subcommands:
//! `list`, `test -b <filter>` (re-run banked bugs matching a signature),
//! `loop -n N`.

use super::driver::Signature;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunsJson {
    pub seed: u64,
    pub commit_sha: String,
    /// Wall-clock; under `--features sim` a fixed logical stamp so run
    /// artifacts are byte-stable (contract §0 A3: timestamps are confined to
    /// artifacts, never plan bytes).
    pub timestamp_utc: String,
    pub signature: Signature,
    pub class: String,
    pub sev: String,
    pub detail: String,
    pub step_idx: usize,
    pub cli: Vec<String>,
    pub profile_name: String,
    pub profile_sha256: String,
    /// replay-N flake policy evidence: (attempts, refails).
    pub replay_attempts: u32,
    pub replay_refails: u32,
}

pub struct BugBase {
    pub root: PathBuf,
}

impl BugBase {
    pub fn new(root: &Path) -> Self {
        BugBase { root: root.to_path_buf() }
    }

    pub fn seed_dir(&self, seed: u64) -> PathBuf {
        self.root.join(seed.to_string())
    }

    pub fn bank(
        &self,
        seed: u64,
        plan_text: &str,
        shrunk_text: Option<&str>,
        runs: &RunsJson,
    ) -> Result<PathBuf, String> {
        let dir = self.seed_dir(seed);
        std::fs::create_dir_all(&dir).map_err(|e| format!("bugbase: {}", e))?;
        std::fs::write(dir.join("seed"), format!("{}\n", seed)).map_err(|e| e.to_string())?;
        std::fs::write(dir.join("plan.plan"), plan_text).map_err(|e| e.to_string())?;
        if let Some(s) = shrunk_text {
            std::fs::write(dir.join("shrunk.plan"), s).map_err(|e| e.to_string())?;
        }
        let j = serde_json::to_string_pretty(runs).map_err(|e| e.to_string())?;
        std::fs::write(dir.join("runs.json"), j + "\n").map_err(|e| e.to_string())?;
        Ok(dir)
    }

    pub fn entries(&self) -> Result<Vec<(u64, RunsJson)>, String> {
        let mut out = Vec::new();
        if !self.root.exists() {
            return Ok(out);
        }
        let mut seeds: Vec<u64> = Vec::new();
        for e in std::fs::read_dir(&self.root).map_err(|e| e.to_string())? {
            let e = e.map_err(|e| e.to_string())?;
            if let Ok(seed) = e.file_name().to_string_lossy().parse::<u64>() {
                seeds.push(seed);
            }
        }
        seeds.sort_unstable(); // deterministic listing order
        for seed in seeds {
            let p = self.seed_dir(seed).join("runs.json");
            let text = std::fs::read_to_string(&p).map_err(|e| format!("{}: {}", p.display(), e))?;
            let runs: RunsJson =
                serde_json::from_str(&text).map_err(|e| format!("{}: {}", p.display(), e))?;
            out.push((seed, runs));
        }
        Ok(out)
    }

    pub fn load_plan(&self, seed: u64, shrunk: bool) -> Result<String, String> {
        let name = if shrunk { "shrunk.plan" } else { "plan.plan" };
        let p = self.seed_dir(seed).join(name);
        std::fs::read_to_string(&p).map_err(|e| format!("{}: {}", p.display(), e))
    }

    /// Entries whose signature key contains `filter` (substring match on
    /// `class|sqlstate|site`).
    pub fn matching(&self, filter: &str) -> Result<Vec<(u64, RunsJson)>, String> {
        Ok(self
            .entries()?
            .into_iter()
            .filter(|(_, r)| r.signature.to_key().contains(filter))
            .collect())
    }
}

pub fn now_utc_string() -> String {
    #[cfg(feature = "sim")]
    {
        // Logical stamp under sim builds: byte-stable artifacts (§0 A3).
        return "sim-logical-time".to_string();
    }
    #[cfg(not(feature = "sim"))]
    {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        format!("epoch:{}", d.as_secs())
    }
}
