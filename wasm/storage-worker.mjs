// storage-worker.mjs — Node's entry for the `--fs broker` storage coordinator.
// Node resolves a `.mjs` worker entry as ESM without leaning on syntax detection; the browser
// uses storage-worker.js directly as a module worker. All of the logic lives in that one file.
import './storage-worker.js';
