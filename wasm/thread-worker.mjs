// thread-worker.mjs — Node's entry for the wasm32-wasip1-threads worker.
// Node resolves a `.mjs` worker entry as ESM without leaning on syntax
// detection; the browser uses thread-worker.js directly as a module worker.
// All of the logic lives in that one file.
import './thread-worker.js';
