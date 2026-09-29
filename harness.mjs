// Regression test per pi-cron-bg / cron_bg_repeat.
//
// Verifica il ciclo: start -> task schedulato -> evento terminale -> RIARMO.
// Mocks the Pi API + EventBus, isolates HOME in a temp dir (does not touch real jobs).
//
// Uso:  node harness.mjs
//
// Nota: estensione .mjs => Pi NON la carica come estensione (carica solo .ts/.js).
import { execSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';

let EXT_DIR;
try {
  EXT_DIR = path.dirname(new URL(import.meta.url).pathname);
} catch (err) {
  console.error('Impossibile risolvere la directory dello script:', err);
  process.exit(2);
}
const EXT_INDEX = path.join(EXT_DIR, 'index.ts');

const REQ = 'pi-background-tasks:request:v1';
const RES = 'pi-background-tasks:response:v1';
const TERM = 'pi-background-tasks:terminal:v1';

// ---- 1. Isola HOME (os.homedir() legge process.env.HOME) ----
const workRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-harness-'));
const fakeHome = path.join(workRoot, 'home');
fs.mkdirSync(path.join(fakeHome, '.pi', 'timers'), { recursive: true });
process.env.HOME = fakeHome;

// ---- 2. Workspace con deps risolvibili (typebox, jiti) ----
const ws = path.join(workRoot, 'ws');
fs.mkdirSync(path.join(ws, 'node_modules'), { recursive: true });
fs.copyFileSync(EXT_INDEX, path.join(ws, 'index.ts'));

function resolvePiNodeModules() {
  let piBin;
  try {
    piBin = execSync('command -v pi', { encoding: 'utf-8' }).trim();
  } catch (err) {
    throw new Error('"pi" binary not found in PATH: ' + err.message);
  }
  const real = fs.realpathSync(piBin);
  const idx = real.indexOf(`${path.sep}node_modules${path.sep}`);
  if (idx === -1) throw new Error("Pi's node_modules not found from: " + real);
  return real.slice(0, idx + '/node_modules'.length);
}
const NM = resolvePiNodeModules();
const piPkgNM = path.join(NM, '@earendil-works', 'pi-coding-agent', 'node_modules');
for (const dep of ['typebox', 'jiti']) {
  fs.symlinkSync(path.join(piPkgNM, dep), path.join(ws, 'node_modules', dep), 'dir');
}

// ---- 3. EventBus mock ----
const listeners = new Map();
const bus = {
  on(channel, cb) {
    if (!listeners.has(channel)) listeners.set(channel, new Set());
    listeners.get(channel).add(cb);
    return () => listeners.get(channel).delete(cb);
  },
  emit(channel, data) {
    for (const cb of [...(listeners.get(channel) ?? [])]) cb(data);
  },
};

// ---- 4. Mock pi-background-tasks: risponde a ogni 'run' con un task id ----
let taskCounter = 0;
const scheduledTasks = [];
bus.on(REQ, (frame) => {
  if (frame?.operation !== 'run') return;
  taskCounter += 1;
  const id = `task-${taskCounter}`;
  scheduledTasks.push({ id, command: frame.payload.command, name: frame.payload.name });
  setImmediate(() =>
    bus.emit(RES, { request_id: frame.request_id, ok: true, result: { id } }),
  );
});

// ---- 5. Mock API Pi ----
const tools = {};
const commands = {};
const lifecycle = {};
const ctx = {
  hasUI: true,
  ui: { setWidget() {}, setStatus() {}, notify() {}, theme: { fg: (_c, s) => s } },
};
const pi = {
  events: bus,
  on: (ev, cb) => { lifecycle[ev] = cb; },
  registerTool: (t) => { tools[t.name] = t; },
  registerCommand: (n, c) => { commands[n] = c; },
};

// ---- 6. Carica l'estensione e avvia ----
const jitiEntry = pathToFileURL(path.join(piPkgNM, 'jiti', 'lib', 'jiti.mjs')).href;
const { createJiti } = await import(jitiEntry);
const jiti = createJiti(import.meta.url);
const mod = await jiti.import(path.join(ws, 'index.ts'));
mod.default(pi);
if (lifecycle.session_start) await lifecycle.session_start({}, ctx);

const results = [];
function check(name, cond, extra = '') {
  results.push({ name, ok: !!cond });
  console.log(`${cond ? 'PASS' : 'FAIL'} | ${name}${extra ? ' | ' + extra : ''}`);
}

check('tool cron_bg_repeat registrato', !!tools.cron_bg_repeat);
check('tool cron_bg_wait registrato', !!tools.cron_bg_wait);

// ---- 7. start ----
const startRes = await tools.cron_bg_repeat.execute(
  null, { action: 'start', duration: '1s', label: 'HARNESS', wakePrompt: 'test harness' }, null, null, ctx,
);
check('start ok', startRes?.details?.ok, JSON.stringify(startRes?.details));
check('task schedulato dopo start', scheduledTasks.length === 1, `scheduled=${scheduledTasks.length}`);
check('comando usa sleep', /sleep 1 /.test(scheduledTasks[0]?.command || ''), scheduledTasks[0]?.command);

// Attende che la persistenza asincrona di scheduledTaskId sia completata
// (scheduleRecurringJob risolve dopo setImmediate -> microtask).
await new Promise((r) => setTimeout(r, 50));

// ---- 8. evento terminale => RIARMO (il cuore del fix) ----
bus.emit(TERM, { schema_version: 'pi-background-tasks.extension-terminal.v1', task: { id: scheduledTasks[scheduledTasks.length - 1].id, status: 'completed' } });
await new Promise((r) => setTimeout(r, 300));
check('RIARMO: nuovo task dopo il completamento', scheduledTasks.length === 2, `scheduled=${scheduledTasks.length}`);

// ---- 9. persistenza su disco ----
const jobFile = path.join(fakeHome, '.pi', 'timers', 'recurring-jobs.json');
let jobs;
try {
  jobs = JSON.parse(fs.readFileSync(jobFile, 'utf-8'));
} catch (err) {
  check('job persistito su disco', false, 'JSON illeggibile: ' + err.message);
  process.exit(1);
}
check('job persistito su disco', Array.isArray(jobs) && jobs.length === 1);
check('lastRunAt valorizzato', jobs[0]?.lastRunAt != null, `lastRunAt=${jobs[0]?.lastRunAt}`);
check('nextRunAt avanzato di un intervallo', jobs[0]?.nextRunAt - jobs[0]?.createdAt === 2000, `delta=${jobs[0]?.nextRunAt - jobs[0]?.createdAt}`);

// ---- 10. stop => niente riarmo ----
const stopRes = await tools.cron_bg_repeat.execute(null, { action: 'stop', jobId: jobs[0].id }, null, null, ctx);
check('stop ok', stopRes?.details?.ok);
const before = scheduledTasks.length;
bus.emit(TERM, { schema_version: 'pi-background-tasks.extension-terminal.v1', task: { id: scheduledTasks[scheduledTasks.length - 1].id, status: 'completed' } });
await new Promise((r) => setTimeout(r, 300));
check('after stop it does NOT re-arm', scheduledTasks.length === before, `scheduled=${scheduledTasks.length}`);

const failed = results.filter((r) => !r.ok);
console.log(`\n=== ESITO: ${results.length - failed.length}/${results.length} PASS ===`);
fs.rmSync(workRoot, { recursive: true, force: true });
process.exit(failed.length === 0 ? 0 : 1);
