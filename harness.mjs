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
  // The re-arm and the widget filter are scoped to the CURRENT session: without
  // a session id here every job would be skipped, and the harness could not tell
  // a broken re-arm from a missing mock.
  sessionManager: { getSessionId: () => 'harness-session' },
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

// ---- 11. RIARMO DOPO UN RIAVVIO: un job col deadline ancora NEL FUTURO ----
// Un processo nuovo non ha nessun task vivo per nessun job: l'evento terminale
// che riarma viaggia su un EventBus IN-PROCESS e muore col processo precedente.
// Prima del fix il riarmo scartava l'id stantio SOLO se il job era gia' scaduto:
// un job col deadline ancora davanti teneva l'id, la guardia `if
// (!job.scheduledTaskId)` lo saltava, e non scattava mai piu'.
const jobFile2 = path.join(fakeHome, '.pi', 'timers', 'recurring-jobs.json');
const futureJob = {
  id: 'future-job', label: 'FUTURE', intervalSeconds: 3600, wakePrompt: 'x',
  active: true, nextRunAt: Date.now() + 3600_000, createdAt: Date.now(),
  lastRunAt: null, scheduledTaskId: 'task-from-a-dead-process',
  ownerSessionId: 'harness-session',
};
fs.writeFileSync(jobFile2, JSON.stringify([futureJob]));
const beforeRestart = scheduledTasks.length;
await lifecycle.session_start({ reason: 'resume' }, ctx);
await new Promise((r) => setTimeout(r, 300));
check(
  'BUG1: un job col deadline nel futuro viene RIARMATO al riavvio',
  scheduledTasks.length === beforeRestart + 1,
  `scheduled=${scheduledTasks.length} (atteso ${beforeRestart + 1})`,
);
let afterRestart = [];
try { afterRestart = JSON.parse(fs.readFileSync(jobFile2, 'utf-8')); } catch (err) {
  check('BUG1: stato del job rileggibile dopo il riavvio', false, 'JSON illeggibile: ' + err.message);
}
check(
  'BUG1: il deadline futuro NON viene spostato (non si salta l\'occorrenza)',
  afterRestart[0]?.nextRunAt === futureJob.nextRunAt,
  `nextRunAt=${afterRestart[0]?.nextRunAt} atteso=${futureJob.nextRunAt}`,
);
check(
  'BUG1: l\'id stantio e\' stato sostituito da un task nuovo',
  afterRestart[0]?.scheduledTaskId !== 'task-from-a-dead-process',
  `scheduledTaskId=${afterRestart[0]?.scheduledTaskId}`,
);

// ---- 12. TIMER SCADUTO: la pulizia non deve dipendere dalla UI ----
// La pulizia stava DENTRO updateTuiWidget, dopo la guardia `if (!currentCtx ||
// !currentCtx.hasUI) return;`: senza UI usciva prima e il timer scaduto restava
// nel file per sempre (misurato: mtime fermo a 10 ore prima, timer di ieri dentro).
const timersFile = path.join(fakeHome, '.pi', 'timers', 'active-timers.json');
fs.writeFileSync(timersFile, JSON.stringify([{
  id: 'stale-timer', label: 'STALE', targetTimestamp: Date.now() - 60_000,
  totalSeconds: 60, sessionPid: 999_999, hasWakePrompt: false,
  sessionId: 'harness-session',
}]));
const ctxNoUI = { ...ctx, hasUI: false };
await lifecycle.session_start({ reason: 'resume' }, ctxNoUI);
await new Promise((r) => setTimeout(r, 250));
let leftTimers = [];
try { leftTimers = JSON.parse(fs.readFileSync(timersFile, 'utf-8')); } catch { leftTimers = []; }
check(
  'BUG2: un timer scaduto viene tolto anche SENZA UI',
  leftTimers.length === 0,
  `rimasti=${leftTimers.length}`,
);

// ---- 13. DIAGNOSTICI: le decisioni finiscono nel log ----
// `debugLog` e' dietro cfg.debug e il log non esisteva: l'estensione decideva
// senza lasciare traccia, ed e' il motivo per cui il sintomo e' sopravvissuto.
const logFile = path.join(fakeHome, '.pi', 'timers', 'cron-bg.log');
let logText = '';
try { logText = fs.readFileSync(logFile, 'utf-8'); } catch { /* assente */ }
check('BUG3: cron-bg.log esiste e non e\' vuoto', logText.length > 0, `bytes=${logText.length}`);
check(
  'BUG3: il log dice che un job e\' stato riarmato',
  /re-?arm|riarm/i.test(logText),
  logText.split('\n').filter(Boolean).slice(-1)[0] ?? '',
);

// ---- 14. ISOLAMENTO E PULIZIA ORFANI: nessuna adozione cross-sessione ----
// Un job appartiene SOLO alla sessione che lo ha creato. Una sessione diversa
// o un avvio con /new NON deve MAI adottare job altrui.
// Gli orfani con pid morto o senza sessione vengono disattivati e prunati
// all'avvio per mantenere pulito il file di stato.
const jobFile3 = path.join(fakeHome, '.pi', 'timers', 'recurring-jobs.json');
const deadPidJob = {
  id: 'dead-owner', label: 'DEAD-OWNER', intervalSeconds: 3600, wakePrompt: 'x',
  active: true, nextRunAt: Date.now() - 60_000, createdAt: Date.now(),
  lastRunAt: null, scheduledTaskId: 'task-of-a-dead-process',
  ownerSessionId: 'a-dead-session', ownerPid: 999_999,
};
const livePidJob = {
  id: 'live-owner', label: 'LIVE-OWNER', intervalSeconds: 3600, wakePrompt: 'x',
  active: true, nextRunAt: Date.now() + 3600_000, createdAt: Date.now(),
  lastRunAt: null, scheduledTaskId: null,
  ownerSessionId: 'a-live-session', ownerPid: process.pid,
};
const noPidJob = {
  id: 'legacy-no-pid', label: 'LEGACY-NO-PID', intervalSeconds: 3600, wakePrompt: 'x',
  active: true, nextRunAt: Date.now() + 3600_000, createdAt: Date.now(),
  lastRunAt: null, scheduledTaskId: null,
  ownerSessionId: 'a-live-session',
};
// Legacy orphan job without owner
const legacyOverdueJob = {
  id: 'legacy-overdue', label: 'LEGACY-OVERDUE', intervalSeconds: 3600, wakePrompt: 'x',
  active: true, nextRunAt: Date.now() - 60_000, createdAt: Date.now() - 23 * 3600_000,
  lastRunAt: null, scheduledTaskId: 'task-of-a-dead-legacy',
  ownerSessionId: '',
};
fs.writeFileSync(jobFile3, JSON.stringify([deadPidJob, livePidJob, noPidJob, legacyOverdueJob]));
const beforeAdopt = scheduledTasks.length;
// `new`: avvio pulito, non deve adottare NESSUN job orfano
await lifecycle.session_start({ reason: 'new' }, ctx);
await new Promise((r) => setTimeout(r, 300));
check(
  'ISOLAMENTO: nessun job orfano viene adottato o rischedulato su /new',
  scheduledTasks.length === beforeAdopt,
  `scheduled=${scheduledTasks.length} (atteso ${beforeAdopt})`,
);
let afterAdopt = [];
try { afterAdopt = JSON.parse(fs.readFileSync(jobFile3, 'utf-8')); } catch { afterAdopt = []; }
const byId = (id) => afterAdopt.find((j) => j.id === id);
check(
  'PULIZIA: il job col pid morto viene rimosso (prunato)',
  byId('dead-owner') === undefined,
  `dead-owner=${byId('dead-owner')?.id}`,
);
check(
  'PULIZIA: il job orfano legacy viene rimosso',
  byId('legacy-overdue') === undefined,
  `legacy-overdue=${byId('legacy-overdue')?.id}`,
);
check(
  'PROTEZIONE: il job di una sessione VIVA non viene toccato',
  byId('live-owner')?.ownerSessionId === 'a-live-session' && byId('live-owner')?.ownerPid === process.pid,
  `owner=${byId('live-owner')?.ownerSessionId}`,
);
check(
  'PROTEZIONE: il job di una sessione altrui senza pid non viene toccato se ha ownerSessionId',
  byId('legacy-no-pid')?.ownerSessionId === 'a-live-session',
  `owner=${byId('legacy-no-pid')?.ownerSessionId}`,
);

// ---- 14b. RESUME: la sessione riarma SOLO i PROPRI job ----
const ownResumeJob = {
  id: 'own-resume-job', label: 'OWN-JOB', intervalSeconds: 1800, wakePrompt: 'wake',
  active: true, nextRunAt: Date.now() - 1000, createdAt: Date.now() - 2000,
  lastRunAt: null, scheduledTaskId: 'stale-task',
  ownerSessionId: 'harness-session', ownerPid: 888_888,
};
fs.writeFileSync(jobFile3, JSON.stringify([ownResumeJob, livePidJob]));
const beforeResume = scheduledTasks.length;
await lifecycle.session_start({ reason: 'resume' }, ctx);
await new Promise((r) => setTimeout(r, 300));
check(
  'RESUME: la sessione riarma il proprio job su resume',
  scheduledTasks.length === beforeResume + 1,
  `scheduled=${scheduledTasks.length} (atteso ${beforeResume + 1})`,
);
let afterResume = [];
try { afterResume = JSON.parse(fs.readFileSync(jobFile3, 'utf-8')); } catch { afterResume = []; }
const ownJobAfter = afterResume.find((j) => j.id === 'own-resume-job');
check(
  'RESUME: il pid del proprio job viene aggiornato al processo corrente',
  ownJobAfter?.ownerPid === process.pid,
  `pid=${ownJobAfter?.ownerPid} atteso=${process.pid}`,
);

// ---- 15. PRUNE: anche un pid morto rende un job rimovibile ----
fs.writeFileSync(jobFile3, JSON.stringify([deadPidJob, livePidJob]));
await tools.cron_bg_repeat.execute(null, { action: 'prune' }, null, null, ctx);
await new Promise((r) => setTimeout(r, 200));
let afterPrune = [];
try { afterPrune = JSON.parse(fs.readFileSync(jobFile3, 'utf-8')); } catch { afterPrune = []; }
check(
  'PRUNE: il job col pid morto sparisce, quello col pid vivo resta',
  afterPrune.length === 1 && afterPrune[0]?.id === 'live-owner',
  `rimasti=${afterPrune.map((j) => j.id).join(',')}`,
);

// ---- 16. SLASH COMMAND: /cron-repeat-prune ----
check('comando /cron-repeat-prune registrato', typeof commands['cron-repeat-prune']?.handler === 'function');
fs.writeFileSync(jobFile3, JSON.stringify([deadPidJob, livePidJob]));
await commands['cron-repeat-prune'].handler('', ctx);
let afterSlashPrune = [];
try { afterSlashPrune = JSON.parse(fs.readFileSync(jobFile3, 'utf-8')); } catch { afterSlashPrune = []; }
check(
  'COMMAND /cron-repeat-prune: rimuove il job morto e preserva quello vivo',
  afterSlashPrune.length === 1 && afterSlashPrune[0]?.id === 'live-owner',
  `rimasti=${afterSlashPrune.map((j) => j.id).join(',')}`,
);

// ---- 17. SLASH COMMAND: /cron-repeat-list filtri di sessione ----
check('comando /cron-repeat-list registrato', typeof commands['cron-repeat-list']?.handler === 'function');
let lastNotify = '';
const notifyCtx = {
  ...ctx,
  ui: { ...ctx.ui, notify: (msg) => { lastNotify = msg; } },
};
await commands['cron-repeat-list'].handler('', notifyCtx);
check(
  'COMMAND /cron-repeat-list: non mostra i job di altre sessioni (live-owner nascosto)',
  !lastNotify.includes('LIVE-OWNER') && (lastNotify.includes('hidden') || lastNotify.includes('Nessun') || lastNotify.includes('No')),
  lastNotify,
);

const failed = results.filter((r) => !r.ok);
console.log(`\n=== ESITO: ${results.length - failed.length}/${results.length} PASS ===`);
fs.rmSync(workRoot, { recursive: true, force: true });
process.exit(failed.length === 0 ? 0 : 1);
