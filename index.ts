import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent';
import { Type } from 'typebox';
import { randomUUID } from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';

// EventBus channels exposed by pi-background-tasks
const BG_REQUEST_CHANNEL = 'pi-background-tasks:request:v1';
const BG_RESPONSE_CHANNEL = 'pi-background-tasks:response:v1';
const BG_STATUS_CHANNEL = 'pi-background-tasks:terminal:v1';
const BG_REQUEST_SCHEMA = 'pi-background-tasks.extension-request.v1';

// Shared registry file for one-shot timers and recurring jobs
const SHARED_TIMERS_DIR = path.join(os.homedir(), '.pi', 'timers');
const SHARED_TIMERS_FILE = path.join(SHARED_TIMERS_DIR, 'active-timers.json');
const RECURRING_JOBS_FILE = path.join(SHARED_TIMERS_DIR, 'recurring-jobs.json');

interface SharedTimerEntry {
  id: string;
  label: string;
  targetTimestamp: number;
  totalSeconds: number;
  sessionPid: number;
  hasWakePrompt: boolean;
  /** Id of the session that created the timer (timers are not shared across sessions). */
  sessionId?: string;
}

interface RecurringJob {
  id: string;
  label: string;
  intervalSeconds: number;
  wakePrompt: string;
  active: boolean;
  nextRunAt: number;
  createdAt: number;
  lastRunAt: number | null;
  scheduledTaskId: string | null;
  /**
   * Id of the job's owning session (SessionHeader.id: stable on resume,
   * different on /new and /fork). Jobs are NOT shared across sessions: only the
   * owning session re-arms them. A job without an owner is an "orphan" (legacy).
   */
  ownerSessionId?: string;
  /**
   * Consecutive failed scheduling attempts. Bounded so a job cannot spin
   * forever when the background-task backend is unavailable: at the cap the
   * job is deactivated instead of retried, and the count resets on success.
   */
  retryCount?: number;
  /** Why the job was deactivated by the retry cap, for `list` output. */
  disabledReason?: string;
}

function ensureTimersDir() {
  if (!fs.existsSync(SHARED_TIMERS_DIR)) {
    try {
      fs.mkdirSync(SHARED_TIMERS_DIR, { recursive: true });
    } catch {}
  }
}

function readSharedTimers(): Map<string, SharedTimerEntry> {
  ensureTimersDir();
  const map = new Map<string, SharedTimerEntry>();
  if (!fs.existsSync(SHARED_TIMERS_FILE)) return map;
  try {
    const raw = fs.readFileSync(SHARED_TIMERS_FILE, 'utf-8');
    const data = JSON.parse(raw);
    if (Array.isArray(data)) {
      const now = Date.now();
      for (const item of data) {
        if (item && item.id && item.targetTimestamp > now - 2000) {
          map.set(item.id, item);
        }
      }
    }
  } catch {}
  return map;
}

/**
 * Atomic write: temp file + rename.
 *
 * Without this, a crash or a concurrent read during the write leaves a
 * truncated JSON file. The consequence is that readSharedTimers() calls
 * JSON.parse on partial content, the silent catch discards ALL timers, and
 * surviving recurring jobs are lost on reload. rename() is atomic on POSIX.
 */
function writeJsonAtomic(filePath: string, value: unknown) {
  ensureTimersDir();
  const tmpPath = `${filePath}.${process.pid}.tmp`;
  try {
    fs.writeFileSync(tmpPath, JSON.stringify(value, null, 2), 'utf-8');
    fs.renameSync(tmpPath, filePath);
  } catch {
    // clean up the temp file on failure
    try { if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath); } catch {}
  }
}

function writeSharedTimers(map: Map<string, SharedTimerEntry>) {
  writeJsonAtomic(SHARED_TIMERS_FILE, Array.from(map.values()));
}

function readRecurringJobs(): Map<string, RecurringJob> {
  ensureTimersDir();
  const map = new Map<string, RecurringJob>();
  if (!fs.existsSync(RECURRING_JOBS_FILE)) return map;
  try {
    const raw = fs.readFileSync(RECURRING_JOBS_FILE, 'utf-8');
    const data = JSON.parse(raw);
    if (Array.isArray(data)) {
      for (const item of data) {
        if (item && item.id) {
          map.set(item.id, item);
        }
      }
    }
  } catch {}
  return map;
}

function writeRecurringJobs(map: Map<string, RecurringJob>) {
  writeJsonAtomic(RECURRING_JOBS_FILE, Array.from(map.values()));
}

// ---------------------------------------------------------------------------
// Cross-process lock
//
// recurring-jobs.json is shared by every Pi session, i.e. by several OS
// processes. Each mutation is a read-modify-write, so two sessions that
// interleave lose one side's change: the loser writes back a snapshot taken
// before the winner's write, and the winner's job vanishes from disk with no
// error. Verified: A reads, B writes, A writes -> B's job is gone.
//
// The lock is an exclusive-create file. Sleeps use Atomics.wait so the wait
// blocks this thread instead of spinning on the event loop.
// ---------------------------------------------------------------------------

const JOBS_LOCK_FILE = path.join(SHARED_TIMERS_DIR, '.recurring-jobs.lock');
/** Give up rather than hang the extension forever. */
const LOCK_TIMEOUT_MS = 2000;
/** A lock older than this is treated as abandoned (crashed holder). */
const LOCK_STALE_MS = 10_000;

const sleepBuffer = new Int32Array(new SharedArrayBuffer(4));

function sleepSync(ms: number) {
  try {
    Atomics.wait(sleepBuffer, 0, 0, ms);
  } catch {
    // Atomics.wait is unavailable on the main thread in some runtimes: fall
    // back to a busy spin, bounded and short.
    const until = Date.now() + ms;
    while (Date.now() < until) { /* spin */ }
  }
}

/** Runs fn while holding the exclusive lock. Always releases. */
function withJobsLock<T>(fn: () => T): T {
  ensureTimersDir();
  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  let fd: number | null = null;

  for (;;) {
    try {
      fd = fs.openSync(JOBS_LOCK_FILE, 'wx');
      break;
    } catch {
      // Break an abandoned lock, otherwise the process died holding it.
      try {
        const st = fs.statSync(JOBS_LOCK_FILE);
        if (Date.now() - st.mtimeMs > LOCK_STALE_MS) {
          fs.unlinkSync(JOBS_LOCK_FILE);
          continue;
        }
      } catch { /* vanished under us, retry immediately */ }
      if (Date.now() > deadline) {
        // Proceed unlocked rather than lose the operation entirely: a stale
        // snapshot is still better than a hard failure, and the window is
        // sub-millisecond in practice.
        return fn();
      }
      sleepSync(5);
    }
  }

  try {
    return fn();
  } finally {
    try {
      if (fd !== null) fs.closeSync(fd);
    } catch { /* best-effort */ }
    try {
      fs.unlinkSync(JOBS_LOCK_FILE);
    } catch { /* best-effort */ }
  }
}

/**
 * Read-modify-write of the job registry under the cross-process lock, so the
 * mutation is applied to the freshest snapshot rather than a stale one.
 */
function mutateJobs<T>(mutate: (jobs: Map<string, RecurringJob>) => T): T {
  return withJobsLock(() => {
    const jobs = readRecurringJobs();
    const result = mutate(jobs);
    writeRecurringJobs(jobs);
    return result;
  });
}

/** Consecutive failed scheduling attempts before a job is disabled. */
const RETRY_LIMIT = 5;
/** Base delay for the scheduling retry backoff. */
const RETRY_BASE_MS = 30_000;
/** Backoff ceiling, so a long outage does not push retries hours apart. */
const RETRY_MAX_MS = 10 * 60_000;

/** Exponential backoff: 30s, 60s, 120s, 240s, ... capped at RETRY_MAX_MS. */
function retryDelayMs(attempt: number): number {
  const exp = RETRY_BASE_MS * Math.pow(2, Math.max(0, attempt - 1));
  return Math.min(RETRY_MAX_MS, Math.round(exp));
}

const MIN_DURATION_SECONDS = 1;
const MAX_DURATION_SECONDS = 365 * 24 * 3600; // one year, to avoid overflow

// ---------------------------------------------------------------------------
// i18n
// ---------------------------------------------------------------------------

type Lang = 'en' | 'it';

const FALLBACK: Lang = 'en';

function primaryOf(tag: string): string | null {
  if (typeof tag !== 'string') return null;
  // Same regex as pi-anti-amnesia/i18n.mjs. A naive split on "." returns
  // "it_it" for "it_IT.UTF-8" and "it-it" for "it-IT": neither is a supported
  // language, so the value was skipped and the Intl fallback was skipped too,
  // making an Italian box resolve to English.
  const m = /^\s*([A-Za-z]{2,3})(?:-|_)/.exec(tag) ?? /^\s*([A-Za-z]{2,3})\s*$/.exec(tag);
  return m ? m[1].toLowerCase() : null;
}

function isSupported(primary: string): primary is Lang {
  return primary === 'en' || primary === 'it';
}

/**
 * Resolves the system language once, at module load.
 *
 * Mirrors pi-anti-amnesia's i18n.resolveLanguage() on purpose: within one Pi
 * session several extensions emit prompts, and a model fed mixed-language
 * instructions degrades. An unsupported locale must therefore SKIP to the next
 * signal (and finally to Intl) rather than short-circuit to English, otherwise
 * pi-cron-bg would answer "en" on an it-IT box while pi-anti-amnesia answers
 * "it" for the very same environment.
 *
 * Order: LC_ALL > LC_MESSAGES > LANG > LANGUAGE, then Intl, then English.
 */
function detectLang(): Lang {
  const env = process.env;
  for (const name of ['LC_ALL', 'LC_MESSAGES', 'LANG', 'LANGUAGE']) {
    const tag = env?.[name];
    if (!tag || tag === 'C' || tag === 'POSIX') continue;
    const primary = primaryOf(tag);
    if (isSupported(primary)) return primary;
  }
  try {
    const icu = primaryOf(Intl.DateTimeFormat().resolvedOptions().locale ?? '');
    if (isSupported(icu)) return icu;
  } catch { /* no ICU data */ }
  return FALLBACK;
}

const LANG: Lang = detectLang();

/**
 * Localised catalog. Every user-facing default lives here, so a prompt is
 * never half Italian and half English: the whole wake reminder follows the
 * system language.
 */
const I18N = {
  en: {
    noRecurringJobs: 'No recurring job active.',
    recurringJobsList: 'Recurring jobs:',
    defaultLabel: 'Recurring',
    recurringAction: (label: string) => `Recurring action: ${label}`,
    defaultWaitLabel: (duration: string) => `Wait (${duration})`,
    recurringWakeHeader: (seconds: number) => `[RECURRING WAKE RECOVERY (${seconds}s)]`,
    waitWakeHeader: (seconds: number) => `[WAKE TIMER EXPIRED (${seconds}s)]`,
    contextReminderHeader: '--- CONTEXT REMINDER ON WAKE ---',
    bgConfirmTimeout: 'Timeout waiting for confirmation from pi-background-tasks',
  },
  it: {
    noRecurringJobs: 'Nessun job ricorrente attivo.',
    recurringJobsList: 'Job ricorrenti attivi:',
    defaultLabel: 'Ricorrente',
    recurringAction: (label: string) => `Azione ricorrente: ${label}`,
    defaultWaitLabel: (duration: string) => `Pausa (${duration})`,
    recurringWakeHeader: (seconds: number) => `[SVEGLIA RECUPERO RECURRENT (${seconds}s)]`,
    waitWakeHeader: (seconds: number) => `[SVEGLIA TIMER CRON SCADUTO (${seconds}s)]`,
    contextReminderHeader: '--- PROMEMORIA CONTESTO RISVEGLIO ---',
    bgConfirmTimeout: 'Timeout attesa conferma da pi-background-tasks',
  },
} as const satisfies Record<Lang, {
  noRecurringJobs: string;
  recurringJobsList: string;
  defaultLabel: string;
  recurringAction: (label: string) => string;
  defaultWaitLabel: (duration: string) => string;
  recurringWakeHeader: (seconds: number) => string;
  waitWakeHeader: (seconds: number) => string;
  contextReminderHeader: string;
  bgConfirmTimeout: string;
}>;

/** Localised string for the active language. */
function t<K extends keyof (typeof I18N)['en']>(key: K): (typeof I18N)['en'][K] {
  return I18N[LANG][key];
}

function parseDurationToSeconds(duration: string): number {
  const match = duration.trim().match(/^(\d+)\s*(s|m|h)?$/i);
  if (!match) {
    const parsed = parseInt(duration, 10);
    if (!isNaN(parsed) && parsed > 0) return parsed;
    throw new Error(`Invalid duration: "${duration}". Use formats like "10m", "600s", "1h" or a number of seconds.`);
  }
  const value = parseInt(match[1], 10);
  const unit = (match[2] || 's').toLowerCase();
  let seconds: number;
  switch (unit) {
    case 'h': seconds = value * 3600; break;
    case 'm': seconds = value * 60; break;
    case 's':
    default: seconds = value;
  }
  if (seconds < MIN_DURATION_SECONDS) {
    throw new Error(`Minimum allowed duration: ${MIN_DURATION_SECONDS}s. You specified ${seconds}s.`);
  }
  if (seconds > MAX_DURATION_SECONDS) {
    throw new Error(`Maximum allowed duration: ${MAX_DURATION_SECONDS}s (${365} days). You specified ${seconds}s.`);
  }
  return seconds;
}

function formatRemaining(secondsLeft: number): string {
  if (secondsLeft <= 0) return '0s';
  const m = Math.floor(secondsLeft / 60);
  const s = secondsLeft % 60;
  if (m > 0) {
    return `${m}m${s > 0 ? `${s}s` : ''}`;
  }
  return `${s}s`;
}

/**
 * Neutralizes a string for use inside POSIX single quotes.
 * Escapes both the closing character and backslashes, otherwise a label
 * or a prompt containing an apostrophe can close the string and inject
 * arbitrary commands into the shell that will be executed.
 */
function shellQuote(value: string): string {
  return value.replace(/['\\]/g, (c) => `'\\${c}'`);
}

function buildWakeShell(seconds: number, label: string, wakePrompt: string): string {
  // seconds comes from parseDurationToSeconds (integer) but stays defensive:
  // a non-numeric value here would end up verbatim in the shell line.
  const safeSeconds = Number.isFinite(seconds) ? Math.max(0, Math.floor(seconds)) : 0;
  const safeLabel = shellQuote(label.trim().slice(0, 120));
  const safePrompt = shellQuote(wakePrompt.trim().slice(0, 2000));
  return `sleep ${safeSeconds} && printf '%s\\n' '${t('recurringWakeHeader')(safeSeconds)}' '--- ${safeLabel} ---' '${safePrompt}'`;
}

export default function (pi: ExtensionAPI) {
  let currentCtx: ExtensionContext | undefined;
  let updateInterval: NodeJS.Timeout | undefined;
  let currentSessionId: string | undefined;
  /**
   * True when this extension instance has lost its pi/ctx (reload or
   * session replacement). From that moment on it must no longer touch `pi`/`ctx`.
   */
  let disposed = false;
  /** Runtime generation: incremented on every session (re)start. */
  let generation = 0;
  const pendingTimeouts = new Set<NodeJS.Timeout>();

  function clearPendingTimeouts() {
    for (const t of pendingTimeouts) {
      try { clearTimeout(t); } catch { /* best-effort cleanup */ }
    }
    pendingTimeouts.clear();
  }

  /**
   * Detects the "stale" state of the Pi runtime: after reload/replacement the
   * runtime is invalidated and any call to `pi.events.*`/`ctx.*` throws
   * "This extension ctx is stale after session replacement or reload".
   * We run a harmless probe: `pi.events.on` for a dummy channel is rejected
   * by `assertActive()` (no side effects if still active).
   */
  function isStale(): boolean {
    if (disposed) return true;
    try {
      const probe = pi.events.on('pi-cron-bg:__stale_probe__', () => {});
      if (typeof probe === 'function') {
        try { probe(); } catch { /* best-effort */ }
      }
      return false;
    } catch {
      return true;
    }
  }

  function trackTimeout(fn: () => void, ms: number): NodeJS.Timeout {
    const t = setTimeout(() => {
      pendingTimeouts.delete(t);
      // Safety shortcut: do not run callbacks on a stale runtime.
      if (disposed) return;
      fn();
    }, ms);
    pendingTimeouts.add(t);
    return t;
  }

  function markStale() {
    disposed = true;
    clearPendingTimeouts();
    if (updateInterval) {
      try { clearInterval(updateInterval); } catch { /* best-effort cleanup */ }
      updateInterval = undefined;
    }
  }

  function readCurrentSessionId(ctx: ExtensionContext | undefined): string | undefined {
    try {
      const sm: any = (ctx as any)?.sessionManager;
      if (sm && typeof sm.getSessionId === 'function') return sm.getSessionId() as string;
    } catch { /* session unavailable */ }
    return undefined;
  }

  /**
   * Does a job belong to this session?
   * - if we cannot determine the current session -> legacy behaviour (treat as ours)
   * - a job without an owner (orphan) belongs to no known session
   */
  function jobIsOurs(job: RecurringJob): boolean {
    // Degraded context: we cannot tell which session we are. Claiming *every*
    // job here would let one session re-arm all 503 jobs of the whole machine.
    // Fall back to true orphans only (no owner), never a foreign-owned job.
    if (!currentSessionId) return !job.ownerSessionId;
    if (!job.ownerSessionId) return false;
    return job.ownerSessionId === currentSessionId;
  }

  function updateTuiWidget() {
    if (disposed) return;
    if (!currentCtx || !currentCtx.hasUI) return;
    try {
      const now = Date.now();
      const map = readSharedTimers();
      let cleaned = false;
      for (const [id, timer] of map.entries()) {
        if (now >= timer.targetTimestamp + 2000) {
          map.delete(id);
          cleaned = true;
        }
      }
      if (cleaned) writeSharedTimers(map);

      const sessionTimers: SharedTimerEntry[] = [];
      for (const timer of map.values()) {
        // Timers are not shared across sessions: scope by sessionId, with
        // a pid fallback for legacy entries that have no sessionId.
        if (currentSessionId && timer.sessionId) {
          if (timer.sessionId !== currentSessionId) continue;
        } else if (timer.sessionPid !== process.pid) {
          continue;
        }
        sessionTimers.push(timer);
      }

      const recurringJobs = readRecurringJobs();
      const activeRecurring: RecurringJob[] = [];
      for (const job of recurringJobs.values()) {
        if (!job.active) continue;
        // Show only THIS session's jobs: no shared/foreign jobs.
        if (job.ownerSessionId !== currentSessionId) continue;
        activeRecurring.push(job);
      }

      if (sessionTimers.length === 0 && activeRecurring.length === 0) {
        currentCtx.ui.setWidget('pi-cron-bg', undefined);
        currentCtx.ui.setStatus('pi-cron-bg', undefined);
        return;
      }

      const parts: string[] = [];
      for (const timer of sessionTimers) {
        const remainingSec = Math.max(0, Math.round((timer.targetTimestamp - now) / 1000));
        parts.push(`⏱ [${timer.label}: ${formatRemaining(remainingSec)}]`);
      }
      for (const job of activeRecurring) {
        const remainingSec = Math.max(0, Math.round((job.nextRunAt - now) / 1000));
        parts.push(`🔁 [${job.label}: ${formatRemaining(remainingSec)}]`);
      }

      const theme = currentCtx.ui.theme;
      const widgetLine = theme.fg('accent', '● ACTIVE TIMERS: ') + theme.fg('warning', parts.join('  '));
      currentCtx.ui.setWidget('pi-cron-bg', [widgetLine]);
      currentCtx.ui.setStatus('pi-cron-bg', theme.fg('accent', parts.join(' | ')));
    } catch {
      // If the runtime really is stale (reload/replacement) disarm; a transient
      // error must not kill the rendering loop.
      if (isStale()) markStale();
    }
  }

  function startUpdateLoop() {
    if (disposed) return;
    if (!updateInterval) {
      const gen = generation;
      updateInterval = setInterval(() => {
        if (disposed || gen !== generation) return;
        updateTuiWidget();
      }, 1000);
      updateTuiWidget();
    }
  }

  function registerTimer(id: string, label: string, seconds: number, hasWakePrompt: boolean) {
    const map = readSharedTimers();
    map.set(id, {
      id,
      label,
      targetTimestamp: Date.now() + seconds * 1000,
      totalSeconds: seconds,
      sessionPid: process.pid,
      hasWakePrompt,
      sessionId: currentSessionId,
    });
    writeSharedTimers(map);
    updateTuiWidget();
  }

  function removeTimer(id: string) {
    const map = readSharedTimers();
    if (map.has(id)) {
      map.delete(id);
      writeSharedTimers(map);
      updateTuiWidget();
    }
  }

  function scheduleRecurringJob(job: RecurringJob) {
    if (disposed) return;
    if (isStale()) { markStale(); return; }
    if (!job.active) return;
    // Never re-arm jobs from other sessions (or orphans) from this instance.
    if (!jobIsOurs(job)) return;
    const gen = generation;
    const delayMs = Math.max(0, job.nextRunAt - Date.now());
    const seconds = Math.max(1, Math.round(delayMs / 1000));
    const taskLabel = job.label;
    const taskName = `CronRepeat: ${taskLabel}`;
    const requestId = randomUUID();

    const runPayload = {
      name: taskName,
      command: buildWakeShell(seconds, taskLabel, job.wakePrompt),
      isAgent: false,
      timeoutSeconds: seconds + 90,
      notifyOnCompletion: true,
      triggerOnCompletion: true,
    };

    const requestFrame = {
      schema_version: BG_REQUEST_SCHEMA,
      request_id: requestId,
      operation: 'run' as const,
      payload: runPayload,
    };

    // 1) Subscribe to the response: access `pi` outside the Promise executor,
    //    so a stale ctx does not produce an unhandled rejected promise.
    let resolveStart: ((v: { ok: boolean; result?: any; error?: string }) => void) | undefined;
    let unsubscribe: (() => void) | undefined;
    try {
      const unsub = pi.events.on(BG_RESPONSE_CHANNEL, (data: any) => {
        if (data && data.request_id === requestId && resolveStart) {
          if (typeof unsub === 'function') {
            try { unsub(); } catch { /* best-effort */ }
          }
          if (data.ok) resolveStart({ ok: true, result: data.result });
          else resolveStart({ ok: false, error: data.error });
        }
      });
      unsubscribe = typeof unsub === 'function' ? unsub : () => {};
    } catch {
      // pi/ctx stale: the instance is dead, no crash and no retry.
      markStale();
      return;
    }

    const startPromise = new Promise<{ ok: boolean; result?: any; error?: string }>((resolve) => {
      resolveStart = resolve;
    });

    // 2) Send the request: protected against a stale ctx.
    try {
      pi.events.emit(BG_REQUEST_CHANNEL, requestFrame);
    } catch {
      try { unsubscribe?.(); } catch { /* best-effort */ }
      markStale();
      return;
    }

    Promise.race([
      startPromise,
      new Promise<{ ok: boolean; error: string }>((_, reject) =>
        trackTimeout(() => reject(new Error('Timeout avvio job ricorrente')), 5000)
      ),
    ])
      .then((outcome) => {
        if (disposed || gen !== generation || isStale()) return;
        const jobs = readRecurringJobs();
        const current = jobs.get(job.id);
        if (!current || !current.active) return;
        if (!outcome.ok) {
          const attempts = (current.retryCount ?? 0) + 1;
          current.retryCount = attempts;
          if (attempts >= RETRY_LIMIT) {
            // Give up. An unbounded retry keeps the session permanently busy —
            // one wakeup every 30s per job, forever — and never surfaces the
            // problem to the operator.
            current.active = false;
            current.disabledReason = `disabled after ${attempts} failed scheduling attempts (last error: ${outcome.error})`;
          }
          current.scheduledTaskId = null;
          mutateJobs((jobs) => { jobs.set(current.id, current); });
          if (current.active) {
            const retryGen = generation;
            trackTimeout(() => {
              if (disposed || retryGen !== generation) return;
              scheduleRecurringJob(current);
            }, retryDelayMs(attempts));
          }
          return;
        }
        const task = outcome.ok && outcome.result ? outcome.result : undefined;
        current.scheduledTaskId = task?.id || requestId;
        current.retryCount = 0;
        current.disabledReason = undefined;
        mutateJobs((jobs) => { jobs.set(current.id, current); });
        updateTuiWidget();
      })
      .catch(() => {
        if (disposed || gen !== generation) return;
        const jobs = readRecurringJobs();
        const current = jobs.get(job.id);
        if (!current) return;
        current.scheduledTaskId = null;
        jobs.set(current.id, current);
        writeRecurringJobs(jobs);
      });
  }

  function handleTaskCompletion(taskId: string, status: string) {
    if (disposed) return;
    if (isStale()) { markStale(); return; }
    if (!taskId || status === 'running') return;
    // Read-modify-write under the cross-process lock, so a concurrent session
    // adding or stopping a job is not lost. The re-arm is deliberately left
    // OUTSIDE the lock: it performs async work and must never hold the lock.
    const outcome = mutateJobs((jobs) => {
      for (const job of jobs.values()) {
        if (job.scheduledTaskId === taskId) {
          job.scheduledTaskId = null;
          job.lastRunAt = Date.now();
          let rearm = false;
          if (job.active && jobIsOurs(job) && (status === 'completed' || status === 'failed' || status === 'killed')) {
            // Skip missed occurrences instead of replaying them. A bare
            // `nextRunAt + interval` walks the schedule forward one slot per
            // completion, so after an N-hour suspension every missed slot fires
            // back-to-back (delayMs clamps to 0 while nextRunAt is in the past).
            // An 8h-old 5-minute heartbeat would spawn 96 immediate tasks.
            const now = Date.now();
            const slot = job.nextRunAt + job.intervalSeconds * 1000;
            job.nextRunAt = slot <= now ? now + job.intervalSeconds * 1000 : slot;
            rearm = true;
          }
          jobs.set(job.id, job);
          return { changed: true, rearm, job };
        }
      }
      return { changed: false, rearm: false, job: null };
    });
    if (outcome.rearm && outcome.job) scheduleRecurringJob(outcome.job);
    if (outcome.changed) updateTuiWidget();

    // One-shot timers: remove from the widget
    const timers = readSharedTimers();
    if (timers.has(taskId) && status !== 'running') {
      removeTimer(taskId);
    }
  }

  // Listen for terminal state updates from pi-background-tasks
  // (real channel: 'pi-background-tasks:terminal:v1', payload { schema_version, task })
  // NB: runtime.invalidate() (reload/replacement) drops all EventBus
  // subscriptions, so this must be (re)registered on every session_start.
  let statusSubscribed = false;
  function subscribeStatusChannel() {
    if (statusSubscribed) return;
    try {
      pi.events.on(BG_STATUS_CHANNEL, (data: any) => {
        if (disposed || !data) return;
        const task = data.task ?? data;
        const taskId = task.id || task.taskId || data.id || data.taskId;
        const status = task.status || data.status;
        if (taskId && status) {
          handleTaskCompletion(taskId, status);
        }
      });
      statusSubscribed = true;
    } catch { /* bus stale: no subscription */ }
  }
  subscribeStatusChannel();

  // Store the context and start the rendering loop.
  // Ownership: recurring jobs belong to ONE session (SessionHeader.id).
  //  - startup / resume / reload -> re-arms ONLY this session's jobs
  //    (legacy orphans without an owner get adopted and marked)
  //  - new / fork                -> starts CLEAN: adopts no existing job
  pi.on('session_start', async (event, ctx) => {
    disposed = false;
    // New generation: invalidates timers/retries captured by the previous one.
    generation += 1;
    statusSubscribed = false;
    subscribeStatusChannel();
    currentCtx = ctx;
    currentSessionId = readCurrentSessionId(ctx);
    clearPendingTimeouts();
    startUpdateLoop();

    const reason = (event as any)?.reason as string | undefined;
    const freshSession = reason === 'new' || reason === 'fork';

    if (!freshSession) {
      const jobs = readRecurringJobs();
      let changed = false;
      for (const job of jobs.values()) {
        if (!job.active) continue;
        // Tight anchoring: re-arms ONLY this session's jobs.
        // A job from another session (or without an owner) is never touched.
        if (!currentSessionId || job.ownerSessionId !== currentSessionId) continue;
        if (job.nextRunAt <= Date.now()) {
          job.nextRunAt = Date.now() + job.intervalSeconds * 1000;
          changed = true;
        }
        if (!job.scheduledTaskId) {
          changed = true;
          scheduleRecurringJob(job);
        }
      }
      if (changed) writeRecurringJobs(jobs);
    }

    updateTuiWidget();
  });

  // Before a session change (/new, /resume, switch): immediately stop every
  // pending timer of the current instance so no retry fires on a stale ctx.
  pi.on('session_before_switch', async () => {
    clearPendingTimeouts();
    if (updateInterval) {
      try { clearInterval(updateInterval); } catch { /* best-effort cleanup */ }
      updateInterval = undefined;
    }
  });

  // Cleanup on session shutdown
  pi.on('session_shutdown', async () => {
    markStale();
    if (currentCtx && currentCtx.hasUI) {
      try {
        currentCtx.ui.setWidget('pi-cron-bg', undefined);
        currentCtx.ui.setStatus('pi-cron-bg', undefined);
      } catch { /* ctx stale during shutdown */ }
    }
  });

  // Start the loop right away if hot-loaded
  startUpdateLoop();

  // Register the LLM tool 'cron_bg_wait' (existing one-shot timer)
  pi.registerTool({
    name: 'cron_bg_wait',
    label: 'Cron BG Wait',
    description: 'Schedules a non-blocking timed wakeup via a native background task (sleep). Consumes no tokens while waiting. Shows a fixed always-visible widget above the editor in every tmux session with a real-time countdown.',
    parameters: Type.Object({
      duration: Type.String({
        description: 'Wait time before the wakeup (e.g. "10m", "600s", "30m", "1h")',
      }),
      reason: Type.Optional(Type.String({
        description: 'Short task label (e.g. "Ronda ronda-004", "Waiting for Dev test", "LLM breather")',
      })),
      wakePrompt: Type.Optional(Type.String({
        description: 'Dense wakeup reminder: list the current objective, the latest status, the last SPEC/task and the key files to review first on return.',
      })),
    }),
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      currentCtx = ctx;
      currentSessionId = readCurrentSessionId(ctx) ?? currentSessionId;
      let seconds: number;
      try {
        seconds = parseDurationToSeconds(params.duration);
      } catch (err: any) {
        return {
          content: [{ type: 'text', text: `Error: ${err.message}` }],
          details: { ok: false, error: err.message },
        };
      }

      const taskLabel = params.reason ? params.reason.trim() : t('defaultWaitLabel')(params.duration);
      const taskName = `Cron: ${taskLabel}`;
      const requestId = randomUUID();

      let shellCmd = `sleep ${seconds}`;
      if (params.wakePrompt && params.wakePrompt.trim().length > 0) {
        // Reuse the same hardened quoting as the recurring path: the previous
        // inline escaping handled apostrophes only (a backslash could cancel
        // the next escape) and applied no length cap to the prompt.
        const safePrompt = shellQuote(params.wakePrompt.trim().slice(0, 2000));
        shellCmd =
          `sleep ${seconds} && printf '%s\\n' '${t('waitWakeHeader')(seconds)}' ` +
          `'${t('contextReminderHeader')}' '${safePrompt}'`;
      }

      const runPayload = {
        name: taskName,
        command: shellCmd,
        isAgent: false,
        timeoutSeconds: seconds + 90,
        notifyOnCompletion: true,
        triggerOnCompletion: true,
      };

      const requestFrame = {
        schema_version: BG_REQUEST_SCHEMA,
        request_id: requestId,
        operation: 'run',
        payload: runPayload,
      };

      try {
        let resolveStart: ((v: { ok: boolean; result?: any; error?: string }) => void) | undefined;
        let unsubscribe: (() => void) | undefined;
        try {
          const unsub = pi.events.on(BG_RESPONSE_CHANNEL, (data: any) => {
            if (data && data.request_id === requestId && resolveStart) {
              if (typeof unsub === 'function') {
                try { unsub(); } catch { /* best-effort */ }
              }
              if (data.ok) resolveStart({ ok: true, result: data.result });
              else resolveStart({ ok: false, error: data.error });
            }
          });
          unsubscribe = typeof unsub === 'function' ? unsub : () => {};
        } catch {
          return {
            content: [{ type: 'text', text: `Cannot start the background timer: context stale (session reload/replacement)` }],
            details: { ok: false, error: 'stale context' },
          };
        }

        const startPromise = new Promise<{ ok: boolean; result?: any; error?: string }>((resolve) => {
          resolveStart = resolve;
        });

        try {
          pi.events.emit(BG_REQUEST_CHANNEL, requestFrame);
        } catch {
          try { unsubscribe?.(); } catch { /* best-effort */ }
          return {
            content: [{ type: 'text', text: `Cannot start the background timer: context stale (session reload/replacement)` }],
            details: { ok: false, error: 'stale context' },
          };
        }

        const outcome = await Promise.race([
          startPromise,
          new Promise<{ ok: boolean; error: string }>((_, reject) =>
            setTimeout(() => reject(new Error(t('bgConfirmTimeout'))), 5000)
          ),
        ]);

        if (!outcome.ok) {
          return {
            content: [{ type: 'text', text: `Cannot start the background timer: ${outcome.error}` }],
            details: { ok: false, error: outcome.error },
          };
        }

        const task = outcome.result;
        const taskId = task?.id || requestId;

        registerTimer(taskId, taskLabel, seconds, !!params.wakePrompt);

        return {
          content: [
            {
              type: 'text',
              text: `Scheduled wait successfully: ${taskName} (${seconds}s).\nFixed widget active above the editor, with a countdown shared across all tmux sessions.`,
            },
          ],
          details: { ok: true, taskId, seconds, hasWakePrompt: !!params.wakePrompt },
        };
      } catch (err: any) {
        return {
          content: [
            {
              type: 'text',
              text: `Error while registering the timer: ${err.message}`,
            },
          ],
          details: { ok: false, error: err.message },
        };
      }
    },
  });

  // Tools and commands for persistent recurring cron
  pi.registerTool({
    name: 'cron_bg_repeat',
    label: 'Cron BG Repeat',
    description: 'Persistent recurring scheduler: once set, it re-arms itself at every deadline and survives session restarts. Actions: start, stop, list, prune.',
    parameters: Type.Object({
      action: Type.String({
        description: 'Action: "start" launches a recurring job, "stop" disables it, "list" shows THIS SESSION\'S jobs, "prune" removes orphan jobs from dead sessions.',
      }),
      duration: Type.Optional(Type.String({
        description: 'Job interval (e.g. "10m", "600s", "1h"). Only used for start.',
      })),
      label: Type.Optional(Type.String({
        description: 'Short recurring job label (e.g. "Site audit round"). Only used for start.',
      })),
      wakePrompt: Type.Optional(Type.String({
        description: 'Dense reminder shown at every recurring wakeup.',
      })),
      jobId: Type.Optional(Type.String({
        description: 'Id of the job to stop. Only used for stop.',
      })),
    }),
    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      currentCtx = ctx;
      currentSessionId = readCurrentSessionId(ctx) ?? currentSessionId;
      const action = typeof params.action === 'string' ? params.action.trim().toLowerCase() : '';

      if (action === 'list') {
        const allJobs = readRecurringJobs();
        // Session scope: without this filter the agent saw ALL jobs from ALL
        // sessions (500+ accumulated entries), including those of dead
        // sessions. jobIsOurs() excludes orphans and foreign jobs.
        const jobs = new Map<string, RecurringJob>();
        for (const [id, job] of allJobs) {
          if (jobIsOurs(job)) jobs.set(id, job);
        }
        const now = Date.now();
        const lines: string[] = [];
        if (jobs.size === 0) {
          lines.push(t('noRecurringJobs'));
        } else {
          lines.push(t('recurringJobsList'));
          for (const job of jobs.values()) {
            const remainingSec = Math.max(0, Math.round((job.nextRunAt - now) / 1000));
            lines.push(`- ${job.id} | ${job.label} | every ${formatRemaining(job.intervalSeconds)} | next in ${formatRemaining(remainingSec)} | active=${job.active}`);
          }
          const hidden = allJobs.size - jobs.size;
          if (hidden > 0) {
            lines.push(`(${hidden} jobs from other sessions hidden — use the "prune" action to remove orphaned ones.)`);
          }
        }
        return {
          content: [{ type: 'text', text: lines.join('\n') }],
          details: { ok: true, jobs: Array.from(jobs.values()), hiddenFromOtherSessions: allJobs.size - jobs.size },
        };
      }

      if (action === 'stop') {
        const jobId = params.jobId?.trim();
        if (!jobId) {
          return { content: [{ type: 'text', text: 'Error: jobId is required for stop.' }], details: { ok: false, error: 'missing jobId' } };
        }
        const stopOutcome = mutateJobs((jobs) => {
          const job = jobs.get(jobId);
          if (!job) return { found: false as const };
          if (!jobIsOurs(job)) return { found: true as const, notOwner: true as const };
          job.active = false;
          job.scheduledTaskId = null;
          jobs.set(job.id, job);
          return { found: true as const, notOwner: false as const, label: job.label };
        });
        if (!stopOutcome.found) {
          return { content: [{ type: 'text', text: `Error: job ${jobId} not found.` }], details: { ok: false, error: 'not found' } };
        }
        // Ownership check: without it any session could stop any other
        // session's recurring job by guessing/reading its id, which breaks the
        // "jobs are not shared across sessions" invariant enforced elsewhere.
        if (stopOutcome.notOwner) {
          return {
            content: [{ type: 'text', text: `Error: job ${jobId} belongs to another session and cannot be stopped from here.` }],
            details: { ok: false, error: 'not owner' },
          };
        }
        updateTuiWidget();
        return {
          content: [{ type: 'text', text: `Recurring job stopped: ${stopOutcome.label} (${jobId}).` }],
          details: { ok: true, jobId },
        };
      }

      if (action === 'start') {
        let seconds: number;
        try {
          seconds = parseDurationToSeconds(params.duration || '10m');
        } catch (err: any) {
          return { content: [{ type: 'text', text: `Duration error: ${err.message}` }], details: { ok: false, error: err.message } };
        }
        const label = (params.label || t('defaultLabel')).trim();
        const wakePrompt = params.wakePrompt || t('recurringAction')(label);
        const jobId = randomUUID();
        const job: RecurringJob = {
          id: jobId,
          label,
          intervalSeconds: seconds,
          wakePrompt,
          active: true,
          nextRunAt: Date.now() + seconds * 1000,
          createdAt: Date.now(),
          lastRunAt: null,
          scheduledTaskId: null,
          ownerSessionId: currentSessionId,
        };
        // Under the cross-process lock so a concurrent session's job is not
        // clobbered by this write-back.
        mutateJobs((jobs) => { jobs.set(job.id, job); });
        scheduleRecurringJob(job);
        return {
          content: [
            {
              type: 'text',
              text: `Recurring job started: ${label} (${jobId}) every ${formatRemaining(seconds)}.\nIt will re-arm automatically at every deadline. Use /cron-repeat stop ${jobId} to stop it.`
            },
          ],
          details: { ok: true, jobId, seconds },
        };
      }

      if (action === 'prune') {
        const pruned = mutateJobs((allJobs) => {
          let n = 0;
          for (const [id, job] of allJobs) {
            if (!jobIsOurs(job)) {
              allJobs.delete(id);
              // NOTE: no timeout cleanup here. `scheduledTaskId` holds a
              // pi-background-tasks task id (a string), whereas `pendingTimeouts`
              // holds NodeJS.Timeout objects from the startup/retry timers, so
              // they can never match. A pending retry is instead neutralised by
              // the `jobIsOurs` guard at the top of scheduleRecurringJob().
              n++;
            }
          }
          return n;
        });
        updateTuiWidget();
        return {
          content: [{ type: 'text', text: pruned > 0
            ? `Pruned ${pruned} jobs not belonging to this session.`
            : 'No orphan job to remove.' }],
          details: { ok: true, pruned },
        };
      }

      return { content: [{ type: 'text', text: 'Error: action must be "start", "stop", "list" or "prune".' }], details: { ok: false, error: 'invalid action' } };
    },
  });

  // Interactive commands for the human operator
  pi.registerCommand('cron-repeat', {
    description: 'Starts a persistent recurring job (e.g. /cron-repeat 10m Check site)',
    handler: async (args, ctx) => {
      currentCtx = ctx;
      currentSessionId = readCurrentSessionId(ctx) ?? currentSessionId;
      const parts = (args || '').trim().split(/\s+/);
      const duration = parts[0] || '10m';
      const label = parts.slice(1).join(' ') || t('defaultLabel');
      let seconds: number;
      try {
        seconds = parseDurationToSeconds(duration);
      } catch (err: any) {
        ctx.ui.notify(err.message, 'error');
        return;
      }
      const jobId = randomUUID();
      const job: RecurringJob = {
        id: jobId,
        label,
        intervalSeconds: seconds,
        wakePrompt: t('recurringAction')(label),
        active: true,
        nextRunAt: Date.now() + seconds * 1000,
        createdAt: Date.now(),
        lastRunAt: null,
        scheduledTaskId: null,
        ownerSessionId: currentSessionId,
      };
      const jobs = readRecurringJobs();
      jobs.set(job.id, job);
      writeRecurringJobs(jobs);
      scheduleRecurringJob(job);
      ctx.ui.notify(`Recurring job started: ${label} (${jobId}) every ${formatRemaining(seconds)}`, 'info');
    },
  });

  pi.registerCommand('cron-repeat-list', {
    description: 'Shows the recurring jobs of the current session.',
    handler: async (_args, ctx) => {
      currentCtx = ctx;
      currentSessionId = readCurrentSessionId(ctx) ?? currentSessionId;
      const jobs = readRecurringJobs();
      const now = Date.now();
      const lines: string[] = [];
      if (jobs.size === 0) {
        lines.push(t('noRecurringJobs'));
      } else {
        lines.push(t('recurringJobsList'));
        for (const job of jobs.values()) {
          const remainingSec = Math.max(0, Math.round((job.nextRunAt - now) / 1000));
          lines.push(`- ${job.id} | ${job.label} | every ${formatRemaining(job.intervalSeconds)} | next in ${formatRemaining(remainingSec)}`);
        }
      }
      ctx.ui.notify(lines.join('\n'), 'info');
    },
  });

  pi.registerCommand('cron-repeat-stop', {
    description: 'Stops a recurring job (e.g. /cron-repeat-stop <id>)',
    handler: async (args, ctx) => {
      currentCtx = ctx;
      currentSessionId = readCurrentSessionId(ctx) ?? currentSessionId;
      const jobId = (args || '').trim();
      if (!jobId) {
        ctx.ui.notify('Error: a job id is required.', 'error');
        return;
      }
      const jobs = readRecurringJobs();
      const job = jobs.get(jobId);
      if (!job) {
        ctx.ui.notify(`Job ${jobId} not found.`, 'error');
        return;
      }
      job.active = false;
      job.scheduledTaskId = null;
      jobs.set(job.id, job);
      writeRecurringJobs(jobs);
      updateTuiWidget();
      ctx.ui.notify(`Recurring job stopped: ${job.label} (${job.id}).`, 'info');
    },
  });
}
