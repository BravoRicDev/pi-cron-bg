# pi-cron-bg — Cron & Background Jobs for Pi

Non-blocking timed wakeups and a persistent recurring scheduler for
[Pi](https://github.com/earendil-works/pi) agents.

---

## 🇬🇧 English

### What it does

Two tools, no polling loop and no idle tokens:

- **`cron_bg_wait`** — schedules a one-shot wakeup through a native background
  task (`sleep`). Consumes **zero tokens while waiting** and renders a fixed
  widget above the editor in every tmux session, with a real-time countdown.
- **`cron_bg_repeat`** — a persistent recurring scheduler. Once started, a job
  re-arms itself at every deadline and survives session restarts. Jobs are
  owned by the session that created them (`SessionHeader.id`), so several Pi
  sessions can run their own rounds without interfering.

### Actions

| Action  | Effect |
|---------|--------|
| `start` | Creates a recurring job (`duration`, `label`, `wakePrompt`). |
| `stop`  | Deactivates a job (`jobId`). Refuses jobs owned by another session. |
| `list`  | Lists **this session's** jobs, and reports how many belong to others. |
| `prune` | Removes jobs not belonging to this session. |

Slash equivalents: `/cron-repeat`, `/cron-repeat-list`, `/cron-repeat-stop`.

### Design notes

- **Bilingual by default.** Every user-facing default follows the system locale
  (`LANG`/`LC_ALL`/`LC_MESSAGES`/`LANGUAGE`, then ICU, then English). The
  language resolver is deliberately identical to `pi-anti-amnesia`, `pi-cwl`
  and `pi-arc`: extensions inject directives into the same model context, and a
  model fed mixed-language instructions degrades.
- **Session ownership.** A job belongs to exactly one session. `list` and
  `stop` enforce it, so an agent never sees or kills another session's rounds.
- **Bounded retries.** A job that cannot be scheduled retries with exponential
  backoff (30s → 60s → … capped at 10 min) and is **disabled after 5 attempts**,
  with the reason recorded. An unbounded retry kept the session permanently
  awake without ever surfacing the problem.
- **Cross-process safe.** `recurring-jobs.json` is shared by every Pi session.
  All read-modify-write cycles run under an exclusive file lock, so two
  sessions cannot clobber each other's jobs.
- **Atomic writes.** State files are written to a temp file and `rename()`d, so
  a crash cannot leave truncated JSON that silently discards every timer.
- **Shell-safe.** Labels and wake prompts are POSIX-quoted (apostrophes *and*
  backslashes) and length-capped before they reach the shell.

### Configuration

Optional, `~/.pi/cron-bg/config.json`:
```json
{ "showWidget": true, "debug": false }
```

State lives in `~/.pi/timers/`.

### Testing

```bash
node harness.mjs   # 11/11: start → schedule → terminal event → re-arm → stop
```

---

## 🇮🇹 Italiano

### Cosa fa

Due tool, nessun polling e nessun token consumato durante l'attesa:

- **`cron_bg_wait`** — pianifica un risveglio temporizzato tramite un
  background task nativo (`sleep`). **Non consuma token durante l'attesa** e
  mostra un widget fisso sopra l'editor in tutte le sessioni tmux, con
  countdown in tempo reale.
- **`cron_bg_repeat`** — scheduler ricorrente persistente. Una volta avviato,
  il job si riarma da solo a ogni scadenza e sopravvive ai restart della
  sessione. I job appartengono alla sessione che li ha creati
  (`SessionHeader.id`), quindi più sessioni Pi possono avere le proprie ronde
  senza interferire.

### Azioni

| Azione  | Effetto |
|---------|---------|
| `start` | Crea un job ricorrente (`duration`, `label`, `wakePrompt`). |
| `stop`  | Disattiva un job (`jobId`). Rifiuta i job di altre sessioni. |
| `list`  | Elenca i job **di questa sessione** e riporta quanti sono altrui. |
| `prune` | Rimuove i job che non appartengono a questa sessione. |

Equivalenti slash: `/cron-repeat`, `/cron-repeat-list`, `/cron-repeat-stop`.

### Note di progetto

- **Bilingue di default.** Ogni default rivolto all'utente segue la locale di
  sistema (`LANG`/`LC_ALL`/`LC_MESSAGES`/`LANGUAGE`, poi ICU, poi inglese). Il
  resolver è identico a `pi-anti-amnesia`, `pi-cwl` e `pi-arc`: le estensioni
  iniettano istruzioni nello stesso contesto del modello, e un modello nutrito
  di direttive in lingue miste degrada.
- **Proprietà di sessione.** Un job appartiene a una sola sessione. `list` e
  `stop` la applicano, così un agente non vede né ferma le ronde altrui.
- **Retry limitati.** Un job che non riesce a essere schedulato riprova con
  backoff esponenziale (30s → 60s → … fino a 10 min) e viene **disattivato dopo
  5 tentativi**, con il motivo registrato. Un retry illimitato teneva la
  sessione perennemente sveglia senza mai segnalare il problema.
- **Sicuro tra processi.** `recurring-jobs.json` è condiviso da tutte le
  sessioni Pi. Tutti i cicli read-modify-write girano sotto un lock esclusivo,
  quindi due sessioni non possono sovrascriversi a vicenda i job.
- **Scritture atomiche.** I file di stato vengono scritti su file temporaneo e
  rinominati con `rename()`: un crash non può lasciare JSON troncato che
  scarterebbe silenziosamente ogni timer.
- **Shell-safe.** Label e prompt sono quotati POSIX (apostrofi *e* backslash) e
  limitati in lunghezza prima di raggiungere la shell.

### Configurazione

Opzionale, `~/.pi/cron-bg/config.json`:
```json
{ "showWidget": true, "debug": false }
```

Lo stato risiede in `~/.pi/timers/`.

### Test

```bash
node harness.mjs   # 11/11: start → schedule → evento terminale → riarmo → stop
```

---

## Licence / Licenza

MIT — vedi [LICENSE](LICENSE).
