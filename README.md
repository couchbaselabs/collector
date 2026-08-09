# QE Greenboard Collector

Polls Jenkins for QE test results and writes per-run docs into Couchbase, where
eventing functions fold them into the greenboard build documents. Targets: server,
sync_gateway, cblite, operator, cao, and capella.

This is a **light, standalone** copy of the collector (the runtime package only) —
extracted so it can live in its own repo, separate from the greenboard dashboard.

## Layout

```
main.py            # entry point + poll loop  ->  python main.py credentials.ini
config.py          # VIEWS (what to poll) + env-driven settings
processors.py      # per-target scrape/normalize
jenkins.py         # Jenkins HTTP client (creds from credentials.ini)
storage.py         # Couchbase writes
parsing.py, models.py, cao.py
cao_dedup.py       # operational one-off:  python cao_dedup.py --apply
requirements.txt
.gitignore
```

## Secrets — created once on the VM, never committed

Two files hold secrets. Both are in `.gitignore`, so you create them **once after the
first clone** and they **persist across every `git pull`** (git never touches ignored files).

**1. `credentials.ini`** — Jenkins basic-auth, one `[section]` per Jenkins base URL
(section header MUST equal the base URL; only hosts needing auth require an entry):

```ini
[http://qe-jenkins1.sc.couchbase.com]
username=YOUR_USER
password=YOUR_TOKEN

[http://qa.sc.couchbase.com]
username=YOUR_USER
password=YOUR_TOKEN

[http://uberjenkins.sc.couchbase.com:8080]
username=YOUR_USER
password=YOUR_TOKEN

[http://sdkbuilds.sc.couchbase.com]
username=YOUR_USER
password=YOUR_TOKEN
```

**2. `.env`** — Couchbase target + tuning (source it before running):

```bash
# Couchbase target (where per-run docs are written)
export CB_HOST=172.23.105.219
export CB_USER=Administrator
export CB_PASS=                 # <-- FILL IN (never commit)

# gb_label catalog cluster (defaults to CB_USER/CB_PASS if omitted)
# export CATALOG_HOST=172.23.217.21

# optional tuning
# export POLL_INTERVAL=120
# export WORKER_POOL_SIZE=16
```

> Note: `CB_PASS` is read from the environment. If your shell profile already exports a
> global `CB_PASS` for something else, the `.env` value (sourced right before running)
> takes precedence — keep them consistent or unset the global.

## First-time setup on the VM

```bash
git clone <repo-url> collector && cd collector
python3 -m venv env && source env/bin/activate
pip install -r requirements.txt

# create the two secret files (persist across pulls):
vi credentials.ini      # paste the Jenkins block above, fill in
vi .env                 # paste the CB block above, fill in CB_PASS

source .env
python main.py credentials.ini
```

## Updating later

```bash
cd collector && git pull
source env/bin/activate && pip install -r requirements.txt   # if deps changed
source .env
python main.py credentials.ini
```

`credentials.ini` and `.env` are untouched by the pull — no re-entry of secrets.

## Notes

- Couchbase creds come from the environment (`CB_*`), Jenkins creds from
  `credentials.ini` — the two are intentionally separate.
- The `capella` view writes per-run docs to the `capella` bucket; the
  `greenboar_capella` eventing folds them into `capella_gb`.
