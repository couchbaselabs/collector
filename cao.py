"""
CAO (Couchbase Autonomous Operator) results parsing.

The cao-testrunner-executor job publishes a `pipeline/results.json` artifact per
build. Unlike server/operator jobs (which expose a Jenkins testReport), CAO's
results are a MATRIX: each build runs one or more `executions`, and every
execution pins a 5+ dimensional combo:

    platform, k8sVersion, openshiftVersion, caoVersion, cbServerVersion, cloudProvider

...and runs a flat list of scenario `tests`, each just passed/failed.

This module is PURE (no Couchbase / Jenkins deps) so it can be unit-tested on a
saved results.json. It turns one build's results.json into a list of "cao_run"
docs -- ONE per execution/combo. gb-v2 later aggregates these by cbServerVersion
(the primary selection axis) in its snapshot layer; the collector only writes the
normalized raw docs here.

NOTE: keep this file PURE ASCII. A literal micro-sign in the duration regex once
got re-encoded to an invalid UTF-8 byte in transfer, which made Python fail to
import the module -- and since processors.py imports it, that took down the ENTIRE
collector. The micro-second unit is matched via a \\u00b5 escape below instead.

Design note -- WHY dims are an open dict + an `upgrade` slot:
    matrixCombo is scalar today, but upgrade runs (server or CAO upgrade) will add
    from->to semantics and possibly extra keys. Storing the full combo verbatim plus
    a dedicated `upgrade` slot means that day is a parse tweak, not a schema
    migration. Do NOT hard-code exactly-five columns downstream.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

# The five (well, six) known matrix dimensions. Kept as data, not baked into the
# doc shape -- everything in matrixCombo is preserved under `dims` regardless.
KNOWN_DIMS = ("platform", "k8sVersion", "openshiftVersion",
              "caoVersion", "cbServerVersion", "cloudProvider")

# ---------------------------------------------------------------------------
# Small pure helpers
# ---------------------------------------------------------------------------

_MICRO = "\u00b5"          # micro sign via escape -> source stays pure ASCII
# Go durations: "9m18.487112818s", "26.9s", "500ms", "1h2m3s", micro via "us"/"<micro>s".
_DUR_RE = re.compile(r"(\d+(?:\.\d+)?)(ms|us|" + _MICRO + r"s|ns|h|m|s)")
_DUR_MS = {"h": 3600_000.0, "m": 60_000.0, "s": 1000.0,
           "ms": 1.0, "us": 0.001, _MICRO + "s": 0.001, "ns": 0.000001}


def parse_go_duration(s: Optional[str]) -> int:
    """'9m18.487112818s' -> 558487 (ms). Tolerant: returns 0 on junk/empty."""
    if not s or not isinstance(s, str):
        return 0
    total = 0.0
    matched = False
    for num, unit in _DUR_RE.findall(s):
        matched = True
        total += float(num) * _DUR_MS[unit]
    return int(round(total)) if matched else 0


def iso_to_ms(s: Optional[str]) -> int:
    """RFC3339 with nanoseconds ('...687923376-07:00') -> epoch ms. 0 on failure."""
    if not s or not isinstance(s, str):
        return 0
    # datetime.fromisoformat accepts at most microseconds -- truncate the fraction.
    m = re.match(r"^(.*\.\d{6})\d*([+-]\d{2}:\d{2}|Z)?$", s)
    if m:
        s = m.group(1) + (m.group(2) or "")
    s = s.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(s).timestamp() * 1000)
    except Exception:
        return 0


def suite_and_test(scenario_file: str):
    """
    '/.../sample_scenarios/dac_tests/00_xdcr_10GB_c_d.yaml' -> ('dac_tests', '00_xdcr_10GB_c_d')

    Suite = the immediate parent directory (the natural "component" grouping);
    test = the file basename without extension.
    """
    if not scenario_file:
        return ("unknown", "unknown")
    parts = [p for p in scenario_file.replace("\\", "/").split("/") if p]
    base = parts[-1] if parts else scenario_file
    test = re.sub(r"\.(ya?ml|json)$", "", base, flags=re.IGNORECASE)
    suite = parts[-2] if len(parts) >= 2 else "unknown"
    return (suite, test)


def orchestrator(dims: Dict[str, Any]) -> Dict[str, str]:
    """Collapse the mutually-exclusive k8s/openshift version into one facet."""
    plat = (dims.get("platform") or "").lower()
    if plat == "openshift" or dims.get("openshiftVersion"):
        return {"type": "openshift", "version": str(dims.get("openshiftVersion") or "")}
    return {"type": "kubernetes", "version": str(dims.get("k8sVersion") or "")}


def combo_id(dims: Dict[str, Any]) -> str:
    """
    Stable id for a combo WITHIN a cbServerVersion (the doc's build key). Hash the
    non-server dims so the same matrix cell across server builds shares an id.
    """
    key = "|".join(str(dims.get(k, "")) for k in
                   ("platform", "k8sVersion", "openshiftVersion", "caoVersion", "cloudProvider"))
    return hashlib.md5(key.encode()).hexdigest()[:12]


def detect_upgrade(dims: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Slot for upgrade runs. Today matrixCombo is scalar so this returns None. When
    upgrade lands (a dim like '7.6.0-x->8.0.0-y' or 'from'/'to' keys), extend here
    ONLY -- the doc shape already carries `upgrade`.
    """
    for field in ("cbServerVersion", "caoVersion"):
        v = str(dims.get(field, ""))
        for sep in ("->", "\u2192", ".."):
            if sep in v:
                frm, _, to = v.partition(sep)
                return {"field": field, "from": frm.strip(), "to": to.strip()}
    return None


def selection_build(dims: Dict[str, Any], upgrade: Optional[Dict[str, str]]) -> str:
    """
    The primary selection axis = cbServerVersion. For an upgrade run we file it
    under the TARGET version (what you're upgrading to), so it surfaces where a
    reader looks for '8.0.0'. The `from` stays queryable via dims/upgrade.
    """
    if upgrade and upgrade["field"] == "cbServerVersion":
        return upgrade["to"] or upgrade["from"]
    return str(dims.get("cbServerVersion") or "")


# ---------------------------------------------------------------------------
# Main entry -- one build's results.json -> list of cao_run docs
# ---------------------------------------------------------------------------

_STATUS_MAP = {"passed": "pass", "failed": "fail", "success": "pass", "failure": "fail"}


def build_cao_docs(data: Dict[str, Any], job_url: str, job_id: Any) -> List[Dict[str, Any]]:
    """Transform results.json -> normalized cao_run docs (one per execution/combo)."""
    if not isinstance(data, dict):
        return []
    job_id = str(data.get("jobId") or job_id or "")
    base   = job_url.rstrip("/")
    docs: List[Dict[str, Any]] = []

    for idx, execution in enumerate(data.get("executions") or []):
        dims = dict(execution.get("matrixCombo") or {})
        upgrade = detect_upgrade(dims)
        build   = selection_build(dims, upgrade)
        if not build:
            continue  # no cbServerVersion -> nothing to file under

        suites: Dict[str, Dict[str, Any]] = {}
        passed = failed = other = 0
        for t in execution.get("tests") or []:
            suite, name = suite_and_test(t.get("scenarioFile", ""))
            outcome = _STATUS_MAP.get(str(t.get("status", "")).lower(), "other")
            if outcome == "pass":
                passed += 1
            elif outcome == "fail":
                failed += 1
            else:
                other += 1
            s = suites.setdefault(suite, {"total": 0, "passed": 0, "failed": 0,
                                          "other": 0, "tests": []})
            s["total"] += 1
            s[{"pass": "passed", "fail": "failed"}.get(outcome, "other")] += 1
            s["tests"].append({
                "name": name,
                "status": outcome,               # normalized: pass | fail | other
                "raw_status": t.get("status"),
                "duration_ms": parse_go_duration(t.get("duration")),
                "error": t.get("error") or "",
                "file": t.get("scenarioFile") or "",
            })

        total = passed + failed + other
        result = "FAILURE" if failed else ("SUCCESS" if passed else "ABORTED")

        docs.append({
            "doc_type":  "cao_run",
            "job_id":    job_id,
            "build_id":  int(job_id) if str(job_id).isdigit() else job_id,
            "combo_index": idx,
            "combo_id":  combo_id(dims),
            "url":       f"{base}/{job_id}/",
            "build":     build,                 # cbServerVersion -- PRIMARY selection axis
            "dims":      dims,                   # full matrixCombo verbatim (open schema)
            "orchestrator": orchestrator(dims), # {type, version} -- k8s|openshift collapsed
            "upgrade":   upgrade,               # None today; from->to when upgrade lands
            "result":    result,                # combo-level roll-up
            "total":     total,
            "passed":    passed,
            "failed":    failed,
            "other":     other,
            "suites":    suites,                # suite -> {counts, tests[]}
            "startTime": execution.get("startTime") or "",
            "endTime":   execution.get("endTime") or "",
            "timestamp": iso_to_ms(execution.get("startTime")),
            "deleted":   False,
            "olderBuild": False,
        })
    return docs
