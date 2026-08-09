"""
One-time cleanup for duplicate cao_run docs.

WHY THIS EXISTS
    An early version of the CAO collector wrote each combo doc under a
    non-deterministic key, so every re-scrape of a build created a NEW doc
    instead of overwriting. The current collector keys deterministically
    (storage.make_key(name, build_id, "-<combo_index>")), so it is idempotent
    going forward -- but builds scraped during the old scheme left orphan
    duplicates behind (e.g. build 51 stored 10x for 2 combos).

WHAT IT DOES
    Groups every cao_run doc by its true identity (server build, executor
    build_id, combo_index), rewrites ONE survivor under the canonical
    deterministic key, and deletes every other copy. After this runs, a future
    re-scrape overwrites the canonical doc in place -- no re-duplication.

    Idempotent and safe to re-run: a already-clean bucket is a no-op.

USAGE (run on the collector host, where the SDK can reach Couchbase):
    export CB_HOST=172.23.105.219 CB_USER=Administrator CB_PASS=...
    python -m collector.cao_dedup            # dry run: report only
    python -m collector.cao_dedup --apply     # actually rewrite + delete
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from collections import defaultdict

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator

import config


def canonical_key(url: str, build_id, combo_index) -> str:
    # Mirror storage.make_key(job_doc.name, build_id, suffix="-<combo_index>").
    # job_doc.name is the Jenkins job name -- the path segment before the build id.
    parts = [p for p in (url or "").rstrip("/").split("/") if p]
    name = parts[-2] if len(parts) >= 2 else "cao-testrunner-executor"
    return hashlib.md5(f"{name}-{build_id}-{combo_index}".encode()).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description="Dedup cao_run docs to canonical keys.")
    ap.add_argument("--apply", action="store_true", help="perform writes/deletes (default: dry run)")
    ap.add_argument("--bucket", default=config.CAO_VIEW.bucket)
    args = ap.parse_args()

    cluster = Cluster(
        f"couchbase://{config.COUCHBASE_HOST}",
        ClusterOptions(PasswordAuthenticator(config.COUCHBASE_USER, config.COUCHBASE_PASS)),
    )
    col = cluster.bucket(args.bucket).default_collection()

    # `build` is a N1QL reserved word -> backtick the field and alias it.
    rows = cluster.query(
        'SELECT META(c).id AS _id, c.`build` AS bld, c.build_id AS bid, '
        'c.combo_index AS cidx, c.url AS url '
        f'FROM `{args.bucket}` c WHERE c.doc_type = "cao_run"'
    )
    groups = defaultdict(list)
    for r in rows:
        ident = (r.get("bld"), r.get("bid"), r.get("cidx"))
        groups[ident].append(r)

    dupe_groups = {k: v for k, v in groups.items() if len(v) > 1}
    total_docs = sum(len(v) for v in groups.values())
    extra = sum(len(v) - 1 for v in dupe_groups.values())
    print(f"[cao_dedup] {total_docs} docs, {len(groups)} unique combos, "
          f"{len(dupe_groups)} duplicated -> {extra} extra docs to remove")
    if not dupe_groups:
        print("[cao_dedup] nothing to do.")
        return 0

    rewrites = deletes = 0
    for (build, build_id, combo_index), docs in dupe_groups.items():
        ids = [d["_id"] for d in docs]
        canon = canonical_key(docs[0].get("url"), build_id, combo_index)
        survivor_id = canon if canon in ids else ids[0]
        print(f"  {build} #{build_id}/combo{combo_index}: {len(docs)} copies "
              f"-> keep {survivor_id[:12]} ({'canonical' if survivor_id == canon else 'first'}), "
              f"drop {len(docs) - 1}")
        if not args.apply:
            continue
        # Ensure a doc exists under the canonical key, then drop everything else.
        if survivor_id != canon:
            full = col.get(survivor_id).content_as[dict]
            col.upsert(canon, full)
            rewrites += 1
        for did in ids:
            if did != canon:
                try:
                    col.remove(did)
                    deletes += 1
                except Exception as exc:
                    print(f"    ! failed to remove {did[:12]}: {exc}")

    if args.apply:
        print(f"[cao_dedup] done: {rewrites} rewritten to canonical, {deletes} deleted.")
    else:
        print("[cao_dedup] dry run -- re-run with --apply to make changes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
