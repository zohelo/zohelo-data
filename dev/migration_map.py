"""Build .github/backlog/migration.yaml: one entry per existing issue of zohelo-data (issue #973).

Reads issues through REST (paged), the register and the backlog files; writes the map. Deleted before merge.
Run from the repository root: PYTHONPATH=. python dev/migration_map.py
"""
import glob
import json
import re
import subprocess
from pathlib import Path

import yaml

REPO = "zohelo/zohelo-data"
ROOT = Path(".")
REG_ID = re.compile(r"\[([A-Z]+(?:-[A-Z]+)*-\d+)\]\s*$")
STAGE_WP = {"Loader": "bronze", "Verification": "bronze", "Archive policy": "ingestion",
            "Analysis": "silver", "Proposed schema": "silver", "dbt models": "silver", "Tests": "silver"}


def rest_issues() -> list[dict]:
    out, page = [], 1
    while True:
        r = subprocess.run(["gh", "api", f"repos/{REPO}/issues?state=all&per_page=100&page={page}"],
                           capture_output=True, text=True, check=True)
        chunk = json.loads(r.stdout)
        if not chunk:
            break
        out += [i for i in chunk if "pull_request" not in i]
        page += 1
    return sorted(out, key=lambda i: i["number"])


# ---- the plan: every key that exists on main ------------------------------------------------------------
reg = yaml.safe_load((ROOT / "ingestion/register.yaml").read_text())["sources"]
reg_by_id = {e["id"]: e for e in reg}
by_system = {e["source_system"]: e["id"] for e in reg if e.get("source_system")}
stages = yaml.safe_load((ROOT / ".github/backlog/stages.yaml").read_text())["stages"]
plan: dict[str, str] = {}  # key -> kind
hand_epics: set[str] = set()
task_titles: dict[str, str] = {}
for f in sorted(glob.glob(".github/backlog/epics/*.yaml")):
    e = yaml.safe_load(open(f))
    plan[e["key"]] = "Epic"
    hand_epics.add(e["key"])
    for d in e["deliverables"]:
        plan[d["key"]] = "Deliverable"
        for t in d.get("tasks", []):
            plan[t["key"]] = t.get("type", "Task")
            task_titles[t["title"].strip().lower()] = t["key"]
for e in reg:  # generated source Epics: stage 1 for every entry, stage Deliverables once approved
    k = "data/src-" + e["id"].lower()
    plan.setdefault(k, "Epic")
    plan.setdefault(k + "/review", "Deliverable")


AREAS = {a["key"] for a in yaml.safe_load((ROOT / ".github/backlog/areas.yaml").read_text())["areas"]}


def epic_key(rid: str) -> str:
    return "data/src-" + rid.lower()


# ---- fixed rules ------------------------------------------------------------------------------------------
CLOSE_POINTER = {
    307: ("data/backlog", "Initiative replaced by the Areas in areas.yaml"),
    308: ("data/ingestion", "Product bucket replaced by Area"), 335: ("data/ingestion", "Product bucket replaced by Area"),
    309: ("data/metadata", "Product bucket replaced by Area"), 310: ("data/metadata", "Product bucket replaced by Area"),
    311: ("data/portal", "Product bucket replaced by Area"), 312: ("data/ops", "Product bucket replaced by Area"),
    313: ("data/ops", "Product bucket replaced by Area"), 314: ("data/ops", "Product bucket replaced by Area"),
    315: ("data/backlog", "Product bucket replaced by Area"), 336: ("data/warehouse", "Product bucket replaced by Area"),
    337: ("data/warehouse", "Product bucket replaced by Area"),
    384: ("data/src-a", "register-section bucket replaced by source Epics under the family Area"),
    385: ("data/src-b", "register-section bucket replaced by source Epics under the family Area"),
    386: ("data/src-b", "register-section bucket replaced by source Epics under the family Area"),
    387: ("data/src-g", "register-section bucket replaced by source Epics under the family Area"),
    388: ("data/src-i", "register-section bucket replaced by source Epics under the family Area"),
    389: ("data/ingestion", "register-section bucket replaced by source Epics under the family Areas"),
    836: ("data/src-cd-015, data/src-pls-002, data/src-plg-011, data/src-ex-005",
          "decision tracked in the Epics of the four old-code sources"),
    842: ("data/backlog", "plan and focus replaced by the backlog"),
    837: ("data/backlog", "owner tickets are triaged into the backlog YAML (docs/backlog.md principle 1)"),
    649: (None, "work-log diary"), 857: (None, "work-log diary"), 858: (None, "weekly-plan diary"),
    795: (None, "usage screenshot, no work"), 341: (None, "weekly-plan diary"), 785: (None, "weekly-plan diary"),
}

# superseded by <deliverable key>: explicit by number (everything not caught by a rule above)
S = {}


def sup(dkey, *nums):
    for n in nums:
        S[n] = dkey


R_ = "data/ops/r2-cleanup"
sup("data/ops/security-baseline/retired-code", 4, 15, 94, 100, 771)
sup("data/warehouse/gold-v1/fold", 334)
sup("data/portal/trim/google", 21, 36, 39)
sup("data/backlog/engine/sync", 2, 12, 293, 295, 360, 362, 363, 693, 694, 695, 696, 697, 698, 699, 612, 613, 614, 615,
    616, 617, 781, 782)
sup("data/ingestion/release-scheduling/schedules", 6, 8, 16, 135, 373, 839)
sup("data/ops/status-freshness/status-json", 10, 767)
sup("data/backlog/engine/migration", 130)
sup("data/src-pls-001/ingestion", 131, 303, 777, 889)
sup("data/src-pls-002/ingestion", 132)
sup("data/src-gl-005/ingestion", 133)
sup("data/src-gl-001/ingestion", 134, 778)
sup("data/portal/catalogue-pages/catalogue", 19, 23, 29, 31, 371, 372, 765, 766, 768, 841)
sup("data/src-plg-001/silver", 25, 27)
sup("data/ops/security-baseline/repo-rules", 44, 354, 598, 608)
sup("data/ops/operations-fixes/alerts", 297, 375, 769)
sup("data/backlog/engine/rules", 298, 299, 378, 780)
sup("data/ops/operations-fixes/watch", 300, 339, 340, 353, 374, 597, 779, 802, 807, 816, 829)
sup("data/backlog/engine/project", 301, 382, 787, 817, 818, 819)
sup("data/src-gl-001/silver", 302, 875)
sup("data/src-pls-002/silver", 304)
sup("data/src-pls-002/review", 783)
sup("data/ingestion/register-restructure/fields", 296, 359, 361, 618, 619, 620)
sup("data/backlog/engine/bootstrap", 342, 379, 381, 383, 599, 600, 772, 775, 776, 822)
sup(R_ + "/retired-landing", 343, 881, 865, 879)
sup(R_ + "/orphans", 344, 352, 376, 589, 592, 594, 770, 784, 815, 820, 835, 845, 871)
sup("data/ops/quality-audit/checks", 345, 377)
sup("data/ops/quality-audit/audit", 380, 773, 774, 786)
sup("data/src-pls-001/development", 346)
sup("data/src-plg-001/review", 593)
sup("data/src-plg-001/bronze", 803)
sup(R_ + "/inventory", 351, 588, 590, 833)
sup(R_ + "/scratch", 591)
sup("data/ops/security-baseline/dependencies", 355, 595, 596, 601)
sup("data/ops/org-platform-move/portal-pages", 356, 604, 605)
sup("data/ops/org-platform-move/scheduler-app", 606)
sup("data/ops/credentials/check", 607, 609)
sup("data/ops/security-baseline/exit-check", 357)
sup("data/ops/usage-monitoring/script", 358, 610, 611)
sup("data/warehouse/gold-v1/decisions", 602, 603)
sup("data/warehouse/gold-v1/coverage", 364, 365, 366, 700, 743, 744, 745, 746, 747, 748, 749, 751, 752, 868, 870)
sup("data/warehouse/entity-event/entity", 750)
sup("data/metadata/semantic-manifest/generate", 367, 753, 754)
sup("data/metadata/catalogue-v1/kpis-40", 368, 755, 756, 757, 758, 759)
sup("data/metadata/catalogue-v1/models", 369, 760, 761, 762)
sup("data/portal/catalogue-pages/lineage", 370, 763, 764)
sup("data/src-gl-001/gold", 840)
sup("data/src-pls-007/ingestion", 856)
sup("data/src-pls-001/bronze", 876)
sup("data/src-ex-005/review", 906)
ENTITY_DIMS = {"address", "document", "facility", "institution", "organisation", "person", "political_actor", "security"}


def superseded(i: dict) -> str | None:
    n, t = i["number"], i["title"]
    if n in S:
        return S[n]
    m = re.match(r"Dimension: (\w+)$", t)
    if m:
        d = m.group(1)
        if d == "geography":
            return "data/warehouse/gold-v1/geography"
        return "data/warehouse/entity-event/entity" if d in ENTITY_DIMS else "data/warehouse/gold-v1/coverage"
    return None


def main() -> None:
    issues = rest_issues()
    entries, problems = [], []
    seen_epic: dict[str, int] = {}
    for i in issues:
        n, t, body = i["number"], i["title"], i.get("body") or ""
        labels = {l["name"] for l in i["labels"]}
        e = {"issue": n, "title": t, "state": i["state"]}
        km = re.match(r"<!-- zohelo:key=(\S+)\s*-->", body)
        rm = REG_ID.search(t)
        wp_base = re.sub(r"\s*\[[^\]]+\]\s*$", "", t)
        if km:
            e |= {"action": "keep", "key": km.group(1), "reason": "already adopted (carries a zohelo:key marker)"}
        elif 563 <= n <= 582:
            rid = rm.group(1) if rm else "EX-005"
            stage = "bronze" if n % 2 else "silver"  # 563 bronze, 564 silver, ...
            assert t.rstrip().endswith("Bronze" + (f" [{rid}]" if rm else "")) == (stage == "bronze"), n
            e |= {"action": "adopt", "type": "Deliverable", "key": f"{epic_key(rid)}/{stage}",
                  "reason": f"stage Deliverable {stage} of {rid}"}
        elif n in CLOSE_POINTER:
            ptr, why = CLOSE_POINTER[n]
            e |= {"action": "close", "key": None, "reason": why, "replacement": ptr}
        elif "alert" in labels or t.startswith("Pipeline failed:"):
            sm = re.match(r"Pipeline failed: (\w+)", t)
            rid = by_system.get(sm.group(1)) if sm else None
            if rid:
                e |= {"action": "adopt", "type": "Bug", "key": epic_key(rid),
                      "reason": f"alert adopted as Bug under the Epic of source {sm.group(1)} ({rid})"}
            else:
                tgt = "data/backlog/engine/rules"
                e |= {"action": "close", "key": None, "replacement": tgt,
                      "reason": f"superseded by {tgt} (alert of '{sm.group(1) if sm else '?'}', not a source: alerts get a parent through notify_failure)"}
        elif (rm or (n == 325)) and wp_base not in STAGE_WP:
            rid = rm.group(1) if rm else "EX-005"
            if rid not in reg_by_id:
                problems.append(f"#{n}: unknown register id {rid}")
            e |= {"action": "adopt", "type": "Epic", "key": epic_key(rid),
                  "reason": f"register work package of {rid}: its source Epic"}
            seen_epic.setdefault(epic_key(rid), n)
        elif (rm or re.search(r"\[opendata\]$", t)) and wp_base in STAGE_WP:
            rid = rm.group(1) if rm else "EX-005"
            tgt = f"{epic_key(rid)}/{STAGE_WP[wp_base]}"
            e |= {"action": "close", "key": None, "replacement": tgt, "reason": f"superseded by {tgt}"}
        elif t.strip().lower() in task_titles:
            e |= {"action": "adopt", "type": "Task", "key": task_titles[t.strip().lower()],
                  "reason": "title equals a Task title"}
        else:
            tgt = superseded(i)
            if not tgt:
                problems.append(f"#{n}: no rule for {t!r}")
                continue
            e |= {"action": "close", "key": None, "replacement": tgt, "reason": f"superseded by {tgt}"}
        entries.append(e)
    # checks
    adopt_keys = [e["key"] for e in entries if e["action"] in ("adopt",) and e.get("type") != "Bug"]
    for e in entries:
        if e["action"] in ("adopt", "keep") and e["key"] not in plan and e["key"] not in {"data/src-" + x["id"].lower() for x in reg}:
            problems.append(f"#{e['issue']}: key {e['key']} not in the plan")
        if e["action"] == "close" and e.get("replacement"):
            for k in e["replacement"].split(", "):
                if k not in plan and k not in AREAS:
                    problems.append(f"#{e['issue']}: replacement {k} not in the plan")
    dup = {k for k in adopt_keys if adopt_keys.count(k) > 1}
    if dup:
        problems.append(f"adopt keys claimed twice: {sorted(dup)}")
    if problems:
        raise SystemExit("\n".join(problems))
    out = {"issues": entries}
    hdr = ("# Migration map of the old zohelo-data issues (backlog Task data/backlog/engine/migration/map, #973).\n"
           "# One entry per issue: action adopt (issue becomes `key`; `type` is the issue type it gets), close (with the\n"
           "# `replacement` key) or keep (already carries its key). Alerts adopt as Bug under their source Epic key.\n"
           "# Consumed by data/backlog/engine/migration/apply; removed by data/backlog/engine/migration/cleanup.\n")
    Path(".github/backlog/migration.yaml").write_text(
        hdr + yaml.safe_dump(out, sort_keys=False, allow_unicode=True, width=140))
    from collections import Counter
    print(len(entries), Counter(e["action"] for e in entries))


if __name__ == "__main__":
    main()
