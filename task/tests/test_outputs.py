"""PCX task verifier — compares agent output to ground truth."""

import json
import math
import sys
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
GT_PATH = TESTS_DIR / "ground_truth.json"

# Agent output paths (injected by Harbor at verify time)
STATE_PATH = Path("/app/out/state.jsonl")
PIPES_PATH = Path("/app/out/pipes.jsonl")
SUMMARY_PATH = Path("/app/out/summary.json")

PASS = 0
FAIL = 1

results = []


def check(name, passed, detail=""):
    results.append((name, passed, detail))
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {name}" + (f": {detail}" if detail else ""))


def load_jsonl(path):
    lines = []
    with open(path) as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"invalid JSON on line {i+1} of {path}: {e}")
    return lines


def main():
    gt = json.loads(GT_PATH.read_text())
    sessions = {s["name"]: s for s in gt["sessions"]}

    # ── Test 1: state.jsonl exists and is valid JSON ──────────────────────────
    try:
        state_rows = load_jsonl(STATE_PATH)
        check("state_jsonl_valid", True)
    except Exception as e:
        check("state_jsonl_valid", False, str(e))
        state_rows = []

    # ── Test 2: state.jsonl key set matches ground truth ─────────────────────
    gt_state_keys = set()
    for s in gt["sessions"]:
        for row in s["state"]:
            gt_state_keys.add((s["name"], row["thread_id"], row["var"]))

    agent_state_keys = set()
    for row in state_rows:
        try:
            agent_state_keys.add((row["session"], row["thread_id"], row["var"]))
        except KeyError:
            pass

    missing = gt_state_keys - agent_state_keys
    extra = agent_state_keys - gt_state_keys
    check("state_key_set",
          not missing and not extra,
          f"missing={len(missing)} extra={len(extra)}")

    # ── Test 3: state.jsonl ordering ─────────────────────────────────────────
    if state_rows:
        keys = [(r.get("session",""), r.get("thread_id",0), r.get("var",""))
                for r in state_rows]
        check("state_ordering", keys == sorted(keys),
              "rows not sorted by (session, thread_id, var)")
    else:
        check("state_ordering", False, "no rows to check")

    # ── Test 4: state.jsonl values ────────────────────────────────────────────
    agent_state = {}
    for row in state_rows:
        k = (row.get("session"), row.get("thread_id"), row.get("var"))
        agent_state[k] = row.get("value")

    wrong_vals = 0
    for s in gt["sessions"]:
        for row in s["state"]:
            k = (s["name"], row["thread_id"], row["var"])
            if agent_state.get(k) != row["value"]:
                wrong_vals += 1

    check("state_values", wrong_vals == 0, f"{wrong_vals} wrong values")

    # ── Test 5: pipes.jsonl key set ───────────────────────────────────────────
    try:
        pipes_rows = load_jsonl(PIPES_PATH)
        check("pipes_jsonl_valid", True)
    except Exception as e:
        check("pipes_jsonl_valid", False, str(e))
        pipes_rows = []

    gt_pipe_keys = {(s["name"], p["pipe_id"])
                    for s in gt["sessions"] for p in s["pipes"]}
    agent_pipe_keys = {(r.get("session"), r.get("pipe_id")) for r in pipes_rows}
    check("pipes_key_set",
          gt_pipe_keys == agent_pipe_keys,
          f"missing={gt_pipe_keys - agent_pipe_keys} extra={agent_pipe_keys - gt_pipe_keys}")

    # ── Test 6: pipes.jsonl values ────────────────────────────────────────────
    agent_pipes = {(r.get("session"), r.get("pipe_id")): r for r in pipes_rows}
    pipe_wrong = 0
    for s in gt["sessions"]:
        for p in s["pipes"]:
            k = (s["name"], p["pipe_id"])
            a = agent_pipes.get(k, {})
            if (a.get("remaining") != p["remaining"] or
                    a.get("total_written") != p["total_written"] or
                    a.get("total_read") != p["total_read"]):
                pipe_wrong += 1
    check("pipes_values", pipe_wrong == 0, f"{pipe_wrong} wrong pipe entries")

    # ── Test 7: summary.json checksum ─────────────────────────────────────────
    try:
        summary = json.loads(SUMMARY_PATH.read_text())
        if not isinstance(summary, list):
            check("summary_checksum", False, "summary.json must be a JSON array")
        else:
            agent_by_name = {s["name"]: s for s in summary}
            wrong_cs = 0
            for s in gt["sessions"]:
                a = agent_by_name.get(s["name"], {})
                if a.get("checksum") != s["checksum"]:
                    wrong_cs += 1
            check("summary_checksum", wrong_cs == 0,
                  f"{wrong_cs} sessions with wrong checksum")
    except Exception as e:
        check("summary_checksum", False, str(e))

    # ── Result ────────────────────────────────────────────────────────────────
    passed = sum(1 for _, p, _ in results if p)
    total = len(results)
    print(f"\n{passed}/{total} tests passed")

    reward = 1.0 if all(p for _, p, _ in results) else 0.0
    print(f"reward: {reward}")
    return 0 if reward == 1.0 else 1


if __name__ == "__main__":
    sys.exit(main())
