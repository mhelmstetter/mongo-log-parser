#!/usr/bin/env python3
"""
Summarize and analyze MongoDB slow query shapes from a logQueryCsv output file.

Usage:
    python3 analyze_query_shapes.py results.csv
"""

import csv
import json
import sys
from collections import defaultdict
from statistics import mean


# ── CSV loading ───────────────────────────────────────────────────────────────

def load_csv(filename):
    with open(filename, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── Grouping ──────────────────────────────────────────────────────────────────

def group_by_shape(rows):
    groups = defaultdict(list)
    for row in rows:
        key = row["QueryHash"].strip() or "(none)"
        groups[key].append(row)
    return dict(sorted(groups.items(), key=lambda x: len(x[1]), reverse=True))


# ── Numeric helpers ───────────────────────────────────────────────────────────

def to_int(val):
    try:
        return int(val) if val and val.strip() else None
    except (ValueError, AttributeError):
        return None


def fmt_ms(ms):
    if ms is None:
        return "n/a"
    if ms >= 60_000:
        return f"{ms/60_000:.1f}m"
    if ms >= 1_000:
        return f"{ms/1_000:.1f}s"
    return f"{ms}ms"


def fmt_bytes(b):
    if b is None:
        return "n/a"
    if b >= 1 << 30:
        return f"{b/(1<<30):.1f} GB"
    if b >= 1 << 20:
        return f"{b/(1<<20):.1f} MB"
    if b >= 1 << 10:
        return f"{b/(1<<10):.1f} KB"
    return f"{b} B"


def stats(values):
    """Return (min, max, avg) or None if list is empty."""
    v = [x for x in values if x is not None]
    if not v:
        return None
    return min(v), max(v), mean(v)


# ── Filter shape extraction ───────────────────────────────────────────────────

def extract_shape(filter_str):
    """
    Return a compact structural representation of a MongoDB filter:
    field names + operators, no values.
    """
    if not filter_str or not filter_str.strip():
        return ""
    try:
        obj = json.loads(filter_str)
        return _fmt_node(obj)
    except json.JSONDecodeError:
        return filter_str[:120]


def _fmt_node(obj):
    if isinstance(obj, dict):
        parts = []
        for k, v in obj.items():
            if k in ("$and", "$or", "$nor"):
                if isinstance(v, list):
                    inner = " | ".join(_fmt_node(item) for item in v)
                    parts.append(f"{k}[{inner}]")
                else:
                    parts.append(f"{k}[{_fmt_node(v)}]")
            elif k.startswith("$"):
                parts.append(k)
            else:
                if isinstance(v, dict):
                    ops = [op for op in v if op.startswith("$")]
                    parts.append(f"{k}[{','.join(ops)}]" if ops else f"{k}{{obj}}")
                elif isinstance(v, list):
                    parts.append(f"{k}[$in]")
                else:
                    parts.append(f"{k}[=]")
        return "{" + ", ".join(parts) + "}"
    if isinstance(obj, list):
        return "[" + " | ".join(_fmt_node(i) for i in obj) + "]"
    return "?"


def find_duplicate_fields(filter_str):
    """
    Return list of field names whose predicate appears verbatim more than once
    in a top-level $and.  Split range queries (one $gte clause + one $lte clause
    on the same field) are not flagged — only truly identical clauses are.
    """
    if not filter_str:
        return []
    try:
        obj = json.loads(filter_str)
        if not isinstance(obj, dict) or "$and" not in obj:
            return []
        # Collect operator sets per field
        field_ops = defaultdict(list)
        for clause in obj["$and"]:
            if isinstance(clause, dict):
                for k, v in clause.items():
                    if not k.startswith("$"):
                        ops = frozenset(v.keys()) if isinstance(v, dict) else frozenset({"="})
                        field_ops[k].append(ops)
        dupes = []
        for field, op_sets in field_ops.items():
            if len(op_sets) < 2:
                continue
            # Flag only if any operator set appears more than once (true duplicate)
            seen = []
            for ops in op_sets:
                if ops in seen:
                    dupes.append(field)
                    break
                seen.append(ops)
        return dupes
    except Exception:
        return []


def has_time_range(filter_str):
    """
    Heuristic: does the filter have a range bound on a timestamp-like field?
    Handles both the combined form {field: {$gte:x, $lte:y}} and the split
    $and form [{field:{$gte:x}}, {field:{$lte:y}}].
    """
    if not filter_str:
        return False
    try:
        obj = json.loads(filter_str)
        return _search_time_range(obj)
    except Exception:
        return False


def _search_time_range(obj):
    TIME_WORDS = ("time", "date", "ts", "timestamp", "at")
    LOWER_OPS  = {"$gte", "$gt"}
    UPPER_OPS  = {"$lte", "$lt"}

    if isinstance(obj, dict):
        # Combined form: {field: {$gte: x, $lte: y}}
        for k, v in obj.items():
            if not k.startswith("$") and isinstance(v, dict):
                has_lower = bool(LOWER_OPS & v.keys())
                has_upper = bool(UPPER_OPS & v.keys())
                if has_lower and has_upper and any(w in k.lower() for w in TIME_WORDS):
                    return True

        # Split $and form: [{field:{$gte:x}}, {field:{$lte:y}}, ...]
        if "$and" in obj and isinstance(obj["$and"], list):
            field_ops = defaultdict(set)
            for clause in obj["$and"]:
                if isinstance(clause, dict):
                    for k, v in clause.items():
                        if not k.startswith("$") and isinstance(v, dict):
                            field_ops[k].update(v.keys())
            for k, ops in field_ops.items():
                if LOWER_OPS & ops and UPPER_OPS & ops and any(w in k.lower() for w in TIME_WORDS):
                    return True

        # Recurse into nested structures
        for v in obj.values():
            if _search_time_range(v):
                return True

    if isinstance(obj, list):
        return any(_search_time_range(i) for i in obj)

    return False


# ── Issue detection ───────────────────────────────────────────────────────────

def detect_issues(rows):
    issues = []

    durations   = [to_int(r["DurationMs"])        for r in rows]
    planning    = [to_int(r["PlanningTimeMicros"]) for r in rows]
    docs_ex     = [to_int(r["DocsExamined"])       for r in rows]
    returned    = [to_int(r["Returned"])            for r in rows]
    bytes_read  = [to_int(r["BytesRead"])           for r in rows]

    # Planning timeout: planningTimeMicros ≈ durationMillis * 1000
    planning_timeout = sum(
        1 for d, p in zip(durations, planning)
        if d and p and p >= d * 900  # planning used >=90% of query time
    )
    if planning_timeout:
        issues.append(f"timeout during planning ({planning_timeout}/{len(rows)} executions)")

    # High scan-to-return ratio
    pairs = [(d, r) for d, r in zip(docs_ex, returned) if d is not None and r is not None]
    if pairs:
        ratios = [d / max(r, 1) for d, r in pairs]
        avg_ratio = mean(ratios)
        if avg_ratio > 100:
            issues.append(f"high scan ratio ({avg_ratio:,.0f}:1 docs:returned)")

    # High bytes read
    br = [b for b in bytes_read if b is not None]
    if br:
        avg_gb = mean(br) / (1 << 30)
        if avg_gb >= 1.0:
            issues.append(f"high bytes read (avg {avg_gb:.1f} GB)")

    # Replanning
    replanned = sum(1 for r in rows if r.get("Replanned", "").strip().lower() == "true")
    if replanned:
        issues.append(f"replanned {replanned}/{len(rows)}")

    # No plan summary logged
    has_plan = any(r.get("PlanSummary", "").strip() for r in rows)
    if not has_plan and not planning_timeout:
        issues.append("no plan summary (possible COLLSCAN or timed out before execution)")

    # Duplicate predicates in filter
    sample_filter = next((r["SanitizedFilter"] for r in rows if r.get("SanitizedFilter")), "")
    dupes = find_duplicate_fields(sample_filter)
    if dupes:
        issues.append(f"duplicate predicate(s) in $and: {', '.join(dupes)}")

    # No time-range bound
    if not has_time_range(sample_filter):
        issues.append("no time-range bound on filter")

    return issues


# ── Timestamp range analysis ─────────────────────────────────────────────────

def _collect_ts_bounds(obj, field, bounds):
    """Recursively find $gte/$lte for a named field, merging across $and elements."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == field and isinstance(v, dict):
                if "$gte" in v:
                    bounds["$gte"] = v["$gte"]
                if "$lte" in v:
                    bounds["$lte"] = v["$lte"]
            elif k in ("$and", "$or", "$nor") and isinstance(v, list):
                for item in v:
                    _collect_ts_bounds(item, field, bounds)
    elif isinstance(obj, list):
        for item in obj:
            _collect_ts_bounds(item, field, bounds)


def extract_ts_span_us(filter_str, field="adrLogTimestamp"):
    """Return (gte, lte, span) in microseconds, or None if not present."""
    if not filter_str:
        return None
    try:
        obj = json.loads(filter_str)
        bounds = {}
        _collect_ts_bounds(obj, field, bounds)
        if "$gte" in bounds and "$lte" in bounds:
            gte, lte = bounds["$gte"], bounds["$lte"]
            return gte, lte, lte - gte
    except Exception:
        pass
    return None


def fmt_span_us(span_us):
    s = span_us / 1_000_000
    if s < 60:
        return f"{s:.0f}s"
    if s < 3_600:
        return f"{s/60:.1f}m"
    if s < 86_400:
        return f"{s/3600:.1f}h"
    return f"{s/86400:.1f}d"


def ts_range_stats(rows, field="adrLogTimestamp"):
    spans = []
    for r in rows:
        result = extract_ts_span_us(r.get("SanitizedFilter", ""), field)
        if result:
            spans.append(result[2])
    return spans


def print_ts_range_analysis(rows, total, field="adrLogTimestamp"):
    spans = ts_range_stats(rows, field)
    if not spans:
        return

    buckets = [
        ("< 1 hour",   lambda s: s <  3_600_000_000),
        ("1h – 24h",   lambda s: 3_600_000_000 <= s <  86_400_000_000),
        ("1d – 7d",    lambda s: 86_400_000_000 <= s < 604_800_000_000),
        ("> 7 days",   lambda s: s >= 604_800_000_000),
    ]

    print()
    print("=" * 80)
    print(f"  {field} RANGE ANALYSIS  ({len(spans):,} of {total:,} queries have a range)")
    print("=" * 80)
    print(f"  min span : {fmt_span_us(min(spans))}")
    print(f"  max span : {fmt_span_us(max(spans))}")
    print(f"  avg span : {fmt_span_us(int(mean(spans)))}")
    print(f"  median   : {fmt_span_us(int(sorted(spans)[len(spans)//2]))}")
    print()
    print(f"  {'Window size':<14}  {'Count':>7}  {'%':>6}")
    print(f"  {'─'*14}  {'─'*7}  {'─'*6}")
    for label, test in buckets:
        n = sum(1 for s in spans if test(s))
        print(f"  {label:<14}  {n:>7,}  {n/len(spans)*100:>6.1f}%")
    print()


# ── Field frequency analysis ─────────────────────────────────────────────────

def collect_field_ops(obj, field_ops):
    """
    Recursively walk a filter object, accumulating operator sets per field name.
    Merges across $and/$or so split range queries (two $and elements for the same
    field) are combined into one entry, e.g. adrLogTimestamp[$gte,$lte].
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("$and", "$or", "$nor"):
                if isinstance(v, list):
                    for item in v:
                        collect_field_ops(item, field_ops)
            elif not k.startswith("$"):
                if isinstance(v, dict):
                    for op in v:
                        if op.startswith("$"):
                            field_ops[k].add(op)
                else:
                    field_ops[k].add("$eq")
    elif isinstance(obj, list):
        for item in obj:
            collect_field_ops(item, field_ops)


def analyze_field_frequency(rows):
    """
    Return a dict mapping 'field[ops]' -> count of queries containing that field.
    """
    freq = defaultdict(int)
    for row in rows:
        filter_str = row.get("SanitizedFilter", "")
        if not filter_str:
            continue
        try:
            obj = json.loads(filter_str)
        except json.JSONDecodeError:
            continue
        field_ops = defaultdict(set)
        collect_field_ops(obj, field_ops)
        for field, ops in field_ops.items():
            key = f"{field}[{','.join(sorted(ops))}]"
            freq[key] += 1
    return freq


def print_field_frequency(rows, total):
    freq = analyze_field_frequency(rows)
    if not freq:
        return

    print()
    print("=" * 80)
    print(f"  FIELD FREQUENCY  (across all {total:,} queries)")
    print("=" * 80)
    print(f"  {'Field':<45} {'Count':>7}  {'%':>6}")
    print(f"  {'─'*45} {'─'*7}  {'─'*6}")

    for key, count in sorted(freq.items(), key=lambda x: -x[1]):
        pct = count / total * 100
        print(f"  {key:<45} {count:>7,}  {pct:>6.1f}%")
    print()


# ── Report printing ───────────────────────────────────────────────────────────

def print_report(groups, total, all_rows):
    print()
    print("=" * 80)
    print(f"  QUERY SHAPE ANALYSIS  —  {total} slow queries,  {len(groups)} unique shapes")
    print("=" * 80)

    for query_hash, rows in groups.items():
        count = len(rows)
        pct = count / total * 100

        # Operations breakdown
        op_counts = defaultdict(int)
        for r in rows:
            op_counts[r["Operation"].strip()] += 1
        if len(op_counts) == 1:
            op_str = next(iter(op_counts))
        else:
            op_str = "  ".join(
                f"{op}×{n:,}" for op, n in sorted(op_counts.items(), key=lambda x: -x[1])
            )

        # Duration stats
        dur_vals = [to_int(r["DurationMs"]) for r in rows]
        dur = stats(dur_vals)
        dur_str = (
            f"min={fmt_ms(dur[0])}  max={fmt_ms(dur[1])}  avg={fmt_ms(int(dur[2]))}"
            if dur else "n/a"
        )

        # Scan stats
        keys_st  = stats([to_int(r["KeysExamined"]) for r in rows])
        docs_st  = stats([to_int(r["DocsExamined"])  for r in rows])
        ret_st   = stats([to_int(r["Returned"])       for r in rows])
        bytes_st = stats([to_int(r["BytesRead"])      for r in rows])

        # Plans seen
        plans = list(dict.fromkeys(
            r["PlanSummary"].strip() for r in rows if r.get("PlanSummary", "").strip()
        ))

        # Timestamps
        timestamps = sorted(r["Timestamp"] for r in rows if r.get("Timestamp"))
        time_range = (
            f"{timestamps[0][:19]}  →  {timestamps[-1][:19]}"
            if len(timestamps) >= 2 else (timestamps[0][:19] if timestamps else "n/a")
        )

        # Filter shape
        sample_filter = next((r["SanitizedFilter"] for r in rows if r.get("SanitizedFilter")), "")
        shape = extract_shape(sample_filter)

        issues = detect_issues(rows)

        print()
        print(f"  ┌─ QueryHash: {query_hash}  ({count} occurrences, {pct:.0f}%)")
        print(f"  │  Operation : {op_str}")
        print(f"  │  When      : {time_range}")
        print(f"  │  Duration  : {dur_str}")

        if plans:
            for i, p in enumerate(plans):
                label = "Plan      :" if i == 0 else "           "
                print(f"  │  {label} {p}")
        else:
            print(f"  │  Plan      : (none logged)")

        if keys_st:
            print(f"  │  KeysEx    : min={keys_st[0]:,}  max={keys_st[1]:,}  avg={keys_st[2]:,.0f}")
        if docs_st:
            print(f"  │  DocsEx    : min={docs_st[0]:,}  max={docs_st[1]:,}  avg={docs_st[2]:,.0f}")
        if ret_st:
            print(f"  │  Returned  : min={ret_st[0]:,}  max={ret_st[1]:,}  avg={ret_st[2]:,.0f}")
        if bytes_st:
            print(f"  │  BytesRead : avg={fmt_bytes(int(bytes_st[2]))}  max={fmt_bytes(bytes_st[1])}")

        spans = ts_range_stats(rows)
        if spans:
            span_st = stats(spans)
            print(f"  │  TsWindow  : min={fmt_span_us(span_st[0])}  max={fmt_span_us(span_st[1])}  avg={fmt_span_us(int(span_st[2]))}")

        if issues:
            print(f"  │  ⚠  Issues :")
            for issue in issues:
                print(f"  │       • {issue}")

        # Wrap shape at 70 chars
        shape_label = "  └  Shape   : "
        indent = " " * len(shape_label)
        wrapped = _wrap(shape, 70)
        print(f"{shape_label}{wrapped[0]}")
        for line in wrapped[1:]:
            print(f"{indent}{line}")

    # ── Summary table ─────────────────────────────────────────────────────────
    print()
    print("=" * 80)
    print("  SUMMARY TABLE")
    print("=" * 80)
    print(f"  {'QueryHash':<14} {'N':>4} {'%':>5}  {'Op':<10} {'AvgDur':>8}  Issues")
    print(f"  {'─'*14} {'─'*4} {'─'*5}  {'─'*10} {'─'*8}  {'─'*35}")

    for query_hash, rows in groups.items():
        count = len(rows)
        pct   = count / total * 100
        op    = max(defaultdict(int, {r["Operation"].strip(): 1 for r in rows}),
                    key=lambda k: sum(1 for r in rows if r["Operation"].strip() == k))
        dur_vals = [to_int(r["DurationMs"]) for r in rows]
        dur_st   = stats(dur_vals)
        avg_dur  = fmt_ms(int(dur_st[2])) if dur_st else "n/a"
        issues   = detect_issues(rows)
        first_issue = issues[0] if issues else "—"
        print(f"  {query_hash:<14} {count:>4} {pct:>5.1f}%  {op:<10} {avg_dur:>8}  {first_issue}")

    print_ts_range_analysis(all_rows, total)
    print_field_frequency(all_rows, total)


def _wrap(text, width):
    """Split text into lines of at most `width` characters, breaking at spaces."""
    if len(text) <= width:
        return [text]
    lines = []
    while len(text) > width:
        cut = text.rfind(" ", 0, width)
        if cut == -1:
            cut = width
        lines.append(text[:cut])
        text = text[cut:].lstrip()
    if text:
        lines.append(text)
    return lines


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: analyze_query_shapes.py <results.csv>")
        sys.exit(1)

    filename = sys.argv[1]
    rows = load_csv(filename)
    if not rows:
        print("No data found in", filename)
        sys.exit(1)

    groups = group_by_shape(rows)
    print_report(groups, len(rows), rows)


if __name__ == "__main__":
    main()
