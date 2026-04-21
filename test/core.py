from __future__ import annotations

import unittest
from typing import Optional


import psycopg

def relname(plan: dict) -> Optional[str]:
    alias = plan.get("Alias")
    if alias is not None:
        return alias
    return plan.get("Relation Name")


def explain_plan(query: str, cur: psycopg.Cursor) -> dict:
    explain_query = f"EXPLAIN (FORMAT JSON) {query}"
    cur.execute(explain_query)
    plan_json = cur.fetchone()[0][0]
    return plan_json["Plan"]

def _build_intermediates(plan: dict) -> None:
    if plan["Node Type"] in _ScanOps:
        intermediate = relname(plan)
        plan["Intermediates"] = [intermediate]
    else:
        intermediates = [
            _build_intermediates(child) or child["Intermediates"]
            for child in plan.get("Plans", [])
        ]
        plan["Intermediates"] = [tab for subplan in intermediates for tab in subplan]


_ScanOps = [
    "Seq Scan",
    "Index Scan",
    "Index Only Scan",
    "Bitmap Heap Scan",
]


def determine_join_order(plan: dict) -> str:
    if plan["Node Type"] in _ScanOps:
        rel = relname(plan)
        assert rel is not None, f"Expected a relation name for plan node: {plan}"
        return rel

    nested = [determine_join_order(subplan) for subplan in plan.get("Plans", [])]
    match nested:
        case [lhs, rhs]:
            return f"({lhs} {rhs})"
        case [child]:
            return child
        case _:
            raise ValueError(f"Unexpected plan structure: {plan}")


def determine_operators(plan: dict, *, parallel_workers: int = 0) -> list[str]:
    hints: list[str] = []
    intermediate = " ".join(plan["Intermediates"])

    parallel_subplan = False
    match plan["Node Type"]:
        case "Hash Join":
            operator = "HashJoin"
        case "Merge Join":
            operator = "MergeJoin"
        case "Nested Loop":
            operator = "NestLoop"
        case "Seq Scan":
            operator = "SeqScan"
        case "Index Scan":
            operator = "IdxScan"
        case "Index Only Scan":
            operator = "IdxScan"
        case "Bitmap Heap Scan":
            operator = "BitmapScan"
        case "Materialize":
            operator = "Material"
        case "Memoize":
            operator = "Memo"
        case "Aggregate" | "Hash" | "Sort" | "Limit" | "Bitmap Index Scan":
            operator = None
        case "Gather" | "Gather Merge":
            operator = None
            parallel_subplan = True
            parallel_workers = plan.get("Workers Planned", 0)
        case _:
            raise ValueError(f"Unexpected plan node type: {plan['Node Type']}")

    if operator is not None and parallel_workers:
        hints.append(f"{operator}({intermediate} (workers={parallel_workers}))")
    elif operator is not None:
        hints.append(f"{operator}({intermediate})")
    elif parallel_workers and not parallel_subplan:
        hints.append(f"Result(workers={parallel_workers})")
        parallel_subplan = False

    if not parallel_subplan:
        # reset parallel workers for child plans
        parallel_workers = 0

    if plan["Node Type"] in _ScanOps:
        return hints

    for child in plan.get("Plans", []):
        hints.extend(determine_operators(child, parallel_workers=parallel_workers))

    return hints


def extract_hint_set(plan: dict, *, plan_mode: str) -> str:
    hints: list[str] = ["/*=pg_lab="]
    if plan_mode:
        hints.append(f"Config(plan_mode={plan_mode})")

    _build_intermediates(plan)

    join_order = determine_join_order(plan)
    hints.append(f"JoinOrder({join_order})")
    hints.extend(determine_operators(plan))

    hints.append("*/")
    return "\n".join(hints)

class PostgresTestCase(unittest.TestCase):
    def assertPlansEqual(self, plan1: dict, plan2: dict, msg: str = "") -> None:
        if plan1["Node Type"] != plan2["Node Type"]:
            self.fail(
                f"{msg}\nDifferent operators: {plan1['Node Type']} != {plan2['Node Type']}"
            )

        rel1 = relname(plan1)
        rel2 = relname(plan2)
        if rel1 != rel2:
            self.fail(f"{msg}\nScanning different relations: {plan1} != {plan2}")

        childs1 = plan1.get("Plans", [])
        childs2 = plan2.get("Plans", [])
        if len(childs1) != len(childs2):
            self.fail(
                f"{msg}\nNumber of child plans differ: {len(childs1)} != {len(childs2)}"
            )
        for child1, child2 in zip(childs1, childs2):
            self.assertPlansEqual(child1, child2, msg=msg)
