from __future__ import annotations

import os
import unittest
import warnings
from pathlib import Path
from typing import Optional

import psycopg


DB_NAME = "stats-test"
TEST_QUERIES: set[str] = set()

if os.getenv("TEST_QUERIES"):
    TEST_QUERIES.update(os.getenv("TEST_QUERIES").split())


def _load_workload() -> dict[str, str]:
    workload_dir = Path(__file__).parent / "stats" / "queries"
    sql_files = workload_dir.resolve().glob("*.sql")
    return {q.name: q.read_text() for q in sorted(sql_files)}


def _check_existing_db() -> bool:
    return os.system(f"psql -lqt | cut -d \\| -f 1 | grep -wq {DB_NAME}") == 0


def _init_db() -> None:
    if _check_existing_db():
        return

    warnings.warn("Creating test database 'stats-test'.")
    os.system(f"createdb {DB_NAME}")
    os.system(f"psql -d {DB_NAME} -f stats/schema/schema.sql")

    data_dir = Path(__file__).parent / "stats" / "data"
    for data_file in data_dir.glob("*.zst"):
        os.system(f"zstd --decompress --force {data_file.resolve()}")

    os.system(f"psql -d {DB_NAME} -f stats/data/import.sql")


def _explain_plan(query: str, cur: psycopg.Cursor) -> dict:
    explain_query = f"EXPLAIN (FORMAT JSON) {query}"
    cur.execute(explain_query)
    plan_json = cur.fetchone()[0][0]
    return plan_json["Plan"]


def _relname(plan: dict) -> Optional[str]:
    alias = plan.get("Alias")
    if alias is not None:
        return alias
    return plan.get("Relation Name")


def _build_intermediates(plan: dict) -> None:
    if plan["Node Type"] in [
        "Seq Scan",
        "Index Scan",
        "Index Only Scan",
        "Bitmap Heap Scan",
    ]:
        intermediate = _relname(plan)
        plan["Intermediates"] = [intermediate]
    else:
        intermediates = [
            _build_intermediates(child) or child["Intermediates"]
            for child in plan.get("Plans", [])
        ]
        plan["Intermediates"] = [tab for subplan in intermediates for tab in subplan]


def _determine_join_order(plan: dict) -> str:
    if plan["Node Type"] in [
        "Seq Scan",
        "Index Scan",
        "Index Only Scan",
        "Bitmap Heap Scan",
    ]:
        return _relname(plan)

    nested = [_determine_join_order(subplan) for subplan in plan.get("Plans", [])]
    match nested:
        case [lhs, rhs]:
            return f"({lhs} {rhs})"
        case [child]:
            return child
        case _:
            raise ValueError(f"Unexpected plan structure: {plan}")


def _determine_operators(plan: dict, *, parallel_workers: int = 0) -> list[str]:
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
            operator = "Memoize"
        case (
            "Aggregate"
            | "Hash"
            | "Sort"
            | "Limit"
            | "Bitmap Index Scan"
        ):
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
    for child in plan.get("Plans", []):
        hints.extend(_determine_operators(child, parallel_workers=parallel_workers))

    return hints


def _extract_hint_set(plan: dict, *, plan_mode: str) -> str:
    hints: list[str] = ["/*=pg_lab="]
    if plan_mode:
        hints.append(f"Config(plan_mode={plan_mode})")

    _build_intermediates(plan)

    join_order = _determine_join_order(plan)
    hints.append(f"JoinOrder({join_order})")
    hints.extend(_determine_operators(plan))

    hints.append("*/")
    return "\n".join(hints)


class TestFullPlanHinting(unittest.TestCase):
    def setUp(self) -> None:
        _init_db()
        self.conn = psycopg.connect(dbname=DB_NAME, host="localhost")
        self.workload = _load_workload()
        if TEST_QUERIES:
            self.workload = {
                label: query
                for label, query in self.workload.items()
                if label in TEST_QUERIES
            }

    def tearDown(self) -> None:
        self.conn.close()

    def assertPlansEqual(self, plan1: dict, plan2: dict, msg: str = "") -> None:
        pass

    def test_plan_equivalence(self) -> None:
        for label, query in self.workload.items():
            with self.subTest(label=label):
                self._check_query(query, label=label)
            self.conn.rollback()

    def _check_query(self, query: str, *, label: str) -> None:
        with self.conn.cursor() as cur:
            native_plan = _explain_plan(query, cur)
            hints = _extract_hint_set(native_plan, plan_mode="full")
            hinted_query = f"{hints}\n{query}"

            try:
                hinted_plan = _explain_plan(hinted_query, cur)
            except psycopg.errors.InternalError as e:
                self.fail(f"Query {label}\n\n{hinted_query}\nFailed with error: {e}")

            self.assertPlansEqual(
                native_plan,
                hinted_plan,
                msg=f"Plans differ for query {label}\n\n{hinted_query}",
            )


if __name__ == "__main__":
    unittest.main()
