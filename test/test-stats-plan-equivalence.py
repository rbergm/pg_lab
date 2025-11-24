from __future__ import annotations

import argparse
import os
import textwrap
import unittest
import warnings
from pathlib import Path
from typing import Optional

import psycopg


DB_NAME = "stats-test"


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
    def __init__(
        self, methodName: str = "runTest", *, queries: Optional[set[str]] = None
    ) -> None:
        super().__init__(methodName)
        self._queries = queries

    def setUp(self) -> None:
        _init_db()
        self.conn = psycopg.connect(dbname=DB_NAME, host="localhost")
        self.workload = _load_workload()
        self._filter_workload()

    def tearDown(self) -> None:
        self.conn.close()

    def assertPlansEqual(self, plan1: dict, plan2: dict, msg: str = "") -> None:
        if plan1["Node Type"] != plan2["Node Type"]:
            self.fail(
                f"{msg}\nDifferent operators: {plan1['Node Type']} != {plan2['Node Type']}"
            )

        rel1 = _relname(plan1)
        rel2 = _relname(plan2)
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

    def test_plan_equivalence(self) -> None:
        for label, query in self.workload.items():
            with self.subTest(label=label):
                self._check_query(query, label=label)
            self.conn.rollback()

    def _filter_workload(self) -> None:
        if not self._queries:
            return

        self.workload = {
            label: query
            for label, query in self.workload.items()
            if any(label.startswith(q) for q in self._queries)
        }

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


class CardinalityHintingTests(unittest.TestCase):
    def setUp(self) -> None:
        _init_db()
        self.conn = psycopg.connect(dbname=DB_NAME, host="localhost")

    def tearDown(self):
        self.conn.close()

    def test_plain_rel(self) -> None:
        query = "SELECT * FROM posts"
        target_card = 4242
        with self.conn.cursor() as cur:
            self._check_query(query, card=target_card, intermediate="posts", cur=cur)

    def test_filtered_rel(self) -> None:
        query = "SELECT * FROM users WHERE id > 3000"
        target_card = 1234
        with self.conn.cursor() as cur:
            self._check_query(query, card=target_card, intermediate="users", cur=cur)

    def test_join_rel(self) -> None:
        query = "SELECT * FROM posts p JOIN users u ON p.owneruserid = u.id WHERE u.id > 3000"
        target_card = 5678
        with self.conn.cursor() as cur:
            self._check_query(query, card=target_card, intermediate="p u", cur=cur)

    def _check_query(self, query: str, *, card: int, intermediate: str, cur: psycopg.Cursor) -> None:
        native_plan = _explain_plan(query, cur)
        native_cardinality = native_plan["Plan Rows"]

        hints = f"/*=pg_lab= Card({intermediate} #{card}) */"
        hinted_query = f"{hints}\n{query}"
        hinted_plan = _explain_plan(hinted_query, cur)
        hinted_cardinality = hinted_plan["Plan Rows"]

        self.assertNotEqual(
            native_cardinality,
            hinted_cardinality,
            msg="Cardinality hint did not change the plan cardinality",
        )
        self.assertEqual(
            hinted_cardinality,
            card,
            msg="Cardinality hint did not set the expected cardinality",
        )


if __name__ == "__main__":
    description = textwrap.dedent("""
        Regression test suite for pg_lab plan hinting functionality.

        By default, all tests are run using the normal test discovery mechanism.
        Individual tests can be executed via the different parameters.
    """).strip()

    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--queries", "-q", nargs="*", help="Only runs the full plan hinting test with these specific queries")

    args = parser.parse_args()
    test_queries = set(args.queries) if args.queries else None

    if test_queries:
        suite = unittest.TestSuite()
        suite.addTest(
            TestFullPlanHinting(
                methodName="test_plan_equivalence",
                queries=test_queries,
            )
        )
        runner = unittest.TextTestRunner()
        runner.run(suite)
    else:
        unittest.main()
