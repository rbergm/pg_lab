
import textwrap
import unittest
from pathlib import Path
from typing import Optional
import sys

import argparse
import core


import psycopg

CONNECT_STRING = "dbname=imdb user=postgres host=localhost"
WORKLOAD_DIR = ""
QUERY_GLOB = "*.sql"

def _load_workload() -> dict[str, str]:
    workload_dir = Path(WORKLOAD_DIR)
    sql_files = workload_dir.resolve().glob(QUERY_GLOB)
    return {q.name: q.read_text() for q in sorted(sql_files)}


class TestFullPlanHinting(core.PostgresTestCase):

    def __init__(
        self, methodName: str = "runTest"
    ) -> None:
        super().__init__(methodName)

    def setUp(self) -> None:
        self.conn = psycopg.connect(CONNECT_STRING)
        self.workload = _load_workload()

    def tearDown(self) -> None:
        self.conn.close()

    def test_plan_equivalence(self) -> None:
        for label, query in self.workload.items():
            with self.subTest(label=label):
                self._check_query(query, label=label)
            self.conn.rollback()


    def _check_query(self, query: str, *, label: str) -> None:
        with self.conn.cursor() as cur:
            native_plan = core.explain_plan(query, cur)
            hints = core.extract_hint_set(native_plan, plan_mode="full")
            hinted_query = f"{hints}\n{query}"

            try:
                hinted_plan = core.explain_plan(hinted_query, cur)
            except psycopg.errors.InternalError as e:
                self.fail(f"Query {label}\n\n{hinted_query}\nFailed with error: {e}")

            self.assertPlansEqual(
                native_plan,
                hinted_plan,
                msg=f"Plans differ for query {label}\n\n{hinted_query}",
            )


def eval_query(label: str) -> None:
    workload = _load_workload()
    query = workload.get(label)
    if not query:
        print(f"Query with label {label} not found in workload.")
        return

    conn = psycopg.connect(CONNECT_STRING)
    with conn.cursor() as cur:
        native_plan = core.explain_plan(query, cur)
        hints = core.extract_hint_set(native_plan, plan_mode="full")
        hinted_query = f"{hints}\n{query}"
        print(f"Native plan for query {label}:\n{native_plan}\n")
        print(f"Hinted query for query {label}:\n{hinted_query}\n")

        hinted_plan = core.explain_plan(hinted_query, cur)
        print(f"Hinted plan for query {label}:\n{hinted_plan}\n")
    

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
    parser.add_argument(
        "--connect",
        "-c",
        help="The connection string to use for the tests",
    )
    parser.add_argument(
        "--workload",
        "-w",
        help="The queries to test",
    )
    parser.add_argument(
        "--glob",
        help="The glob pattern for SQL files",
    )
    parser.add_argument("--single", help="Evaluate a single query given by its label.")

    args = parser.parse_args()
    WORKLOAD_DIR = args.workload or WORKLOAD_DIR
    CONNECT_STRING = args.connect or CONNECT_STRING
    QUERY_GLOB = args.glob or QUERY_GLOB

    if args.single:
        eval_query(args.single)
    else:
        unittest.main(argv=[sys.argv[0]])