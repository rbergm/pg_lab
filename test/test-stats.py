from __future__ import annotations

import argparse
import os
import textwrap
import unittest
import warnings
from pathlib import Path
from typing import Optional

import psycopg

import core

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


class Fundamentals(core.PostgresTestCase):
    def setUp(self) -> None:
        _init_db()
        self.conn = psycopg.connect(dbname=DB_NAME, host="localhost")

    def tearDown(self):
        try:
            self.conn.close()
        except psycopg.DatabaseError:
            pass

    def test_idxscan(self) -> None:
        query = """
            /*=pg_lab=
              IdxScan(posts)
             */
            SELECT count(*) FROM posts;
        """

        expected_plan = core.Plan().Aggregate().IndexOnlyScan("posts").explain()

        with self.conn.cursor() as cur:
            actual_plan = core.explain_plan(query, cur)

        self.assertPlansEqual(expected_plan, actual_plan)


class ParallelizationHints(core.PostgresTestCase):
    def test_parallel_scan(self) -> None:
        pass


class FullPlanHinting(core.PostgresTestCase):
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
        try:
            self.conn.close()
        except psycopg.DatabaseError:
            pass

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


class CardinalityHinting(core.PostgresTestCase):
    def setUp(self) -> None:
        _init_db()
        self.conn = psycopg.connect(dbname=DB_NAME, host="localhost")

    def tearDown(self):
        try:
            self.conn.close()
        except psycopg.DatabaseError:
            pass

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

    def _check_query(
        self, query: str, *, card: int, intermediate: str, cur: psycopg.Cursor
    ) -> None:
        native_plan = core.explain_plan(query, cur)
        native_cardinality = native_plan["Plan Rows"]

        hints = f"/*=pg_lab= Card({intermediate} #{card}) */"
        hinted_query = f"{hints}\n{query}"
        hinted_plan = core.explain_plan(hinted_query, cur)
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


class RegressionTests(core.PostgresTestCase):
    def setUp(self) -> None:
        _init_db()
        self.conn = psycopg.connect(dbname=DB_NAME, host="localhost")

    def tearDown(self):
        try:
            self.conn.close()
        except psycopg.DatabaseError:
            pass

    def test_bitmap_only_plan(self) -> None:
        # Fixed in: da1e9916cf9284abe2f5fee540727596d3400934
        query = """
            /*=pg_lab=
              Config(plan_mode=full)
              JoinOrder((((v p) u) b))
            
              NestLoop(v p u b)
              NestLoop(v p u (workers=1))
              NestLoop(v p)
            
              SeqScan(v)
              BitmapScan(p)
              Memo(p)
              BitmapScan(u)
              Bitmapscan(b)
             */
            SELECT count(*)
            FROM users u
            JOIN badges b ON u.id = b.userid
            JOIN  posts p ON p.owneruserid = u.id
            JOIN  votes v ON p.id = v.postid
            AND u.id = v.userid;
        """

        expected_plan = (
            core.Plan()
            .Aggregate()
            .NestedLoop()
            .outer(
                core.Plan()
                .Gather(workers=1)
                .NestedLoop()
                .outer(
                    core.Plan()
                    .NestedLoop()
                    .outer(core.Plan().SequentialScan("votes", alias="v"))
                    .inner(
                        core.Plan()
                        .Memoize()
                        .BitmapHeapScan("posts", alias="p")
                        .BitmapIndexScan()
                    )
                )
                .inner(core.Plan().BitmapHeapScan("users", alias="u").BitmapIndexScan())
            )
            .inner(core.Plan().BitmapHeapScan("badges", alias="b").BitmapIndexScan())
            .explain()
        )

        with self.conn.cursor() as cur:
            actual_plan = core.explain_plan(query, cur)
        self.assertPlansEqual(expected_plan, actual_plan)


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
        "--queries",
        "-q",
        nargs="*",
        help="Only runs the full plan hinting test with these specific queries",
    )

    args = parser.parse_args()
    test_queries = set(args.queries) if args.queries else None

    if test_queries:
        suite = unittest.TestSuite()
        suite.addTest(
            FullPlanHinting(
                methodName="test_plan_equivalence",
                queries=test_queries,
            )
        )
        runner = unittest.TextTestRunner()
        runner.run(suite)
    else:
        unittest.main()
