from __future__ import annotations

import unittest


class RegressionTests(unittest.TestCase):
    def test_parallel_bitmap_scan(self) -> None:
        example_query = """
            /*=pg_lab=
                Result(workers=2)
                BitmapScan(p)
            */
            SELECT count(*)
            FROM posts p
            WHERE id < 100 AND parentid > 100000;
        """

        expected_plan = {}
