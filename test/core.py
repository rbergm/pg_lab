from __future__ import annotations

import unittest
from typing import Optional


def relname(plan: dict) -> Optional[str]:
    alias = plan.get("Alias")
    if alias is not None:
        return alias
    return plan.get("Relation Name")


class PostgresTestCase(unittest.TestCase):
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
