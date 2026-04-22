from __future__ import annotations

import unittest
from typing import Optional, Self


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


class Plan:
    def __init__(
        self, node_type: str = "", *, relname: str = "", alias: str = ""
    ) -> None:
        self.node_type = node_type
        self.relname = relname
        self.alias = alias
        self.children: list[Plan] = []
        self.parent: Plan | None = None
        self.parallel_workers: int = 0

    def Aggregate(self) -> Plan:
        child = Plan("Aggregate")
        child.parent = self
        self.children.append(child)
        return child

    def Gather(self, *, workers: int) -> Plan:
        child = Plan("Gather")
        child.parallel_workers = workers
        child.parent = self
        self.children.append(child)
        return child

    def GatherMerge(self, *, workers: int) -> Plan:
        child = Plan("Gather Merge")
        child.parallel_workers = workers
        child.parent = self
        self.children.append(child)
        return child

    def Memoize(self) -> Plan:
        child = Plan("Memoize")
        child.parent = self
        self.children.append(child)
        return child

    def Materialize(self) -> Plan:
        child = Plan("Materialize")
        child.parent = self
        self.children.append(child)
        return child

    def Sort(self) -> Plan:
        child = Plan("Sort")
        child.parent = self
        self.children.append(child)
        return child

    def Limit(self) -> Plan:
        child = Plan("Limit")
        child.parent = self
        self.children.append(child)
        return child

    def NestedLoop(self) -> JoinNode:
        child = JoinNode("Nested Loop")
        child.parent = self
        self.children.append(child)
        return child

    def HashJoin(self) -> JoinNode:
        child = JoinNode("Hash Join")
        child.parent = self
        self.children.append(child)
        return child

    def MergeJoin(self) -> JoinNode:
        child = JoinNode("Merge Join")
        child.parent = self
        self.children.append(child)
        return child

    def SequentialScan(self, relname: str, *, alias: str = "") -> Plan:
        child = Plan("Seq Scan", relname=relname, alias=alias)
        child.parent = self
        self.children.append(child)
        return child

    def IndexScan(self, relname: str, *, alias: str = "") -> Plan:
        child = Plan("Index Scan", relname=relname, alias=alias)
        child.parent = self
        self.children.append(child)
        return child

    def IndexOnlyScan(self, relname: str, *, alias: str = "") -> Plan:
        child = Plan("Index Only Scan", relname=relname, alias=alias)
        child.parent = self
        self.children.append(child)
        return child

    def BitmapHeapScan(self, relname: str, *, alias: str = "") -> Plan:
        child = Plan("Bitmap Heap Scan", relname=relname, alias=alias)
        child.parent = self
        self.children.append(child)
        return child

    def BitmapIndexScan(self) -> Plan:
        child = Plan("Bitmap Index Scan")
        child.parent = self
        self.children.append(child)
        return child

    def explain(self) -> dict:
        root = self._traverse_upwards()
        return root._build_internal()

    def hint_set(self) -> str:
        return extract_hint_set(self.explain(), plan_mode="full")

    def _traverse_upwards(self) -> Plan:
        node = self
        while node.parent is not None:
            node = node.parent
        return node

    def _build_internal(self) -> dict:
        if not self.node_type:
            assert len(self.children) == 1, (
                "Expected exactly one child for dummy plan node"
            )
            return self.children[0]._build_internal()

        plan: dict = {"Node Type": self.node_type}
        if self.relname:
            plan["Relation Name"] = self.relname
        if self.alias:
            plan["Alias"] = self.alias
        if self.parallel_workers:
            plan["Workers Planned"] = self.parallel_workers

        if not self.children:
            return plan

        plan["Plans"] = [child._build_internal() for child in self.children]
        return plan


class JoinNode(Plan):
    def __init__(self, node_type: str, *, relname: str = "") -> None:
        super().__init__(node_type, relname=relname)

    def outer(self, node: Plan) -> Self:
        node = node._traverse_upwards()
        node.parent = self
        self.children.append(node)
        return self

    def inner(self, node: Plan) -> Self:
        node = node._traverse_upwards()
        node.parent = self
        self.children.append(node)
        return self


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

        children1 = plan1.get("Plans", [])
        children2 = plan2.get("Plans", [])
        if len(children1) != len(children2):
            self.fail(
                f"{msg}\nNumber of child plans differ: {len(children1)} != {len(children2)}"
            )
        for child1, child2 in zip(children1, children2):
            self.assertPlansEqual(child1, child2, msg=msg)
