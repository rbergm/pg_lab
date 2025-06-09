"""
Extracted from pg_gdb by rafonseca

 Author:    Renan Alves Fonseca
    URL:    https://github.com/rafonseca/pg_gdb
License:    GPLv3
"""

from gdb import Value, ValuePrinter, pretty_printers, lookup_type


tNode = lookup_type("Node")
tList = lookup_type("List")


tNodeTag = lookup_type("NodeTag")


skip_empty = True

smart_print = True


class NodePrinter(ValuePrinter):
    def __init__(self, val: Value, label="NODE*") -> None:
        self._val = val
        self._label = label

    def children(self):
        def _iter():
            for field in self._val.type.fields():
                assert field.name is not None
                name = field.name
                value = self._val[name]
                if skip_empty and not value:
                    continue
                yield name, value

        return _iter()


class ListPrinter(ValuePrinter):
    def __init__(self, val: Value, node_type: str) -> None:
        self._val = val
        self._node_type = node_type

    def display_hint(self):
        return "array"

    def children(self):
        def _iter():
            node_type = self._node_type
            ListCell_field = {
                "T_List": "ptr_value",
                "T_OidList": "oid_value",
                "T_IntList": "int_value",
                "T_XidList": "xid_value",
            }

            for i in range(self._val["length"]):
                try:
                    value = self._val["elements"][i][ListCell_field[node_type]]
                    if node_type == "T_List":
                        value = value.cast(tNode.pointer())
                    yield str(i), value

                except Exception as exc:
                    print(exc)
                    continue

        return _iter()


def dispatcher(val: Value):

    if not smart_print:
        return
    if val.type in [
        lookup_type(t)
        for t in ["Plan", "Expr", "Integer", "Tuplestorestate", "TSReadPointer"]
    ]:
        return NodePrinter(val)
    try:
        val = val.dereference()
    except:

        # only dispatch pointer fields. non-pointer fields are handled
        # by other printers
        return

    if val.type == tNodeTag:
        # the dispatcher will be invoked in format_string below. we
        # should early return None, when val is a NodeTag. Otherwise
        # we'll incur in infinite recursion.
        return

    try:
        node_type = val.cast(tNode)["type"].format_string()
        if node_type in [
            "T_Invalid",
            "T_AllocSetContext",  # causes circular ref?
            "T_WithCheckOption",  # workaround: char* gets wrongly converted to T_WithCheckOption
            # note: should avoid char* conversion and remove entry above
        ]:

            return
    except:
        return

    if node_type in ["T_List", "T_IntList", "T_OidList", "T_XidList"]:
        return ListPrinter(val.cast(tList), node_type)

    if node_type.startswith("T_"):
        val = val.cast(lookup_type(node_type[2:]))
        return NodePrinter(val)


if len(pretty_printers) > 1:
    pretty_printers.pop(0)
pretty_printers.insert(0, dispatcher)
