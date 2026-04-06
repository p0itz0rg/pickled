# /// script
# requires-python = ">=3.9"
# ///
"""
Regenerate pickle test fixtures.

Usage:
    uv run scripts/generate_test_data.py
"""

import math
import os
import pickle
import sys

OUT = os.path.join(os.path.dirname(__file__), "..", "test", "data")
MAX_PROTO = 5


def write(name, obj, proto):
    path = os.path.join(OUT, f"{name}_proto{proto}.pickle")
    with open(path, "wb") as fp:
        pickle.dump(obj, fp, proto)


def write_single(name, obj, proto=MAX_PROTO):
    path = os.path.join(OUT, f"{name}.pickle")
    with open(path, "wb") as fp:
        pickle.dump(obj, fp, proto)


longish = 10_000_000_000 * 10_000_000_000


class Class:
    def __init__(self):
        self.attr = 5


class ReduceClass:
    def __reduce__(self):
        return (ReduceClass, ())


test_object = {
    None: None,
    False: (False, True),
    10: 100000,
    longish: longish,
    1.0: 1.0,
    b"bytes": b"bytes",
    "string": "string",
    (1, 2): (1, 2, 3),
    frozenset((42, 0)): frozenset((42, 0)),
    (): [
        [1, 2, 3],
        set([42, 0]),
        {},
        bytearray(b"\x00\x55\xaa\xff"),
    ],
    7: Class(),
}

for proto in range(MAX_PROTO + 1):
    path = os.path.join(OUT, f"tests_py3_proto{proto}.pickle")
    with open(path, "wb") as fp:
        pickle.dump(test_object, fp, proto)

rec_list = []
rec_list.append(([rec_list],))
for proto in range(MAX_PROTO + 1):
    write("test_recursive", rec_list, proto)

write_single("test_unresolvable_global", ReduceClass())


class SimpleClass:
    def __init__(self):
        self.x = 42
        self.name = "hello"


class SlottedClass:
    __slots__ = ["x", "y"]
    def __init__(self):
        self.x = 10
        self.y = 20


class NestedClass:
    def __init__(self):
        self.inner = SimpleClass()
        self.value = [1, 2, 3]


class EmptyClass:
    pass


for proto in range(MAX_PROTO + 1):
    write("test_simple_class", SimpleClass(), proto)
    write("test_nested_class", NestedClass(), proto)
    write("test_empty_class", EmptyClass(), proto)

for proto in range(2, MAX_PROTO + 1):
    write("test_slotted_class", SlottedClass(), proto)

write_single("test_numeric_edges", {
    "float_bigint_equal": (2**53, float(2**53)),
    "float_bigint_off_by_one": (2**53 + 1, float(2**53)),
    "huge_int_vs_float": (2**100, 1.0e30),
    "neg_huge": (-(2**100), -1.0e30),
    "neg_zero": (-0.0, 0.0),
    "int_one_float_one": (1, 1.0),
    "bool_int_float": (True, 1, 1.0),
})

nan = float("nan")
write_single("test_nan_and_zeros", {
    "nan_in_list": [nan, 1, 2],
    "nan_in_set": {nan},
    "neg_zero_in_set": {-0.0, 0.0},
    "int_float_in_set": {1, 1.0},
})

write_single("test_set_dedup", {1, 1.0, True})

d = {}
d[1] = "int"
d[1.0] = "float"
d[True] = "bool"
write_single("test_dict_numeric_keys", d)

print(f"wrote to {os.path.abspath(OUT)}")
