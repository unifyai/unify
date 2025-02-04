import math
import asyncio
import pytest
import threading

import unify
from .helpers import _handle_project


# Functional Compositions #
# ------------------------#


@_handle_project
def test_get_logs_by_value():
    data = {
        "system_prompt": "You are a weather assistant",
        "user_prompt": "hello world",
    }
    assert len(unify.get_logs()) == 0
    log0 = unify.log(**data, skip_duplicates=False)
    log1 = unify.log(**data, skip_duplicates=False)
    retrieved_logs = unify.get_logs_by_value(**data)
    assert len(retrieved_logs) == 2
    for log, retrieved_log in zip((log0, log1), retrieved_logs):
        assert log == retrieved_log
    log0.delete()
    retrieved_logs = unify.get_logs_by_value(**data)
    assert len(retrieved_logs) == 1
    assert log1 == retrieved_logs[0]
    log1.delete()
    assert unify.get_logs_by_value(**data) == []


@_handle_project
def test_get_logs_with_fields():
    assert len(unify.get_logs()) == 0
    unify.log(customer="John Smith")
    assert len(unify.get_logs_with_fields("customer")) == 1
    assert len(unify.get_logs_with_fields("dummy")) == 0
    unify.log(seller="Maggie Jones")
    assert (
        len(
            unify.get_logs_with_fields(
                "customer",
                "seller",
                mode="all",
            ),
        )
        == 0
    )
    assert (
        len(
            unify.get_logs_with_fields(
                "customer",
                "seller",
                mode="any",
            ),
        )
        == 2
    )


@_handle_project
def test_get_logs_without_fields():
    assert len(unify.get_logs()) == 0
    unify.log(customer="John Smith")
    assert len(unify.get_logs_without_fields("customer")) == 0
    assert len(unify.get_logs_without_fields("dummy")) == 1
    unify.log(seller="Maggie Jones")
    assert (
        len(
            unify.get_logs_without_fields(
                "customer",
                "seller",
                mode="all",
            ),
        )
        == 2
    )
    assert (
        len(
            unify.get_logs_without_fields(
                "customer",
                "seller",
                mode="any",
            ),
        )
        == 0
    )


@_handle_project
def test_get_param_by_version():
    unify.log(params={"sys_msg": "you are a helpful assistant"})
    unify.log(params={"sys_msg": "you are a very helpful assistant"})
    unify.log(params={"sys_msg": "you are a genious assistant"})
    assert unify.get_param_by_version("sys_msg", 0) == "you are a helpful assistant"
    assert (
        unify.get_param_by_version("sys_msg", 1) == "you are a very helpful assistant"
    )
    assert unify.get_param_by_version("sys_msg", 2) == "you are a genious assistant"


@_handle_project
def test_get_param_by_value():
    unify.log(params={"sys_msg": "you are a helpful assistant"})
    unify.log(params={"sys_msg": "you are a very helpful assistant"})
    unify.log(params={"sys_msg": "you are a genious assistant"})
    assert unify.get_param_by_value("sys_msg", "you are a helpful assistant") == "0"
    assert (
        unify.get_param_by_value("sys_msg", "you are a very helpful assistant") == "1"
    )
    assert unify.get_param_by_value("sys_msg", "you are a genious assistant") == "2"


@_handle_project
def test_get_experiment_name():
    unify.log(params={"experiment": "first_try"}, x=0)
    unify.log(params={"experiment": "second_try"}, x=1)
    unify.log(params={"experiment": "third_try"}, x=2)
    assert unify.get_experiment_name(-1) == "third_try"
    assert unify.get_experiment_name(-2) == "second_try"
    assert unify.get_experiment_name(-3) == "first_try"
    assert unify.get_experiment_name(0) == "first_try"
    assert unify.get_experiment_name(1) == "second_try"
    assert unify.get_experiment_name(2) == "third_try"


@_handle_project
def test_get_experiment_version():
    unify.log(params={"experiment": "first_try"}, x=0)
    unify.log(params={"experiment": "second_try"}, x=1)
    unify.log(params={"experiment": "third_try"}, x=2)
    assert unify.get_experiment_version("third_try") == "2"
    assert unify.get_experiment_version("second_try") == "1"
    assert unify.get_experiment_version("first_try") == "0"


# Context Handlers #
# -----------------#

# Log


@_handle_project
def test_with_log():

    with unify.Log(a="a"):
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].entries == {"a": "a"}
        unify.add_log_entries(b="b", c="c")
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].entries == {"a": "a", "b": "b", "c": "c"}
        with unify.Log(d="d"):
            logs = unify.get_logs()
            assert len(logs) == 2
            assert logs[0].entries == {"d": "d"}
            unify.add_log_entries(e="e", f="f")
            logs = unify.get_logs()
            assert len(logs) == 2
            assert logs[0].entries == {"d": "d", "e": "e", "f": "f"}
        unify.add_log_entries(g="g")
        logs = unify.get_logs()
        assert len(logs) == 2
        assert logs[1].entries == {"a": "a", "b": "b", "c": "c", "g": "g"}


@_handle_project
def test_global_logging():

    with unify.Log(a="a"):
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].entries == {"a": "a"}
        unify.log(b="b", c="c")
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].entries == {"a": "a", "b": "b", "c": "c"}
        with unify.Log(d="d"):
            logs = unify.get_logs()
            assert len(logs) == 2
            assert logs[0].entries == {"d": "d"}
            unify.log(e="e", f="f")
            logs = unify.get_logs()
            assert len(logs) == 2
            assert logs[0].entries == {"d": "d", "e": "e", "f": "f"}
        unify.log(g="g")
        logs = unify.get_logs()
        assert len(logs) == 2
        assert logs[1].entries == {"a": "a", "b": "b", "c": "c", "g": "g"}


@_handle_project
def test_with_log_threaded():

    def fn(a, b, c, d, e, f, g):
        with unify.Log(a=a):
            unify.add_log_entries(b=b, c=c)
            with unify.Log(d=d):
                unify.add_log_entries(e=e, f=f)
            unify.add_log_entries(g=g)

    threads = [
        threading.Thread(
            target=fn,
            args=[7 * i + j for j in range(7)],
        )
        for i in range(4)
    ]
    [t.start() for t in threads]
    [t.join() for t in threads]

    logs = unify.get_logs()
    entries = [log.entries for log in logs]

    assert sorted([sorted(d.items()) for d in entries]) == [
        [("a", i * 7), ("b", i * 7 + 1), ("c", i * 7 + 2), ("g", i * 7 + 6)]
        for i in range(4)
    ] + [[("d", i * 7 + 3), ("e", i * 7 + 4), ("f", i * 7 + 5)] for i in range(4)]


@_handle_project
@pytest.mark.asyncio
async def test_with_log_async():

    async def fn(a, b, c, d, e, f, g):
        with unify.Log(a=a):
            unify.add_log_entries(b=b, c=c)
            with unify.Log(d=d):
                unify.add_log_entries(e=e, f=f)
            unify.add_log_entries(g=g)

    fns = [fn(*[7 * i + j for j in range(7)]) for i in range(4)]
    await asyncio.gather(*fns)

    logs = unify.get_logs()
    entries = [log.entries for log in logs]

    assert sorted([sorted(d.items()) for d in entries]) == [
        [("a", i * 7), ("b", i * 7 + 1), ("c", i * 7 + 2), ("g", i * 7 + 6)]
        for i in range(4)
    ] + [[("d", i * 7 + 3), ("e", i * 7 + 4), ("f", i * 7 + 5)] for i in range(4)]


# Context


@_handle_project
def test_with_context():

    assert len(unify.get_logs()) == 0
    with unify.Context("Datasets"):
        with unify.Context("TestSet"):
            assert len(unify.get_logs()) == 0
            [unify.log(x=i) for i in range(20)]
            assert len(unify.get_logs()) == 10
        assert len(unify.get_logs()) == 0
        with unify.Context("TestSet[10]"):
            assert len(unify.get_logs()) == 0
            [unify.log(x=i) for i in range(10)]
            assert len(unify.get_logs()) == 10
        assert len(unify.get_logs()) == 0
    assert len(unify.get_logs()) == 0


# ToDo: add threaded test


# ToDo: add asyncio test


# Column Context


@_handle_project
def test_with_column_context():

    unify.log(a="a")
    logs = unify.get_logs()
    assert len(logs) == 1
    assert logs[0].entries == {"a": "a"}
    with unify.ColumnContext("capitalized"):
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].entries == {"a": "a"}
        unify.add_log_entries(logs=logs, b="B")
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].entries == {"a": "a", "capitalized/b": "B"}
        with unify.ColumnContext("vowels"):
            logs = unify.get_logs()
            assert len(logs) == 1
            assert logs[0].entries == {"a": "a", "capitalized/b": "B"}
            unify.add_log_entries(logs=logs, e="E")
            unify.add_log_params(logs=logs, u="U")
            logs = unify.get_logs()
            assert len(logs) == 1
            assert logs[0].entries == {
                "a": "a",
                "capitalized/b": "B",
                "capitalized/vowels/e": "E",
            }
            assert logs[0].params == {
                "capitalized/vowels/u": "U",
            }
            unify.log(a="A")
    logs = unify.get_logs()
    assert len(logs) == 2
    assert logs[0].entries == {
        "a": "a",
        "capitalized/b": "B",
        "capitalized/vowels/e": "E",
    }
    assert logs[1].entries == {
        "capitalized/vowels/a": "A",
    }


@_handle_project
def test_with_column_context_threaded():

    def fn(a, b, e):
        log = unify.log(a=a)
        with unify.ColumnContext("capitalized"):
            log.add_entries(b=b)
            with unify.ColumnContext("vowels"):
                log.add_entries(e=e)
                unify.log(a=a)

    threads = [
        threading.Thread(
            target=fn,
            args=[3 * i + j for j in range(3)],
        )
        for i in range(4)
    ]
    [t.start() for t in threads]
    [t.join() for t in threads]

    logs = unify.get_logs()
    entries = sorted(
        [log.entries for log in logs],
        key=lambda dct: list(dct.values())[0],
    )
    for i, entry in enumerate(entries[0::2]):
        assert entry == {
            "a": i * 3,
            "capitalized/b": i * 3 + 1,
            "capitalized/vowels/e": i * 3 + 2,
        }
    for i, entry in enumerate(entries[1::2]):
        assert entry == {"capitalized/vowels/a": i * 3}


@_handle_project
@pytest.mark.asyncio
async def test_with_column_context_async():

    async def fn(a, b, e):
        log = unify.log(a=a)
        with unify.ColumnContext("capitalized"):
            log.add_entries(b=b)
            with unify.ColumnContext("vowels"):
                log.add_entries(e=e)
                unify.log(a=a)

    fns = [fn(*[3 * i + j for j in range(3)]) for i in range(4)]
    await asyncio.gather(*fns)

    logs = unify.get_logs()
    entries = sorted(
        [log.entries for log in logs],
        key=lambda dct: list(dct.values())[0],
    )
    for i, entry in enumerate(entries[0::2]):
        assert entry == {
            "a": i * 3,
            "capitalized/b": i * 3 + 1,
            "capitalized/vowels/e": i * 3 + 2,
        }
    for i, entry in enumerate(entries[1::2]):
        assert entry == {"capitalized/vowels/a": i * 3}


# Entries


@_handle_project
def test_with_entries():

    with unify.Entries(a="a"):
        logs = unify.get_logs()
        assert len(logs) == 0
        log = unify.log()
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].entries == {"a": "a"}
        unify.add_log_entries(logs=log, b="b", c="c")
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].entries == {"a": "a", "b": "b", "c": "c"}
        with unify.Entries(d="d"):
            unify.add_log_entries(logs=log)
            logs = unify.get_logs()
            assert len(logs) == 1
            assert logs[0].entries == {"a": "a", "b": "b", "c": "c", "d": "d"}
            unify.add_log_entries(logs=log, e="e", f="f")
            logs = unify.get_logs()
            assert len(logs) == 1
            assert logs[0].entries == {
                "a": "a",
                "b": "b",
                "c": "c",
                "d": "d",
                "e": "e",
                "f": "f",
            }
        unify.add_log_entries(logs=log, g="g")
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].entries == {
            "a": "a",
            "b": "b",
            "c": "c",
            "d": "d",
            "e": "e",
            "f": "f",
            "g": "g",
        }


@_handle_project
def test_with_entries_threaded():

    def fn(a, b, c, d, e, f, g):
        with unify.Entries(a=a):
            log = unify.log()
            unify.add_log_entries(logs=log, b=b, c=c)
            with unify.Entries(d=d):
                unify.add_log_entries(logs=log)
                unify.add_log_entries(logs=log, e=e, f=f)
            unify.add_log_entries(logs=log, g=g)

    threads = [
        threading.Thread(
            target=fn,
            args=[7 * i + j for j in range(7)],
        )
        for i in range(4)
    ]
    [t.start() for t in threads]
    [t.join() for t in threads]

    logs = unify.get_logs()
    entries = [log.entries for log in logs]

    assert sorted([sorted(d.items()) for d in entries]) == [
        [
            ("a", i * 7),
            ("b", i * 7 + 1),
            ("c", i * 7 + 2),
            ("d", i * 7 + 3),
            ("e", i * 7 + 4),
            ("f", i * 7 + 5),
            ("g", i * 7 + 6),
        ]
        for i in range(4)
    ]


@_handle_project
@pytest.mark.asyncio
async def test_with_entries_async():

    async def fn(a, b, c, d, e, f, g):
        with unify.Entries(a=a):
            log = unify.log()
            unify.add_log_entries(logs=log, b=b, c=c)
            with unify.Entries(d=d):
                unify.add_log_entries(logs=log)
                unify.add_log_entries(logs=log, e=e, f=f)
            unify.add_log_entries(logs=log, g=g)

    fns = [fn(*[7 * i + j for j in range(7)]) for i in range(4)]
    await asyncio.gather(*fns)

    logs = unify.get_logs()
    entries = [log.entries for log in logs]

    assert sorted([sorted(d.items()) for d in entries]) == [
        [
            ("a", i * 7),
            ("b", i * 7 + 1),
            ("c", i * 7 + 2),
            ("d", i * 7 + 3),
            ("e", i * 7 + 4),
            ("f", i * 7 + 5),
            ("g", i * 7 + 6),
        ]
        for i in range(4)
    ]


# Params


@_handle_project
def test_with_params():

    with unify.Params(a="a"):
        logs = unify.get_logs()
        assert len(logs) == 0
        log = unify.log()
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].params == {"a": "a"}
        unify.add_log_params(logs=log, b="b", c="c")
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].params == {"a": "a", "b": "b", "c": "c"}
        with unify.Params(d="d"):
            unify.add_log_params(logs=log)
            logs = unify.get_logs()
            assert len(logs) == 1
            assert logs[0].params == {"a": "a", "b": "b", "c": "c", "d": "d"}
            unify.add_log_params(logs=log, e="e", f="f")
            logs = unify.get_logs()
            assert len(logs) == 1
            assert logs[0].params == {
                "a": "a",
                "b": "b",
                "c": "c",
                "d": "d",
                "e": "e",
                "f": "f",
            }
        unify.add_log_params(logs=log, g="g")
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].params == {
            "a": "a",
            "b": "b",
            "c": "c",
            "d": "d",
            "e": "e",
            "f": "f",
            "g": "g",
        }


@_handle_project
def test_with_params_threaded():

    def fn(a, b, c, d, e, f, g):
        with unify.Params(a=a):
            log = unify.log()
            unify.add_log_params(logs=log, b=b, c=c)
            with unify.Params(d=d):
                unify.add_log_params(logs=log)
                unify.add_log_params(logs=log, e=e, f=f)
            unify.add_log_params(logs=log, g=g)

    threads = [
        threading.Thread(
            target=fn,
            args=[7 * i + j for j in range(7)],
        )
        for i in range(4)
    ]
    [t.start() for t in threads]
    [t.join() for t in threads]

    logs = unify.get_logs()
    params = [log.params for log in logs]

    assert sorted([sorted(d.items()) for d in params]) == [
        [
            ("a", i * 7),
            ("b", i * 7 + 1),
            ("c", i * 7 + 2),
            ("d", i * 7 + 3),
            ("e", i * 7 + 4),
            ("f", i * 7 + 5),
            ("g", i * 7 + 6),
        ]
        for i in range(4)
    ]


@_handle_project
@pytest.mark.asyncio
async def test_with_params_async():

    async def fn(a, b, c, d, e, f, g):
        with unify.Params(a=a):
            log = unify.log()
            unify.add_log_params(logs=log, b=b, c=c)
            with unify.Params(d=d):
                unify.add_log_params(logs=log)
                unify.add_log_params(logs=log, e=e, f=f)
            unify.add_log_params(logs=log, g=g)

    fns = [fn(*[7 * i + j for j in range(7)]) for i in range(4)]
    await asyncio.gather(*fns)

    logs = unify.get_logs()
    params = [log.params for log in logs]

    assert sorted([sorted(d.items()) for d in params]) == [
        [
            ("a", i * 7),
            ("b", i * 7 + 1),
            ("c", i * 7 + 2),
            ("d", i * 7 + 3),
            ("e", i * 7 + 4),
            ("f", i * 7 + 5),
            ("g", i * 7 + 6),
        ]
        for i in range(4)
    ]


# Experiment


@_handle_project
def test_with_experiment():

    with unify.Experiment(), unify.Params(sys_msg="you are a helpful assistant"):
        unify.log(x=0)
        unify.log(x=1)
    assert len(unify.get_logs_with_fields("experiment")) == 2
    assert unify.get_experiment_name(0) == "0"
    # ToDo work out why this is returning as an int and not a str,
    # the column type is a string, so not sure why it's being cast to an int
    logs = unify.get_logs()[0:2]
    assert [lg.entries for lg in logs] == [{"x": 1}, {"x": 0}]

    with unify.Experiment(), unify.Params(
        sys_msg="you are a very helpful assistant",
    ):
        unify.log(x=1)
        unify.log(x=2)
    assert len(unify.get_logs_with_fields("experiment")) == 4
    assert unify.get_experiment_name(0) == "0"
    assert unify.get_experiment_name(1) == "new_idea"
    logs = unify.get_logs()[0:2]
    assert [lg.entries for lg in logs] == [{"x": 2}, {"x": 1}]

    with unify.Experiment("new_idea"), unify.Params(
        sys_msg="you are a genious assistant",
    ):
        unify.log(x=2)
        unify.log(x=3)
    assert len(unify.get_logs_with_fields("experiment")) == 6
    assert unify.get_experiment_name(0) == "0"
    assert unify.get_experiment_name(1) == "new_idea"
    logs = unify.get_logs()[0:2]
    assert [lg.entries for lg in logs] == [{"x": 3}, {"x": 2}]

    with unify.Experiment(-1, overwrite=True), unify.Params(
        sys_msg="you are a very helpful assistant",
    ):
        unify.log(x=3)
        unify.log(x=4)
    assert len(unify.get_logs_with_fields("experiment")) == 6
    assert unify.get_experiment_name(0) == "0"
    assert unify.get_experiment_name(1) == "new_idea"
    logs = unify.get_logs()[0:2]
    assert [lg.entries for lg in logs] == [{"x": 4}, {"x": 3}]


# Combos


@_handle_project
def test_with_all():

    with unify.Params(a="a"):
        logs = unify.get_logs()
        assert len(logs) == 0
        log = unify.log()
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].params == {"a": "a"}
        unify.add_log_params(logs=log, b="b", c="c")
        logs = unify.get_logs()
        assert len(logs) == 1
        assert logs[0].params == {"a": "a", "b": "b", "c": "c"}
        with unify.Entries(d="d"):
            unify.add_log_entries(logs=log)
            logs = unify.get_logs()
            assert len(logs) == 1
            assert logs[0].entries == {"d": "d"}
            assert logs[0].params == {"a": "a", "b": "b", "c": "c"}
            unify.add_log_entries(logs=log, e="e")
            unify.add_log_params(logs=log, f="f")
            logs = unify.get_logs()
            assert len(logs) == 1
            assert logs[0].entries == {"d": "d", "e": "e"}
            assert logs[0].params == {
                "a": "a",
                "b": "b",
                "c": "c",
                "f": "f",
            }
            with unify.Log():
                unify.add_log_params(g="g")
                unify.add_log_entries(h="h")
                logs = unify.get_logs()
                assert len(logs) == 2
                assert logs[1].params == {"a": "a", "g": "g"}
                assert logs[1].entries == {"d": "d", "h": "h"}
            unify.add_log_entries(logs=log, i="i")
            logs = unify.get_logs()
            assert len(logs) == 2
            assert logs[0].entries == {"d": "d", "e": "e", "i": "i"}
            assert logs[0].params == {
                "a": "a",
                "b": "b",
                "c": "c",
                "f": "f",
            }


@_handle_project
def test_with_all_threaded():
    def fn(a, b, c, d, e, f, g, h, i):
        with unify.Params(a=a):
            log = unify.log()
            unify.add_log_params(logs=log, b=b, c=c)
            with unify.Entries(d=d):
                unify.add_log_entries(logs=log)
                unify.add_log_entries(logs=log, e=e)
                unify.add_log_params(logs=log, f=f)
                with unify.Log():
                    unify.add_log_params(g=g)
                    unify.add_log_entries(h=h)
                unify.add_log_entries(logs=log, i=i)

    threads = [
        threading.Thread(
            target=fn,
            args=[9 * i + j for j in range(9)],
        )
        for i in range(4)
    ]
    [t.start() for t in threads]
    [t.join() for t in threads]

    logs = unify.get_logs()

    params = [log.params for log in logs]
    observed = [sorted(d.items()) for d in sorted(params, key=lambda x: x["a"])]
    for i, obs in enumerate(observed):
        if i % 2 == 0:
            assert obs == [
                ("a", (i / 2) * 9),
                ("b", (i / 2) * 9 + 1),
                ("c", (i / 2) * 9 + 2),
                ("f", (i / 2) * 9 + 5),
            ]
        else:
            assert obs == [
                ("a", math.floor(i / 2) * 9),
                ("g", math.floor(i / 2) * 9 + 6),
            ]
    entries = [log.entries for log in logs]
    observed = [sorted(d.items()) for d in sorted(entries, key=lambda x: x["d"])]
    for i, obs in enumerate(observed):
        if i % 2 == 0:
            assert obs == [
                ("d", (i / 2) * 9 + 3),
                ("e", (i / 2) * 9 + 4),
                ("i", (i / 2) * 9 + 8),
            ]
        else:
            assert obs == [
                ("d", math.floor(i / 2) * 9 + 3),
                ("h", math.floor(i / 2) * 9 + 7),
            ]


@_handle_project
@pytest.mark.asyncio
async def test_with_all_async():

    async def fn(a, b, c, d, e, f, g, h, i):
        with unify.Params(a=a):
            log = unify.log()
            unify.add_log_params(logs=log, b=b, c=c)
            with unify.Entries(d=d):
                unify.add_log_entries(logs=log)
                unify.add_log_entries(logs=log, e=e)
                unify.add_log_params(logs=log, f=f)
                with unify.Log():
                    unify.add_log_params(g=g)
                    unify.add_log_entries(h=h)
                unify.add_log_entries(logs=log, i=i)

    fns = [fn(*[9 * i + j for j in range(9)]) for i in range(4)]
    await asyncio.gather(*fns)

    logs = unify.get_logs()

    params = [log.params for log in logs]
    observed = [sorted(d.items()) for d in sorted(params, key=lambda x: x["a"])]
    for i, obs in enumerate(observed):
        if i % 2 == 0:
            assert obs == [
                ("a", (i / 2) * 9),
                ("b", (i / 2) * 9 + 1),
                ("c", (i / 2) * 9 + 2),
                ("f", (i / 2) * 9 + 5),
            ]
        else:
            assert obs == [
                ("a", math.floor(i / 2) * 9),
                ("g", math.floor(i / 2) * 9 + 6),
            ]
    entries = [log.entries for log in logs]
    observed = [sorted(d.items()) for d in sorted(entries, key=lambda x: x["d"])]
    for i, obs in enumerate(observed):
        if i % 2 == 0:
            assert obs == [
                ("d", (i / 2) * 9 + 3),
                ("e", (i / 2) * 9 + 4),
                ("i", (i / 2) * 9 + 8),
            ]
        else:
            assert obs == [
                ("d", math.floor(i / 2) * 9 + 3),
                ("h", math.floor(i / 2) * 9 + 7),
            ]


if __name__ == "__main__":
    pass
