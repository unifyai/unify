import os
import pytest
from unify.utils._caching import _get_cache
from requests import HTTPError

import unify


def test_log_param():
    project = "test_log_param"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    data = {
        "system_prompt": "You are a mathematician.",
        "dataset": "maths questions",
    }
    assert len(unify.get_logs(project=project)) == 0
    log_id = unify.log(project=project, params=data).id
    project_logs = unify.get_logs(project=project)
    assert len(project_logs) and project_logs[0].id == log_id
    id_log = unify.get_log_by_id(log_id)
    assert len(id_log) and "system_prompt" in id_log.params
    unify.delete_log_fields(field="system_prompt", logs=log_id)
    id_log = unify.get_log_by_id(log_id)
    assert len(id_log) and "system_prompt" not in id_log.params
    unify.add_log_params(
        logs=log_id,
        system_prompt=data["system_prompt"],
    )
    id_log = unify.get_log_by_id(log_id)
    assert len(id_log) and "system_prompt" in id_log.params
    unify.delete_logs(logs=log_id)
    assert len(unify.get_logs(project=project)) == 0
    try:
        unify.get_log_by_id(log_id)
        assert False
    except HTTPError as e:
        assert e.response.status_code == 404
    unify.deactivate()
    unify.delete_project(project)


def test_add_param():
    project = "test_add_param"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    unify.activate(project)
    unify.log(a="a")
    unify.log(b="b")
    unify.log(c="c")
    logs = unify.get_logs()
    assert len(logs) == 3
    entries = logs[0].entries
    entries.pop("timestamp", None)
    assert entries == {"a": "a"}
    assert logs[0].params == {}
    entries = logs[1].entries
    entries.pop("timestamp", None)
    assert entries == {"b": "b"}
    assert logs[1].params == {}
    entries = logs[2].entries
    entries.pop("timestamp", None)
    assert entries == {"c": "c"}
    assert logs[2].params == {}
    unify.add_params(system_prompt="You know the alphabet.")
    logs = unify.get_logs()
    assert len(logs) == 3
    entries = logs[0].entries
    entries.pop("timestamp", None)
    assert entries == {"a": "a"}
    assert logs[0].params == {"system_prompt": "You know the alphabet."}
    entries = logs[1].entries
    entries.pop("timestamp", None)
    assert entries == {"b": "b"}
    assert logs[1].params == {"system_prompt": "You know the alphabet."}
    entries = logs[2].entries
    entries.pop("timestamp", None)
    assert entries == {"c": "c"}
    assert logs[2].params == {"system_prompt": "You know the alphabet."}
    unify.deactivate()
    unify.delete_project(project)


def test_get_params():
    project = "test_get_params"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    unify.activate(project)
    with unify.Params(system_prompt="You know the alphabet"):
        unify.log(a="a")
        with unify.Params(tools="internet"):
            unify.log(b="b")
            unify.log(c="c")
    assert unify.get_params() == ["system_prompt", "tools"]
    unify.deactivate()
    unify.delete_project(project)


def test_log_entry():
    project = "test_log_entry"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    data = {
        "question": "What is 1 + 1?",
        "answer": "It's 2",
    }
    assert len(unify.get_logs(project=project)) == 0
    log_id = unify.log(project=project, **data).id
    project_logs = unify.get_logs(project=project)
    assert len(project_logs) and project_logs[0].id == log_id
    id_log = unify.get_log_by_id(log_id)
    assert len(id_log) and "question" in id_log.entries
    unify.delete_log_fields(field="question", logs=log_id)
    id_log = unify.get_log_by_id(log_id)
    assert len(id_log) and "question" not in id_log.entries
    unify.add_log_entries(logs=log_id, question=data["question"])
    id_log = unify.get_log_by_id(log_id)
    assert len(id_log) and "question" in id_log.entries
    unify.delete_logs(logs=log_id)
    assert len(unify.get_logs(project=project)) == 0
    try:
        unify.get_log_by_id(log_id)
        assert False
    except HTTPError as e:
        assert e.response.status_code == 404
    unify.deactivate()
    unify.delete_project(project)


def test_log_dataset():
    project = "test_log_dataset"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    unify.log(
        project=project,
        dataset=unify.Dataset(["a", "b", "c"], name="letters"),
    )
    logs = unify.get_logs(project=project)
    assert len(logs) == 1
    entries = logs[0].entries
    entries.pop("timestamp", None)
    assert entries == {"dataset": "letters"}
    downloaded = unify.download_dataset("letters")
    assert len(downloaded) == 3
    logs[0].delete()
    unify.delete_dataset("letters")
    unify.deactivate()
    unify.delete_project(project)


def test_duplicate_log_field():
    project = "test_duplicate_log_field"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    data = {
        "system_prompt": "You are a weather assistant",
        "user_prompt": "hello world",
    }
    assert len(unify.get_logs(project=project)) == 0
    log = unify.log(project=project, **data)
    assert len(unify.get_logs(project=project)) == 1
    new_data = {
        "system_prompt": "You are a maths assistant",
        "user_prompt": "hi earth",
    }
    with pytest.raises(Exception):
        log.add_entries(**new_data)
    unify.deactivate()
    unify.delete_project(project)


def test_log_function_logs_code():
    def my_func(a):
        return a + 1

    project = "test_log_function_logs_code"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    unify.log(project=project, my_func=my_func)
    logs = unify.get_logs(project=project)
    assert len(logs) == 1
    entries = logs[0].entries
    entries.pop("timestamp", None)
    assert entries["my_func"] == "    def my_func(a):\n        return a + 1\n"
    unify.deactivate()
    unify.delete_project(project)


def test_atomic_functions():
    project = "test_atomic_functions"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    log1 = {
        "system_prompt": "You are a weather assistant",
        "user_prompt": "hello world",
        "score": 0.2,
    }
    log2 = {
        "system_prompt": "You are a new weather assistant",
        "user_prompt": "hello world",
        "score": 0.3,
    }
    log3 = {
        "system_prompt": "You are a new weather assistant",
        "user_prompt": "nothing",
        "score": 0.8,
    }
    unify.log(project=project, **log1)
    unify.log(project=project, **log2)
    unify.log(project=project, **log3)
    grouped_logs = unify.group_logs(key="system_prompt", project=project)
    assert len(grouped_logs) == 2
    assert sorted([version for version in grouped_logs]) == ["0", "1"]

    logs_metric = unify.get_logs_metric(
        metric="mean",
        key="score",
        filter="'hello' in user_prompt",
        project=project,
    )
    assert logs_metric == 0.25
    unify.deactivate()
    unify.delete_project(project)


def test_log_ordering():
    project = "test_log_ordering"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    for i in range(25):
        unify.log(
            project=project,
            a=i,
            b=i + 1,
            c=i + 2,
        )
    logs = unify.get_logs(project=project)
    for lg in logs:
        entries = lg.entries
        entries.pop("timestamp", None)
        assert list(entries.keys()) == ["a", "b", "c"]
    unify.deactivate()
    unify.delete_project(project)


def test_get_logs():
    project = "test_get_logs"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    logs = unify.get_logs(project=project)
    assert len(logs) == 0, "There should be no logs initially."
    log_data1 = {
        "system_prompt": "You are a weather assistant",
        "user_prompt": "What is the weather today?",
        "score": 0.9,
    }
    unify.log(project=project, **log_data1)
    log_data2 = {
        "system_prompt": "You are a travel assistant",
        "user_prompt": "What is the best route to the airport?",
        "score": 0.7,
    }
    unify.log(project=project, **log_data2)
    log_data3 = {
        "system_prompt": "You are a travel assistant",
        "user_prompt": "What is the best route to the airport?",
        "score": 0.2,
    }
    unify.log(project=project, **log_data3)

    logs = unify.get_logs(project=project)
    assert len(logs) == 3, "There should be 3 logs in the project."
    filtered_logs = unify.get_logs(
        project=project,
        filter="'weather' in user_prompt",
    )
    assert (
        len(filtered_logs) == 1
    ), "There should be 1 log with 'weather' in the user prompt."
    assert (
        filtered_logs[0].entries.get("user_prompt") == log_data1["user_prompt"]
    ), "The filtered log should be the one that asks about the weather."
    nonexistent_logs = unify.get_logs(
        project=project,
        filter="'nonexistent' in user_prompt",
    )
    assert (
        len(nonexistent_logs) == 0
    ), "There should be no logs matching the nonexistent filter."
    multiple_filtered_logs = unify.get_logs(
        project=project,
        filter="'travel' in system_prompt and score < 0.5",
    )
    assert (
        len(multiple_filtered_logs) == 1
    ), "There should be 1 log with 'travel' in the user prompt and score > 0.5."
    bracket_logs = unify.get_logs(
        project=project,
        filter="('weather' in user_prompt) and ('weather' in system_prompt)",
    )
    assert (
        len(bracket_logs) == 1
    ), "There should be 1 log with 'weather' in the user prompt and system prompt."
    assert (
        bracket_logs[0].entries.get("user_prompt") == log_data1["user_prompt"]
    ), "The filtered log should be the one that asks about the weather."
    comparison_logs = unify.get_logs(project=project, filter="score > 0.5")
    assert len(comparison_logs) == 2, "There should be 2 logs with score > 0.5."
    comparison_logs = unify.get_logs(project=project, filter="score == 0.9")
    assert len(comparison_logs) == 1, "There should be 1 log with score == 0.9."
    logical_logs = unify.get_logs(
        project=project,
        filter="score > 0.5 and score < 0.8",
    )
    assert (
        len(logical_logs) == 1
    ), "There should be 1 log with score > 0.5 and score < 0.8."
    logical_logs = unify.get_logs(
        project=project,
        filter="score < 0.5 or score > 0.8",
    )
    assert (
        len(logical_logs) == 2
    ), "There should be 2 logs with score < 0.5 or score > 0.8."
    string_comparison_logs = unify.get_logs(
        project=project,
        filter="user_prompt == 'What is the weather today?'",
    )
    assert (
        len(string_comparison_logs) == 1
    ), "There should be 1 log with user_prompt == 'What is the weather today?'."
    unify.deactivate()
    unify.delete_project(project)


def test_delete_logs():
    project = "test_delete_logs"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    assert len(unify.get_logs(project=project)) == 0
    unify.log(project=project, customer="John Smith")
    unify.log(project=project, customer="Maggie Smith")
    unify.log(project=project, customer="John Terry")
    assert len(unify.get_logs(project=project)) == 3
    deleted_logs = unify.delete_logs_by_value(
        project=project,
        filter="'Smith' in customer",
    )
    assert len(deleted_logs) == 2
    assert set([dl.entries["customer"] for dl in deleted_logs]) == {
        "John Smith",
        "Maggie Smith",
    }
    assert len(unify.get_logs(project=project)) == 1
    deleted_logs = unify.delete_logs_by_value(project=project)
    assert len(deleted_logs) == 1
    assert deleted_logs[0].entries["customer"] == "John Terry"
    assert len(unify.get_logs(project=project)) == 0
    unify.deactivate()
    unify.delete_project(project)


def test_get_source():
    project = "test_get_source"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    source = unify.get_source()
    assert "source = unify.get_source()" in source
    unify.deactivate()
    unify.delete_project(project)


def test_log_caching():
    project = "test_log_caching"
    if project in unify.list_projects():
        unify.delete_project(project)
    unify.create_project(project)
    cache_fname = ".test_log_caching.cache.json"
    if os.path.exists(cache_fname):
        os.remove(cache_fname)
    unify.set_caching(True)
    unify.set_caching_fname(cache_fname)

    # log
    unify.log(project=project, a=0, b=1)
    assert (
        _get_cache(
            fn_name="log",
            kw={"project": project, "a": 0, "b": 1},
        )
        is not None
    )
    log = unify.log(project=project, a=0, b=1)
    assert isinstance(log, unify.Log)

    # add_log_params
    unify.add_log_params(logs=log, p="p")
    assert (
        _get_cache(
            fn_name="add_log_params",
            kw={"logs": log, "p": "p"},
        )
        is not None
    )
    msg = unify.add_log_params(logs=log, p="p")
    assert msg == {"info": "Logs updated successfully!"}

    # add_log_params
    unify.add_log_params(logs=log, p="p")
    assert (
        _get_cache(
            fn_name="add_log_params",
            kw={"logs": log, "p": "p"},
        )
        is not None
    )
    msg = unify.add_log_params(logs=log, p="p")
    assert msg == {"info": "Logs updated successfully!"}

    # add_log_entries
    unify.add_log_entries(logs=log, e="e")
    assert (
        _get_cache(
            fn_name="add_log_entries",
            kw={"logs": log, "e": "e"},
        )
        is not None
    )
    msg = unify.add_log_entries(logs=log, e="e")
    assert msg == {"info": "Logs updated successfully!"}

    # delete_logs
    unify.delete_logs(logs=log)
    assert (
        _get_cache(
            fn_name="delete_logs",
            kw={"logs": log},
        )
        is not None
    )
    msg = unify.delete_logs(logs=log)
    assert msg == {"info": "Logs deleted successfully!"}

    # delete_log_fields
    log = unify.log(project=project, a=1, b=2)
    unify.delete_log_fields(field="a", logs=log)
    assert (
        _get_cache(
            fn_name="delete_log_fields",
            kw={"field": "a", "logs": log},
        )
        is not None
    )
    msg = unify.delete_log_fields(field="a", logs=log)
    assert msg == {"info": "Log field deleted successfully from all logs!"}

    # cleanup
    os.remove(cache_fname)
    unify.delete_project(project)


if __name__ == "__main__":
    pass
