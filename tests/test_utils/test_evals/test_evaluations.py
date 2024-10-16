import unittest
import time
import asyncio

import unify
from requests import HTTPError
import threading


class TestEvaluations(unittest.TestCase):

    def test_project(self):
        name = "my_project"
        if name in unify.list_projects():
            unify.delete_project(name)
        assert unify.list_projects() == []
        unify.create_project(name)
        assert name in unify.list_projects()
        new_name = "my_project1"
        unify.rename_project(name, new_name)
        assert new_name in unify.list_projects()
        unify.delete_project(new_name)
        assert unify.list_projects() == []

    def test_artifacts(self):
        project = "my_project"
        if project in unify.list_projects():
            unify.delete_project(project)
        unify.create_project(project)
        artifacts = {"dataset": "my_dataset", "description": "this is my dataset"}
        assert len(unify.get_artifacts(project)) == 0
        unify.add_artifacts(project=project, **artifacts)
        assert "dataset" in unify.get_artifacts(project)
        unify.delete_artifact(
            "dataset",
            project,
        )
        assert "dataset" not in unify.get_artifacts(project)
        assert "description" in unify.get_artifacts(project)

    def test_log(self):
        project = "my_project"
        if project in unify.list_projects():
            unify.delete_project(project)
        unify.create_project(project)
        version = {
            "system_prompt": "v1",
        }
        log = {
            "system_prompt": "You are a weather assistant",
            "user_prompt": "hello world",
        }
        assert len(unify.get_logs(project)) == 0
        log_id = unify.log(project, version, **log).id
        project_logs = unify.get_logs(project)
        assert len(project_logs) and project_logs[0].id == log_id
        id_log = unify.get_log(log_id)
        assert len(id_log) and "user_prompt" in id_log.entries
        unify.delete_log_entry("user_prompt", log_id)
        id_log = unify.get_log(log_id)
        assert len(id_log) and "user_prompt" not in id_log.entries
        unify.add_log_entries(log_id, user_prompt=log["user_prompt"])
        id_log = unify.get_log(log_id)
        assert len(id_log) and "user_prompt" in id_log.entries
        unify.delete_log(log_id)
        assert len(unify.get_logs(project)) == 0
        try:
            unify.get_log(log_id)
            assert False
        except HTTPError as e:
            assert e.response.status_code == 404

    def test_atomic_functions(self):
        project = "my_project"
        if project in unify.list_projects():
            unify.delete_project(project)
        unify.create_project(project)
        version1 = {
            "system_prompt": "v1",
        }
        log1 = {
            "system_prompt": "You are a weather assistant",
            "user_prompt": "hello world",
            "score": 0.2,
        }
        version2 = {
            "system_prompt": "v2",
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
        unify.log(project, version1, **log1)
        unify.log(project, version2, **log2)
        unify.log(project, version2, **log3)
        grouped_logs = unify.group_logs("system_prompt", project)
        assert len(grouped_logs) == 2
        assert sorted([version for version in grouped_logs]) == ["v1", "v2"]

        logs_metric = unify.get_logs_metric(
            "mean",
            "score",
            filter="'hello' in user_prompt",
            project="my_project",
        )
        assert logs_metric == 0.25

    def test_get_logs(self):
        project = "test_project_get_logs"
        if project in unify.list_projects():
            unify.delete_project(project)
        unify.create_project(project)
        logs = unify.get_logs(project)
        assert len(logs) == 0, "There should be no logs initially."
        log_data1 = {
            "system_prompt": "You are a weather assistant",
            "user_prompt": "What is the weather today?",
            "score": 0.9,
        }
        log1 = unify.log(project=project, **log_data1)
        log_data2 = {
            "system_prompt": "You are a travel assistant",
            "user_prompt": "What is the best route to the airport?",
            "score": 0.7,
        }
        log2 = unify.log(project=project, **log_data2)
        log_data3 = {
            "system_prompt": "You are a travel assistant",
            "user_prompt": "What is the best route to the airport?",
            "score": 0.2,
        }
        log3 = unify.log(project=project, **log_data3)

        logs = unify.get_logs(project)
        assert len(logs) == 3, "There should be 3 logs in the project."
        filtered_logs = unify.get_logs(project, filter="'weather' in user_prompt")
        assert (
            len(filtered_logs) == 1
        ), "There should be 1 log with 'weather' in the user prompt."
        assert (
            filtered_logs[0].entries.get("user_prompt") == log_data1["user_prompt"]
        ), "The filtered log should be the one that asks about the weather."
        nonexistent_logs = unify.get_logs(
            project, filter="'nonexistent' in user_prompt"
        )
        assert (
            len(nonexistent_logs) == 0
        ), "There should be no logs matching the nonexistent filter."
        multiple_filtered_logs = unify.get_logs(
            project, filter="'travel' in system_prompt and score < 0.5"
        )
        assert (
            len(multiple_filtered_logs) == 1
        ), "There should be 1 log with 'travel' in the user prompt and score > 0.5."
        bracket_logs = unify.get_logs(
            project,
            filter="('weather' in user_prompt) and ('weather' in system_prompt)",
        )
        assert (
            len(bracket_logs) == 1
        ), "There should be 1 log with 'weather' in the user prompt and system prompt."
        assert (
            bracket_logs[0].entries.get("user_prompt") == log_data1["user_prompt"]
        ), "The filtered log should be the one that asks about the weather."
        comparison_logs = unify.get_logs(project, filter="score > 0.5")
        assert len(comparison_logs) == 2, "There should be 2 logs with score > 0.5."
        comparison_logs = unify.get_logs(project, filter="score == 0.9")
        assert len(comparison_logs) == 1, "There should be 1 log with score == 0.9."
        logical_logs = unify.get_logs(project, filter="score > 0.5 and score < 0.8")
        assert (
            len(logical_logs) == 1
        ), "There should be 1 log with score > 0.5 and score < 0.8."
        logical_logs = unify.get_logs(project, filter="score < 0.5 or score > 0.8")
        assert (
            len(logical_logs) == 2
        ), "There should be 2 logs with score < 0.5 or score > 0.8."
        string_comparison_logs = unify.get_logs(
            project, filter="user_prompt == 'What is the weather today?'"
        )
        assert (
            len(string_comparison_logs) == 1
        ), "There should be 1 log with user_prompt == 'What is the weather today?'."
        unify.delete_project(project)

    def test_contextual_logging_threaded(self):
        project = "my_project"
        if project in unify.list_projects():
            unify.delete_project(project)
        unify.create_project(project)
        unify.activate(project)

        def inner_fn(data, st):
            time.sleep(st)
            unify.add_log_entries(d2=data)

        @unify.trace()
        def fn1(data1, data2, st):
            unify.add_log_entries(d1=data1)
            inner_fn(data2, st)

        thread1 = threading.Thread(target=fn1, args=("Thread-1", "data1", 1))
        thread2 = threading.Thread(target=fn1, args=("Thread-2", "data2", 2))

        # Start the threads
        thread1.start()
        thread2.start()

        # Wait for both threads to complete
        thread1.join()
        thread2.join()

        logs = unify.get_logs("my_project")
        list1 = [log.entries for log in logs]
        list2 = [{"d1": "Thread-1", "d2": "data1"}, {"d1": "Thread-2", "d2": "data2"}]

        # Sort each dictionary by keys and then sort the list
        assert sorted([sorted(d.items()) for d in list1]) == sorted(
            [sorted(d.items()) for d in list2],
        )


class TestAsyncEvaluations(unittest.IsolatedAsyncioTestCase):
    async def test_contextual_logging_async(self):
        project = "my_project"
        if project in unify.list_projects():
            unify.delete_project(project)
        unify.create_project(project)
        unify.activate(project)

        async def inner_fn(data, st):
            await asyncio.sleep(st)
            unify.add_log_entries(d2=data)

        @unify.trace()
        async def fn1(data1, data2, st):
            unify.add_log_entries(d1=data1)
            await inner_fn(data2, st)

        await asyncio.gather(fn1("Task-1", "data1", 1), fn1("Task-2", "data2", 2))

        logs = unify.get_logs("my_project")
        list1 = [log.entries for log in logs]
        list2 = [{"d1": "Task-1", "d2": "data1"}, {"d1": "Task-2", "d2": "data2"}]

        # Sort each dictionary by keys and then sort the list
        assert sorted([sorted(d.items()) for d in list1]) == sorted(
            [sorted(d.items()) for d in list2],
        )
