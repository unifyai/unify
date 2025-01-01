import os
import random
import string
import pytest

import unify
from unify.universal_api.types import Prompt

dir_path = os.path.dirname(os.path.realpath(__file__))


class TestDatasetConstruction:
    def test_create_dataset_from_str(self) -> None:
        dataset = unify.Dataset("a")
        assert isinstance(dataset[0], str)

    def test_create_dataset_from_list_of_str(self) -> None:
        dataset = unify.Dataset(["a", "b", "c"])
        assert isinstance(dataset[0], str)

    def test_create_dataset_from_prompt(self) -> None:
        dataset = unify.Dataset(Prompt(messages=[{"role": "user", "content": "a"}]))
        assert isinstance(dataset[0], Prompt)

    def test_create_dataset_from_list_of_prompts(self) -> None:
        dataset = unify.Dataset(
            [
                Prompt(messages=[{"role": "user", "content": usr_msg}])
                for usr_msg in ["a", "b", "c"]
            ],
        )
        assert isinstance(dataset[0], Prompt)

    def test_create_dataset_from_dict(self) -> None:
        dataset = unify.Dataset(dict(messages=[{"role": "user", "content": "a"}]))
        assert isinstance(dataset[0], dict)

    def test_create_dataset_from_list_of_dicts(self) -> None:
        dataset = unify.Dataset(
            [
                dict(messages=[{"role": "user", "content": usr_msg}])
                for usr_msg in ["a", "b", "c"]
            ],
        )
        assert isinstance(dataset[0], dict)

    def test_create_dataset_from_dict_w_prompt(self) -> None:
        dataset = unify.Dataset(
            dict(prompt=Prompt(messages=[{"role": "user", "content": "a"}])),
        )
        assert isinstance(dataset[0], dict)

    def test_create_dataset_from_list_of_prompt_dicts(self) -> None:
        dataset = unify.Dataset(
            [
                dict(prompt=Prompt(messages=[{"role": "user", "content": usr_msg}]))
                for usr_msg in ["a", "b", "c"]
            ],
        )
        assert isinstance(dataset[0], dict)

    def test_create_dataset_from_upstream(self) -> None:
        if "TestCreateDatasetFromStr" in unify.list_datasets():
            unify.delete_dataset("TestCreateDatasetFromStr")

        dataset = unify.Dataset(["a", "b", "c"], name="TestCreateDatasetFromStr")
        assert dataset.name not in unify.list_datasets()
        dataset.upload()
        assert "TestCreateDatasetFromStr" in unify.list_datasets()
        dataset = unify.Dataset.from_upstream("TestCreateDatasetFromStr")
        assert isinstance(dataset._raw_data[0], dict)
        unify.delete_dataset("TestCreateDatasetFromStr")
        assert "TestCreateDatasetFromStr" not in unify.list_datasets()


# noinspection PyStatementEffect
class TestDatasetManipulation:
    def test_iterate_over_dataset(self) -> None:
        msgs = ["a", "b", "c"]
        dataset = unify.Dataset(msgs)
        assert len(dataset) == len(msgs)
        for item, msg in zip(dataset, msgs):
            assert isinstance(item, str)
            assert item == msg

    def test_index_dataset(self) -> None:
        dataset = unify.Dataset(["a", "b", "c"])
        assert isinstance(dataset[0], str)
        assert dataset[0] == "a"
        assert isinstance(dataset[1], str)
        assert dataset[1] == "b"
        assert isinstance(dataset[2], str)
        assert dataset[2] == "c"
        assert isinstance(dataset[-1], str)
        assert dataset[-1] == "c"
        with pytest.raises(IndexError):
            dataset[3]

    def test_slice_dataset(self) -> None:
        msgs = ["a", "b", "c", "d"]
        dataset = unify.Dataset(["a", "b", "c", "d"])
        msgs = msgs[1:-1]
        dataset = dataset[1:-1]
        for item, msg in zip(dataset, msgs):
            assert isinstance(item, str)
            assert item == msg

    def test_dataset_contains(self) -> None:
        dataset1 = unify.Dataset(["a", "b", "c"])
        dataset2 = unify.Dataset(["a", "b"])
        assert dataset2 in dataset1
        assert "a" in dataset1
        assert "b" in dataset1
        assert ["b", "c"] in dataset1
        assert "d" not in dataset1
        dataset3 = unify.Dataset(["c", "d"])
        assert dataset3 not in dataset1

    def test_dataset_one_liners(self) -> None:
        dataset = ("a" + unify.Prompt("b")).add("c").set_name("my_dataset")
        assert dataset.name == "my_dataset"
        assert "a" in dataset
        assert unify.Prompt("b") in dataset
        assert "c" in dataset


# noinspection PyTypeChecker
class TestDatasetCombining:
    def test_add_datasets(self) -> None:
        msgs = ["a", "b", "c", "d"]
        dataset1 = unify.Dataset(msgs[0:2])
        dataset2 = unify.Dataset(msgs[2:])
        dataset = dataset1 + dataset2
        assert len(dataset) == len(msgs)
        for item, msg in zip(dataset, msgs):
            assert item == msg

    def test_sum_datasets(self) -> None:
        msgs = ["a", "b", "c", "d"]
        dataset1 = unify.Dataset(msgs[0:2])
        dataset2 = unify.Dataset(msgs[2:])
        dataset = sum([dataset1, dataset2])
        assert len(dataset) == len(msgs)
        for item, msg in zip(dataset, msgs):
            assert item == msg

    def test_sum_variety(self) -> None:
        msgs = [unify.Prompt("a"), "b", unify.Prompt("c"), "d"]
        dataset = sum(msgs)
        assert len(dataset) == len(msgs)
        for item, msg in zip(dataset, msgs):
            assert unify.cast(item, str) == unify.cast(msg, str)

    def test_add_datasets_w_duplicates(self) -> None:
        msgs1 = ["a", "b"]
        msgs2 = ["b", "c"]
        dataset1 = unify.Dataset(msgs1)
        dataset2 = unify.Dataset(msgs2)
        dataset = dataset1 + dataset2
        assert len(dataset) == 3
        for item, msg in zip(dataset, ("a", "b", "c")):
            assert item == msg

    def test_dataset_inplace_addition(self) -> None:
        msgs = ["a", "b", "c", "d"]
        dataset = unify.Dataset(msgs[0:2])
        did = id(dataset)
        dataset2 = unify.Dataset(msgs[2:])
        dataset += dataset2
        assert did == id(dataset)
        assert len(dataset) == len(msgs)
        for item, msg in zip(dataset, msgs):
            assert item == msg

    def test_dataset_single_item_addition(self) -> None:
        dataset = unify.Dataset("a") + "b"
        assert len(dataset) == 2
        assert dataset[0] == "a"
        assert dataset[1] == "b"

    def test_dataset_reverse_addition(self) -> None:
        dataset = "a" + unify.Dataset("b")
        assert len(dataset) == 2
        assert dataset[0] == "a"
        assert dataset[1] == "b"

    def test_dataset_from_prompt_addition(self) -> None:
        dataset = unify.Prompt("a") + unify.Prompt("b")
        assert len(dataset) == 2
        assert dataset[0] == unify.Prompt("a")
        assert dataset[1] == unify.Prompt("b")

    def test_dataset_from_multi_item_addition(self) -> None:
        dataset = "a" + unify.Prompt("b") + "c"
        assert len(dataset) == 3
        assert dataset[0] == "a"
        assert dataset[1] == unify.Prompt("b")
        assert dataset[2] == "c"


class TestDatasetTrimming:
    def test_sub_datasets(self) -> None:
        msgs = ["a", "b", "c", "d"]
        dataset1 = unify.Dataset(msgs)
        dataset2 = unify.Dataset(msgs[2:])
        dataset = dataset1 - dataset2
        assert len(dataset) == 2
        for item, msg in zip(dataset, msgs[0:2]):
            assert item == msg

    def test_sub_datasets_w_non_overlap(self) -> None:
        msgs1 = ["a", "b"]
        msgs2 = ["b", "c"]
        dataset1 = unify.Dataset(msgs1)
        dataset2 = unify.Dataset(msgs2)
        with pytest.raises(AssertionError):
            dataset1 - dataset2

    def test_dataset_inplace_subtraction(self) -> None:
        msgs = ["a", "b", "c", "d"]
        dataset = unify.Dataset(msgs)
        did = id(dataset)
        dataset2 = unify.Dataset(msgs[2:])
        dataset -= dataset2
        assert did == id(dataset)
        assert len(dataset) == 2
        for item, msg in zip(dataset, msgs[0:2]):
            assert item == msg

    def test_dataset_single_item_subtraction(self) -> None:
        dataset = unify.Dataset(["a", "b"]) - "b"
        assert len(dataset) == 1
        assert dataset[0] == "a"

    def test_dataset_reverse_subtraction(self) -> None:
        dataset = ["a", "b"] - unify.Dataset("b")
        assert len(dataset) == 1
        assert dataset[0] == "a"

    def test_dataset_from_prompt_subtraction(self) -> None:
        dataset = unify.Prompt("b") + unify.Prompt("a") - unify.Prompt("b")
        assert len(dataset) == 1
        assert dataset[0] == unify.Prompt("a")


class UploadTesting:
    def __enter__(self):
        if "test_dataset" in unify.list_datasets():
            unify.delete_dataset("test_dataset")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if "test_dataset" in unify.list_datasets():
            unify.delete_dataset("test_dataset")


class TestDatasetUploading:
    def test_dataset_first_upload(self) -> None:
        with UploadTesting():
            dataset = unify.Dataset(["a", "b", "c"], name="test_dataset")
            assert dataset.name not in unify.list_datasets()
            dataset.upload()
            assert dataset.name in unify.list_datasets()

    def test_dataset_upload_w_overwrite(self) -> None:
        with UploadTesting():
            dataset = unify.Dataset(["a", "b", "c"], name="test_dataset")
            assert dataset.name not in unify.list_datasets()
            dataset.upload()
            assert dataset.name in unify.list_datasets()
            assert len(unify.Dataset.from_upstream("test_dataset")) == 3
            dataset -= "c"
            dataset.upload(overwrite=True)
            assert len(unify.Dataset.from_upstream("test_dataset")) == 2

    def test_dataset_upload_wo_overwrite(self):
        with UploadTesting():
            dataset = unify.Dataset(["a", "b", "c"], name="test_dataset")
            assert dataset.name not in unify.list_datasets()
            dataset.upload()
            assert dataset.name in unify.list_datasets()
            assert len(unify.Dataset.from_upstream("test_dataset")) == 3
            dataset += "d"
            dataset.upload()
            assert len(unify.Dataset.from_upstream("test_dataset")) == 4


class DownloadTesting:
    def __enter__(self):
        if "test_dataset" in unify.list_datasets():
            unify.delete_dataset("test_dataset")
        unify.Dataset(["a", "b", "c"], name="test_dataset").upload()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if "test_dataset" in unify.list_datasets():
            unify.delete_dataset("test_dataset")


# noinspection PyStatementEffect
class TestDatasetDownloading:
    def test_dataset_download(self) -> None:
        with DownloadTesting():
            assert "test_dataset" in unify.list_datasets()
            dataset = unify.Dataset.from_upstream("test_dataset")
            # noinspection DuplicatedCode
            assert len(dataset) == 3
            assert dataset[0] == "a"
            assert dataset[1] == "b"
            assert dataset[2] == "c"

    def test_dataset_download_w_overwrite(self) -> None:
        with DownloadTesting():
            assert "test_dataset" in unify.list_datasets()
            dataset = unify.Dataset(["a", "b", "c", "d"], name="test_dataset")
            assert len(dataset) == 4
            assert dataset[3] == "d"
            dataset.download(overwrite=True)
            assert len(dataset) == 3
            with pytest.raises(IndexError):
                dataset[3]

    # noinspection PyTypeChecker
    def test_dataset_download_dict(self) -> None:
        if "test_dataset" in unify.list_datasets():
            unify.delete_dataset("test_dataset")
        msgs = ("a", "b", "c")
        extra = ("A", "B", "C")
        extra_name = "".join(random.choice(string.ascii_lowercase) for _ in range(4))
        data = [{"message": msg, extra_name: ans} for msg, ans in zip(msgs, extra)]
        dataset = unify.Dataset(data, name="test_dataset")
        assert "message" in dataset[0]
        assert extra_name in dataset[0]
        dataset.upload()
        dataset = unify.Dataset.from_upstream("test_dataset")
        for i, (msg, ans) in enumerate(zip(msgs, extra)):
            assert "message" in dataset[i]
            assert dataset[i]["message"] == msg
            assert extra_name in dataset[i]
            assert dataset[i][extra_name] == ans
        unify.delete_dataset("test_dataset")

    def test_dataset_downloading_prompt_ids(self) -> None:
        with DownloadTesting():
            dataset = unify.Dataset.from_upstream("test_dataset")
            for item in dataset._raw_data:
                assert "id" in item
                assert "entry" in item
                assert isinstance(item["id"], str)


class TestDatasetSync:
    def test_sync_uploads(self) -> None:
        with DownloadTesting():
            assert "test_dataset" in unify.list_datasets()
            dataset = unify.Dataset(["a", "b", "c", "d"], name="test_dataset")
            dataset.sync()
            dataset.download()
            assert len(dataset) == 4
            assert dataset[0] == "a"
            assert dataset[1] == "b"
            assert dataset[2] == "c"
            assert dataset[3] == "d"

    def test_sync_downloads(self) -> None:
        with DownloadTesting():
            assert "test_dataset" in unify.list_datasets()
            dataset = unify.Dataset(["a", "b"], name="test_dataset")
            dataset.sync()
            dataset.download()
            assert len(dataset) == 3
            assert dataset[0] == "a"
            assert dataset[1] == "b"
            assert dataset[2] == "c"

    def test_sync_achieves_superset(self) -> None:
        with DownloadTesting():
            assert "test_dataset" in unify.list_datasets()
            dataset = unify.Dataset(["a", "b", "d"], name="test_dataset")
            dataset.sync()
            assert len(dataset) == 4
            assert dataset[0] == "a"
            assert dataset[1] == "b"
            assert dataset[2] == "c"
            assert dataset[3] == "d"


if __name__ == "__main__":
    pass
