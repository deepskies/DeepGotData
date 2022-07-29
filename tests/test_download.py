from DeepGotData.api.download import Download, StreamData

import pytest
import os
import numpy as np

def test_not_connected():
    # TODO ???? How
    pass


def test_init():
    data_name = "test"
    batch_size = 32
    dl = Download(data_name, batch_size)
    assert dl.dataset == data_name
    assert dl.batch_size == batch_size

    data_name = "test2"
    dl = Download(data_name)
    assert dl.dataset == data_name
    assert dl.batch_size is None


def test_stream_init():
    blobs = ["", "", ""]
    bucket = None # Should be a google bucket object but hey thus is life
    batch_size = 1
    streamer = StreamData(blobs, bucket, batch_size)

    assert streamer.blobs == blobs
    assert streamer.bucket == bucket
    assert streamer.batch_size == batch_size
    assert streamer.batches == 3
    assert streamer.batch == 0


# def test_table_view_real_project():
#     expected_data_path = ""
#     dataset_name = ""
#     download = Download(dataset_name)
#     data_path = download._find_dataset()
#
#     assert data_path == expected_data_path
#
#
# def test_table_view_fake_project():
#     with pytest.raises(AssertionError):
#         fake_dataset = "fake_name"
#         dl = Download(fake_dataset)
#         dl._find_dataset()
#
#
# def test_download():
#     dataset_name = ""
#     download_path = "test_dir/download/"
#     dl = Download(dataset_name)
#     dl.download(path=download_path)
#
#     assert os.path.exists(download_path)
#     assert len(os.listdir(download_path))!=0
#
#
# def test_download_invalid_path():
#     dataset_name = ""
#     download_path = "test_dir/download/file.csv"
#     dl = Download(dataset_name)
#     with pytest.raises(AssertionError):
#         dl.download(path=download_path)
#
#
# def test_download_invalid_dataset():
#     fake_dataset = "fake_name"
#     download_path = "test_dir/download/"
#     dl = Download(fake_dataset)
#     with pytest.raises(AssertionError):
#         dl.download(path=download_path)
#
#
# def test_stream_call():
#     dataset_name = ""
#     dl = Download(dataset_name, batch_size=1)
#     streamer = dl.download()
#
#     assert isinstance(streamer, StreamData)
#
#
# def test_stream_call_invalid_dataset():
#     fake_dataset = "fake_name"
#     dl = Download(fake_dataset, batch_size=1)
#     with pytest.raises(AssertionError):
#         streamer = dl.download()
#
#
# def test_stream_batch_to_large():
#     dataset_name = ""
#     dl = Download(dataset_name, batch_size=10000000)
#     with pytest.raises(AssertionError):
#         streamer = dl.download()
#
#
# def test_streams():
#     dataset_name = ""
#     dl = Download(dataset_name, batch_size=1)
#     streamer = dl.download()
#     image = streamer.next()
#
#     assert type(image) == list
#     assert type(image[0]) == np.array
#
#
# def test_streams_multisize_batch():
#     dataset_name = ""
#     dl = Download(dataset_name, batch_size=3)
#     streamer = dl.download()
#     image = streamer.next()
#
#     assert len(image) == 3
#     assert type(image) == list
#     assert type(image[0]) == np.array
