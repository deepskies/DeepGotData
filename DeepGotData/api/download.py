#from google.cloud import storage, bigquery
import numpy as np
import os
import json


class Download:
    def __init__(self, dataset, batch_size=None):
        self.batch_size = batch_size
        self.dataset = dataset

    def _find_dataset(self):
        # TODO Correct query table names
        # TODO Verify query syntax
        query = f"SELECT BucketName FROM table WHERE DatasetName = {self.dataset}"
        sql_client = bigquery.Client()
        results = sql_client.query(query)

        assert len(results) != 0, f"Could not find table with name {self.dataset}"
        return results[0]["BucketName"]

    def download(self, path=None):

        storage_client = storage.Client()

        bucket_name = self._find_dataset()
        bucket = storage_client.bucket(bucket_name)
        blobs = storage_client.list_blobs(bucket_name)

        if path is not None:
            if not os.path.exists(path):
                os.makedirs(path)

            for image in blobs:
                blob = bucket.blob(image.name)
                file_name = image.name.split("/")[-1]
                download_path = f"{path}/{file_name}"
                blob.download_to_filename(download_path)

        else:
            assert self.batch_size is not None, "Must have set batch size to stream data"
            return StreamData(blobs, bucket, self.batch_size)


class StreamData:
    def __init__(self, blobs, bucket, batch_size):

        self.blobs = blobs
        self.bucket = bucket
        self.batch_size = batch_size
        self.batches = len(blobs)/batch_size
        self.batch = 0

    def next(self):
        lower_index = self.batch * len(self.blobs)
        upper_index = (self.batch+1) * len(self.blobs)

        if upper_index >= len(self.blobs):
            upper_index = -1

        blob_paths = self.blobs[lower_index:upper_index]
        images = []
        for blob in blob_paths:
            blob = self.bucket.blob(blob.name)
            image = json.loads(blob.download_as_string())
            image = np.array(image)

            images.append(image)

        self.batch += 1

        return np.array(images)

