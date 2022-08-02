import logging
import tempfile
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from google.cloud import storage
from google.cloud.storage import Bucket, Client

logger = logging.getLogger(__name__)


def upload_blob_from_str(bucket: Bucket, file_contents: str, dest: str) -> str:
    """Upload a string as a file to blob storage

    :param bucket: The bucket object containing the file
    :param src: the local path/to/file
    :param file_contents: The file contents
    :returns the gs://path/to/the/file.ext in the bucket
    """
    blob = bucket.blob(dest)
    blob.upload_from_string(file_contents)
    return f"gs://{bucket.name}{dest}"


def blob_exists(bucket: Bucket, src: str) -> bool:
    """Checks if a blob exists

    :param bucket: The bucket object containing the file
    :param src: path/to/the/file.ext in the bucket
    :return: True / False of whether the blob exists
    """
    blob = bucket.blob(src)
    return blob.exists()

def download_blob(bucket: Bucket, src: str):
    """
    Download file from blob storage to temp file. Example usage:

    >>> with download_blob(bucket, "path/to/blob.csv") as temp_file:  # doctest: +SKIP
    ...    df = pd.read_csv(temp_file)

    :param bucket: The bucket object containing the file
    :param src: path/to/the/file.ext in the bucket
    :return: the temporary file (can be used as a context manager)
        Returns an object with a file-like interface;
        the name of the file is accessible as its 'name' attribute.
        The file will be automatically deleted when it is closed
    """
    blob = bucket.blob(src)
    tmp = tempfile.NamedTemporaryFile()
    blob.download_to_filename(tmp.name)
    tmp.seek(0)
    return tmp

def list_blob_names(
    storage_client: storage.Client, bucket: Bucket, prefix: Optional[str] = None, suffix: Optional[str] = ""
) -> List[str]:
    """Returns the list of blob names.

    :param storage_client: The storage client
    :param bucket: The bucket we use for storage and inference
    :param prefix: Only includes blob names that start with this (optional)
    :param suffix: Only includes blob names that end with this (optional)
    :return: List of the names of the blobs in the bucket (matching the prefix/suffix)
    """
    blob_names = [blob.name for blob in storage_client.list_blobs(bucket, prefix=prefix) if blob.name.endswith(suffix)]
    return blob_names

def load_file_from_bucket(bucket, relative_path, verbose=False):
    if blob_exists(bucket, relative_path):
        if verbose:
            print("Check. found results in bucket.")
        with download_blob(bucket, relative_path) as tmp:
            results = pd.read_csv(tmp, low_memory=False)
    else:
        raise FileNotFoundError(f"Could not find this results file: {relative_path}")
    return results