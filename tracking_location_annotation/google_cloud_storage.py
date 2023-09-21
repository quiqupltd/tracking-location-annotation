"""
module to upload csv result files to google cloud storage
"""
from functools import cache

from google.cloud import storage  # type: ignore

from tracking_location_annotation.common import log
from tracking_location_annotation.common.benchmark import measure

logger = log.get_logger(__name__)

storage_client = storage.Client(project="quiqup-datascince")


@cache
def _get_bucket() -> storage.Bucket:
    """
    connects to google clpud storage bucket
    """
    bucket = storage_client.bucket("quiqup-tl-annotation")
    return bucket


@measure("gcs_upload_filename")
def upload_filename(local_filename: str, remote_dir: str) -> None:
    """
    Uploads the given local filename to the destination directory in the bucket.
    The uploaded blob has the name of the local filename appended to the provided
    destination directory.
    """
    logger.info("[google.storage] uploading filename %s", local_filename)
    bucket = _get_bucket()
    bucket.blob(f"{remote_dir}/{local_filename}").upload_from_filename(str(local_filename))
