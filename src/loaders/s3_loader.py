"""
MinIO / S3 loader.
 
Responsibilities
----------------
* Upload JSON payloads and arbitrary files to an S3-compatible bucket.
* Implement BaseLoader.load_batch for row-oriented batch writes.
 
S3-specific parameters (bucket, key_prefix) belong in __init__, not in
load_batch — that keeps the signature compatible with BaseLoader (LSP).
"""
 
import io
import json
import logging
from typing import Any, Dict, List, Optional
 
import boto3
from botocore.exceptions import BotoCoreError, ClientError
 
from src.loaders.base_loader import BaseLoader, LoadResult
from src.utils.config import minio as cfg
 
logger = logging.getLogger(__name__)
 
 
class S3LoaderError(Exception):
    """Base for all S3/MinIO loader errors."""
 
 
class S3ClientError(S3LoaderError):
    """Raised when the boto3 client cannot be created."""
 
 
class S3UploadError(S3LoaderError):
    """Raised when a PUT operation fails."""
 
 
class S3DownloadError(S3LoaderError):
    """Raised when a GET operation fails."""
 
 
class S3BucketError(S3LoaderError):
    """Raised when a bucket operation (create, head) fails."""
 
 
def _build_client(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str,
) -> boto3.client:
    """
    Create and return a boto3 S3 client.
 
    Separated from S3Loader.__init__ so that connection errors surface
    as S3ClientError rather than a generic exception, and so the factory
    can be patched independently in tests.
 
    Raises:
        S3ClientError: When boto3 fails to initialise the client.
    """
    try:
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
    except (ClientError, BotoCoreError) as exc:
        logger.error("Failed to create S3 client: %s", exc)
        raise S3ClientError(f"Failed to create S3 client: {exc}") from exc
 
 
class S3Loader(BaseLoader):
    """
    Writes data to a MinIO (or any S3-compatible) endpoint.
 
    JSON batch uploads are newline-delimited (NDJSON) so Greenplum's external
    table reader and most analytics tools can process them line by line.
 
    Usage::
 
        loader = S3Loader(bucket="raw-data", key_prefix="hh_ru/2024-01-01")
        loader.load(records=vacancies)
 
        # Or upload an arbitrary JSON object directly:
        loader.upload_json(data=vacancies, key="hh_ru/2024-01-01/vacancies.json")
    """
 
    def __init__(
        self,
        bucket: str = cfg.bucket_raw,
        key_prefix: str = "",
        endpoint_url: str = cfg.endpoint_url,
        access_key: str = cfg.root_user,
        secret_key: str = cfg.root_password,
        region: str = cfg.region,
        batch_size: int = 1000,
    ) -> None:
        super().__init__(batch_size=batch_size)
        self._bucket = bucket
        self._key_prefix = key_prefix.rstrip("/")
        self._client = _build_client(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
        )
 
    @property
    def destination_name(self) -> str:
        return "minio"
 
    # ------------------------------------------------------------------
    # BaseLoader contract
    # ------------------------------------------------------------------
 
    def load_batch(
        self,
        records: List[Dict[str, Any]],
        **kwargs: Any,
    ) -> LoadResult:
        """
        Upload *records* as a single NDJSON file.
 
        The object key is ``{key_prefix}/{key_suffix}`` where key_prefix is
        set at construction time and key_suffix can be passed via kwargs.
 
        Args:
            records: List of dicts to serialise.
            **kwargs:
                key_suffix (str): filename appended to key_prefix.
                                  Defaults to ``"batch.ndjson"``.
                metadata (dict, optional): S3 object metadata.
 
        Returns:
            LoadResult with rows_written set to len(records) on success.
        """
        key_suffix: str = kwargs.get("key_suffix", "batch.ndjson")
        key = f"{self._key_prefix}/{key_suffix}" if self._key_prefix else key_suffix
        metadata: Optional[Dict[str, str]] = kwargs.get("metadata")
 
        result = LoadResult(destination=self.destination_name)
        try:
            ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
            self._put_object(
                bucket=self._bucket,
                key=key,
                body=ndjson.encode("utf-8"),
                content_type="application/x-ndjson",
                metadata=metadata,
            )
            result.rows_written = len(records)
            logger.info(
                "load_batch: wrote %d rows to s3://%s/%s",
                result.rows_written, self._bucket, key,
            )
        except S3UploadError as exc:
            result.rows_failed = len(records)
            result.details = str(exc)
            logger.error("load_batch failed: %s", exc)
        return result
 
    # ------------------------------------------------------------------
    # High-level helpers
    # ------------------------------------------------------------------
    
    def upload_json(
        self,
        data: Any,
        key: str,
        bucket: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        indent: Optional[int] = None,
    ) -> None:
        """
        Serialise *data* as JSON and upload to bucket/key.
 
        Args:
            data: Any JSON-serialisable object.
            key: Object key (path inside the bucket).
            bucket: Overrides the bucket set in __init__.
            metadata: Optional S3 object metadata (string-to-string map).
            indent: JSON indentation (None = compact).
        """
        target = bucket or self._bucket
        body = json.dumps(data, ensure_ascii=False, indent=indent).encode("utf-8")
        self._put_object(
            bucket=target,
            key=key,
            body=body,
            content_type="application/json",
            metadata=metadata,
        )
        logger.info("Uploaded JSON | bucket=%s key=%s size=%d B", target, key, len(body))
 
    def download_json(self, key: str, bucket: Optional[str] = None) -> Any:
        """
        Download and deserialise a JSON object.
 
        Args:
            key: Object key.
            bucket: Overrides the bucket set in __init__.
 
        Returns:
            Parsed Python object.
 
        Raises:
            S3DownloadError: When the object does not exist or cannot be read.
        """
        target = bucket or self._bucket
        try:
            response = self._client.get_object(Bucket=target, Key=key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except (ClientError, BotoCoreError) as exc:
            raise S3DownloadError(f"Failed to download s3://{target}/{key}: {exc}") from exc
        
    def download_ndjson(self, key: str, bucket: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Download and parse an NDJSON file line by line.

        Args:
            key: Object key.
            bucket: Overrides the bucket set in __init__.

        Returns:
            List of dicts, one per NDJSON line.

        Raises:
            S3DownloadError: When the object does not exist or cannot be read.
        """
        target = bucket or self._bucket
        try:
            response = self._client.get_object(Bucket=target, Key=key)
            records = []
            for line in response["Body"].iter_lines():
                if line:  # Пропускаем пустые строки
                    records.append(json.loads(line.decode("utf-8")))
            return records
        except (ClientError, BotoCoreError) as exc:
            raise S3DownloadError(f"Failed to download s3://{target}/{key}: {exc}") from exc

    def ensure_bucket(self, bucket: Optional[str] = None) -> None:
        """Create bucket if it does not already exist."""
        target = bucket or self._bucket
        try:
            self._client.head_bucket(Bucket=target)
        except ClientError:
            try:
                self._client.create_bucket(Bucket=target)
                logger.info("Created bucket: %s", target)
            except (ClientError, BotoCoreError) as exc:
                raise S3BucketError(f"Failed to create bucket '{target}': {exc}") from exc
 
    def list_objects(self, prefix: str = "", bucket: Optional[str] = None) -> List[str]:
        """Return all object keys with the given prefix."""
        target = bucket or self._bucket
        paginator = self._client.get_paginator("list_objects_v2")
        keys: List[str] = []
        for page in paginator.paginate(Bucket=target, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
 
    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------
 
    def _put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        try:
            self._client.put_object(
                Bucket=bucket,
                Key=key,
                Body=io.BytesIO(body),
                ContentType=content_type,
                Metadata=metadata or {},
            )
        except (ClientError, BotoCoreError) as exc:
            raise S3UploadError(f"PUT s3://{bucket}/{key} failed: {exc}") from exc