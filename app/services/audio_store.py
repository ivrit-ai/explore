"""Audio storage backends.

Episodes live either on a locally-mounted directory or in an S3 bucket.
Both are exposed through the same small interface so routes never need to
know which one is in use:

    store.response(request, source)   -> a streaming/file response
    store.ffmpeg_input(source)        -> context manager giving an ffmpeg -i argument
    store.local_copy(source)          -> context manager giving a real filesystem path

`source` is the episode identifier used throughout the app, e.g.
``"929/2014.12.17 סימנים"``. The ``.opus`` suffix is optional.
"""

from __future__ import annotations

import logging
import os
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from email.utils import formatdate
from pathlib import Path
from typing import Iterator, Optional

from fastapi import HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse

logger = logging.getLogger(__name__)

_AUDIO_EXT = ".opus"
_STREAM_CHUNK = 1024 * 256


def _safe_key(source: str) -> Optional[str]:
    """Normalise an episode identifier to a relative key, or None if unsafe.

    `source` reaches us from URL path parameters as well as from the index, so
    traversal segments are rejected outright rather than resolved.
    """
    parts = [p for p in source.strip("/").split("/") if p]
    if not parts or any(p in (".", "..") for p in parts):
        return None
    if not parts[-1].endswith(_AUDIO_EXT):
        parts[-1] += _AUDIO_EXT
    return "/".join(parts)


class AudioStore(ABC):
    """Read-only access to the episode audio collection."""

    @abstractmethod
    def exists(self, source: str) -> bool:
        """Whether audio for this episode is available."""

    @abstractmethod
    def response(self, request: Request, source: str) -> Response:
        """Serve the episode, honouring HTTP Range requests.

        Raises HTTPException(404) when the episode has no audio.
        """

    @abstractmethod
    def ffmpeg_input(self, source: str):
        """Context manager yielding an argument ffmpeg can pass to ``-i``."""

    @abstractmethod
    def local_copy(self, source: str):
        """Yield a real filesystem path for this episode.

        For remote backends this downloads the object and removes it on exit.
        """

    @property
    def is_remote(self) -> bool:
        """True when reads cross the network, so retries may be worth it."""
        return False

    def describe(self) -> str:
        return self.__class__.__name__


class LocalAudioStore(AudioStore):
    """Episodes read straight off a mounted directory."""

    def __init__(self, audio_dir: str | Path):
        self.audio_dir = Path(audio_dir)

    def describe(self) -> str:
        return f"local:{self.audio_dir}"

    def _path(self, source: str) -> Optional[Path]:
        key = _safe_key(source)
        return self.audio_dir.joinpath(*key.split("/")) if key else None

    def exists(self, source: str) -> bool:
        path = self._path(source)
        return path is not None and path.is_file()

    def response(self, request: Request, source: str) -> Response:
        path = self._path(source)
        if path is None or not path.is_file():
            raise HTTPException(status_code=404, detail=f"Audio file not found for {source}")
        # FileResponse handles Range requests natively via Starlette.
        return FileResponse(str(path), media_type="audio/opus")

    @contextmanager
    def ffmpeg_input(self, source: str) -> Iterator[str]:
        path = self._path(source)
        if path is None or not path.is_file():
            raise FileNotFoundError(source)
        yield str(path)

    @contextmanager
    def local_copy(self, source: str) -> Iterator[str]:
        with self.ffmpeg_input(source) as path:
            yield path


class S3AudioStore(AudioStore):
    """Episodes read from an S3 bucket.

    Range requests are forwarded to S3 verbatim, so seeking in the player
    costs the app only the bytes the client actually asked for. Credentials
    come from the standard boto3 chain (environment, ~/.aws, instance role).
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "audio",
        region: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        presign_ttl: int = 3600,
    ):
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.presign_ttl = presign_ttl
        self._client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint_url,
            config=Config(retries={"max_attempts": 5, "mode": "standard"}),
        )

    @property
    def is_remote(self) -> bool:
        return True

    def describe(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"

    def _key(self, source: str) -> Optional[str]:
        key = _safe_key(source)
        if key is None:
            return None
        return f"{self.prefix}/{key}" if self.prefix else key

    @staticmethod
    def _is_missing(err) -> bool:
        code = err.response.get("Error", {}).get("Code", "")
        return code in ("404", "NoSuchKey", "NoSuchBucket")

    def exists(self, source: str) -> bool:
        from botocore.exceptions import ClientError

        key = self._key(source)
        if key is None:
            return False
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as err:
            if self._is_missing(err):
                return False
            raise

    def response(self, request: Request, source: str) -> Response:
        from botocore.exceptions import ClientError

        key = self._key(source)
        if key is None:
            raise HTTPException(status_code=404, detail=f"Audio file not found for {source}")
        kwargs = {"Bucket": self.bucket, "Key": key}
        range_header = request.headers.get("range")
        if range_header:
            kwargs["Range"] = range_header

        try:
            obj = self._client.get_object(**kwargs)
        except ClientError as err:
            if self._is_missing(err):
                raise HTTPException(status_code=404, detail=f"Audio file not found for {source}")
            if err.response.get("Error", {}).get("Code") == "InvalidRange":
                raise HTTPException(status_code=416, detail="Requested range not satisfiable")
            logger.exception("S3 get_object failed for %s", key)
            raise HTTPException(status_code=502, detail="Audio backend error")

        body = obj["Body"]

        def stream() -> Iterator[bytes]:
            try:
                while chunk := body.read(_STREAM_CHUNK):
                    yield chunk
            finally:
                body.close()

        headers = {"Accept-Ranges": "bytes"}
        if obj.get("ContentLength") is not None:
            headers["Content-Length"] = str(obj["ContentLength"])
        if obj.get("ETag"):
            headers["ETag"] = obj["ETag"]
        if obj.get("LastModified"):
            headers["Last-Modified"] = formatdate(
                obj["LastModified"].timestamp(), usegmt=True
            )

        content_range = obj.get("ContentRange")
        if content_range:
            headers["Content-Range"] = content_range

        return StreamingResponse(
            stream(),
            status_code=206 if content_range else 200,
            media_type="audio/opus",
            headers=headers,
        )

    def _presign(self, key: str) -> str:
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=self.presign_ttl,
        )

    @contextmanager
    def ffmpeg_input(self, source: str) -> Iterator[str]:
        """A presigned URL, so ffmpeg range-reads only the part it needs."""
        key = self._key(source)
        if key is None or not self.exists(source):
            raise FileNotFoundError(source)
        yield self._presign(key)

    @contextmanager
    def local_copy(self, source: str) -> Iterator[str]:
        from botocore.exceptions import ClientError

        key = self._key(source)
        if key is None:
            raise FileNotFoundError(source)
        fd, tmp = tempfile.mkstemp(suffix=_AUDIO_EXT)
        os.close(fd)
        try:
            try:
                self._client.download_file(self.bucket, key, tmp)
            except ClientError as err:
                if self._is_missing(err):
                    raise FileNotFoundError(key)
                raise
            yield tmp
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def build_audio_store(
    *,
    source: Optional[str] = None,
    data_dir: Optional[str | Path] = None,
    audio_dir: Optional[str | Path] = None,
    bucket: Optional[str] = None,
    prefix: Optional[str] = None,
    region: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    presign_ttl: Optional[int] = None,
) -> AudioStore:
    """Build the audio backend from explicit arguments, falling back to env vars.

    ``source`` is one of ``local``, ``s3`` or ``auto`` (the default): auto picks
    S3 when a bucket is configured and the local directory otherwise.
    """
    source = (source or os.environ.get("EXPLORE_AUDIO_SOURCE") or "auto").lower()
    bucket = bucket or os.environ.get("EXPLORE_S3_BUCKET")

    if source == "auto":
        source = "s3" if bucket else "local"

    if source == "s3":
        if not bucket:
            raise ValueError(
                "Audio source 's3' requires a bucket "
                "(--s3-bucket or EXPLORE_S3_BUCKET)"
            )
        return S3AudioStore(
            bucket=bucket,
            prefix=prefix if prefix is not None else os.environ.get("EXPLORE_S3_PREFIX", "audio"),
            region=region
            or os.environ.get("EXPLORE_S3_REGION")
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION"),
            endpoint_url=endpoint_url or os.environ.get("EXPLORE_S3_ENDPOINT_URL"),
            presign_ttl=int(
                presign_ttl
                if presign_ttl is not None
                else os.environ.get("EXPLORE_S3_PRESIGN_TTL", 3600)
            ),
        )

    if source != "local":
        raise ValueError(f"Unknown audio source: {source!r} (expected 'local', 's3' or 'auto')")

    if audio_dir is None:
        if data_dir is None:
            raise ValueError("Audio source 'local' requires a data directory")
        audio_dir = Path(data_dir) / "audio"
    return LocalAudioStore(audio_dir)
