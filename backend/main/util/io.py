import io
from contextlib import contextmanager
from tempfile import SpooledTemporaryFile
from typing import IO, Iterator
from urllib.parse import urlunparse

import fsspec  # type: ignore[import-untyped]
from django.core.files.uploadedfile import UploadedFile

DEFAULT_BUFFER_SIZE = io.DEFAULT_BUFFER_SIZE
DEFAULT_MAX_TEMP_FILE_BUFFER_SIZE = 20 * 1024 * 1024  # 20 MiB


@contextmanager
def open_source(source: str | UploadedFile) -> Iterator[IO[bytes]]:
    if isinstance(source, str):
        with fsspec.open(source, mode="rb") as f:
            yield f
    else:
        source.seek(0)
        yield source
        # Don't close — Django manages lifecycle


@contextmanager
def open_sink(sink: str | IO[bytes]) -> Iterator[IO[bytes]]:
    if isinstance(sink, str):
        with fsspec.open(sink, mode="wb") as f:
            yield f
    else:
        yield sink
        # Don't close — caller manages lifecycle


def open_temp_file() -> SpooledTemporaryFile[bytes]:
    return SpooledTemporaryFile(max_size=DEFAULT_MAX_TEMP_FILE_BUFFER_SIZE)


def get_uri(file: str | UploadedFile) -> str:
    if isinstance(file, str):
        return file

    return urlunparse(("upload", "", file.name, "", "", ""))
