import os
import shutil
import hashlib
import tempfile
import requests
from contextlib import contextmanager
from tempfile import TemporaryDirectory, mkdtemp


@contextmanager
def safe_open_write(filename, *args, **kwargs):
    with tempfile.TemporaryDirectory() as tempdir:
        temp_filename = os.path.join(tempdir, "file")
        with open(temp_filename, *args, **kwargs) as f:
            yield f
        shutil.move(temp_filename, filename)


def http_stream_download(filename, requests_kwargs, max_bytes=None):
    m = hashlib.sha256()
    with requests.get(stream=True, **requests_kwargs) as res:
        res.raise_for_status()
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with safe_open_write(filename, 'wb') as f:
            num_bytes = 0
            for chunk in res.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    m.update(chunk)
                    num_bytes += len(chunk)
                    if max_bytes and num_bytes > max_bytes:
                        raise StreamDownloadMaxBytesExceeded(f"Downloaded {num_bytes} bytes, which exceeds the limit of {max_bytes} bytes")
    return m.hexdigest()


class StreamDownloadMaxBytesExceeded(Exception):
    pass


@contextmanager
def tempdir(keep=False):
    if keep:
        tmpdir = mkdtemp()
        print("Keeping tempdir: {}".format(tmpdir))
        yield tmpdir
    else:
        with TemporaryDirectory() as tmpdir:
            yield tmpdir
