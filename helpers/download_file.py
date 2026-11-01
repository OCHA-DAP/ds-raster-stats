"""
Download a file from a URL, or every matching file in an FTP folder, e.g.
CHIRPS-GEFS COGs to inspect locally.

Streams to a `.part` file and only renames it once the download completes, so
an interrupted download never leaves a truncated file behind. Retries with
backoff, since the CHC data server is intermittently unreachable.

Usage:
    python helpers/download_file.py <url> [output_dir]

A URL whose filename has a wildcard downloads every matching file in that FTP
folder over one connection, like `wget ftp://.../*.tif`.

Examples:
    python helpers/download_file.py \
        ftp://data.chc.ucsb.edu/pub/org/chc/products/CHIRPS-GEFS/v3/daily/global/2001/01/01/c3g_2001.01.01.tif \
        downloads
    python helpers/download_file.py \
        "ftp://data.chc.ucsb.edu/pub/org/chc/products/CHIRPS-GEFS/v3/daily/global/2001/01/01/*.tif" \
        downloads
"""

import fnmatch
import ftplib
import logging
import os
import random
import sys
import time
import uuid
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import coloredlogs
import requests

logger = logging.getLogger(__name__)
coloredlogs.install(level="INFO", logger=logger)

# Status codes worth retrying: rate limiting and transient server errors.
# Any other 4xx (e.g. 404 for a missing file) won't change on retry.
RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_WAIT_SECONDS = 600

# CHC documents its FTP URLs as ftp://data.chc.ucsb.edu/pub/org/chc/products/...
# but anonymous logins start inside /pub/org/chc, so those paths need the
# prefix removed
CHC_FTP_ROOT = "/pub/org/chc"

# Shared across calls so consecutive downloads are spaced out
_last_request_time = 0.0


def _throttle(min_interval):
    """Sleep so requests start at least `min_interval` seconds apart."""
    global _last_request_time
    wait = _last_request_time + min_interval - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _last_request_time = time.monotonic()


def _retry_after_seconds(response):
    """
    Parse a `Retry-After` header, given either as seconds or an HTTP date.
    Returns None if the header is missing or unreadable.
    """
    value = (
        response.headers.get("Retry-After") if response is not None else None
    )
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
    except (TypeError, ValueError):
        return None


def _ftp_candidate_paths(path):
    """The path as given, then without the CHC FTP root if it has one."""
    paths = [path]
    if path.startswith(CHC_FTP_ROOT + "/"):
        paths.append(path[len(CHC_FTP_ROOT) :])
    return paths


def _ftp_connect(parsed, timeout):
    """Log in (anonymously unless the URL has credentials), binary mode."""
    ftp = ftplib.FTP(parsed.hostname, timeout=timeout)
    ftp.login(parsed.username or "anonymous", parsed.password or "")
    # SIZE needs binary mode on some servers
    ftp.voidcmd("TYPE I")
    return ftp


def _ftp_cwd(ftp, path, url):
    """Change to an FTP folder, raising FileNotFoundError if it isn't there."""
    for candidate in _ftp_candidate_paths(path):
        try:
            ftp.cwd(candidate)
            return
        except ftplib.error_perm:
            continue
    raise FileNotFoundError(f"{url}: folder not found")


def _download_http(url, part_path, timeout, chunk_size):
    """Stream an HTTP(S) URL to `part_path`. Returns the expected size."""
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        expected = int(response.headers.get("Content-Length", 0))
        with open(part_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)
    return expected


def _download_ftp(url, part_path, timeout, chunk_size):
    """
    Download an FTP URL to `part_path`, logging in anonymously unless the URL
    has credentials. Returns the expected size. A permanent FTP error (5xx,
    e.g. 550 for a missing file) is raised as FileNotFoundError.
    """
    parsed = urlparse(url)
    with _ftp_connect(parsed, timeout[1]) as ftp:
        for path in _ftp_candidate_paths(parsed.path):
            try:
                expected = ftp.size(path)
                break
            except ftplib.error_perm:
                continue
        else:
            raise FileNotFoundError(f"{url}: file not found")
        with open(part_path, "wb") as f:
            ftp.retrbinary(f"RETR {path}", f.write, blocksize=chunk_size)
    return expected or 0


def download_file(
    url,
    output_dir=".",
    retries=5,
    timeout=(10, 120),
    chunk_size=1024**2,
    min_interval=1.0,
):
    """
    Download a file from an HTTP(S) or FTP URL into a local directory.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    output_dir : str
        The directory to save the file in. Created if it doesn't exist.
    retries : int
        Number of attempts before giving up.
    timeout : tuple of (float, float)
        Connect and read timeouts in seconds. FTP uses the read timeout for
        the whole connection.
    chunk_size : int
        Number of bytes to write per chunk.
    min_interval : float
        Minimum seconds between the start of consecutive requests, to stay
        under the server's rate limit.

    Returns
    -------
    str
        The path of the downloaded file.

    Raises
    ------
    FileNotFoundError
        If an FTP server reports the file as unavailable (not retried).
    requests.HTTPError
        If an HTTP server returns a non-retryable status, e.g. 404.
    """
    is_ftp = urlparse(url).scheme == "ftp"
    fetch = _download_ftp if is_ftp else _download_http
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.basename(urlparse(url).path)
    path = os.path.join(output_dir, filename)
    # Unique per call, so parallel tasks fetching the same file don't write
    # into each other's partial download
    part_path = f"{path}.{uuid.uuid4().hex[:8]}.part"
    if os.path.exists(path):
        logger.info(f"Already downloaded: {path}")
        return path

    for attempt in range(1, retries + 1):
        try:
            _throttle(min_interval)
            logger.info(f"Downloading {url} (attempt {attempt}/{retries})...")
            expected = fetch(url, part_path, timeout, chunk_size)

            size = os.path.getsize(part_path)
            if expected and size != expected:
                raise IOError(f"Got {size} bytes, expected {expected}")

            os.replace(part_path, path)
            logger.info(f"Saved {size / 1024**2:.1f} MB to {path}")
            return path
        except (requests.RequestException, *ftplib.all_errors) as e:
            logger.warning(f"Download failed: {e}")
            if os.path.exists(part_path):
                os.remove(part_path)

            # Missing files won't appear on retry. Temporary FTP errors (4xx,
            # e.g. 421 too many connections) fall through and are retried.
            if isinstance(e, (FileNotFoundError, ftplib.error_perm)):
                raise
            response = getattr(e, "response", None)
            status = response.status_code if response is not None else None
            if status is not None and status not in RETRY_STATUSES:
                raise
            if attempt == retries:
                raise

            # Prefer the server's Retry-After (sent with 429/503); otherwise
            # back off exponentially, with jitter so retries don't line up
            wait = _retry_after_seconds(response)
            if wait is None:
                wait = 2**attempt + random.uniform(0, 1)
            wait = min(wait, MAX_WAIT_SECONDS)
            logger.info(f"Retrying in {wait:.0f}s...")
            time.sleep(wait)


def download_ftp_folder(
    folder_url,
    output_dir=".",
    pattern="*.tif",
    retries=5,
    timeout=120,
    chunk_size=1024**2,
    min_interval=1.0,
):
    """
    Download every file matching `pattern` in an FTP folder over a single
    connection, like `wget ftp://host/folder/*.tif`.

    Files already in `output_dir` are skipped, so a retry after a dropped
    connection picks up where it left off.

    Parameters
    ----------
    folder_url : str
        The FTP URL of the folder, e.g. ftp://host/path/to/folder/
    output_dir : str
        The directory to save the files in. Created if it doesn't exist.
    pattern : str
        Shell-style wildcard that filenames must match.
    retries : int
        Number of connection attempts before giving up.
    timeout : float
        Socket timeout in seconds.
    chunk_size : int
        Number of bytes to write per chunk.
    min_interval : float
        Minimum seconds between the start of consecutive connections.

    Returns
    -------
    list of str
        Paths of the matching files, sorted by filename. Empty if the folder
        has no matching files.

    Raises
    ------
    FileNotFoundError
        If the folder doesn't exist (not retried).
    """
    parsed = urlparse(folder_url)
    folder = parsed.path.rstrip("/") or "/"
    os.makedirs(output_dir, exist_ok=True)
    part_path = None

    for attempt in range(1, retries + 1):
        try:
            # _throttle(min_interval)
            with _ftp_connect(parsed, timeout) as ftp:
                _ftp_cwd(ftp, folder, folder_url)
                try:
                    names = sorted(fnmatch.filter(ftp.nlst(), pattern))
                except ftplib.error_perm:
                    # Some servers answer an empty listing with 550
                    names = []
                logger.info(
                    f"{len(names)} file(s) matching {pattern} in {folder_url}"
                )

                for name in names:
                    path = os.path.join(output_dir, name)
                    if os.path.exists(path):
                        continue
                    part_path = f"{path}.{uuid.uuid4().hex[:8]}.part"
                    expected = ftp.size(name) or 0
                    with open(part_path, "wb") as f:
                        ftp.retrbinary(
                            f"RETR {name}", f.write, blocksize=chunk_size
                        )
                    size = os.path.getsize(part_path)
                    if expected and size != expected:
                        raise IOError(
                            f"{name}: got {size} bytes, expected {expected}"
                        )
                    os.replace(part_path, path)
                    part_path = None
                    logger.info(f"Saved {size / 1024**2:.1f} MB to {path}")

            return [os.path.join(output_dir, name) for name in names]
        except FileNotFoundError:
            raise
        except ftplib.all_errors as e:
            logger.warning(f"Folder download failed: {e}")
            if part_path and os.path.exists(part_path):
                os.remove(part_path)
            part_path = None
            # Temporary errors (4xx, e.g. 421 too many connections), dropped
            # connections and truncated files are retried; 5xx aren't
            if isinstance(e, ftplib.error_perm) or attempt == retries:
                raise
            wait = min(2**attempt + random.uniform(0, 1), MAX_WAIT_SECONDS)
            logger.info(f"Retrying in {wait:.0f}s...")
            time.sleep(wait)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python helpers/download_file.py <url> [output_dir]")
    url = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "."
    folder, _, filename = url.rpartition("/")
    if urlparse(url).scheme == "ftp" and any(c in filename for c in "*?["):
        download_ftp_folder(f"{folder}/", output_dir, pattern=filename)
    else:
        download_file(url, output_dir)
