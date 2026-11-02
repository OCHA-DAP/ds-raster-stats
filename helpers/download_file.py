"""
Download files from an FTP server, e.g. CHIRPS-GEFS COGs from the CHC server.

The pipeline uses `download_ftp_files` (through
`src.utils.cog_utils.download_chirps_cogs`) to fetch each date chunk's COGs
before processing, over several connections at once. The module can also be
run on its own to download a COG to inspect locally.

Each file is streamed to a uniquely named `.part` file and only renamed once
its size has been checked, so a failed download never leaves a truncated file
under the real name. Files that are already downloaded are skipped. Failed
connections are retried with exponential backoff, since the CHC server is
intermittently unreachable and limits concurrent connections.

Usage:
    python helpers/download_file.py <ftp_url> [output_dir]

Downloads the file at `ftp_url` into `output_dir` (the current directory by
default). Only ftp:// URLs are supported. The command exits with an error if
the file isn't on the server.

Example:
    python helpers/download_file.py
    ftp://data.chc.ucsb.edu/pub/org/chc/products/CHIRPS-GEFS/v3/daily/global/2001/01/01/c3g_2001.01.01.tif downloads
"""

import ftplib
import logging
import os
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import coloredlogs

logger = logging.getLogger(__name__)
coloredlogs.install(level="INFO", logger=logger)

# Upper bound on the wait between retries, in seconds
MAX_WAIT_SECONDS = 600

# CHC documents its FTP URLs as ftp://data.chc.ucsb.edu/pub/org/chc/products/...
# but anonymous logins start inside /pub/org/chc, so those paths need the
# prefix removed
CHC_FTP_ROOT = "/pub/org/chc"


def _ftp_candidate_paths(path):
    """
    Paths to try for `path` on the server: the path as given, then, if it
    starts with the CHC FTP root, the same path without it.
    """
    paths = [path]
    if path.startswith(CHC_FTP_ROOT + "/"):
        paths.append(path[len(CHC_FTP_ROOT) :])
    return paths


def _ftp_connect(parsed, timeout):
    """
    Connect to the host in `parsed` (a parsed FTP URL) and log in,
    anonymously unless the URL has credentials. Returns the connection, set
    to binary mode.
    """
    ftp = ftplib.FTP(parsed.hostname, timeout=timeout)
    ftp.login(parsed.username or "anonymous", parsed.password or "")
    # SIZE needs binary mode on some servers
    ftp.voidcmd("TYPE I")
    return ftp


def _retry(func, retries, what):
    """
    Call `func` up to `retries` times, waiting between attempts with a
    backoff that doubles each time. Temporary FTP errors (4xx, e.g. 421 too
    many connections), dropped connections and truncated files are retried;
    FileNotFoundError and permanent (5xx) FTP errors are raised straight away.
    """
    for attempt in range(1, retries + 1):
        try:
            return func()
        except FileNotFoundError:
            raise
        except ftplib.all_errors as e:
            logger.warning(f"{what} failed: {e}")
            if isinstance(e, ftplib.error_perm) or attempt == retries:
                raise
            wait = min(2**attempt + random.uniform(0, 1), MAX_WAIT_SECONDS)
            logger.info(f"Retrying in {wait:.0f}s...")
            time.sleep(wait)


def _fetch_file(ftp, remote, path, chunk_size, expected=None):
    """
    Download the file at `remote` (relative to the connection's current
    folder, or absolute) to `path`.

    It's written to a uniquely named `.part` file, checked against the size
    the server reports, then renamed to `path`. The `.part` file is removed
    if the download fails.
    """
    part_path = f"{path}.{uuid.uuid4().hex[:8]}.part"
    try:
        if expected is None:
            expected = ftp.size(remote) or 0
        with open(part_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote}", f.write, blocksize=chunk_size)
        size = os.path.getsize(part_path)
        if expected and size != expected:
            raise IOError(f"{remote}: got {size} bytes, expected {expected}")
        os.replace(part_path, path)
    finally:
        if os.path.exists(part_path):
            os.remove(part_path)
    logger.info(f"Saved {size / 1024**2:.1f} MB to {path}")


def _remote_size(ftp, path):
    """
    Find `path` on the server, trying each of its candidate paths. Returns
    the path that exists and its size, or (None, None) if none does.
    """
    for candidate in _ftp_candidate_paths(path):
        try:
            return candidate, ftp.size(candidate) or 0
        except ftplib.error_perm:
            continue
    return None, None


def _download_url_share(files, missing, opts):
    """
    Download `files`, a list of (url, path) pairs, over one connection at a
    time, retrying with a new connection if it fails. Files already at their
    path are skipped, so a retry picks up where the last attempt left off.
    URLs that aren't on the server are added to `missing`.
    """

    def attempt():
        ftp, netloc = None, None
        try:
            for url, path in files:
                if os.path.exists(path) or url in missing:
                    continue
                parsed = urlparse(url)
                # One connection per server, reconnecting if the host changes
                if parsed.netloc != netloc:
                    if ftp is not None:
                        ftp.close()
                    ftp = _ftp_connect(parsed, opts["timeout"])
                    netloc = parsed.netloc
                remote, expected = _remote_size(ftp, parsed.path)
                if remote is None:
                    missing.add(url)
                    continue
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                _fetch_file(ftp, remote, path, opts["chunk_size"], expected)
        finally:
            if ftp is not None:
                ftp.close()

    _retry(attempt, opts["retries"], "Download")


def download_ftp_files(
    files, retries=5, timeout=120, chunk_size=1024**2, connections=1
):
    """
    Download a list of FTP URLs to the given paths, over one or more
    connections at once.

    Parameters
    ----------
    files : list of (str, str)
        (url, path) pairs: the FTP URL of each file and the local path to
        save it to.
    retries : int
        Number of connection attempts before giving up, per connection.
    timeout : float
        Timeout in seconds for connecting and for each socket operation.
    chunk_size : int
        Block size in bytes for the FTP transfer.
    connections : int
        Number of FTP connections to download over at once.

    Returns
    -------
    list of str
        URLs that aren't on the server, in the order of `files`. They're
        skipped rather than retried.
    """
    todo = [(url, path) for url, path in files if not os.path.exists(path)]
    opts = {"retries": retries, "timeout": timeout, "chunk_size": chunk_size}
    missing = set()
    n_shares = max(1, min(connections, len(todo)))
    shares = [todo[i::n_shares] for i in range(n_shares)]

    if n_shares == 1:
        _download_url_share(todo, missing, opts)
    else:
        with ThreadPoolExecutor(max_workers=n_shares) as executor:
            futures = [
                executor.submit(_download_url_share, share, missing, opts)
                for share in shares
            ]
            # result() re-raises the error of any connection that gave up
            for future in futures:
                future.result()
    return [url for url, _ in files if url in missing]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(
            "Usage: python helpers/download_file.py <ftp_url> [output_dir]"
        )
    url = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "."
    parsed_url = urlparse(url)
    if parsed_url.scheme != "ftp":
        sys.exit(f"Only ftp:// URLs are supported, got {url}")
    filename = os.path.basename(parsed_url.path)
    if not filename:
        sys.exit(f"No filename in {url}")
    if download_ftp_files([(url, os.path.join(output_dir, filename))]):
        sys.exit(f"{url} isn't on the server")
