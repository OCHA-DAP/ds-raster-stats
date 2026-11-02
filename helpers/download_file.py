"""
Download files from an FTP folder, e.g. CHIRPS-GEFS COGs from the CHC server.

The pipeline uses `download_ftp_folder` (through
`src.utils.cog_utils.download_chirps_cogs`) to fetch each issue date's COGs
before processing. The module can also be run on its own to download COGs to
inspect locally.

Each file is streamed to a uniquely named `.part` file and only renamed once
its size has been checked, so a failed download never leaves a truncated file
under the real name. Files that are already downloaded are skipped. Failed
connections are retried with exponential backoff, since the CHC server is
intermittently unreachable and limits concurrent connections.

Usage:
    python helpers/download_file.py <ftp_url> [output_dir]

`ftp_url` can be:
- a single file, which is downloaded on its own
- a filename with a wildcard (`*`, `?` or `[...]`), which downloads every
  matching file in that folder over one connection, like `wget ftp://.../*.tif`
- a folder ending in `/`, which downloads every `*.tif` in it

Only ftp:// URLs are supported. `output_dir` defaults to the current
directory. The command exits with an error if nothing matches.

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


def _ftp_cwd(ftp, path, url):
    """
    Change to the folder at `path`, trying each of its candidate paths.
    Raises FileNotFoundError, naming `url`, if none of them exists.
    """
    for candidate in _ftp_candidate_paths(path):
        try:
            ftp.cwd(candidate)
            return
        except ftplib.error_perm:
            continue
    raise FileNotFoundError(f"{url}: folder not found")


def download_ftp_folder(
    folder_url,
    output_dir=".",
    pattern="*.tif",
    retries=5,
    timeout=120,
    chunk_size=1024**2,
):
    """
    Download every file matching `pattern` in an FTP folder over a single
    connection, like `wget ftp://host/folder/*.tif`.

    Each file is written to a uniquely named `.part` file, checked against
    the size the server reports, then renamed to its real name. Files already
    in `output_dir` are skipped, so a retry after a dropped connection, or a
    later run, picks up where the last one left off.

    Parameters
    ----------
    folder_url : str
        The FTP URL of the folder, e.g. ftp://host/path/to/folder/. Paths
        starting with the CHC FTP root (/pub/org/chc) are also tried without
        it.
    output_dir : str
        The directory to save the files in. Created if it doesn't exist.
    pattern : str
        Shell-style wildcard matched against the filenames in the folder. A
        plain filename only matches itself.
    retries : int
        Number of connection attempts before giving up.
    timeout : float
        Timeout in seconds for connecting and for each socket operation.
    chunk_size : int
        Block size in bytes for the FTP transfer.

    Returns
    -------
    list of str
        Paths of the matching files in `output_dir`, sorted by filename,
        including ones that were already downloaded. Empty if the folder has
        no matching files.

    Raises
    ------
    FileNotFoundError
        If the folder doesn't exist. Not retried.
    ftplib.error_perm
        On a permanent (5xx) FTP error. Not retried.
    ftplib.Error, OSError or EOFError
        The last error, if every attempt fails with a temporary FTP error,
        a dropped connection or a truncated file.
    """
    parsed = urlparse(folder_url)
    folder = parsed.path.rstrip("/") or "/"
    os.makedirs(output_dir, exist_ok=True)
    part_path = None

    for attempt in range(1, retries + 1):
        try:
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
            # Temporary FTP errors (4xx, e.g. 421 too many connections),
            # dropped connections and truncated files are retried, after a
            # backoff that doubles each attempt; permanent (5xx) errors aren't
            if isinstance(e, ftplib.error_perm) or attempt == retries:
                raise
            wait = min(2**attempt + random.uniform(0, 1), MAX_WAIT_SECONDS)
            logger.info(f"Retrying in {wait:.0f}s...")
            time.sleep(wait)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(
            "Usage: python helpers/download_file.py <ftp_url> [output_dir]"
        )
    url = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "."
    if urlparse(url).scheme != "ftp":
        sys.exit(f"Only ftp:// URLs are supported, got {url}")
    folder, _, filename = url.rpartition("/")
    # A plain filename is a pattern that only matches itself, and a URL
    # ending in "/" has no filename, so it downloads every .tif in the folder
    paths = download_ftp_folder(
        f"{folder}/", output_dir, pattern=filename or "*.tif"
    )
    if not paths:
        sys.exit(f"Nothing matching {filename or '*.tif'} in {folder}/")
