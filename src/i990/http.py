"""Tiny HTTP helpers built on urllib. Resumable downloads, retries."""
from __future__ import annotations

import logging
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterator

from .config import USER_AGENT

log = logging.getLogger(__name__)


def _opener() -> urllib.request.OpenerDirector:
    opener = urllib.request.build_opener()
    opener.addheaders = [("User-Agent", USER_AGENT)]
    return opener


def head(url: str, timeout: int = 30) -> dict[str, str]:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return {k.lower(): v for k, v in r.headers.items()}


def get_bytes(url: str, timeout: int = 60, retries: int = 3) -> bytes:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            wait = 2 ** attempt
            log.warning("GET %s failed (%s), retrying in %ds", url, e, wait)
            time.sleep(wait)
    assert last_err is not None
    raise last_err


def _is_fatal_http(err: Exception) -> bool:
    """4xx responses (except 408/429) are not worth retrying."""
    if isinstance(err, urllib.error.HTTPError):
        code = err.code
        if 400 <= code < 500 and code not in (408, 429):
            return True
    return False


def download_resumable(
    url: str,
    dest: Path,
    timeout: int = 120,
    retries: int = 5,
    chunk_size: int = 1 << 20,
) -> Path:
    """Download url -> dest. Resumes if dest.part exists. Idempotent if dest exists."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")

    # Already done?
    if dest.exists():
        return dest

    # Determine total size via HEAD, for progress + validation.
    total: int | None = None
    try:
        h = head(url, timeout=timeout)
        if "content-length" in h:
            total = int(h["content-length"])
    except urllib.error.HTTPError as e:
        if _is_fatal_http(e):
            raise
        log.warning("HEAD %s failed: %s (proceeding without content-length)", url, e)
    except Exception as e:
        log.warning("HEAD %s failed: %s (proceeding without content-length)", url, e)

    last_err: Exception | None = None
    for attempt in range(retries):
        existing = part.stat().st_size if part.exists() else 0
        if total is not None and existing >= total:
            part.rename(dest)
            return dest

        req_headers = {"User-Agent": USER_AGENT}
        if existing > 0:
            req_headers["Range"] = f"bytes={existing}-"

        try:
            req = urllib.request.Request(url, headers=req_headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                mode = "ab"
                # Some servers ignore Range and reply with the full body.
                # If we appended that response to an existing .part file,
                # the archive would be corrupted.
                if existing > 0 and "content-range" not in {k.lower() for k in r.headers}:
                    mode = "wb"
                    existing = 0
                    log.warning("server ignored Range for %s; restarting download", url)
                with open(part, mode) as out:
                    while True:
                        chunk = r.read(chunk_size)
                        if not chunk:
                            break
                        out.write(chunk)
            if total is None or part.stat().st_size >= total:
                part.rename(dest)
                return dest
            # partial: loop and resume
        except urllib.error.HTTPError as e:
            if _is_fatal_http(e):
                raise
            last_err = e
            wait = min(60, 2 ** attempt)
            log.warning(
                "download %s attempt %d failed (%s), retrying in %ds",
                url, attempt + 1, e, wait,
            )
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_err = e
            wait = min(60, 2 ** attempt)
            log.warning(
                "download %s attempt %d failed (%s), retrying in %ds",
                url, attempt + 1, e, wait,
            )
            time.sleep(wait)

    if last_err:
        raise last_err
    raise RuntimeError(f"download of {url} did not complete")
