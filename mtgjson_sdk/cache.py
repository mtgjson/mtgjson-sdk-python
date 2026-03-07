"""Version-aware CDN download and local file cache manager."""

from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path
from typing import Any

import httpx

from .config import CDN_BASE, JSON_FILES, META_URL, PARQUET_FILES, default_cache_dir

logger = logging.getLogger("mtgjson_sdk")


class CacheManager:
    """Downloads and caches MTGJSON data files from the CDN.

    Checks Meta.json for version changes and re-downloads when stale.
    Individual files are downloaded lazily on first access.
    """

    def __init__(
        self,
        cache_dir: Path | str | None = None,
        *,
        offline: bool = False,
        timeout: float = 120.0,
        on_progress: Any | None = None,
    ) -> None:
        """Create a cache manager.

        Args:
            cache_dir: Directory for cached data files. Defaults to a
                platform-appropriate cache directory.
            offline: If True, never download from CDN (use cached files only).
            timeout: HTTP request timeout in seconds (default 120).
            on_progress: Optional callback
                ``(filename, bytes_downloaded, total_bytes)``
                called during file downloads.
        """
        self.cache_dir = Path(cache_dir) if cache_dir else default_cache_dir()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.offline = offline
        self.timeout = timeout
        self._client: httpx.Client | None = None
        self._remote_version: str | None = None
        self._on_progress = on_progress

    @property
    def client(self) -> httpx.Client:
        """Lazy HTTP client, created on first use."""
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout, follow_redirects=True)
        return self._client

    def close(self) -> None:
        """Close the HTTP client, if open."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def _local_version(self) -> str | None:
        version_file = self.cache_dir / "version.txt"
        if version_file.exists():
            return version_file.read_text().strip()
        return None

    def _save_version(self, version: str) -> None:
        (self.cache_dir / "version.txt").write_text(version)

    def remote_version(self) -> str | None:
        """Fetch the current MTGJSON version from Meta.json on the CDN.

        Returns:
            Version string (e.g. ``"5.2.2+20240101"``), or None if
            offline or the CDN is unreachable.
        """
        if self._remote_version:
            return self._remote_version
        if self.offline:
            return None
        try:
            resp = self.client.get(META_URL)
            resp.raise_for_status()
            data = resp.json()
            self._remote_version = data.get("data", {}).get("version") or data.get(
                "meta", {}
            ).get("version")
            return self._remote_version
        except (httpx.HTTPError, KeyError, json.JSONDecodeError):
            logger.warning("Failed to fetch MTGJSON version from CDN")
            return None

    def is_stale(self) -> bool:
        """Check if local cache is out of date compared to the CDN.

        Returns:
            True if there is no local cache or the CDN has a newer version.
            False if up to date or if the CDN is unreachable.
        """
        local = self._local_version()
        if local is None:
            return True
        remote = self.remote_version()
        if remote is None:
            return False  # Can't check, assume fresh
        return local != remote

    def _download_file(self, filename: str, dest: Path) -> None:
        """Download a single file from the CDN.

        Downloads to a temp file first and renames on success, so an
        interrupted download never leaves a corrupt partial file behind.
        Calls ``on_progress(filename, bytes_downloaded, total_bytes)``
        after each chunk if a progress callback was provided.
        """
        url = f"{CDN_BASE}/{filename}"
        logger.info("Downloading %s", url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_dest = dest.with_suffix(dest.suffix + ".tmp")
        try:
            with self.client.stream("GET", url) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0)) or None
                downloaded = 0
                with open(tmp_dest, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=65536):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if self._on_progress:
                            self._on_progress(filename, downloaded, total)
            tmp_dest.replace(dest)
        except BaseException:
            # Clean up partial temp file on any error (including KeyboardInterrupt)
            tmp_dest.unlink(missing_ok=True)
            raise

    def ensure_parquet(self, view_name: str) -> Path:
        """Ensure a parquet file is cached locally, downloading if needed.

        Args:
            view_name: Logical view name (e.g. ``"cards"``, ``"sets"``).

        Returns:
            Local filesystem path to the cached parquet file.

        Raises:
            FileNotFoundError: If offline and the file is not cached.
            KeyError: If *view_name* is not a known parquet file.
        """
        filename = PARQUET_FILES[view_name]
        local_path = self.cache_dir / filename
        if not local_path.exists() or self.is_stale():
            if self.offline:
                if local_path.exists():
                    return local_path
                raise FileNotFoundError(
                    f"Parquet file {filename} not cached and offline mode is enabled"
                )
            self._download_file(filename, local_path)
            # Update version after successful download
            version = self.remote_version()
            if version:
                self._save_version(version)
        return local_path

    def ensure_json(self, name: str) -> Path:
        """Ensure a JSON file is cached locally, downloading if needed.

        Args:
            name: Logical file name (e.g. ``"meta"``, ``"all_prices_today"``).

        Returns:
            Local filesystem path to the cached JSON file.

        Raises:
            FileNotFoundError: If offline and the file is not cached.
            KeyError: If *name* is not a known JSON file.
        """
        filename = JSON_FILES[name]
        local_path = self.cache_dir / filename
        if not local_path.exists() or self.is_stale():
            if self.offline:
                if local_path.exists():
                    return local_path
                raise FileNotFoundError(
                    f"JSON file {filename} not cached and offline mode is enabled"
                )
            self._download_file(filename, local_path)
            version = self.remote_version()
            if version:
                self._save_version(version)
        return local_path

    def load_json(self, name: str) -> dict:
        """Load and parse a JSON file (handles .gz transparently).

        If the cached file is corrupt (truncated download, disk error),
        it is deleted automatically so the next call re-downloads a fresh copy.

        Args:
            name: Logical file name (e.g. ``"meta"``, ``"keywords"``).

        Returns:
            Parsed JSON as a dict.

        Raises:
            FileNotFoundError: If the file is corrupt (removed) or not cached.
        """
        path = self.ensure_json(name)
        try:
            if path.suffix == ".gz":
                with gzip.open(path, "rt", encoding="utf-8") as f:
                    return json.load(f)
            else:
                return json.loads(path.read_text(encoding="utf-8"))
        except (
            gzip.BadGzipFile,
            EOFError,
            json.JSONDecodeError,
            OSError,
            UnicodeDecodeError,
        ) as e:
            logger.warning("Corrupt cache file %s: %s — removing", path.name, e)
            path.unlink(missing_ok=True)
            raise FileNotFoundError(
                f"Cache file '{path.name}' was corrupt and has been removed. "
                f"Retry to re-download. Original error: {e}"
            ) from e

    def clear(self) -> None:
        """Remove all cached files and recreate the cache directory."""
        import shutil

        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
