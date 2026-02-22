"""Tests for the cache manager."""

import pytest

from mtgjson_sdk.cache import CacheManager


def test_cache_dir_created(tmp_path):
    cache_dir = tmp_path / "test_cache"
    cache = CacheManager(cache_dir, offline=True)
    assert cache_dir.exists()
    cache.close()


def test_local_version_none(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=True)
    assert cache._local_version() is None
    cache.close()


def test_save_and_read_version(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=True)
    cache._save_version("5.2.2+20250101")
    assert cache._local_version() == "5.2.2+20250101"
    cache.close()


def test_stale_when_no_version(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=True)
    assert cache.is_stale() is True
    cache.close()


def test_not_stale_when_version_saved(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=True)
    cache._save_version("5.2.2")
    # Offline mode can't check remote, so assumes fresh
    assert cache.is_stale() is False
    cache.close()


def test_clear(tmp_path):
    cache_dir = tmp_path / "cache"
    cache = CacheManager(cache_dir, offline=True)
    cache._save_version("test")
    assert (cache_dir / "version.txt").exists()
    cache.clear()
    assert not (cache_dir / "version.txt").exists()
    assert cache_dir.exists()  # Dir recreated
    cache.close()


# === Corrupt file recovery tests ===


def test_load_json_corrupt_removed(tmp_path):
    """Corrupt JSON file is deleted and FileNotFoundError raised."""
    cache = CacheManager(tmp_path / "cache", offline=True)
    corrupt_path = cache.cache_dir / "Meta.json"
    corrupt_path.write_bytes(b"\x00\xff\xfe invalid json bytes")

    with pytest.raises(FileNotFoundError, match="corrupt"):
        cache.load_json("meta")

    # File should have been removed
    assert not corrupt_path.exists()
    cache.close()


def test_load_json_corrupt_gzip_removed(tmp_path):
    """Corrupt .gz file is deleted and FileNotFoundError raised."""
    cache = CacheManager(tmp_path / "cache", offline=True)
    corrupt_path = cache.cache_dir / "AllPricesToday.json.gz"
    corrupt_path.write_bytes(b"this is not gzip data at all")

    with pytest.raises(FileNotFoundError, match="corrupt"):
        cache.load_json("all_prices_today")

    assert not corrupt_path.exists()
    cache.close()


def test_load_json_truncated_removed(tmp_path):
    """Truncated JSON file is deleted and FileNotFoundError raised."""
    cache = CacheManager(tmp_path / "cache", offline=True)
    truncated_path = cache.cache_dir / "Meta.json"
    truncated_path.write_text('{"data": {', encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="corrupt"):
        cache.load_json("meta")

    assert not truncated_path.exists()
    cache.close()
