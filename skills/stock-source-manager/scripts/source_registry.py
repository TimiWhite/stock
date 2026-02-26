#!/usr/bin/env python3
"""Source registry helpers and validator CLI for stock-source-manager."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

SOURCE_TYPES = {"news", "filing", "macro", "research", "social", "custom"}
SOURCE_KEYS = {"id", "name", "url", "type", "enabled"}
ID_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")

PATH_CANDIDATES = (
    "config/sources.json",
    "configs/sources.json",
    "data/sources.json",
    "sources.json",
    "ai-stock/sources.json",
)


class SourceRegistryError(Exception):
    """Domain-specific error for source registry operations."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SourceRegistryError(message)


def discover_registry_path(registry_path: str | None = None, cwd: str | None = None) -> Path:
    """Return explicit registry path or discover from known candidates."""
    if registry_path:
        explicit = Path(registry_path).expanduser().resolve()
        _require(explicit.exists(), f"Registry file not found: {explicit}")
        _require(explicit.is_file(), f"Registry path is not a file: {explicit}")
        return explicit

    base = Path(cwd or os.getcwd()).resolve()
    for candidate in PATH_CANDIDATES:
        path = (base / candidate).resolve()
        if path.exists() and path.is_file():
            return path

    joined = ", ".join(PATH_CANDIDATES)
    raise SourceRegistryError(
        "No registry file found in default candidates. "
        f"Looked for: {joined}. Use --registry <path>."
    )


def _validate_source(source: Any, index: int) -> dict[str, Any]:
    _require(isinstance(source, dict), f"sources[{index}] must be an object.")
    keys = set(source.keys())
    missing = SOURCE_KEYS - keys
    extra = keys - SOURCE_KEYS
    _require(not missing, f"sources[{index}] missing required keys: {sorted(missing)}.")
    _require(not extra, f"sources[{index}] has unsupported keys: {sorted(extra)}.")

    source_id = source.get("id")
    _require(isinstance(source_id, str), f"sources[{index}].id must be a string.")
    _require(bool(ID_PATTERN.fullmatch(source_id)), f"sources[{index}].id must be kebab-case.")

    name = source.get("name")
    _require(isinstance(name, str), f"sources[{index}].name must be a string.")
    _require(name.strip() != "", f"sources[{index}].name must be non-empty.")

    url = source.get("url")
    _require(isinstance(url, str), f"sources[{index}].url must be a string.")
    parsed = urlparse(url)
    _require(parsed.scheme in {"http", "https"}, f"sources[{index}].url must use http/https.")
    _require(bool(parsed.netloc), f"sources[{index}].url must include host.")

    source_type = source.get("type")
    _require(isinstance(source_type, str), f"sources[{index}].type must be a string.")
    _require(
        source_type in SOURCE_TYPES,
        f"sources[{index}].type must be one of {sorted(SOURCE_TYPES)}.",
    )

    enabled = source.get("enabled")
    _require(isinstance(enabled, bool), f"sources[{index}].enabled must be boolean.")

    return {
        "id": source_id,
        "name": name,
        "url": url,
        "type": source_type,
        "enabled": enabled,
    }


def load_registry(path: Path) -> dict[str, Any]:
    """Load and validate registry JSON from file."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SourceRegistryError(f"Invalid JSON in {path}: {exc}") from exc

    _require(isinstance(raw, dict), "Registry root must be a JSON object.")
    _require("version" in raw, "Registry missing 'version'.")
    _require(raw.get("version") == 1, "Registry 'version' must equal 1.")
    _require("sources" in raw, "Registry missing 'sources'.")

    sources = raw.get("sources")
    _require(isinstance(sources, list), "Registry 'sources' must be a list.")

    normalized_sources: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for idx, source in enumerate(sources):
        normalized = _validate_source(source, idx)
        _require(normalized["id"] not in seen_ids, f"Duplicate source id: {normalized['id']}.")
        seen_ids.add(normalized["id"])
        normalized_sources.append(normalized)

    return {"version": 1, "sources": normalized_sources}


def parse_patch_json(patch_json: str | None) -> dict[str, Any] | None:
    """Parse JSON patch string into dict."""
    if patch_json is None:
        return None
    try:
        value = json.loads(patch_json)
    except json.JSONDecodeError as exc:
        raise SourceRegistryError(f"Invalid patch JSON: {exc}") from exc

    _require(isinstance(value, dict), "Patch JSON must be an object.")
    return value


def _count_enabled(sources: list[dict[str, Any]]) -> int:
    return sum(1 for item in sources if item["enabled"])


def build_stats(registry: dict[str, Any]) -> dict[str, int]:
    sources = registry["sources"]
    return {"total": len(sources), "enabled": _count_enabled(sources)}


def _build_diff(before: dict[str, Any] | None, after: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    fields = sorted(SOURCE_KEYS)
    diff: dict[str, dict[str, Any]] = {}
    for field in fields:
        before_value = None if before is None else before.get(field)
        after_value = None if after is None else after.get(field)
        if before_value != after_value:
            diff[field] = {"before": before_value, "after": after_value}
    return diff


def apply_operation(
    registry: dict[str, Any],
    op: str,
    source_id: str,
    patch: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return transformed registry and preview payload for add/update/delete."""
    _require(bool(ID_PATTERN.fullmatch(source_id)), "--id must be kebab-case.")
    sources: list[dict[str, Any]] = deepcopy(registry["sources"])
    index_map = {item["id"]: idx for idx, item in enumerate(sources)}
    idx = index_map.get(source_id)

    if op == "add":
        _require(idx is None, f"Source id already exists: {source_id}.")
        _require(patch is not None, "add operation requires --patch-json.")
        _require("id" not in patch or patch["id"] == source_id, "patch id must match --id.")
        candidate = {**patch, "id": source_id}
        created = _validate_source(candidate, 0)
        sources.append(created)
        before, after = None, created
    elif op == "update":
        _require(idx is not None, f"Source id not found: {source_id}.")
        _require(patch is not None, "update operation requires --patch-json.")
        _require("id" not in patch or patch["id"] == source_id, "patch id must match --id.")
        candidate = {**sources[idx], **patch, "id": source_id}
        updated = _validate_source(candidate, idx)
        before, after = sources[idx], updated
        sources[idx] = updated
    elif op == "delete":
        _require(idx is not None, f"Source id not found: {source_id}.")
        _require(not patch, "delete operation must not include --patch-json.")
        before = sources[idx]
        del sources[idx]
        after = None
    else:
        raise SourceRegistryError(f"Unsupported op: {op}")

    result = {"version": 1, "sources": sources}
    return {
        "registry": result,
        "before": before,
        "after": after,
        "diff": _build_diff(before, after),
    }


def write_registry_atomic(path: Path, registry: dict[str, Any]) -> None:
    """Write registry using temp file + atomic replace in same directory."""
    parent = path.parent
    _require(parent.exists(), f"Registry directory does not exist: {parent}")
    _require(parent.is_dir(), f"Registry parent is not a directory: {parent}")
    fd, temp_path = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(registry, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
        os.replace(temp_path, path)
    except Exception as exc:  # pragma: no cover - defensive cleanup
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise SourceRegistryError(f"Failed to write registry atomically: {exc}") from exc


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def _build_validate_payload(path: Path, registry: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "valid",
        "registry_path": str(path),
        "version": registry["version"],
        "stats": build_stats(registry),
        "candidate_paths": list(PATH_CANDIDATES),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate stock source registry JSON.")
    parser.add_argument("--validate", action="store_true", help="Validate registry structure and source schema.")
    parser.add_argument("--registry", help="Registry path. If omitted, auto-discover from known candidates.")
    args = parser.parse_args()

    if not args.validate:
        parser.error("Only --validate mode is supported in source_registry.py")

    try:
        path = discover_registry_path(args.registry)
        registry = load_registry(path)
        _print_json(_build_validate_payload(path, registry))
        return 0
    except SourceRegistryError as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
