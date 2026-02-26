#!/usr/bin/env python3
"""Preview add/update/delete operations for stock source registries."""

from __future__ import annotations

import argparse
import json

from source_registry import (
    SourceRegistryError,
    apply_operation,
    build_stats,
    discover_registry_path,
    load_registry,
    parse_patch_json,
)


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def main() -> int:
    parser = argparse.ArgumentParser(description="Preview stock source registry mutations.")
    parser.add_argument("--op", required=True, choices=["add", "update", "delete"])
    parser.add_argument("--id", required=True, help="Target source id (kebab-case).")
    parser.add_argument("--patch-json", help="JSON object used for add/update.")
    parser.add_argument("--registry", help="Registry path. If omitted, auto-discover from known candidates.")
    args = parser.parse_args()

    try:
        path = discover_registry_path(args.registry)
        current = load_registry(path)
        before_stats = build_stats(current)
        patch = parse_patch_json(args.patch_json)

        result = apply_operation(current, args.op, args.id, patch)
        after_stats = build_stats(result["registry"])
        payload = {
            "status": "preview",
            "op": args.op,
            "id": args.id,
            "registry_path": str(path),
            "before": result["before"],
            "after": result["after"],
            "diff": result["diff"],
            "stats_before": before_stats,
            "stats_after": after_stats,
        }
        _print_json(payload)
        return 0
    except SourceRegistryError as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
