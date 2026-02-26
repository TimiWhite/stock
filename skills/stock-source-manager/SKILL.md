---
name: stock-source-manager
description: Manage AI stock-picking source registries with safe add/update/delete workflows on JSON files. Use when Codex needs to add, modify, remove, validate, or preview stock data sources for an AI stock web app, especially when enforcing a fixed schema, unique IDs, URL/type validation, path auto-discovery, and preview-before-apply changes.
---

# Stock Source Manager

## Overview

Enable deterministic management of stock source registries with a strict JSON schema and a default "preview before apply" policy.  
Use bundled scripts to discover registry paths, validate data, generate before/after diffs, and apply atomic writes.

## Fixed Schema

Use this schema as the baseline contract:

```json
{
  "version": 1,
  "sources": [
    {
      "id": "unique-kebab-case-id",
      "name": "Source Name",
      "url": "https://example.com/feed",
      "type": "news",
      "enabled": true
    }
  ]
}
```

Source `type` must be one of:
- `news`
- `filing`
- `macro`
- `research`
- `social`
- `custom`

Read detailed constraints in [references/schema.md](references/schema.md).

## Workflow

### Step 1: Locate registry

1. Prefer explicit `--registry <path>` if user provides it.
2. Otherwise auto-discover in this exact order:
- `./config/sources.json`
- `./configs/sources.json`
- `./data/sources.json`
- `./sources.json`
- `./ai-stock/sources.json`
3. If no candidate exists, stop and request an explicit path.

Read lookup details in [references/path_discovery.md](references/path_discovery.md).

### Step 2: Validate current state

Run:

```bash
python scripts/source_registry.py --validate --registry <path>
```

Require:
- valid JSON
- top-level object with `version` and `sources`
- unique source `id`
- `id` kebab-case
- `url` scheme is `http` or `https`
- `enabled` is boolean

### Step 3: Preview change

Always preview before mutation:

```bash
python scripts/source_preview.py --op add --id sec-filings --patch-json '{"name":"SEC Filings","url":"https://www.sec.gov/edgar","type":"filing","enabled":true}' --registry <path>
```

Expected preview output includes:
- selected registry path
- before/after object for target source
- changed fields
- total/enabled counts before and after

### Step 4: Apply change with confirmation

Apply only with explicit confirmation flag:

```bash
python scripts/source_apply.py --op update --id sec-filings --patch-json '{"enabled":false}' --registry <path> --confirm
```

Apply script must:
- re-validate input and operation
- perform atomic write via temporary file + rename
- output final summary and counts

### Step 5: Report completion

Return:
- operation status (`applied`)
- changed source `id`
- final total sources count
- final enabled sources count

## Operation Rules

- `add`: reject if `id` already exists.
- `update`: reject if `id` does not exist.
- `delete`: reject if `id` does not exist.
- Do not allow unsupported `type` values.
- Do not auto-create missing directories.
- Do not perform network calls.
- Do not access secrets or environment credentials.

## Script Map

- `scripts/source_registry.py`: shared validation/path logic + `--validate` CLI
- `scripts/source_preview.py`: non-mutating preview CLI
- `scripts/source_apply.py`: mutating apply CLI (requires `--confirm`)
