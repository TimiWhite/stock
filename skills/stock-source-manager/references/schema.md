# Source Schema Reference

## Registry Shape

Registry file must be a JSON object:

```json
{
  "version": 1,
  "sources": []
}
```

## Source Object

Each source must contain exactly these fields:

```json
{
  "id": "sec-filings",
  "name": "SEC Filings",
  "url": "https://www.sec.gov/edgar",
  "type": "filing",
  "enabled": true
}
```

Field constraints:

- `id`: string, unique, kebab-case (`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
- `name`: string, non-empty after trim
- `url`: string, must be `http://` or `https://` and include host
- `type`: enum `news|filing|macro|research|social|custom`
- `enabled`: boolean

## Operation Constraints

- `add`: requires `patch-json` with all non-id fields; rejects duplicate id.
- `update`: requires `patch-json`; target id must exist.
- `delete`: forbids `patch-json`; target id must exist.
- `--confirm` is mandatory for apply script.

## Output Contract

Preview/apply scripts return JSON payload containing:

- `status`
- `op`
- `id`
- `registry_path`
- `before`
- `after`
- `diff`
- `stats_before`
- `stats_after`
