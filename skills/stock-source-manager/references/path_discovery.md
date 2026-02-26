# Path Discovery Reference

When `--registry` is omitted, resolve the registry file in this exact order (relative to current working directory):

1. `./config/sources.json`
2. `./configs/sources.json`
3. `./data/sources.json`
4. `./sources.json`
5. `./ai-stock/sources.json`

Rules:

- Return the first existing file.
- If none exists, stop and ask for explicit `--registry`.
- Do not create directories automatically.
- Do not create registry files implicitly.
