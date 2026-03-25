# databricks-identity-access-management

Repository layout for Databricks IAM automation and versioned identity definitions.

## Structure

```text
.
├── automation-engine/     # CLI, Python package, docs, dependency metadata
├── identity-definitions.example/  # Safe example definitions for GitHub
└── identity-definitions/          # Real company definitions, ignored locally
```

## Working Model

- `automation-engine/` contains the executable codebase.
- `identity-definitions.example/` contains fake definitions safe to publish.
- `identity-definitions/` contains the real desired state applied to Databricks and is gitignored.
- By default, the engine reads definitions from the sibling `identity-definitions/` directory.

## Quick Start

```bash
cp -R identity-definitions.example identity-definitions
cd automation-engine
uv sync
uv run dbxctl validate
uv run dbxctl sync --dry-run
```

## Documentation

- Engine guide: [automation-engine/README.md](automation-engine/README.md)
- English manual: [automation-engine/docs/USER_MANUAL.md](automation-engine/docs/USER_MANUAL.md)
- Portuguese manual: [automation-engine/docs/MANUAL_USUARIO.md](automation-engine/docs/MANUAL_USUARIO.md)
