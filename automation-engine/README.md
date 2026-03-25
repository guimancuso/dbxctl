# dbxctl Automation Engine

Idempotent CLI for managing users, groups, service principal memberships, and workspace assignments in Databricks Account via SCIM API.

## Features

- **Declarative identity management** -- Apply versioned YAML definitions
- **Idempotent sync** -- Run multiple times safely with the same result
- **Dry-run mode** -- Preview all changes before applying them
- **Cross-validation** -- Catch configuration errors before any API call
- **Protection mechanism** -- Safeguard system entities and critical accounts from accidental deletion
- **Granular control** -- Sync users, groups, memberships, or workspaces independently
- **Mixed memberships** -- Reconcile group members as users, groups, and service principals

## Repository Layout

```text
..
├── automation-engine/
│   ├── dbx_iam/
│   ├── dbxctl.py
│   ├── docs/
│   └── pyproject.toml
├── identity-definitions.example/
│   ├── account/
│   ├── principals/
│   ├── memberships/
│   └── workspace-access/
└── identity-definitions/
    ├── account/
    │   └── settings.yaml
    ├── principals/
    │   ├── users.yaml
    │   └── groups.yaml
    ├── memberships/
    │   ├── business/
    │   ├── roles/
    │   ├── service-principals/
    │   └── workspace/
    └── workspace-access/
```

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Databricks Account with admin access
- Databricks CLI profiles configured in `~/.databrickscfg`

### Installation

```bash
cp -R ../identity-definitions.example ../identity-definitions
cd automation-engine
uv sync
```

### Usage

```bash
# Validate identity definitions from ../identity-definitions
uv run dbxctl validate

# Preview changes
uv run dbxctl sync --dry-run

# Apply changes
uv run dbxctl sync
```

### Alternate Definitions Directory

```bash
uv run dbxctl validate --config-dir /path/to/identity-definitions
DBXCTL_CONFIG_DIR=/path/to/identity-definitions uv run dbxctl sync --dry-run
```

### Example Definitions

Safe example YAML files are available in `../identity-definitions.example/`.

## Documentation

- [User Manual (English)](docs/USER_MANUAL.md)
- [Manual do Usuario (Portugues)](docs/MANUAL_USUARIO.md)

## License

[MIT](LICENSE)
