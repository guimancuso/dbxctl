# dbxctl

Idempotent CLI for managing users, groups, service principal memberships, and workspace assignments in Databricks Account via SCIM API.

## Features

- **Declarative configuration** -- Define your desired state in YAML files
- **Idempotent sync** -- Run multiple times safely with the same result
- **Dry-run mode** -- Preview all changes before applying them
- **Cross-validation** -- Catch configuration errors before any API call
- **Protection mechanism** -- Safeguard system entities and critical accounts from accidental deletion
- **Granular control** -- Sync users, groups, memberships, or workspaces independently
- **Mixed memberships** -- Reconcile group members as users, groups, and service principals

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Databricks Account with admin access
- Databricks CLI profiles configured in `~/.databrickscfg`

### Installation

```bash
git clone https://github.com/guimancuso/dbxctl.git
# or via SSH
git clone git@github.com:guimancuso/dbxctl.git
cd dbxctl
uv sync
```

### Usage

```bash
# Validate configuration
dbxctl validate

# Preview changes
dbxctl sync --dry-run

# Apply changes
dbxctl sync
```

### Commands

| Command | Description |
|---------|-------------|
| `dbxctl validate` | Validate YAML configuration (no API calls) |
| `dbxctl users [--dry-run]` | Synchronize users |
| `dbxctl groups [--dry-run]` | Synchronize groups |
| `dbxctl members [--dry-run]` | Synchronize group memberships |
| `dbxctl workspaces [--dry-run]` | Synchronize workspace assignments |
| `dbxctl sync [--dry-run]` | Run all sync steps in order |

## Configuration

The `config/` directory is gitignored (it contains real credentials and user data). To get started, copy the example configuration:

```bash
cp -r .config.example config
```

Then edit the files in `config/` with your actual values:

```
config/
├── settings.yaml          # Account credentials, workspaces, protection lists
├── users.yaml             # Users to manage
├── groups.yaml            # Groups to manage
├── memberships/           # One file per group with member emails
│   └── GRP-EXAMPLE.yaml
└── workspaces/            # One file per workspace with group assignments
    └── production.yaml
```

## Documentation

- [User Manual (English)](docs/USER_MANUAL.md)
- [Manual do Usuario (Portugues)](docs/MANUAL_USUARIO.md)

## License

[MIT](LICENSE)
