# dbxctl - User Manual

**dbxctl** is a command-line tool for idempotent management of users, groups, memberships, and workspace assignments in Databricks Account.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
   - [settings.yaml](#settingsyaml)
   - [users.yaml](#usersyaml)
   - [groups.yaml](#groupsyaml)
   - [Membership Files](#membership-files)
   - [Workspace Assignment Files](#workspace-assignment-files)
5. [Commands](#commands)
   - [validate](#validate)
   - [users](#users)
   - [groups](#groups)
   - [members](#members)
   - [workspaces](#workspaces)
   - [sync](#sync)
6. [Dry-Run Mode](#dry-run-mode)
7. [Protection Mechanism](#protection-mechanism)
8. [Examples](#examples)

---

## Overview

dbxctl reads YAML configuration files and synchronizes the desired state with your Databricks Account via the SCIM API (Databricks SDK). It is **idempotent** — running it multiple times produces the same result without side effects.

The sync process follows this order:
1. Create groups
2. Create users
3. Reconcile memberships (add/remove members)
4. Assign groups to workspaces

Resources not present in the YAML files are removed (unless protected).

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- A Databricks Account with admin access
- Databricks CLI profiles configured in `~/.databrickscfg`

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd <repository-name>/automation-engine

# Install dependencies and the project
uv sync
```

After installation, the `dbxctl` command will be available in your PATH.

## Configuration

By default, all identity definition files are read from the sibling `../identity-definitions/` directory. In the public GitHub version, use `../identity-definitions.example/` as the template and copy it to `../identity-definitions/` before running the tool. If you prefer to keep definitions in a separate path or repository, use `--config-dir` or the `DBXCTL_CONFIG_DIR` environment variable.

### account/settings.yaml

Main configuration file with account credentials, workspace definitions, and protection lists.

```yaml
account:
  host: "https://accounts.cloud.databricks.com"
  account_id: "your-account-id"
  profile: "your-databricks-cli-profile"

workspaces:
  - name: "production"
    host: "https://dbc-xxxxx.cloud.databricks.com"
    profile: "ws-prd-profile"

  - name: "development"
    host: "https://dbc-yyyyy.cloud.databricks.com"
    profile: "ws-dev-profile"

# Emails and groups protected from deletion
protected_emails:
  - "admin@company.com"

protected_groups:
  - "GRP-ALL-WORKSPACE-ADMIN"
```

| Field | Required | Description |
|-------|----------|-------------|
| `account.host` | Yes | Databricks accounts console URL |
| `account.account_id` | Yes | Your Databricks account ID |
| `account.profile` | Yes | Databricks CLI profile name for account-level operations |
| `workspaces[].name` | Yes | Logical name for the workspace (must match workspace YAML filenames) |
| `workspaces[].host` | Yes | Workspace URL |
| `workspaces[].profile` | Yes | Databricks CLI profile for workspace-level operations |
| `protected_emails` | No | List of emails that will never be deleted |
| `protected_groups` | No | List of group names that will never be deleted |

Example using a different configuration directory:

```bash
cp -R ../identity-definitions.example ../identity-definitions
dbxctl validate --config-dir /path/to/identity-definitions
DBXCTL_CONFIG_DIR=/path/to/identity-definitions dbxctl sync --dry-run
```

### principals/users.yaml

Defines all users to be managed in the Databricks Account.

```yaml
users:
  - email: john.doe@company.com
    display_name: John Doe
  - email: jane.smith@company.com
    display_name: Jane Smith
```

| Field | Required | Description |
|-------|----------|-------------|
| `email` | Yes | User email (must be a valid email format) |
| `display_name` | Yes | User display name in Databricks |

### principals/groups.yaml

Defines all groups to be managed.

```yaml
groups:
  - name: GRP-DATA-ENGINEERS
    description: "Data Engineering team"
  - name: GRP-DATA-ANALYSTS
    description: "Data Analytics team"
  - name: SVC-AUTOMATION
    description: "Service principals for automation"
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Group name (letters, numbers, dots, hyphens, underscores only) |
| `description` | No | Group description |

### Membership Files

Located under `identity-definitions/memberships/`. Each file is named after a group (e.g., `GRP-DATA-ENGINEERS.yaml`) and defines its desired members.

Recommended subfolders:
- `memberships/business/` for business and functional groups
- `memberships/roles/` for `ROLE-*` groups
- `memberships/workspace/` for workspace administration groups
- `memberships/service-principals/` for future service principal-oriented group definitions

```yaml
# identity-definitions/memberships/business/GRP-DATA-ENGINEERS.yaml
users:
  - john.doe@company.com
  - jane.smith@company.com

groups:
  - GRP-DATA-ANALYSTS

service_principals:
  - 11111111-2222-3333-4444-555555555555
```

The group name is derived from the filename (without `.yaml` extension).
The legacy format with a flat list of user emails is still supported for backward compatibility.

| Field | Required | Description |
|-------|----------|-------------|
| `users` | No | List of user emails. Each email must exist in `users.yaml`. |
| `groups` | No | List of group names to include as nested members. Each group must exist in `groups.yaml`. |
| `service_principals` | No | List of Databricks service principal `application_id` values (UUID format). |

### Workspace Assignment Files

Located in `identity-definitions/workspace-access/`. Each file is named after a workspace (e.g., `production.yaml`) and defines which groups have access and with what permission level.

```yaml
# identity-definitions/workspace-access/production.yaml
- group: GRP-DATA-ENGINEERS
  permission: USER

- group: GRP-ALL-WORKSPACE-ADMIN
  permission: ADMIN
```

| Field | Required | Description |
|-------|----------|-------------|
| `group` | Yes | Group name (must exist in groups.yaml) |
| `permission` | Yes | `USER` or `ADMIN` |

## Commands

### validate

Validates all YAML configuration files without making any API calls. Performs cross-reference checks.

```bash
dbxctl validate
```

Checks performed:
- File existence and YAML syntax
- Required fields present
- Email format validation
- Duplicate detection (emails, groups, members)
- Cross-reference: membership emails exist in users.yaml
- Cross-reference: membership groups exist in groups.yaml
- Validation: membership service principal application IDs use UUID format
- Cross-reference: workspace groups exist in groups.yaml
- Cross-reference: workspace names match settings.yaml

### users

Synchronizes users with the Databricks Account.

```bash
dbxctl users [--dry-run]
```

- Creates users present in YAML but not in Databricks
- Deletes users present in Databricks but not in YAML (unless protected)

### groups

Synchronizes groups with the Databricks Account.

```bash
dbxctl groups [--dry-run]
```

- Creates groups present in YAML but not in Databricks
- Deletes groups present in Databricks but not in YAML (unless protected or system groups)

### members

Synchronizes group memberships.

```bash
dbxctl members [--dry-run]
```

- Adds users to groups as defined in membership files
- Removes users from groups if not in the membership file

### workspaces

Synchronizes group-to-workspace assignments.

```bash
dbxctl workspaces [--dry-run]
```

- Assigns groups to workspaces with the specified permission level
- Removes group assignments not present in the YAML

### sync

Runs all synchronization steps in the correct order.

```bash
dbxctl sync [--dry-run]
```

Equivalent to running: `groups` -> `users` -> `members` -> `workspaces`

## Dry-Run Mode

All commands that modify data support the `--dry-run` flag. When enabled, dbxctl will show what actions **would** be taken without actually executing them.

```bash
dbxctl sync --dry-run
```

Output labels in dry-run mode:
- `DRY-RUN Would create: ...`
- `DRY-RUN Would delete: ...`
- `DRY-RUN Would add: ...`
- `DRY-RUN Would remove: ...`

## Protection Mechanism

dbxctl has multiple layers of protection to prevent accidental deletion:

1. **System entities** — Databricks built-in groups (`admins`, `users`, `account users`) and system email patterns (`databricks`, `serviceprincipals`) are always protected.

2. **Explicit protection** — Emails and groups listed in `protected_emails` and `protected_groups` in `settings.yaml` are never deleted.

3. **Validation** — The `validate` command catches configuration errors before any API calls.

4. **Dry-run** — Preview all changes before applying them.

## Examples

### Full workflow

```bash
# 1. Validate configuration
dbxctl validate

# 2. Preview changes
dbxctl sync --dry-run

# 3. Apply changes
dbxctl sync
```

### Sync only users

```bash
dbxctl users --dry-run
dbxctl users
```

### Verbose mode

```bash
dbxctl -v sync --dry-run
```

### Directory structure

```text
repository-root/
├── automation-engine/
│   ├── dbx_iam/
│   ├── dbxctl.py
│   ├── docs/
│   └── pyproject.toml
└── identity-definitions/
    ├── account/
    │   └── settings.yaml
    ├── principals/
    │   ├── users.yaml
    │   └── groups.yaml
    ├── memberships/
    │   ├── business/
    │   │   ├── GRP-DATA-ENGINEERS.yaml
    │   │   └── GRP-DATA-ANALYSTS.yaml
    │   ├── roles/
    │   ├── service-principals/
    │   └── workspace/
    └── workspace-access/
        ├── production.yaml
        └── development.yaml
```
