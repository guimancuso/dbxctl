import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from rich.console import Console
from rich.table import Table

from dbx_iam.models import (
    AccountSettings,
    GroupConfig,
    MembershipConfig,
    Settings,
    UserConfig,
    WorkspaceAssignmentConfig,
    WorkspaceGroupEntry,
    WorkspaceSettings,
)

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
GROUP_NAME_REGEX = re.compile(r"^[a-zA-Z0-9._-]+$")
GROUP_MEMBER_PREFIXES = ("GRP-", "ROLE-", "SVC-")

console = Console()


@dataclass
class ValidationIssue:
    level: str  # "ERROR" or "WARNING"
    source: str  # source file
    message: str


@dataclass
class ValidationResult:
    issues: list[ValidationIssue] = field(default_factory=list)

    def error(self, source: str, message: str) -> None:
        self.issues.append(ValidationIssue("ERROR", source, message))

    def warning(self, source: str, message: str) -> None:
        self.issues.append(ValidationIssue("WARNING", source, message))

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.level == "ERROR"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.level == "WARNING"]

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def print_report(self) -> None:
        if not self.issues:
            console.print("[bold green]Validation OK - no issues found.[/bold green]\n")
            return

        table = Table(title="Validation Results")
        table.add_column("Level", style="bold", width=8)
        table.add_column("File", style="dim")
        table.add_column("Message")

        for issue in self.issues:
            style = "red" if issue.level == "ERROR" else "yellow"
            table.add_row(f"[{style}]{issue.level}[/{style}]", issue.source, issue.message)

        console.print(table)
        console.print(f"\n  Errors: {len(self.errors)}  |  Warnings: {len(self.warnings)}\n")


def _check_file_exists(path: Path, result: ValidationResult) -> bool:
    if not path.exists():
        result.error(str(path.name), f"File not found: {path}")
        return False
    return True


def _check_yaml_loadable(path: Path, result: ValidationResult) -> dict | None:
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
        if data is None:
            result.error(str(path.name), "Empty YAML file")
            return None
        if not isinstance(data, dict):
            result.error(str(path.name), "YAML root must be a dictionary")
            return None
        return data
    except yaml.YAMLError as e:
        result.error(str(path.name), f"Invalid YAML: {e}")
        return None


def load_settings(config_dir: Path = CONFIG_DIR) -> Settings:
    path = config_dir / "settings.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Settings file not found: {path}")

    with open(path) as f:
        data = yaml.safe_load(f)

    if not data or "account" not in data:
        raise ValueError("settings.yaml must contain the 'account' key")

    acct = data["account"]
    for key in ("host", "account_id", "profile"):
        if key not in acct or not acct[key]:
            raise ValueError(f"settings.yaml: account.{key} is required")

    if "<" in acct.get("host", "") or "<" in acct.get("account_id", ""):
        raise ValueError("settings.yaml: replace the placeholders (<...>) with actual values")

    account = AccountSettings(
        host=acct["host"],
        account_id=acct["account_id"],
        profile=acct["profile"],
    )

    workspaces = []
    for i, ws in enumerate(data.get("workspaces", [])):
        for key in ("name", "host", "profile"):
            if key not in ws or not ws[key]:
                raise ValueError(f"settings.yaml: workspaces[{i}].{key} is required")
        workspaces.append(
            WorkspaceSettings(name=ws["name"], host=ws["host"], profile=ws["profile"])
        )

    protected_emails = [e.lower() for e in data.get("protected_emails", []) or []]
    protected_groups = [g.lower() for g in data.get("protected_groups", []) or []]

    return Settings(
        account=account,
        workspaces=workspaces,
        protected_emails=protected_emails,
        protected_groups=protected_groups,
    )


def load_users(config_dir: Path = CONFIG_DIR) -> list[UserConfig]:
    path = config_dir / "users.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Users file not found: {path}")

    with open(path) as f:
        data = yaml.safe_load(f)

    if not data or "users" not in data:
        raise ValueError("users.yaml must contain the 'users' key")

    users = []
    for i, u in enumerate(data["users"]):
        if "email" not in u or not u["email"]:
            raise ValueError(f"users.yaml: users[{i}] missing 'email' field")
        if "display_name" not in u or not u["display_name"]:
            raise ValueError(f"users.yaml: users[{i}] ({u.get('email', '?')}) missing 'display_name' field")
        if not EMAIL_REGEX.match(u["email"]):
            raise ValueError(f"users.yaml: invalid email '{u['email']}'")
        users.append(UserConfig(email=u["email"], display_name=u["display_name"]))

    seen: set[str] = set()
    duplicates: set[str] = set()
    for u in users:
        lower = u.email.lower()
        if lower in seen:
            duplicates.add(u.email)
        seen.add(lower)
    if duplicates:
        raise ValueError(f"Duplicate emails in users.yaml: {duplicates}")

    return users


def load_groups(config_dir: Path = CONFIG_DIR) -> list[GroupConfig]:
    path = config_dir / "groups.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Groups file not found: {path}")

    with open(path) as f:
        data = yaml.safe_load(f)

    if not data or "groups" not in data:
        raise ValueError("groups.yaml must contain the 'groups' key")

    groups = []
    for i, g in enumerate(data["groups"]):
        if "name" not in g or not g["name"]:
            raise ValueError(f"groups.yaml: groups[{i}] missing 'name' field")
        if not re.match(r"^[a-zA-Z0-9._-]+$", g["name"]):
            raise ValueError(
                f"groups.yaml: invalid group name '{g['name']}' "
                "(use only letters, numbers, dots, hyphens, and underscores)"
            )
        groups.append(
            GroupConfig(name=g["name"], description=g.get("description", ""))
        )

    seen: set[str] = set()
    duplicates: set[str] = set()
    for g in groups:
        lower = g.name.lower()
        if lower in seen:
            duplicates.add(g.name)
        seen.add(lower)
    if duplicates:
        raise ValueError(f"Duplicate groups in groups.yaml: {duplicates}")

    return groups


def load_memberships(config_dir: Path = CONFIG_DIR) -> list[MembershipConfig]:
    memberships_dir = config_dir / "memberships"
    if not memberships_dir.exists():
        return []

    memberships = []
    for filepath in sorted(memberships_dir.glob("*.yaml")):
        with open(filepath) as f:
            data = yaml.safe_load(f)

        group_name = filepath.stem

        if not data:
            continue

        # Formato novo: {users: [...], groups: [...]}
        # Formato legado: lista plana de e-mails (compatibilidade retroativa)
        if isinstance(data, list):
            users = data
            group_members: list[str] = []
        elif isinstance(data, dict):
            users = data.get("users", []) or []
            group_members = data.get("groups", []) or []
        else:
            raise ValueError(f"{filepath.name}: invalid format (expected list or dict with 'users'/'groups' keys)")

        if not isinstance(users, list):
            raise ValueError(f"{filepath.name}: 'users' must be a list")
        if not isinstance(group_members, list):
            raise ValueError(f"{filepath.name}: 'groups' must be a list")

        for j, email in enumerate(users):
            if not isinstance(email, str) or not email.strip():
                raise ValueError(f"{filepath.name}: users[{j}] is invalid (must be an email)")
            if not EMAIL_REGEX.match(email):
                raise ValueError(f"{filepath.name}: users[{j}] invalid email '{email}'")

        for j, gname in enumerate(group_members):
            if not isinstance(gname, str) or not gname.strip():
                raise ValueError(f"{filepath.name}: groups[{j}] is invalid (must be a group name)")
            if not GROUP_NAME_REGEX.match(gname):
                raise ValueError(f"{filepath.name}: groups[{j}] invalid group name '{gname}'")
            if not gname.startswith(GROUP_MEMBER_PREFIXES):
                raise ValueError(
                    f"{filepath.name}: groups[{j}] '{gname}' must start with one of: "
                    + ", ".join(GROUP_MEMBER_PREFIXES)
                )

        # Detectar duplicatas
        seen_users: set[str] = set()
        dup_users: set[str] = set()
        for email in users:
            lower = email.lower()
            if lower in seen_users:
                dup_users.add(email)
            seen_users.add(lower)
        if dup_users:
            raise ValueError(f"{filepath.name}: duplicate users: {dup_users}")

        seen_groups: set[str] = set()
        dup_groups: set[str] = set()
        for gname in group_members:
            lower = gname.lower()
            if lower in seen_groups:
                dup_groups.add(gname)
            seen_groups.add(lower)
        if dup_groups:
            raise ValueError(f"{filepath.name}: duplicate groups: {dup_groups}")

        memberships.append(
            MembershipConfig(group=group_name, users=users, groups=group_members)
        )

    return memberships


VALID_PERMISSIONS = {"USER", "ADMIN"}


def _parse_group_entry(entry, filepath: Path, index: int) -> WorkspaceGroupEntry:
    """Convert a YAML item into a WorkspaceGroupEntry. Required format: dict with 'group' and 'permission'."""
    if not isinstance(entry, dict):
        raise ValueError(
            f"{filepath.name}: group[{index}] invalid format - each entry must be a dictionary "
            f"with 'group' and 'permission' (e.g., group: NAME, permission: USER)"
        )

    if "group" not in entry or not entry["group"]:
        raise ValueError(f"{filepath.name}: group[{index}] missing 'group' field")

    if "permission" not in entry or not entry["permission"]:
        raise ValueError(
            f"{filepath.name}: group[{index}] ('{entry.get('group', '?')}') missing 'permission' field "
            f"- set 'permission: USER' or 'permission: ADMIN'"
        )

    permission = str(entry["permission"]).upper()
    if permission not in VALID_PERMISSIONS:
        raise ValueError(
            f"{filepath.name}: group[{index}] ('{entry['group']}') invalid permission '{entry['permission']}' "
            f"- valid values: USER, ADMIN"
        )

    return WorkspaceGroupEntry(group=entry["group"], permission=permission)


def load_workspace_assignments(config_dir: Path = CONFIG_DIR) -> list[WorkspaceAssignmentConfig]:
    workspaces_dir = config_dir / "workspaces"
    if not workspaces_dir.exists():
        return []

    assignments = []
    for filepath in sorted(workspaces_dir.glob("*.yaml")):
        with open(filepath) as f:
            data = yaml.safe_load(f)

        workspace_name = filepath.stem

        if not data:
            continue

        if not isinstance(data, list):
            raise ValueError(f"{filepath.name}: invalid format (expected list of groups)")

        entries = []
        for i, entry in enumerate(data):
            entries.append(_parse_group_entry(entry, filepath, i))

        # Detect duplicate groups in the same file
        seen: set[str] = set()
        duplicates: set[str] = set()
        for e in entries:
            lower = e.group.lower()
            if lower in seen:
                duplicates.add(e.group)
            seen.add(lower)
        if duplicates:
            raise ValueError(f"{filepath.name}: duplicate groups: {duplicates}")

        assignments.append(
            WorkspaceAssignmentConfig(workspace=workspace_name, groups=entries)
        )

    return assignments


def validate_all(config_dir: Path = CONFIG_DIR) -> ValidationResult:
    """Cross-validate all YAML files before calling the API."""
    result = ValidationResult()

    # 1. Check if files exist
    settings_ok = _check_file_exists(config_dir / "settings.yaml", result)
    users_ok = _check_file_exists(config_dir / "users.yaml", result)
    groups_ok = _check_file_exists(config_dir / "groups.yaml", result)

    # 2. Load settings
    known_workspaces: set[str] = set()
    if settings_ok:
        data = _check_yaml_loadable(config_dir / "settings.yaml", result)
        if data:
            acct = data.get("account", {})
            for key in ("host", "account_id", "profile"):
                if not acct.get(key):
                    result.error("settings.yaml", f"account.{key} not defined")
            if "<" in acct.get("host", "") or "<" in acct.get("account_id", ""):
                result.error("settings.yaml", "Placeholders (<...>) not replaced")
            for i, ws in enumerate(data.get("workspaces", [])):
                if "<" in ws.get("host", ""):
                    result.warning("settings.yaml", f"workspaces[{i}].host contains placeholder")
                if ws.get("name"):
                    known_workspaces.add(ws["name"].lower())

    # 3. Load users
    known_emails: dict[str, str] = {}
    if users_ok:
        try:
            users = load_users(config_dir)
            known_emails = {u.email.lower(): u.email for u in users}
        except ValueError as e:
            result.error("users.yaml", str(e))

    # 4. Load groups
    known_groups: dict[str, str] = {}
    if groups_ok:
        try:
            groups = load_groups(config_dir)
            known_groups = {g.name.lower(): g.name for g in groups}
        except ValueError as e:
            result.error("groups.yaml", str(e))

    # 5. Validate memberships with cross-reference
    memberships_dir = config_dir / "memberships"
    if not memberships_dir.exists():
        result.warning("memberships/", "Memberships directory does not exist")
    elif not sorted(memberships_dir.glob("*.yaml")):
        result.warning("memberships/", "No membership files found")

    all_membership_groups: set[str] = set()
    yaml_files = sorted(memberships_dir.glob("*.yaml")) if memberships_dir.exists() else []
    for filepath in yaml_files:
        fname = filepath.name
        group_name = filepath.stem

        try:
            with open(filepath) as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            result.error(fname, f"Invalid YAML: {e}")
            continue

        all_membership_groups.add(group_name.lower())

        # Does the referenced group exist in groups.yaml?
        if known_groups and group_name.lower() not in known_groups:
            result.error(
                fname,
                f"Group '{group_name}' is not defined in groups.yaml",
            )

        # Formato novo: {users: [...], groups: [...]}; legado: lista plana de e-mails
        if isinstance(data, list):
            members = data
            group_members: list = []
        elif isinstance(data, dict):
            members = data.get("users", []) or []
            group_members = data.get("groups", []) or []
        else:
            members = []
            group_members = []

        if not members and not group_members:
            result.warning(fname, f"Group '{group_name}' has no members defined")
            continue

        # Valida membros do tipo usuário (e-mails)
        for email in members:
            if not isinstance(email, str):
                result.error(fname, f"Invalid user member (not a string): {email}")
                continue
            if not EMAIL_REGEX.match(email):
                result.error(fname, f"Invalid email: '{email}'")
                continue
            if known_emails and email.lower() not in known_emails:
                result.error(
                    fname,
                    f"User '{email}' is not defined in users.yaml",
                )

        # Valida membros do tipo grupo
        for gname in group_members:
            if not isinstance(gname, str):
                result.error(fname, f"Invalid group member (not a string): {gname}")
                continue
            if not GROUP_NAME_REGEX.match(gname):
                result.error(fname, f"Invalid group name: '{gname}'")
                continue
            if not gname.startswith(GROUP_MEMBER_PREFIXES):
                result.error(
                    fname,
                    f"Group member '{gname}' must start with one of: "
                    + ", ".join(GROUP_MEMBER_PREFIXES),
                )
                continue
            if known_groups and gname.lower() not in known_groups:
                result.error(
                    fname,
                    f"Group '{gname}' is not defined in groups.yaml",
                )

    # 6. Groups defined in groups.yaml without a membership file
    #    Groups with SVC- prefix are ignored (service principals, no membership)
    for g_lower, g_original in known_groups.items():
        if g_original.startswith("SVC-"):
            continue
        if g_lower not in all_membership_groups:
            result.warning(
                "memberships/",
                f"Group '{g_original}' defined in groups.yaml but has no membership file",
            )

    # 7. Validate workspace assignments with cross-reference
    workspaces_dir = config_dir / "workspaces"
    if not workspaces_dir.exists():
        return result

    ws_yaml_files = sorted(workspaces_dir.glob("*.yaml"))
    if not ws_yaml_files:
        return result

    for filepath in ws_yaml_files:
        fname = filepath.name
        ws_name = filepath.stem

        try:
            with open(filepath) as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            result.error(fname, f"Invalid YAML: {e}")
            continue

        # Does the referenced workspace exist in settings.yaml?
        if known_workspaces and ws_name.lower() not in known_workspaces:
            result.error(
                fname,
                f"Workspace '{ws_name}' is not defined in settings.yaml",
            )

        if not data or not isinstance(data, list):
            result.warning(fname, f"Workspace '{ws_name}' has no groups defined")
            continue

        for i, entry in enumerate(data):
            if not isinstance(entry, dict):
                result.error(
                    fname,
                    f"group[{i}] invalid format - each entry must be a dictionary "
                    f"with 'group' and 'permission' (e.g., group: NAME, permission: USER)",
                )
                continue

            group_name = entry.get("group", "")
            if not group_name:
                result.error(fname, f"group[{i}] missing 'group' field")
                continue

            if "permission" not in entry or not entry["permission"]:
                result.error(
                    fname,
                    f"group[{i}] ('{group_name}') missing 'permission' field "
                    f"- set 'permission: USER' or 'permission: ADMIN'",
                )
                continue

            permission = str(entry["permission"]).upper()
            if permission not in VALID_PERMISSIONS:
                result.error(
                    fname,
                    f"group[{i}] ('{group_name}') invalid permission '{entry['permission']}' "
                    f"- valid values: USER, ADMIN",
                )

            # Does the referenced group exist in groups.yaml?
            if known_groups and group_name.lower() not in known_groups:
                result.error(
                    fname,
                    f"Group '{group_name}' is not defined in groups.yaml",
                )

    # Warning for workspaces in settings.yaml without an assignment file
    assigned_workspaces = {f.stem.lower() for f in ws_yaml_files}
    for ws_name in known_workspaces:
        if ws_name not in assigned_workspaces:
            result.warning(
                "workspaces/",
                f"Workspace '{ws_name}' defined in settings.yaml but has no assignment file",
            )

    return result
