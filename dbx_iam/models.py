from dataclasses import dataclass, field


@dataclass
class UserConfig:
    email: str
    display_name: str


@dataclass
class GroupConfig:
    name: str
    description: str = ""


@dataclass
class MembershipConfig:
    group: str
    members: list[str] = field(default_factory=list)


@dataclass
class WorkspaceGroupEntry:
    group: str
    permission: str  # "USER" or "ADMIN"


@dataclass
class WorkspaceAssignmentConfig:
    workspace: str
    groups: list[WorkspaceGroupEntry] = field(default_factory=list)


@dataclass
class WorkspaceSettings:
    name: str
    host: str
    profile: str


@dataclass
class AccountSettings:
    host: str
    account_id: str
    profile: str


@dataclass
class Settings:
    account: AccountSettings
    workspaces: list[WorkspaceSettings] = field(default_factory=list)
    protected_emails: list[str] = field(default_factory=list)
    protected_groups: list[str] = field(default_factory=list)
