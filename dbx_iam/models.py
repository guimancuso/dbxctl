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
    users: list[str] = field(default_factory=list)    # e-mails de usuários
    groups: list[str] = field(default_factory=list)   # nomes de grupos (GRP-, ROLE-, SVC-)
    service_principals: list[str] = field(default_factory=list)  # application IDs de service principals


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
