from databricks.sdk import AccountClient, WorkspaceClient

from dbx_iam.models import Settings


def get_account_client(settings: Settings) -> AccountClient:
    return AccountClient(
        host=settings.account.host,
        account_id=settings.account.account_id,
        profile=settings.account.profile,
    )


def get_workspace_client(settings: Settings, workspace_name: str) -> WorkspaceClient:
    for ws in settings.workspaces:
        if ws.name == workspace_name:
            return WorkspaceClient(
                host=ws.host,
                profile=ws.profile,
            )
    raise ValueError(f"Workspace '{workspace_name}' not found in settings.yaml")
