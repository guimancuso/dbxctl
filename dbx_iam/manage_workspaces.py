import logging

from databricks.sdk import AccountClient
from databricks.sdk.service import iam
from rich.console import Console
from rich.table import Table

from dbx_iam.models import Settings, WorkspaceAssignmentConfig

logger = logging.getLogger(__name__)
console = Console()


def _resolve_workspace_ids(client: AccountClient, settings: Settings) -> dict[str, int]:
    """Map workspace name (settings.yaml) -> workspace_id (numeric) via Account API."""
    # Build map of deployment_name -> workspace_id from the API
    api_workspaces = {}
    for ws in client.workspaces.list():
        if ws.deployment_name and ws.workspace_id:
            api_workspaces[ws.deployment_name.lower()] = ws.workspace_id

    # For each workspace in settings, extract deployment_name from the host URL
    result: dict[str, int] = {}
    for ws in settings.workspaces:
        # host e.g.: "https://dbc-a30c0e04-0436.cloud.databricks.com"
        host = ws.host.rstrip("/")
        # Extract deployment_name: part between "https://" and ".cloud.databricks.com"
        deployment = host.replace("https://", "").split(".")[0].lower()

        if deployment in api_workspaces:
            result[ws.name.lower()] = api_workspaces[deployment]
        else:
            logger.warning(
                "Workspace '%s' (host=%s) not found in Account API", ws.name, ws.host
            )

    return result


def sync_workspace_assignments(
    client: AccountClient,
    assignments: list[WorkspaceAssignmentConfig],
    settings: Settings,
    dry_run: bool = False,
) -> dict:
    stats = {"added": 0, "removed": 0, "skipped": 0, "errors": 0}

    console.print("\n[bold]Synchronizing workspace assignments...[/bold]\n")

    # Resolve workspace_id for each workspace
    ws_id_map = _resolve_workspace_ids(client, settings)

    # Index of existing groups: display_name(lower) -> (id_str, display_name_original)
    groups_by_name: dict[str, tuple[str, str]] = {}
    for g in client.groups.list():
        if g.display_name and g.id:
            groups_by_name[g.display_name.lower()] = (g.id, g.display_name)

    for assignment in assignments:
        ws_lower = assignment.workspace.lower()
        console.print(f"  [bold]Workspace: {assignment.workspace}[/bold]")

        if ws_lower not in ws_id_map:
            console.print(f"    [red]ERROR[/red] Workspace '{assignment.workspace}' not found in Account API")
            stats["errors"] += 1
            continue

        workspace_id = ws_id_map[ws_lower]
        console.print(f"    workspace_id: {workspace_id}")

        # Fetch current assignments for the workspace
        current_assignments: dict[int, list[iam.WorkspacePermission]] = {}
        current_group_names: dict[int, str] = {}
        try:
            for pa in client.workspace_assignment.list(workspace_id):
                if pa.principal and pa.principal.group_name and pa.principal.principal_id:
                    pid = pa.principal.principal_id
                    current_assignments[pid] = pa.permissions or []
                    current_group_names[pid] = pa.principal.group_name
        except Exception as e:
            console.print(f"    [red]ERROR[/red] Fetching workspace assignments: {e}")
            stats["errors"] += 1
            continue

        console.print(f"    Current groups in workspace: {len(current_assignments)}")

        # Build desired state: principal_id -> permission
        desired: dict[int, iam.WorkspacePermission] = {}
        for entry in assignment.groups:
            group_lower = entry.group.lower()
            if group_lower not in groups_by_name:
                console.print(f"    [red]ERROR[/red] Group '{entry.group}' does not exist in Databricks")
                stats["errors"] += 1
                continue

            group_id_str, group_display = groups_by_name[group_lower]
            principal_id = int(group_id_str)
            desired_perm = iam.WorkspacePermission.ADMIN if entry.permission == "ADMIN" else iam.WorkspacePermission.USER
            desired[principal_id] = desired_perm

            # Check if already assigned with the same permission
            if principal_id in current_assignments:
                current_perms = current_assignments[principal_id]
                if desired_perm in current_perms:
                    console.print(f"    [yellow]SKIP[/yellow]    {group_display} ({entry.permission}, already assigned)")
                    stats["skipped"] += 1
                    continue

            # Add or update
            if dry_run:
                console.print(f"    [cyan]DRY-RUN[/cyan] Would assign: {group_display} ({entry.permission})")
                stats["added"] += 1
                continue

            try:
                client.workspace_assignment.update(
                    workspace_id=workspace_id,
                    principal_id=principal_id,
                    permissions=[desired_perm],
                )
                stats["added"] += 1
                console.print(f"    [green]ADDED[/green]   {group_display} ({entry.permission})")
            except Exception as e:
                stats["errors"] += 1
                console.print(f"    [red]ERROR[/red]   {group_display}: {e}")
                logger.error("Error assigning %s to workspace %s: %s", group_display, assignment.workspace, e)

        # Remove groups that are in the workspace but not in the YAML
        # Only remove managed groups (those that exist in groups_by_name)
        managed_group_ids = {int(gid) for gid, _ in groups_by_name.values()}
        for pid, group_name in current_group_names.items():
            if pid in desired or pid not in managed_group_ids:
                continue

            if dry_run:
                console.print(f"    [cyan]DRY-RUN[/cyan] Would remove: {group_name}")
                stats["removed"] += 1
                continue

            try:
                client.workspace_assignment.delete(
                    workspace_id=workspace_id,
                    principal_id=pid,
                )
                stats["removed"] += 1
                console.print(f"    [red]REMOVED[/red] {group_name}")
            except Exception as e:
                stats["errors"] += 1
                console.print(f"    [red]ERROR[/red]   Removing {group_name}: {e}")
                logger.error("Error removing %s from workspace %s: %s", group_name, assignment.workspace, e)

        console.print()

    _print_summary(stats, dry_run)
    return stats


def _print_summary(stats: dict, dry_run: bool) -> None:
    table = Table(title="\nSummary - Workspace Assignments" + (" (DRY-RUN)" if dry_run else ""))
    table.add_column("Status", style="bold")
    table.add_column("Count", justify="right")

    table.add_row("Assigned" if not dry_run else "To assign", str(stats["added"]), style="green")
    table.add_row("Removed" if not dry_run else "To remove", str(stats["removed"]), style="red")
    table.add_row("Already assigned", str(stats["skipped"]), style="yellow")
    table.add_row("Errors", str(stats["errors"]), style="red")

    console.print(table)
