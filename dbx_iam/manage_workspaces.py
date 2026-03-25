import logging

from databricks.sdk import AccountClient
from databricks.sdk.service import iam
from rich.console import Console

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
    show_unchanged: bool = False,
) -> dict:
    stats = {"added": 0, "removed": 0, "skipped": 0, "errors": 0}
    changed_items: list[str] = []

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
        workspace_skipped = 0
        workspace_added = 0
        workspace_removed = 0
        workspace_errors = 0

        if ws_lower not in ws_id_map:
            console.print(f"  [red]ERROR[/red] workspace {assignment.workspace}: not found in Account API")
            stats["errors"] += 1
            workspace_errors += 1
            continue

        workspace_id = ws_id_map[ws_lower]

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
            console.print(f"  [red]ERROR[/red] workspace {assignment.workspace}: fetching assignments failed: {e}")
            stats["errors"] += 1
            workspace_errors += 1
            continue

        # Build desired state: principal_id -> permission
        desired: dict[int, iam.WorkspacePermission] = {}
        for entry in assignment.groups:
            group_lower = entry.group.lower()
            if group_lower not in groups_by_name:
                console.print(f"  [red]ERROR[/red] workspace {assignment.workspace}: group '{entry.group}' does not exist in Databricks")
                stats["errors"] += 1
                workspace_errors += 1
                continue

            group_id_str, group_display = groups_by_name[group_lower]
            principal_id = int(group_id_str)
            desired_perm = iam.WorkspacePermission.ADMIN if entry.permission == "ADMIN" else iam.WorkspacePermission.USER
            desired[principal_id] = desired_perm

            # Check if already assigned with the same permission
            if principal_id in current_assignments:
                current_perms = current_assignments[principal_id]
                if desired_perm in current_perms:
                    if show_unchanged:
                        console.print(f"    [yellow]SKIP[/yellow]    {group_display} ({entry.permission}, already assigned)")
                    stats["skipped"] += 1
                    workspace_skipped += 1
                    continue

            # Add or update
            if dry_run:
                stats["added"] += 1
                workspace_added += 1
                changed_items.append(
                    f"+ workspace {assignment.workspace}: assign {group_display} ({entry.permission})"
                )
                continue

            try:
                client.workspace_assignment.update(
                    workspace_id=workspace_id,
                    principal_id=principal_id,
                    permissions=[desired_perm],
                )
                stats["added"] += 1
                workspace_added += 1
                changed_items.append(
                    f"+ workspace {assignment.workspace}: assign {group_display} ({entry.permission})"
                )
            except Exception as e:
                stats["errors"] += 1
                workspace_errors += 1
                console.print(f"  [red]ERROR[/red] workspace {assignment.workspace}: assigning '{group_display}' failed: {e}")
                logger.error("Error assigning %s to workspace %s: %s", group_display, assignment.workspace, e)

        # Remove groups that are in the workspace but not in the YAML
        # Only remove managed groups (those that exist in groups_by_name)
        managed_group_ids = {int(gid) for gid, _ in groups_by_name.values()}
        for pid, group_name in current_group_names.items():
            if pid in desired or pid not in managed_group_ids:
                continue

            if dry_run:
                stats["removed"] += 1
                workspace_removed += 1
                changed_items.append(f"- workspace {assignment.workspace}: remove {group_name}")
                continue

            try:
                client.workspace_assignment.delete(
                    workspace_id=workspace_id,
                    principal_id=pid,
                )
                stats["removed"] += 1
                workspace_removed += 1
                changed_items.append(f"- workspace {assignment.workspace}: remove {group_name}")
            except Exception as e:
                stats["errors"] += 1
                workspace_errors += 1
                console.print(f"  [red]ERROR[/red] workspace {assignment.workspace}: removing '{group_name}' failed: {e}")
                logger.error("Error removing %s from workspace %s: %s", group_name, assignment.workspace, e)

        if workspace_added or workspace_removed or workspace_errors or show_unchanged:
            parts = []
            if workspace_added:
                parts.append(f"{'to assign' if dry_run else 'assigned'}={workspace_added}")
            if workspace_removed:
                parts.append(f"{'to remove' if dry_run else 'removed'}={workspace_removed}")
            if workspace_errors:
                parts.append(f"errors={workspace_errors}")
            if workspace_skipped and show_unchanged:
                parts.append(f"unchanged={workspace_skipped}")
            if not parts:
                parts.append("no changes")
            prefix = "PLAN" if dry_run and (workspace_added or workspace_removed) else "WORKSPACE"
            console.print(f"  [bold]{prefix}[/bold] {assignment.workspace}: " + ", ".join(parts))

    _print_summary(stats, dry_run)
    color = "cyan" if dry_run else "green"
    for item in changed_items:
        console.print(f"  [{color}]{item}[/{color}]")
    return stats


def _print_summary(stats: dict, dry_run: bool) -> None:
    if dry_run:
        console.print(
            f"\n[bold]Plan:[/bold] {stats['added']} to assign, {stats['removed']} to remove, "
            f"{stats['errors']} errors."
        )
        return

    console.print(
        f"\n[bold green]Apply complete:[/bold green] {stats['added']} assigned, {stats['removed']} removed, "
        f"{stats['errors']} errors."
    )
