import logging

from databricks.sdk import AccountClient
from rich.console import Console
from rich.table import Table

from dbx_iam.models import GroupConfig, MembershipConfig

logger = logging.getLogger(__name__)
console = Console()

# Databricks system groups that must NEVER be deleted.
SYSTEM_GROUPS = {
    "admins",
    "users",
    "account users",
}


def _is_protected(name: str, protected_groups: list[str]) -> bool:
    name_lower = name.lower()
    if name_lower in SYSTEM_GROUPS:
        return True
    if name_lower in protected_groups:
        return True
    return False


def sync_groups(
    client: AccountClient,
    groups: list[GroupConfig],
    memberships: list[MembershipConfig] | None = None,
    dry_run: bool = False,
    protected_groups: list[str] | None = None,
) -> dict:
    protected = protected_groups or []
    stats = {"created": 0, "skipped": 0, "deleted": 0, "protected": 0, "errors": 0}
    membership_groups = {m.group.lower() for m in (memberships or [])}

    console.print("\n[bold]Synchronizing groups...[/bold]\n")

    existing: dict[str, object] = {}
    existing_original: dict[str, str] = {}
    for g in client.groups.list():
        if g.display_name:
            existing[g.display_name.lower()] = g
            existing_original[g.display_name.lower()] = g.display_name

    desired_groups = {g.name.lower() for g in groups}

    console.print(f"  Existing groups in account: {len(existing)}")
    console.print(f"  Groups defined in YAML:     {len(groups)}\n")

    # --- Create missing groups ---
    console.print("  [bold]Creation:[/bold]")
    has_creation = False
    for group_cfg in groups:
        name_lower = group_cfg.name.lower()

        if name_lower in existing:
            logger.debug("SKIP: group '%s' already exists", group_cfg.name)
            stats["skipped"] += 1
            console.print(f"    [yellow]SKIP[/yellow]    {group_cfg.name}")
            continue

        has_creation = True
        if dry_run:
            console.print(f"    [cyan]DRY-RUN[/cyan] Would create: {group_cfg.name}")
            stats["created"] += 1
            continue

        try:
            client.groups.create(display_name=group_cfg.name)
            stats["created"] += 1
            console.print(f"    [green]CREATED[/green] {group_cfg.name}")
        except Exception as e:
            stats["errors"] += 1
            console.print(f"    [red]ERROR[/red]   {group_cfg.name}: {e}")
            logger.error("Error creating group %s: %s", group_cfg.name, e)

    if not has_creation and stats["skipped"] == len(groups):
        console.print("    All groups already exist.")

    # --- Delete groups not in the YAML ---
    orphans = {name: g for name, g in existing.items() if name not in desired_groups}

    if orphans:
        console.print(f"\n  [bold]Removal ({len(orphans)} group(s) not in YAML):[/bold]")
        for name_lower, group_obj in sorted(orphans.items()):
            original_name = existing_original.get(name_lower, name_lower)
            group_id = getattr(group_obj, "id", None)

            if _is_protected(name_lower, protected):
                stats["protected"] += 1
                console.print(f"    [magenta]PROTECTED[/magenta] {original_name} (system group)")
                continue

            # Warn if the group is still referenced in a membership YAML file
            if name_lower in membership_groups:
                console.print(
                    f"    [bold yellow]WARNING[/bold yellow]   Group '{original_name}' has a membership "
                    f"file but is not in groups.yaml -- will not be processed"
                )

            if dry_run:
                console.print(f"    [cyan]DRY-RUN[/cyan] Would delete: {original_name}")
                stats["deleted"] += 1
                continue

            try:
                client.groups.delete(id=group_id)
                stats["deleted"] += 1
                console.print(f"    [red]DELETED[/red] {original_name}")
            except Exception as e:
                stats["errors"] += 1
                console.print(f"    [red]ERROR[/red]   Deleting {original_name}: {e}")
                logger.error("Error deleting group %s: %s", original_name, e)
    else:
        console.print("\n  [bold]Removal:[/bold] No orphan groups found.")

    _print_summary(stats, dry_run)
    return stats


def _print_summary(stats: dict, dry_run: bool) -> None:
    table = Table(title="\nSummary - Groups" + (" (DRY-RUN)" if dry_run else ""))
    table.add_column("Status", style="bold")
    table.add_column("Count", justify="right")

    table.add_row("Created" if not dry_run else "To create", str(stats["created"]), style="green")
    table.add_row("Already exist", str(stats["skipped"]), style="yellow")
    table.add_row("Deleted" if not dry_run else "To delete", str(stats["deleted"]), style="red")
    table.add_row("Protected (not deleted)", str(stats["protected"]), style="magenta")
    table.add_row("Errors", str(stats["errors"]), style="red")

    console.print(table)
