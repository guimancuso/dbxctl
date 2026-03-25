import logging

from databricks.sdk import AccountClient
from rich.console import Console

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
    show_unchanged: bool = False,
) -> dict:
    protected = protected_groups or []
    stats = {"created": 0, "skipped": 0, "deleted": 0, "protected": 0, "errors": 0}
    membership_groups = {m.group.lower() for m in (memberships or [])}
    changed_creates: list[str] = []
    changed_deletes: list[str] = []

    console.print("\n[bold]Synchronizing groups...[/bold]\n")

    existing: dict[str, object] = {}
    existing_original: dict[str, str] = {}
    for g in client.groups.list():
        if g.display_name:
            existing[g.display_name.lower()] = g
            existing_original[g.display_name.lower()] = g.display_name

    desired_groups = {g.name.lower() for g in groups}

    for group_cfg in groups:
        name_lower = group_cfg.name.lower()

        if name_lower in existing:
            logger.debug("SKIP: group '%s' already exists", group_cfg.name)
            stats["skipped"] += 1
            continue

        if dry_run:
            stats["created"] += 1
            changed_creates.append(group_cfg.name)
            continue

        try:
            client.groups.create(display_name=group_cfg.name)
            stats["created"] += 1
            changed_creates.append(group_cfg.name)
        except Exception as e:
            stats["errors"] += 1
            console.print(f"    [red]ERROR[/red]   {group_cfg.name}: {e}")
            logger.error("Error creating group %s: %s", group_cfg.name, e)

    # --- Delete groups not in the YAML ---
    orphans = {name: g for name, g in existing.items() if name not in desired_groups}

    if orphans:
        for name_lower, group_obj in sorted(orphans.items()):
            original_name = existing_original.get(name_lower, name_lower)
            group_id = getattr(group_obj, "id", None)

            if _is_protected(name_lower, protected):
                stats["protected"] += 1
                continue

            # Warn if the group is still referenced in a membership YAML file
            if name_lower in membership_groups:
                console.print(
                    f"  [bold yellow]WARNING[/bold yellow]   Group '{original_name}' has a membership "
                    f"file but is not in groups.yaml -- will not be processed"
                )

            if dry_run:
                stats["deleted"] += 1
                changed_deletes.append(original_name)
                continue

            try:
                client.groups.delete(id=group_id)
                stats["deleted"] += 1
                changed_deletes.append(original_name)
            except Exception as e:
                stats["errors"] += 1
                console.print(f"    [red]ERROR[/red]   Deleting {original_name}: {e}")
                logger.error("Error deleting group %s: %s", original_name, e)

    if stats["created"]:
        label = "to create" if dry_run else "created"
        console.print(f"  [green]{'PLAN' if dry_run else 'CREATE'}[/green] groups {label}: {stats['created']}")
    if stats["deleted"]:
        label = "to delete" if dry_run else "deleted"
        console.print(f"  [red]{'PLAN' if dry_run else 'DELETE'}[/red] groups {label}: {stats['deleted']}")
    if stats["protected"]:
        console.print(f"  [magenta]PROTECTED[/magenta] groups kept: {stats['protected']}")
    if stats["skipped"] and show_unchanged:
        console.print(f"  [yellow]UNCHANGED[/yellow] groups already in desired state: {stats['skipped']}")
    if not any((stats["created"], stats["deleted"], stats["protected"], stats["errors"])) and not show_unchanged:
        console.print("  No group changes needed.")

    _print_summary(stats, dry_run)
    color = "cyan" if dry_run else "green"
    for item in changed_creates:
        console.print(f"  [{color}]+[/{color}] create group {item}")
    for item in changed_deletes:
        console.print(f"  [{color}]-[/{color}] delete group {item}")
    return stats


def _print_summary(stats: dict, dry_run: bool) -> None:
    if dry_run:
        console.print(
            f"\n[bold]Plan:[/bold] {stats['created']} to create, {stats['deleted']} to delete, "
            f"{stats['protected']} protected, {stats['errors']} errors."
        )
        return

    console.print(
        f"\n[bold green]Apply complete:[/bold green] {stats['created']} created, {stats['deleted']} deleted, "
        f"{stats['protected']} protected, {stats['errors']} errors."
    )
