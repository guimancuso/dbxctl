import logging

from databricks.sdk import AccountClient
from rich.console import Console

from dbx_iam.models import UserConfig

logger = logging.getLogger(__name__)
console = Console()

# Databricks system/service users that must NEVER be deleted.
# The SDK returns service principals mixed with users in some accounts.
SYSTEM_EMAIL_PATTERNS = (
    "databricks",
    "serviceprincipals",
)


def _is_protected(email: str, protected_emails: list[str]) -> bool:
    email_lower = email.lower()
    if email_lower in protected_emails:
        return True
    for pattern in SYSTEM_EMAIL_PATTERNS:
        if pattern in email_lower:
            return True
    return False


def sync_users(
    client: AccountClient,
    users: list[UserConfig],
    dry_run: bool = False,
    protected_emails: list[str] | None = None,
    show_unchanged: bool = False,
) -> dict:
    protected = protected_emails or []
    stats = {"created": 0, "skipped": 0, "deleted": 0, "protected": 0, "errors": 0}
    changed_creates: list[str] = []
    changed_deletes: list[str] = []

    console.print("\n[bold]Synchronizing users...[/bold]\n")

    existing: dict[str, object] = {}
    existing_original: dict[str, str] = {}
    for u in client.users.list():
        if u.user_name:
            existing[u.user_name.lower()] = u
            existing_original[u.user_name.lower()] = u.user_name

    desired_emails = {u.email.lower() for u in users}

    for user_cfg in users:
        email_lower = user_cfg.email.lower()

        if email_lower in existing:
            logger.debug("SKIP: %s already exists", user_cfg.email)
            stats["skipped"] += 1
            continue

        if dry_run:
            stats["created"] += 1
            changed_creates.append(f"{user_cfg.display_name} ({user_cfg.email})")
            continue

        try:
            client.users.create(
                user_name=user_cfg.email,
                display_name=user_cfg.display_name,
            )
            stats["created"] += 1
            changed_creates.append(f"{user_cfg.display_name} ({user_cfg.email})")
        except Exception as e:
            stats["errors"] += 1
            console.print(f"    [red]ERROR[/red]   {user_cfg.display_name} ({user_cfg.email}): {e}")
            logger.error("Error creating user %s: %s", user_cfg.email, e)

    # --- Delete users not in the YAML ---
    orphans = {email: u for email, u in existing.items() if email not in desired_emails}

    if orphans:
        for email_lower, user_obj in sorted(orphans.items()):
            original_email = existing_original.get(email_lower, email_lower)
            display = getattr(user_obj, "display_name", None) or original_email
            user_id = getattr(user_obj, "id", None)

            if _is_protected(email_lower, protected):
                stats["protected"] += 1
                continue

            if dry_run:
                stats["deleted"] += 1
                changed_deletes.append(f"{display} ({original_email})")
                continue

            try:
                client.users.delete(id=user_id)
                stats["deleted"] += 1
                changed_deletes.append(f"{display} ({original_email})")
            except Exception as e:
                stats["errors"] += 1
                console.print(f"    [red]ERROR[/red]   Deleting {display} ({original_email}): {e}")
                logger.error("Error deleting user %s: %s", original_email, e)

    if stats["created"]:
        label = "to create" if dry_run else "created"
        console.print(f"  [green]{'PLAN' if dry_run else 'CREATE'}[/green] users {label}: {stats['created']}")
    if stats["deleted"]:
        label = "to delete" if dry_run else "deleted"
        console.print(f"  [red]{'PLAN' if dry_run else 'DELETE'}[/red] users {label}: {stats['deleted']}")
    if stats["protected"]:
        console.print(f"  [magenta]PROTECTED[/magenta] users kept: {stats['protected']}")
    if stats["skipped"] and show_unchanged:
        console.print(f"  [yellow]UNCHANGED[/yellow] users already in desired state: {stats['skipped']}")
    if not any((stats["created"], stats["deleted"], stats["protected"], stats["errors"])) and not show_unchanged:
        console.print("  No user changes needed.")

    _print_summary(stats, dry_run)
    color = "cyan" if dry_run else "green"
    for item in changed_creates:
        console.print(f"  [{color}]+[/{color}] create user {item}")
    for item in changed_deletes:
        console.print(f"  [{color}]-[/{color}] delete user {item}")
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
