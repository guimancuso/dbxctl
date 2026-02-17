import logging

from databricks.sdk import AccountClient
from rich.console import Console
from rich.table import Table

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
) -> dict:
    protected = protected_emails or []
    stats = {"created": 0, "skipped": 0, "deleted": 0, "protected": 0, "errors": 0}

    console.print("\n[bold]Synchronizing users...[/bold]\n")

    existing: dict[str, object] = {}
    existing_original: dict[str, str] = {}
    for u in client.users.list():
        if u.user_name:
            existing[u.user_name.lower()] = u
            existing_original[u.user_name.lower()] = u.user_name

    desired_emails = {u.email.lower() for u in users}

    console.print(f"  Existing users in account: {len(existing)}")
    console.print(f"  Users defined in YAML:     {len(users)}\n")

    # --- Create missing users ---
    console.print("  [bold]Creation:[/bold]")
    has_creation = False
    for user_cfg in users:
        email_lower = user_cfg.email.lower()

        if email_lower in existing:
            logger.debug("SKIP: %s already exists", user_cfg.email)
            stats["skipped"] += 1
            console.print(f"    [yellow]SKIP[/yellow]    {user_cfg.display_name} ({user_cfg.email})")
            continue

        has_creation = True
        if dry_run:
            console.print(f"    [cyan]DRY-RUN[/cyan] Would create: {user_cfg.display_name} ({user_cfg.email})")
            stats["created"] += 1
            continue

        try:
            client.users.create(
                user_name=user_cfg.email,
                display_name=user_cfg.display_name,
            )
            stats["created"] += 1
            console.print(f"    [green]CREATED[/green] {user_cfg.display_name} ({user_cfg.email})")
        except Exception as e:
            stats["errors"] += 1
            console.print(f"    [red]ERROR[/red]   {user_cfg.display_name} ({user_cfg.email}): {e}")
            logger.error("Error creating user %s: %s", user_cfg.email, e)

    if not has_creation and stats["skipped"] == len(users):
        console.print("    All users already exist.")

    # --- Delete users not in the YAML ---
    orphans = {email: u for email, u in existing.items() if email not in desired_emails}

    if orphans:
        console.print(f"\n  [bold]Removal ({len(orphans)} user(s) not in YAML):[/bold]")
        for email_lower, user_obj in sorted(orphans.items()):
            original_email = existing_original.get(email_lower, email_lower)
            display = getattr(user_obj, "display_name", None) or original_email
            user_id = getattr(user_obj, "id", None)

            if _is_protected(email_lower, protected):
                stats["protected"] += 1
                console.print(f"    [magenta]PROTECTED[/magenta] {display} ({original_email})")
                continue

            if dry_run:
                console.print(f"    [cyan]DRY-RUN[/cyan] Would delete: {display} ({original_email})")
                stats["deleted"] += 1
                continue

            try:
                client.users.delete(id=user_id)
                stats["deleted"] += 1
                console.print(f"    [red]DELETED[/red] {display} ({original_email})")
            except Exception as e:
                stats["errors"] += 1
                console.print(f"    [red]ERROR[/red]   Deleting {display} ({original_email}): {e}")
                logger.error("Error deleting user %s: %s", original_email, e)
    else:
        console.print("\n  [bold]Removal:[/bold] No orphan users found.")

    _print_summary(stats, dry_run)
    return stats


def _print_summary(stats: dict, dry_run: bool) -> None:
    table = Table(title="\nSummary - Users" + (" (DRY-RUN)" if dry_run else ""))
    table.add_column("Status", style="bold")
    table.add_column("Count", justify="right")

    table.add_row("Created" if not dry_run else "To create", str(stats["created"]), style="green")
    table.add_row("Already exist", str(stats["skipped"]), style="yellow")
    table.add_row("Deleted" if not dry_run else "To delete", str(stats["deleted"]), style="red")
    table.add_row("Protected (not deleted)", str(stats["protected"]), style="magenta")
    table.add_row("Errors", str(stats["errors"]), style="red")

    console.print(table)
