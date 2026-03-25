import argparse
import logging
import sys

from rich.console import Console

from dbx_iam.client import get_account_client
from dbx_iam.config_loader import (
    load_groups,
    load_memberships,
    load_settings,
    load_users,
    load_workspace_assignments,
    validate_all,
)
from dbx_iam.manage_groups import sync_groups
from dbx_iam.manage_memberships import sync_memberships
from dbx_iam.manage_users import sync_users
from dbx_iam.manage_workspaces import sync_workspace_assignments

console = Console()


def _run_validation() -> bool:
    """Run cross-validation of YAML files. Returns True if OK, False if there are errors."""
    console.print("[bold]Validating configuration...[/bold]\n")
    result = validate_all()
    result.print_report()

    if result.has_errors:
        console.print("[bold red]Fix the errors above before proceeding.[/bold red]\n")
        return False

    if result.warnings:
        console.print("[yellow]Warnings found (non-blocking).[/yellow]\n")

    return True


def cmd_validate(args: argparse.Namespace) -> None:
    ok = _run_validation()
    if not ok:
        sys.exit(1)


def cmd_users(args: argparse.Namespace) -> None:
    if not _run_validation():
        sys.exit(1)
    settings = load_settings()
    client = get_account_client(settings)
    users = load_users()
    sync_users(
        client,
        users,
        dry_run=args.dry_run,
        protected_emails=settings.protected_emails,
        show_unchanged=args.verbose,
    )


def cmd_groups(args: argparse.Namespace) -> None:
    if not _run_validation():
        sys.exit(1)
    settings = load_settings()
    client = get_account_client(settings)
    groups = load_groups()
    memberships = load_memberships()
    sync_groups(
        client, groups,
        memberships=memberships,
        dry_run=args.dry_run,
        protected_groups=settings.protected_groups,
        show_unchanged=args.verbose,
    )


def cmd_members(args: argparse.Namespace) -> None:
    if not _run_validation():
        sys.exit(1)
    settings = load_settings()
    client = get_account_client(settings)
    memberships = load_memberships()
    sync_memberships(client, memberships, dry_run=args.dry_run, show_unchanged=args.verbose)


def cmd_workspaces(args: argparse.Namespace) -> None:
    if not _run_validation():
        sys.exit(1)
    settings = load_settings()
    client = get_account_client(settings)
    assignments = load_workspace_assignments()
    sync_workspace_assignments(
        client,
        assignments,
        settings,
        dry_run=args.dry_run,
        show_unchanged=args.verbose,
    )


def cmd_sync(args: argparse.Namespace) -> None:
    if not _run_validation():
        sys.exit(1)

    settings = load_settings()
    client = get_account_client(settings)
    groups = load_groups()
    users = load_users()
    memberships = load_memberships()
    assignments = load_workspace_assignments()

    # Order: create groups -> create users -> reconcile memberships
    # -> assign groups to workspaces

    console.print("[bold]== Step 1/4: Create groups ==[/bold]")
    sync_groups(
        client, groups,
        memberships=memberships,
        dry_run=args.dry_run,
        protected_groups=settings.protected_groups,
        show_unchanged=args.verbose,
    )

    console.print("\n[bold]== Step 2/4: Create users ==[/bold]")
    sync_users(
        client,
        users,
        dry_run=args.dry_run,
        protected_emails=settings.protected_emails,
        show_unchanged=args.verbose,
    )

    console.print("\n[bold]== Step 3/4: Reconcile memberships ==[/bold]")
    sync_memberships(client, memberships, dry_run=args.dry_run, show_unchanged=args.verbose)

    console.print("\n[bold]== Step 4/4: Assign groups to workspaces ==[/bold]")
    sync_workspace_assignments(
        client,
        assignments,
        settings,
        dry_run=args.dry_run,
        show_unchanged=args.verbose,
    )

    console.print("\n[bold green]Sync completed.[/bold green]")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dbxctl",
        description="dbxctl - Idempotent management of users, groups, and memberships in Databricks",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable detailed logging (DEBUG) and show unchanged items",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # validate
    p_validate = subparsers.add_parser("validate", help="Validate configuration (no API calls)")
    p_validate.set_defaults(func=cmd_validate)

    # users
    p_users = subparsers.add_parser("users", help="Synchronize users")
    p_users.add_argument("--dry-run", action="store_true", help="Simulate without executing")
    p_users.set_defaults(func=cmd_users)

    # groups
    p_groups = subparsers.add_parser("groups", help="Synchronize groups")
    p_groups.add_argument("--dry-run", action="store_true", help="Simulate without executing")
    p_groups.set_defaults(func=cmd_groups)

    # members
    p_members = subparsers.add_parser("members", help="Synchronize memberships")
    p_members.add_argument("--dry-run", action="store_true", help="Simulate without executing")
    p_members.set_defaults(func=cmd_members)

    # workspaces
    p_workspaces = subparsers.add_parser("workspaces", help="Synchronize group-to-workspace assignments")
    p_workspaces.add_argument("--dry-run", action="store_true", help="Simulate without executing")
    p_workspaces.set_defaults(func=cmd_workspaces)

    # sync
    p_sync = subparsers.add_parser("sync", help="Synchronize everything (create, reconcile, and remove)")
    p_sync.add_argument("--dry-run", action="store_true", help="Simulate without executing")
    p_sync.set_defaults(func=cmd_sync)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        args.func(args)
    except Exception as e:
        console.print(f"\n[bold red]Fatal error:[/bold red] {e}")
        logging.exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()
