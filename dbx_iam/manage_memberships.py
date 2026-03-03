import logging

from databricks.sdk import AccountClient
from databricks.sdk.service import iam
from rich.console import Console
from rich.table import Table

from dbx_iam.models import MembershipConfig

logger = logging.getLogger(__name__)
console = Console()


def sync_memberships(
    client: AccountClient,
    memberships: list[MembershipConfig],
    dry_run: bool = False,
) -> dict:
    stats = {"added": 0, "removed": 0, "skipped": 0, "errors": 0, "warnings": 0}
    missing_users: list[str] = []
    missing_groups_as_members: list[str] = []
    missing_groups: list[str] = []

    console.print("\n[bold]Synchronizing memberships...[/bold]\n")

    # Index of existing users: email -> id, id -> original email
    users_by_email: dict[str, str] = {}
    users_id_to_email: dict[str, str] = {}
    for u in client.users.list():
        if u.user_name and u.id:
            users_by_email[u.user_name.lower()] = u.id
            users_id_to_email[u.id] = u.user_name

    # Index of existing groups: name -> id, id -> display_name
    groups_by_name: dict[str, str] = {}
    groups_id_to_name: dict[str, str] = {}
    for g in client.groups.list():
        if g.display_name and g.id:
            groups_by_name[g.display_name.lower()] = g.id
            groups_id_to_name[g.id] = g.display_name

    for membership in memberships:
        group_lower = membership.group.lower()
        console.print(f"  [bold]Group: {membership.group}[/bold]")

        if group_lower not in groups_by_name:
            console.print(f"    [red]ERROR[/red] Group '{membership.group}' does not exist in Databricks")
            missing_groups.append(membership.group)
            stats["errors"] += 1
            continue

        group_id = groups_by_name[group_lower]

        # Fetch current members via individual GET (list does not return members in Account API)
        current_member_ids: set[str] = set()
        try:
            group_detail = client.groups.get(id=group_id)
            if group_detail.members:
                for m in group_detail.members:
                    if m.value:
                        current_member_ids.add(m.value)
        except Exception as e:
            console.print(f"    [red]ERROR[/red] Fetching group members: {e}")
            stats["errors"] += 1
            continue

        console.print(f"    Current members: {len(current_member_ids)}")
        desired_member_ids: set[str] = set()

        # --- Adicionar usuários ---
        for email in membership.users:
            email_lower = email.lower()

            if email_lower not in users_by_email:
                console.print(f"    [bold red]WARNING[/bold red] User '{email}' does not exist in Databricks")
                missing_users.append(email)
                stats["warnings"] += 1
                continue

            user_id = users_by_email[email_lower]
            desired_member_ids.add(user_id)

            if user_id in current_member_ids:
                console.print(f"    [yellow]SKIP[/yellow]    {email} (already a member)")
                stats["skipped"] += 1
                continue

            if dry_run:
                console.print(f"    [cyan]DRY-RUN[/cyan] Would add user: {email}")
                stats["added"] += 1
                continue

            try:
                client.groups.patch(
                    id=group_id,
                    operations=[
                        iam.Patch(
                            op=iam.PatchOp.ADD,
                            value={"members": [{"value": user_id}]},
                        )
                    ],
                    schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                )
                stats["added"] += 1
                console.print(f"    [green]ADDED[/green]   {email}")
            except Exception as e:
                stats["errors"] += 1
                console.print(f"    [red]ERROR[/red]   {email}: {e}")
                logger.error("Error adding %s to group %s: %s", email, membership.group, e)

        # --- Adicionar grupos como membros ---
        for gname in membership.groups:
            gname_lower = gname.lower()

            if gname_lower not in groups_by_name:
                console.print(f"    [bold red]WARNING[/bold red] Group '{gname}' does not exist in Databricks")
                missing_groups_as_members.append(gname)
                stats["warnings"] += 1
                continue

            member_group_id = groups_by_name[gname_lower]
            desired_member_ids.add(member_group_id)

            if member_group_id in current_member_ids:
                console.print(f"    [yellow]SKIP[/yellow]    {gname} (already a member)")
                stats["skipped"] += 1
                continue

            if dry_run:
                console.print(f"    [cyan]DRY-RUN[/cyan] Would add group: {gname}")
                stats["added"] += 1
                continue

            try:
                client.groups.patch(
                    id=group_id,
                    operations=[
                        iam.Patch(
                            op=iam.PatchOp.ADD,
                            value={"members": [{"value": member_group_id}]},
                        )
                    ],
                    schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                )
                stats["added"] += 1
                console.print(f"    [green]ADDED[/green]   {gname} (group)")
            except Exception as e:
                stats["errors"] += 1
                console.print(f"    [red]ERROR[/red]   {gname}: {e}")
                logger.error("Error adding group %s to group %s: %s", gname, membership.group, e)

        # --- Remove members not in the list ---
        orphan_ids = current_member_ids - desired_member_ids
        if orphan_ids:
            for orphan_id in orphan_ids:
                # Identificar se o orphan é usuário ou grupo para exibição
                orphan_label = (
                    users_id_to_email.get(orphan_id)
                    or groups_id_to_name.get(orphan_id)
                    or orphan_id
                )

                if dry_run:
                    console.print(f"    [cyan]DRY-RUN[/cyan] Would remove: {orphan_label}")
                    stats["removed"] += 1
                    continue

                try:
                    client.groups.patch(
                        id=group_id,
                        operations=[
                            iam.Patch(
                                op=iam.PatchOp.REMOVE,
                                path=f'members[value eq "{orphan_id}"]',
                            )
                        ],
                        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                    )
                    stats["removed"] += 1
                    console.print(f"    [red]REMOVED[/red] {orphan_label}")
                except Exception as e:
                    stats["errors"] += 1
                    console.print(f"    [red]ERROR[/red]   Removing {orphan_label}: {e}")
                    logger.error(
                        "Error removing %s from group %s: %s", orphan_label, membership.group, e
                    )

        console.print()

    _print_summary(stats, dry_run, missing_users, missing_groups_as_members, missing_groups)
    return stats


def _print_summary(
    stats: dict,
    dry_run: bool,
    missing_users: list[str],
    missing_groups_as_members: list[str],
    missing_groups: list[str],
) -> None:
    table = Table(title="\nSummary - Memberships" + (" (DRY-RUN)" if dry_run else ""))
    table.add_column("Status", style="bold")
    table.add_column("Count", justify="right")

    table.add_row("Added" if not dry_run else "To add", str(stats["added"]), style="green")
    table.add_row("Removed" if not dry_run else "To remove", str(stats["removed"]), style="red")
    table.add_row("Already members", str(stats["skipped"]), style="yellow")
    table.add_row("Members not found", str(stats["warnings"]), style="bold red")
    table.add_row("Errors", str(stats["errors"]), style="red")

    console.print(table)

    if missing_users:
        console.print("\n[bold red]Users not found in Databricks:[/bold red]")
        for email in sorted(set(missing_users)):
            console.print(f"  - {email}")

    if missing_groups_as_members:
        console.print("\n[bold red]Groups (members) not found in Databricks:[/bold red]")
        for name in sorted(set(missing_groups_as_members)):
            console.print(f"  - {name}")

    if missing_groups:
        console.print("\n[bold red]Target groups not found in Databricks:[/bold red]")
        for name in sorted(set(missing_groups)):
            console.print(f"  - {name}")
