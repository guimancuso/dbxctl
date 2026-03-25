import logging

from databricks.sdk import AccountClient
from databricks.sdk.service import iam
from rich.console import Console

from dbx_iam.models import MembershipConfig

logger = logging.getLogger(__name__)
console = Console()


def sync_memberships(
    client: AccountClient,
    memberships: list[MembershipConfig],
    dry_run: bool = False,
    show_unchanged: bool = False,
) -> dict:
    stats = {"added": 0, "removed": 0, "skipped": 0, "errors": 0, "warnings": 0}
    missing_users: list[str] = []
    missing_groups_as_members: list[str] = []
    missing_service_principals: list[str] = []
    missing_groups: list[str] = []
    changed_items: list[str] = []

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

    # Index of existing service principals: application_id -> id, id -> label
    service_principals_by_application_id: dict[str, str] = {}
    service_principals_id_to_label: dict[str, str] = {}
    for sp in client.service_principals.list():
        if sp.application_id and sp.id:
            service_principals_by_application_id[sp.application_id.lower()] = sp.id
            service_principals_id_to_label[sp.id] = (
                f"{sp.display_name} [{sp.application_id}]"
                if sp.display_name
                else sp.application_id
            )

    for membership in memberships:
        group_lower = membership.group.lower()
        group_skipped = 0
        group_added = 0
        group_removed = 0
        group_warnings = 0
        group_errors = 0

        if group_lower not in groups_by_name:
            console.print(f"  [red]ERROR[/red] group {membership.group}: target group does not exist in Databricks")
            missing_groups.append(membership.group)
            stats["errors"] += 1
            group_errors += 1
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
            console.print(f"  [red]ERROR[/red] group {membership.group}: fetching members failed: {e}")
            stats["errors"] += 1
            group_errors += 1
            continue

        desired_member_ids: set[str] = set()

        # --- Add users ---
        for email in membership.users:
            email_lower = email.lower()

            if email_lower not in users_by_email:
                console.print(f"  [bold yellow]WARNING[/bold yellow] group {membership.group}: user '{email}' does not exist in Databricks")
                missing_users.append(email)
                stats["warnings"] += 1
                group_warnings += 1
                continue

            user_id = users_by_email[email_lower]
            desired_member_ids.add(user_id)

            if user_id in current_member_ids:
                if show_unchanged:
                    console.print(f"    [yellow]SKIP[/yellow]    {email} (already a member)")
                stats["skipped"] += 1
                group_skipped += 1
                continue

            if dry_run:
                stats["added"] += 1
                group_added += 1
                changed_items.append(f"+ group {membership.group}: add user {email}")
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
                group_added += 1
                changed_items.append(f"+ group {membership.group}: add user {email}")
            except Exception as e:
                stats["errors"] += 1
                group_errors += 1
                console.print(f"  [red]ERROR[/red] group {membership.group}: adding user '{email}' failed: {e}")
                logger.error("Error adding %s to group %s: %s", email, membership.group, e)

        # --- Add groups as members ---
        for gname in membership.groups:
            gname_lower = gname.lower()

            if gname_lower not in groups_by_name:
                console.print(f"  [bold yellow]WARNING[/bold yellow] group {membership.group}: nested group '{gname}' does not exist in Databricks")
                missing_groups_as_members.append(gname)
                stats["warnings"] += 1
                group_warnings += 1
                continue

            member_group_id = groups_by_name[gname_lower]
            desired_member_ids.add(member_group_id)

            if member_group_id in current_member_ids:
                if show_unchanged:
                    console.print(f"    [yellow]SKIP[/yellow]    {gname} (already a member)")
                stats["skipped"] += 1
                group_skipped += 1
                continue

            if dry_run:
                stats["added"] += 1
                group_added += 1
                changed_items.append(f"+ group {membership.group}: add nested group {gname}")
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
                group_added += 1
                changed_items.append(f"+ group {membership.group}: add nested group {gname}")
            except Exception as e:
                stats["errors"] += 1
                group_errors += 1
                console.print(f"  [red]ERROR[/red] group {membership.group}: adding nested group '{gname}' failed: {e}")
                logger.error("Error adding group %s to group %s: %s", gname, membership.group, e)

        # --- Add service principals ---
        for application_id in membership.service_principals:
            application_id_lower = application_id.lower()

            if application_id_lower not in service_principals_by_application_id:
                console.print(
                    f"  [bold yellow]WARNING[/bold yellow] group {membership.group}: service principal '{application_id}' does not exist in Databricks"
                )
                missing_service_principals.append(application_id)
                stats["warnings"] += 1
                group_warnings += 1
                continue

            service_principal_id = service_principals_by_application_id[application_id_lower]
            service_principal_label = service_principals_id_to_label.get(service_principal_id, application_id)
            desired_member_ids.add(service_principal_id)

            if service_principal_id in current_member_ids:
                if show_unchanged:
                    console.print(f"    [yellow]SKIP[/yellow]    {service_principal_label} (already a member)")
                stats["skipped"] += 1
                group_skipped += 1
                continue

            if dry_run:
                stats["added"] += 1
                group_added += 1
                changed_items.append(
                    f"+ group {membership.group}: add service principal {service_principal_label}"
                )
                continue

            try:
                client.groups.patch(
                    id=group_id,
                    operations=[
                        iam.Patch(
                            op=iam.PatchOp.ADD,
                            value={"members": [{"value": service_principal_id}]},
                        )
                    ],
                    schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                )
                stats["added"] += 1
                group_added += 1
                changed_items.append(
                    f"+ group {membership.group}: add service principal {service_principal_label}"
                )
            except Exception as e:
                stats["errors"] += 1
                group_errors += 1
                console.print(
                    f"  [red]ERROR[/red] group {membership.group}: adding service principal '{service_principal_label}' failed: {e}"
                )
                logger.error(
                    "Error adding service principal %s to group %s: %s",
                    application_id,
                    membership.group,
                    e,
                )

        # --- Remove members not in the list ---
        orphan_ids = current_member_ids - desired_member_ids
        if orphan_ids:
            for orphan_id in orphan_ids:
                # Resolve display name for the orphan (could be a user or a group)
                orphan_label = (
                    users_id_to_email.get(orphan_id)
                    or groups_id_to_name.get(orphan_id)
                    or service_principals_id_to_label.get(orphan_id)
                    or orphan_id
                )

                if dry_run:
                    stats["removed"] += 1
                    group_removed += 1
                    changed_items.append(f"- group {membership.group}: remove {orphan_label}")
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
                    group_removed += 1
                    changed_items.append(f"- group {membership.group}: remove {orphan_label}")
                except Exception as e:
                    stats["errors"] += 1
                    group_errors += 1
                    console.print(
                        f"  [red]ERROR[/red] group {membership.group}: removing '{orphan_label}' failed: {e}"
                    )
                    logger.error(
                        "Error removing %s from group %s: %s", orphan_label, membership.group, e
                    )

        if group_added or group_removed or group_warnings or group_errors or show_unchanged:
            parts = []
            if group_added:
                parts.append(f"{'to add' if dry_run else 'added'}={group_added}")
            if group_removed:
                parts.append(f"{'to remove' if dry_run else 'removed'}={group_removed}")
            if group_warnings:
                parts.append(f"warnings={group_warnings}")
            if group_errors:
                parts.append(f"errors={group_errors}")
            if group_skipped and show_unchanged:
                parts.append(f"unchanged={group_skipped}")
            if not parts:
                parts.append("no changes")
            prefix = "PLAN" if dry_run and (group_added or group_removed) else "GROUP"
            console.print(f"  [bold]{prefix}[/bold] {membership.group}: " + ", ".join(parts))

    _print_summary(
        stats,
        dry_run,
        missing_users,
        missing_groups_as_members,
        missing_service_principals,
        missing_groups,
    )
    color = "cyan" if dry_run else "green"
    for item in changed_items:
        console.print(f"  [{color}]{item}[/{color}]")
    return stats


def _print_summary(
    stats: dict,
    dry_run: bool,
    missing_users: list[str],
    missing_groups_as_members: list[str],
    missing_service_principals: list[str],
    missing_groups: list[str],
) -> None:
    if dry_run:
        console.print(
            f"\n[bold]Plan:[/bold] {stats['added']} to add, {stats['removed']} to remove, "
            f"{stats['warnings']} warnings, {stats['errors']} errors."
        )
    else:
        console.print(
            f"\n[bold green]Apply complete:[/bold green] {stats['added']} added, {stats['removed']} removed, "
            f"{stats['warnings']} warnings, {stats['errors']} errors."
        )

    if missing_users:
        console.print("\n[bold red]Users not found in Databricks:[/bold red]")
        for email in sorted(set(missing_users)):
            console.print(f"  - {email}")

    if missing_groups_as_members:
        console.print("\n[bold red]Groups (members) not found in Databricks:[/bold red]")
        for name in sorted(set(missing_groups_as_members)):
            console.print(f"  - {name}")

    if missing_service_principals:
        console.print("\n[bold red]Service principals not found in Databricks:[/bold red]")
        for application_id in sorted(set(missing_service_principals)):
            console.print(f"  - {application_id}")

    if missing_groups:
        console.print("\n[bold red]Target groups not found in Databricks:[/bold red]")
        for name in sorted(set(missing_groups)):
            console.print(f"  - {name}")
