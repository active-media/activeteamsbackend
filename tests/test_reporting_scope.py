import pytest
from fastapi import HTTPException

from reporting_core.scope import build_report_scope


def test_user_without_organization_gets_403():
    with pytest.raises(HTTPException) as error:
        build_report_scope({"role": "admin"})

    assert error.value.status_code == 403
    assert "Organization" in error.value.detail


@pytest.mark.parametrize("role", ["user", "registrant"])
def test_non_reporting_roles_get_403(role):
    with pytest.raises(HTTPException) as error:
        build_report_scope({"role": role, "Organization": "Example Org"})

    assert error.value.status_code == 403


def test_admin_gets_organization_scope():
    scope = build_report_scope({"role": "admin", "Organization": "Example Org"})

    assert scope.org_filter == {"Organization": "Example Org"}
    assert scope.visible_leader_ids is None


def test_leader_gets_only_own_hierarchy():
    user = {
        "role": "leader",
        "Organization": "Example Org",
        "user_id": "leader-1",
        "email": "leader@example.test",
    }
    people = [
        {"_id": "leader-1", "Email": "leader@example.test", "Name": "Alex Leader"},
        {"_id": "child-1", "LeaderPath": ["leader-1", "child-1"]},
        {"_id": "other-1", "LeaderPath": ["other-1"]},
    ]

    scope = build_report_scope(user, people)

    assert scope.visible_leader_ids == {"leader-1", "child-1"}


def test_leader_without_resolvable_person_gets_403():
    with pytest.raises(HTTPException) as error:
        build_report_scope({"role": "leader", "Organization": "Example Org", "user_id": "missing"}, [])

    assert error.value.status_code == 403