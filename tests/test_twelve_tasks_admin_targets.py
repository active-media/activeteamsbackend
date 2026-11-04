from datetime import date

from Reports import twelve_tasks


def test_report_uses_exact_admin_week_targets_and_actuals(monkeypatch):
    target_rows = [
        {
            "person_id": "person-1",
            "target": 8,
            "week_identifier": "2026-W45",
            "organization": "Example Org",
            "is_default": False,
        },
        {
            "person_id": "person-deleted",
            "target": 3,
            "week_identifier": "2026-W45",
            "organization": "Example Org",
            "is_default": False,
        },
        {
            "person_id": "",
            "target": 5,
            "week_identifier": "2026-W45",
            "organization": "Example Org",
            "is_default": True,
        },
    ]
    monkeypatch.setattr(
        twelve_tasks,
        "sb_get_admin_task_targets",
        lambda org, weeks: (
            target_rows
            if org == {"Organization": "Example Org"} and weeks == ["2026-W45"]
            else []
        ),
    )
    monkeypatch.setattr(
        twelve_tasks,
        "_fetch_target_people",
        lambda ids: {
            "person-1": {
                "_id": "person-1",
                "Name": "Alex",
                "Surname": "Leader",
                "Gender": "Female",
                "Email": "alex@example.test",
            }
        },
    )
    monkeypatch.setattr(
        twelve_tasks,
        "_fetch_completed_tasks",
        lambda start, end, org_filter: [
            {
                "status": "completed",
                "completedAt": "2026-10-28T12:00:00+00:00",
                "assigned_to_email": "alex@example.test",
            },
            {
                "status": "completed",
                "completedAt": "2026-11-03T12:00:00+00:00",
                "assignedfor": "unresolved",
                "assigned_to_email": "alex@example.test",
            },
            {
                "status": "completed",
                "completedAt": "2026-11-04T12:00:00+00:00",
                "assignedfor": "person-deleted",
            },
        ],
    )

    report = twelve_tasks.sb_get_twelve_tasks_report(
        period="thisWeek",
        org_filter={"Organization": "Example Org"},
        start_date=date(2026, 11, 2),
        end_date=date(2026, 11, 8),
    )

    assert report["empty_state"] is None
    assert report["rows"] == [
        {
            "person_id": "person-1",
            "name": "Alex Leader",
            "gender": "Female",
            "week_identifier": "2026-W45",
            "last_week": 1,
            "target": 8,
            "this_week": 1,
        },
        {
            "person_id": "person-deleted",
            "name": "person-deleted",
            "gender": "",
            "week_identifier": "2026-W45",
            "last_week": 0,
            "target": 3,
            "this_week": 0,
        },
        {
            "person_id": "",
            "name": "Leader default",
            "gender": "",
            "week_identifier": "2026-W45",
            "last_week": 0,
            "target": 5,
            "this_week": 0,
        },
    ]


def test_report_returns_empty_state_when_week_has_no_admin_targets(monkeypatch):
    monkeypatch.setattr(twelve_tasks, "sb_get_admin_task_targets", lambda org, weeks: [])
    monkeypatch.setattr(
        twelve_tasks,
        "_fetch_completed_tasks",
        lambda start, end, org_filter: [],
    )

    report = twelve_tasks.sb_get_twelve_tasks_report(
        period="thisWeek",
        org_filter={"Organization": "Example Org"},
        start_date=date(2026, 11, 2),
        end_date=date(2026, 11, 8),
    )

    assert report["rows"] == []
    assert report["empty_state"] == "No tasks set for this week"
