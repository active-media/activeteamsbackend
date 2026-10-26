"""Run from the folder that contains `reporting_core` (activeteamsbackend):
    pytest tests/test_cells_drilldown.py
External modules (auth, supabase) are stubbed so no database is needed."""
import sys, types, pathlib
from datetime import date

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1] / "activeteamsbackend"
sys.path.insert(0, str(ROOT))

for name, attrs in {
    "auth": {}, "auth.utils": {"get_current_user": lambda: None},
    "supabase_helpers": {}, "supabase_helpers.supabase_connection": {"supabase": None, "supabase_admin": None},
}.items():
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules.setdefault(name, mod)

from fastapi import HTTPException  # noqa: E402
from reporting_core import scope as S  # noqa: E402
from reporting_core import cells_drilldown as C  # noqa: E402


def person(pid, name, surname, leader_id=None, path=()):
    return {"_id": pid, "Name": name, "Surname": surname, "Email": f"{pid}@x.org",
            "Organization": "Org", "LeaderId": leader_id, "LeaderPath": list(path)}

# Ps (root) -> Nash (12) -> tegra (144) -> Wena, Benny (1728)
#                       -> Sanje (144) -> Ishmael (1728)
#            -> Other (12) -> Zed (144)
PEOPLE = [
    person("ps", "Ps", "Gavin"),
    person("nash", "Nash", "N", "ps", ["ps"]),
    person("tegra", "tegra", "T", "nash", ["ps", "nash"]),
    person("sanje", "Sanje", "S", "nash", ["ps", "nash"]),
    person("wena", "Wena", "W", "tegra", ["ps", "nash", "tegra"]),
    person("benny", "Benny", "B", "tegra", ["ps", "nash", "tegra"]),
    person("ishmael", "Ishmael", "I", "sanje", ["ps", "nash", "sanje"]),
    person("other", "Other", "O", "ps", ["ps"]),
    person("zed", "Zed", "Z", "other", ["ps", "other"]),
]


def test_leader_at_12_sees_every_depth_but_not_sideways_or_up():
    assert S.downline_ids("nash", PEOPLE) == {"nash", "tegra", "sanje", "wena", "benny", "ishmael"}


def test_leader_at_144_sees_only_own_downline():
    assert S.downline_ids("tegra", PEOPLE) == {"tegra", "wena", "benny"}
    assert S.downline_ids("sanje", PEOPLE) == {"sanje", "ishmael"}
    assert S.downline_ids("wena", PEOPLE) == {"wena"}


def test_direct_only_excludes_grandchildren():
    assert S.downline_ids("nash", PEOPLE, direct_only=True) == {"nash", "tegra", "sanje"}


def test_unknown_leader_is_empty():
    assert S.downline_ids("nobody", PEOPLE) == set()


def test_existing_scope_walk_unchanged():
    assert S._leader_ids({"email": "nash@x.org"}, PEOPLE) == S.downline_ids("nash", PEOPLE)
    assert S._leader_ids({"user_id": "tegra"}, PEOPLE) == {"tegra", "wena", "benny"}
    assert S._leader_ids({"email": "ghost@x.org"}, PEOPLE) == set()


def scope_for(uid):
    return S.ReportScope({"Organization": "Org"}, False, "leader", uid, S.downline_ids(uid, PEOPLE))


def test_144_leader_gets_403_for_the_12_above_and_for_siblings():
    tegra = scope_for("tegra")
    for forbidden in ("nash", "sanje", "ishmael", "other"):
        with pytest.raises(HTTPException) as exc:
            S.ensure_visible(tegra, forbidden)
        assert exc.value.status_code == 403
    S.ensure_visible(tegra, "wena")  # own downline is fine


def test_12_leader_can_see_144_and_1728_below():
    nash = scope_for("nash")
    for allowed in ("tegra", "sanje", "wena", "ishmael"):
        S.ensure_visible(nash, allowed)
    with pytest.raises(HTTPException):
        S.ensure_visible(nash, "zed")


def test_admin_sees_everyone():
    S.ensure_visible(S.ReportScope({"Organization": "Org"}, False, "admin", "a", None), "zed")


# ---------------- payloads ----------------
class P:  # minimal ResolvedPeriod
    token = "thisMonth"; start = date(2026, 10, 1); end = date(2026, 10, 31)


def sess(event_id, name, leader, day, count, dnm=False, week="W1"):
    return {"id": f"{event_id}-{day}", "event_id": event_id, "session_date": f"2026-10-{day:02d}",
            "week_identifier": week, "checked_in_count": count, "is_did_not_meet": dnm,
            "events": {"event_name": name, "event_leader": leader, "location": "Main"}}

SESSIONS = [
    sess("c-tegra", "Tegra Cell", "tegra T", 5, 10), sess("c-tegra", "Tegra Cell", "tegra T", 12, 14, week="W2"),
    sess("c-wena", "Wena Cell", "Wena W", 6, 6), sess("c-benny", "Benny Cell", "Benny B", 6, 4, dnm=True),
    sess("c-ish", "Ishmael Cell", "Ishmael I", 7, 8),
    sess("c-zed", "Zed Cell", "Zed Z", 7, 20),
]


def test_leader_payload_tegra_sees_own_and_disciples_only():
    vis = S.downline_ids("tegra", PEOPLE)
    out = C.leader_payload("tegra", vis, PEOPLE, SESSIONS, "downline", P)
    cells = {r["event_id"] for r in out["tables"][1]["rows"]}
    assert cells == {"c-tegra", "c-wena", "c-benny"}          # no Ishmael, no Zed
    assert out["summary"]["total_attendance"] == 30            # did-not-meet adds 0
    assert out["summary"]["meetings_held"] == 3 and out["summary"]["did_not_meet"] == 1
    leaders = {r["leader_id"] for r in out["tables"][0]["rows"]}
    assert leaders == {"tegra", "wena", "benny"}


def test_leader_payload_nash_sees_whole_tree():
    vis = S.downline_ids("nash", PEOPLE)
    out = C.leader_payload("nash", vis, PEOPLE, SESSIONS, "downline", P)
    assert {r["event_id"] for r in out["tables"][1]["rows"]} == {"c-tegra", "c-wena", "c-benny", "c-ish"}


def test_ambiguous_leader_name_is_never_attributed():
    people = PEOPLE + [person("tegra2", "tegra", "T", "other", ["ps", "other"])]  # same full name
    vis = S.downline_ids("nash", people)
    out = C.leader_payload("nash", vis, people, SESSIONS, "downline", P)
    assert "c-tegra" not in {r["event_id"] for r in out["tables"][1]["rows"]}
    assert out["meta"]["ambiguous_cells"] == 1


def test_cell_payload():
    history = [s for s in SESSIONS if s["event_id"] == "c-tegra"]
    out = C.cell_payload("c-tegra", history, PEOPLE, "2026-10-01", "2026-10-31", P)
    assert out["meta"]["leader_id"] == "tegra" and out["meta"]["title"] == "Tegra Cell"
    assert out["summary"]["total_attendance"] == 24 and out["summary"]["average_attendance"] == 12.0
    assert [r["date"] for r in out["tables"][0]["rows"]] == ["2026-10-12", "2026-10-05"]
    assert [p["y"] for p in out["series"][0]["points"]] == [10, 14]


def test_empty_period_has_no_division_errors():
    out = C.cell_payload("c-tegra", [s for s in SESSIONS if s["event_id"] == "c-tegra"], PEOPLE, "2027-01-01", "2027-01-31", P)
    assert out["summary"]["average_attendance"] is None and out["tables"][0]["rows"] == []