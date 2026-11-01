"""Database layer: one function per report, each returns the payload documented in its builder module.

NOT WIRED YET - the Supabase table/column names for these reports are not confirmed.
Each stub lists what the query has to produce. Fill them in once the schema is confirmed
(see README.md, "Data needed per report").

params: dict with the filters from the request (term_start, term_end, week_start, week_end, ...)
org_filter: {"Organization": "..."} or None for super admins (same object main.py's _build_stats_org_filter returns)
"""


def _todo(name, needs):
    raise NotImplementedError(f"{name}: wire to Supabase. Needs: {needs}")


def fetch_life_class(params, org_filter):
    _todo("life_class", "Life Class event sessions in the term, attendees per session with person.gender, leader12/leader144 "
          "(from LeaderPath), registration type (Guide / First Time / Second Time) -> {'MEN': [...], 'WOMEN': [...]}")


def fetch_school_of_leaders(params, org_filter):
    _todo("school_of_leaders", "School of Leaders sessions (10 weeks), attendees, cohort label (Destiny 1 & 2, 3 & 4 ...), "
          "which sheet/group the person belongs to (Sheet1 vs YOML - meaning to be confirmed)")


def fetch_plan_40(params, org_filter):
    _todo("plan_40", "Plan 40 sessions (5 weeks), attendees by gender with leader12/leader144; roster of leaders per gender")


def fetch_potential_leaders(params, org_filter):
    _todo("potential_leaders", "NEW DATA: a flag/table for 'potential leader' (person_id, leader12_id, comment) "
          "plus 'Leaders' = existing leaders under each Leader@12")


def fetch_twelve_tasks(params, org_filter):
    _todo("twelve_tasks", "per leader: completed-task count last week, weekly target, completed-task count this week "
          "(existing sb_get_twelve_tasks_report likely already has these)")


def fetch_service_target(params, org_filter):
    _todo("service_target", "per leader: target + actual headcount for Friday and Sunday services "
          "(existing sb_get_service_target_report likely already has these)")


FETCHERS = {
    "life_class": fetch_life_class, "school_of_leaders": fetch_school_of_leaders, "plan_40": fetch_plan_40,
    "potential_leaders": fetch_potential_leaders, "twelve_tasks": fetch_twelve_tasks, "service_target": fetch_service_target,
}
