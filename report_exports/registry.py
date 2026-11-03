from . import life_class, school_of_leaders, plan_40, potential_leaders, twelve_tasks, service_target, staff_interns_youth

# report_type -> (builder, download filename stem)
REPORTS = {
    "life_class":        (life_class.build,        "Life_Class"),
    "school_of_leaders": (school_of_leaders.build, "School_of_Leaders"),
    "plan_40":           (plan_40.build,           "Plan_40"),
    "potential_leaders": (potential_leaders.build, "Potential_Leaders"),
    "twelve_tasks":      (twelve_tasks.build,      "Twelve_Tasks"),
    "service_target":    (service_target.build,    "Service_Target"),
    "staff_interns_youth": (staff_interns_youth.build, "Staff_Interns_Youth"),
}


def build_report(report_type: str, payload: dict) -> bytes:
    return REPORTS[report_type][0](payload)
