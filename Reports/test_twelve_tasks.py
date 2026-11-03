from Reports.twelve_tasks import (
    sb_get_twelve_tasks_report,
    export_twelve_tasks_xlsx
)

r = sb_get_twelve_tasks_report(
    leader12_id="leader-12-test-1",
    org_filter={"Organization": "Springfield"}
)

for row in r["rows"]:
    print(row)

export_twelve_tasks_xlsx(r, "test_output.xlsx")

print("Excel file created successfully.")