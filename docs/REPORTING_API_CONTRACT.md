# Reporting API Contract

All endpoints are served by FastAPI under `/reporting` and require the existing bearer token. Allowed roles are `admin`, `super_admin`, `leader`, and `leaderAt12`. A non-super user must have an Organization. Refer to Scope rules below.

## Period tokens

Weeks use Monday-Sunday and dates are inclusive UTC calendar dates. Today's worked example is **2026-10-05**:

| Token | Resolved range on 2026-10-05 |
| --- | --- |
| `today` | 2026-10-05 to 2026-10-05 |
| `thisWeek` | 2026-10-05 to 2026-10-11 |
| `thisMonth` | 2026-10-01 to 2026-10-31 |
| `previous7` | 2026-09-28 to 2026-10-04 |
| `previousWeek` | 2026-09-28 to 2026-10-04 |
| `previousMonth` | 2026-09-01 to 2026-09-30 |
| `thisQuarter` | 2026-10-01 to 2026-12-31 |
| `yearly` | 2026-01-01 to 2026-12-31 |
| `YYYY-MM` | Calendar month, e.g. `2026-09` is 2026-09-01 to 2026-09-30 |
| explicit dates | `start_date` and `end_date` together override `period`; `period` metadata token is `custom` |

Invalid dates, one-sided explicit ranges, or reversed ranges return `422`.

## Scope rules

- `super_admin` or `is_supreme_admin=true`: all organizations; `org_filter=null`.
- `admin`: all reporting data in the user's Organization.
- `leader` and `leaderAt12`: own person record and descendants resolved from `LeaderId`, `LeaderPath`, and hierarchy leader fields, within their Organization.
- Missing Organization for any non-super user, unresolved leader hierarchy, `user`, and `registrant`: `403`.
- Super-admin history and generation requests must supply `organization`; history reads always filter by that organization.

## GET /reporting/catalog

**Params:** none. **Roles:** all reporting roles above. **Success:** list of all 8 report types, their params, supported exports, and `implemented` flag.

Example request: `GET /reporting/catalog`

Example success response generated from the route:

```json
[
  {
    "report_type": "twelve_tasks",
    "title": "Twelve Tasks",
    "description": "Completed task counts for tracked people and targets.",
    "params": [
      {
        "name": "period",
        "required": false,
        "allowed_values": [
          "today",
          "thisWeek",
          "thisMonth",
          "previous7",
          "previousWeek",
          "previousMonth",
          "thisQuarter",
          "yearly",
          "YYYY-MM"
        ]
      },
      {
        "name": "start_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "end_date",
        "required": false,
        "allowed_values": null
      }
    ],
    "supports_export": [
      "csv",
      "xlsx"
    ],
    "implemented": true
  },
  {
    "report_type": "cells_graph",
    "title": "Cells Graph",
    "description": "Cell attendance and active-cell trends.",
    "params": [
      {
        "name": "period",
        "required": false,
        "allowed_values": [
          "today",
          "thisWeek",
          "thisMonth",
          "previous7",
          "previousWeek",
          "previousMonth",
          "thisQuarter",
          "yearly",
          "YYYY-MM"
        ]
      },
      {
        "name": "start_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "end_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "campus",
        "required": false,
        "allowed_values": null
      }
    ],
    "supports_export": [
      "csv",
      "xlsx"
    ],
    "implemented": true
  },
  {
    "report_type": "life_class",
    "title": "Life Class",
    "description": "Life Class attendance report.",
    "params": [
      {
        "name": "period",
        "required": false,
        "allowed_values": [
          "today",
          "thisWeek",
          "thisMonth",
          "previous7",
          "previousWeek",
          "previousMonth",
          "thisQuarter",
          "yearly",
          "YYYY-MM"
        ]
      },
      {
        "name": "start_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "end_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "campus",
        "required": false,
        "allowed_values": null
      }
    ],
    "supports_export": [
      "csv",
      "xlsx"
    ],
    "implemented": false
  },
  {
    "report_type": "school_of_leaders",
    "title": "School of Leaders",
    "description": "School of Leaders attendance report.",
    "params": [
      {
        "name": "period",
        "required": false,
        "allowed_values": [
          "today",
          "thisWeek",
          "thisMonth",
          "previous7",
          "previousWeek",
          "previousMonth",
          "thisQuarter",
          "yearly",
          "YYYY-MM"
        ]
      },
      {
        "name": "start_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "end_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "campus",
        "required": false,
        "allowed_values": null
      }
    ],
    "supports_export": [
      "csv",
      "xlsx"
    ],
    "implemented": false
  },
  {
    "report_type": "plan_40",
    "title": "Plan 40",
    "description": "Plan 40 progress report.",
    "params": [
      {
        "name": "period",
        "required": false,
        "allowed_values": [
          "today",
          "thisWeek",
          "thisMonth",
          "previous7",
          "previousWeek",
          "previousMonth",
          "thisQuarter",
          "yearly",
          "YYYY-MM"
        ]
      },
      {
        "name": "start_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "end_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "campus",
        "required": false,
        "allowed_values": null
      }
    ],
    "supports_export": [
      "csv",
      "xlsx"
    ],
    "implemented": false
  },
  {
    "report_type": "staff_interns_youth",
    "title": "Staff/Interns/Youth",
    "description": "Staff, interns, and youth report.",
    "params": [
      {
        "name": "period",
        "required": false,
        "allowed_values": [
          "today",
          "thisWeek",
          "thisMonth",
          "previous7",
          "previousWeek",
          "previousMonth",
          "thisQuarter",
          "yearly",
          "YYYY-MM"
        ]
      },
      {
        "name": "start_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "end_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "campus",
        "required": false,
        "allowed_values": null
      }
    ],
    "supports_export": [
      "csv",
      "xlsx"
    ],
    "implemented": false
  },
  {
    "report_type": "school_cell",
    "title": "School Cell",
    "description": "Weekly School Cell attendance and leader summary.",
    "params": [
      {
        "name": "period",
        "required": false,
        "allowed_values": [
          "today",
          "thisWeek",
          "thisMonth",
          "previous7",
          "previousWeek",
          "previousMonth",
          "thisQuarter",
          "yearly",
          "YYYY-MM"
        ]
      },
      {
        "name": "start_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "end_date",
        "required": false,
        "allowed_values": null
      }
    ],
    "supports_export": [
      "csv",
      "xlsx"
    ],
    "implemented": true
  },
  {
    "report_type": "service_target",
    "title": "Service Target",
    "description": "Weekly leader service targets.",
    "params": [
      {
        "name": "period",
        "required": false,
        "allowed_values": [
          "today",
          "thisWeek",
          "thisMonth",
          "previous7",
          "previousWeek",
          "previousMonth",
          "thisQuarter",
          "yearly",
          "YYYY-MM"
        ]
      },
      {
        "name": "start_date",
        "required": false,
        "allowed_values": null
      },
      {
        "name": "end_date",
        "required": false,
        "allowed_values": null
      }
    ],
    "supports_export": [
      "csv",
      "xlsx"
    ],
    "implemented": true
  }
]
```

**Errors:** `401` invalid/missing token; `403` denied scope; `422` N/A; `501` N/A; `500` unexpected catalog failure.

## GET /reporting/dashboard

**Params:** `period` optional (`today`, `thisWeek`, `thisMonth`, `previous7`, `previousWeek`, `previousMonth`, `thisQuarter`, `yearly`, `YYYY-MM`); `start_date` and `end_date` optional together (`YYYY-MM-DD`); `campus` optional campus ID from the organization-scoped `campuses` list. Explicit dates take precedence. **Roles:** all reporting roles above.

Example request: `GET /reporting/dashboard?period=2025-02&campus=North`

Example success response generated from mocked helper data and validated by `DashboardEnvelope`:

```json
{
  "meta": {
    "report_type": "dashboard",
    "title": "Reporting Dashboard",
    "period": {
      "start": "2025-02-01",
      "end": "2025-02-28",
      "label": "February 2025",
      "token": "2025-02"
    },
    "campus": "campus-north",
    "generated_at": "2026-10-05T12:06:48.524754Z"
  },
  "campuses": [
    { "campus_id": "campus-north", "label": "North" },
    { "campus_id": "campus-south", "label": "South" }
  ],
  "kpis": [
    {
      "key": "total_attendance",
      "label": "Total Attendance",
      "value": 24,
      "delta": 20.0,
      "delta_label": null,
      "format": "number",
      "hint": "3 new visitors"
    },
    {
      "key": "active_cells",
      "label": "Active Cells",
      "value": 2,
      "delta": null,
      "delta_label": null,
      "format": "number",
      "hint": "4 completed sessions"
    },
    {
      "key": "life_class",
      "label": "Life Class",
      "value": 16,
      "delta": null,
      "delta_label": null,
      "format": "number",
      "hint": "20 registered"
    },
    {
      "key": "school_of_leaders",
      "label": "School of Leaders",
      "value": null,
      "delta": null,
      "delta_label": null,
      "format": "number",
      "hint": "No verified source data"
    },
    {
      "key": "plan_40",
      "label": "Plan 40",
      "value": null,
      "delta": null,
      "delta_label": null,
      "format": "number",
      "hint": "No verified source data"
    },
    {
      "key": "service_target",
      "label": "Service Target",
      "value": 30,
      "delta": null,
      "delta_label": null,
      "format": "number",
      "hint": "Target 30"
    },
    {
      "key": "twelve_tasks",
      "label": "Twelve Tasks",
      "value": 4,
      "delta": null,
      "delta_label": null,
      "format": "number",
      "hint": "Target 6"
    },
    {
      "key": "staff_interns_youth",
      "label": "Staff/Interns/Youth",
      "value": null,
      "delta": null,
      "delta_label": null,
      "format": "number",
      "hint": "No verified source data"
    }
  ],
  "tables": [
    {
      "key": "ministry_performance",
      "title": "Ministry Performance",
      "columns": [
        {
          "key": "ministry",
          "label": "Ministry",
          "format": null
        },
        {
          "key": "registered",
          "label": "Registered",
          "format": "number"
        },
        {
          "key": "attended",
          "label": "Attended",
          "format": "number"
        },
        {
          "key": "completed",
          "label": "Completed",
          "format": "number"
        },
        {
          "key": "attendance_percent",
          "label": "Attendance %",
          "format": "percent"
        },
        {
          "key": "completion_percent",
          "label": "Completion %",
          "format": "percent"
        }
      ],
      "rows": [
        { "ministry": "Life Class", "registered": 20, "attended": 16, "completed": null, "attendance_percent": 80.0, "completion_percent": null },
        { "ministry": "School of Leaders", "registered": null, "attended": null, "completed": null, "attendance_percent": null, "completion_percent": null },
        { "ministry": "Plan 40", "registered": null, "attended": null, "completed": null, "attendance_percent": null, "completion_percent": null },
        { "ministry": "School Cell", "registered": null, "attended": 12, "completed": null, "attendance_percent": null, "completion_percent": null }
      ]
    },
    {
      "key": "cells",
      "title": "Cells",
      "columns": [
        {
          "key": "name",
          "label": "Cell",
          "format": null
        },
        {
          "key": "leader",
          "label": "Leader",
          "format": null
        },
        {
          "key": "attendance",
          "label": "Attendance",
          "format": "number"
        },
        {
          "key": "location",
          "label": "Campus",
          "format": null
        }
      ],
      "rows": [
        {
          "name": "Central Cell",
          "leader": "Alex Leader",
          "attendance": 24,
          "location": "North"
        }
      ]
    }
  ],
  "series": [
    {
      "key": "attendance_trend",
      "title": "Attendance Trend",
      "type": "line",
      "points": [
        {
          "x": "2025-02-01",
          "y": 24,
          "group": null
        }
      ]
    },
    {
      "key": "campus_comparison",
      "title": "Campus Comparison",
      "type": "bar",
      "points": [
        {
          "x": "North",
          "y": 24,
          "group": "campus-north"
        }
      ]
    },
    {
      "key": "active_cells_trend",
      "title": "Active Cells Trend",
      "type": "line",
      "points": [
        {
          "x": "2025-02-01",
          "y": 2,
          "group": null
        }
      ]
    }
  ]
}
```

**Errors:** `401`, `403`, `422`; `501` N/A; `500` calculation failure.

## GET /reporting/reports/{report_type}

**Params:** `report_type` is `twelve_tasks`, `cells_graph`, `life_class`, `school_of_leaders`, `plan_40`, `staff_interns_youth`, `school_cell`, or `service_target`; shared `period`, `start_date`, `end_date`, and optional `campus` as above. **Roles:** all reporting roles above.

Example request: `GET /reporting/reports/twelve_tasks?period=2025-02`

Example success response generated from mocked helper data and validated by `ReportEnvelope`:

```json
{
  "meta": {
    "report_type": "twelve_tasks",
    "title": "Twelve Tasks",
    "period": {
      "start": "2025-02-01",
      "end": "2025-02-28",
      "label": "February 2025",
      "token": "2025-02"
    },
    "campus": null,
    "generated_at": "2026-10-05T12:06:48.545922Z"
  },
  "kpis": [
    {
      "key": "completed_tasks",
      "label": "Completed Tasks",
      "value": 4,
      "delta": null,
      "delta_label": null,
      "format": "number",
      "hint": null
    },
    {
      "key": "tracked_people",
      "label": "Tracked People",
      "value": 1,
      "delta": null,
      "delta_label": null,
      "format": "number",
      "hint": null
    }
  ],
  "tables": [
    {
      "key": "twelve_tasks",
      "title": "Twelve Tasks",
      "columns": [
        {
          "key": "name",
          "label": "Name",
          "format": null
        },
        {
          "key": "last_week",
          "label": "Last Week",
          "format": "number"
        },
        {
          "key": "target",
          "label": "Target",
          "format": "number"
        },
        {
          "key": "this_week",
          "label": "This Week",
          "format": "number"
        }
      ],
      "rows": [
        {
          "person_id": "person-1",
          "name": "Alex Leader",
          "last_week": 2,
          "target": 3,
          "this_week": 4
        }
      ]
    }
  ],
  "series": []
}
```

Example `501` response for a known unimplemented report:

```json
{
  "detail": {
    "code": "not_implemented",
    "report_type": "life_class",
    "message": "The life_class report is not implemented."
  }
}
```

**Errors:** `401`, `403`, `422`, `501` known report not implemented, `500` calculation failure; `404` unknown report type.

## GET /reporting/reports/{report_type}/export

**Params:** `report_type` as above; required `format` is `csv`, `xlsx`, or `pdf`; shared `period`, `start_date`, `end_date`, optional `campus`. **Roles:** all reporting roles above.

Example request: `GET /reporting/reports/twelve_tasks/export?format=csv&period=2025-02`

Example CSV success body generated from the CSV route:

```csv
Twelve Tasks

Name,Last Week,Target,This Week

Alex Leader,2,3,4
```

CSV responses use `text/csv` and a `Content-Disposition` filename; XLSX responses use the Office Open XML spreadsheet media type. PDF is not implemented.

**Errors:** `401`, `403`, `422`, `501` known report or PDF export not implemented, `500` export failure; `404` unknown report type.

## POST /reporting/reports/{report_type}/generate

**Params:** `report_type` and shared `period`, `start_date`, `end_date`, optional `campus`; optional `organization` is accepted only for a super-admin and is required for that role. No request body. **Roles:** all reporting roles above. Persists the envelope to `report_runs` and returns the saved record, including its `id`.

Example request: `POST /reporting/reports/twelve_tasks/generate?period=2025-02`

Example success response generated from the route:

```json
{
  "id": "e1b27f2e-7052-4ab5-a5cd-825add094aff",
  "org": "Example Org",
  "report_type": "twelve_tasks",
  "params": {
    "period": "2025-02",
    "start_date": "2025-02-01",
    "end_date": "2025-02-28",
    "campus": null
  },
  "payload": {
    "meta": {
      "report_type": "twelve_tasks",
      "title": "Twelve Tasks",
      "period": {
        "start": "2025-02-01",
        "end": "2025-02-28",
        "label": "February 2025",
        "token": "2025-02"
      },
      "campus": null,
      "generated_at": "2026-10-05T12:06:48.615219Z"
    },
    "kpis": [
      {
        "key": "completed_tasks",
        "label": "Completed Tasks",
        "value": 4,
        "delta": null,
        "delta_label": null,
        "format": "number",
        "hint": null
      },
      {
        "key": "tracked_people",
        "label": "Tracked People",
        "value": 1,
        "delta": null,
        "delta_label": null,
        "format": "number",
        "hint": null
      }
    ],
    "tables": [
      {
        "key": "twelve_tasks",
        "title": "Twelve Tasks",
        "columns": [
          {
            "key": "name",
            "label": "Name",
            "format": null
          },
          {
            "key": "last_week",
            "label": "Last Week",
            "format": "number"
          },
          {
            "key": "target",
            "label": "Target",
            "format": "number"
          },
          {
            "key": "this_week",
            "label": "This Week",
            "format": "number"
          }
        ],
        "rows": [
          {
            "person_id": "person-1",
            "name": "Alex Leader",
            "last_week": 2,
            "target": 3,
            "this_week": 4
          }
        ]
      }
    ],
    "series": []
  },
  "generated_by": "admin-1",
  "created_at": "2026-10-05T12:00:00Z"
}
```

**Errors:** `401`, `403`, `422`, `501` known report not implemented, `500` calculation/persistence failure; `404` unknown report type.

## GET /reporting/history

**Params:** optional `limit`, integer 1-200, default 50; optional `organization`, required for super-admin and must equal the caller's Organization for everyone else. All queries filter by the resolved organization. **Roles:** all reporting roles above.

Example request: `GET /reporting/history?limit=10`

Example success response generated from the route:

```json
[
  {
    "id": "e1b27f2e-7052-4ab5-a5cd-825add094aff",
    "org": "Example Org",
    "report_type": "twelve_tasks",
    "params": {
      "period": "2025-02",
      "start_date": "2025-02-01",
      "end_date": "2025-02-28",
      "campus": null
    },
    "payload": {
      "meta": {
        "report_type": "twelve_tasks",
        "title": "Twelve Tasks",
        "period": {
          "start": "2025-02-01",
          "end": "2025-02-28",
          "label": "February 2025",
          "token": "2025-02"
        },
        "campus": null,
        "generated_at": "2026-10-05T12:06:48.615219Z"
      },
      "kpis": [
        {
          "key": "completed_tasks",
          "label": "Completed Tasks",
          "value": 4,
          "delta": null,
          "delta_label": null,
          "format": "number",
          "hint": null
        },
        {
          "key": "tracked_people",
          "label": "Tracked People",
          "value": 1,
          "delta": null,
          "delta_label": null,
          "format": "number",
          "hint": null
        }
      ],
      "tables": [
        {
          "key": "twelve_tasks",
          "title": "Twelve Tasks",
          "columns": [
            {
              "key": "name",
              "label": "Name",
              "format": null
            },
            {
              "key": "last_week",
              "label": "Last Week",
              "format": "number"
            },
            {
              "key": "target",
              "label": "Target",
              "format": "number"
            },
            {
              "key": "this_week",
              "label": "This Week",
              "format": "number"
            }
          ],
          "rows": [
            {
              "person_id": "person-1",
              "name": "Alex Leader",
              "last_week": 2,
              "target": 3,
              "this_week": 4
            }
          ]
        }
      ],
      "series": []
    },
    "generated_by": "admin-1",
    "created_at": "2026-10-05T12:00:00Z"
  }
]
```

**Errors:** `401`, `403`, `422` invalid limit, `501` N/A, `500` history query failure.

## GET /reporting/history/{run_id}

**Params:** `run_id` is a report run UUID; optional `organization`, required for super-admin and must equal the caller's Organization for everyone else. All queries filter by the resolved organization. **Roles:** all reporting roles above; organization-scoped users can only retrieve their Organization's runs.

Example request: `GET /reporting/history/e1b27f2e-7052-4ab5-a5cd-825add094aff`

Example success response generated from the route:

```json
{
  "id": "e1b27f2e-7052-4ab5-a5cd-825add094aff",
  "org": "Example Org",
  "report_type": "twelve_tasks",
  "params": {
    "period": "2025-02",
    "start_date": "2025-02-01",
    "end_date": "2025-02-28",
    "campus": null
  },
  "payload": {
    "meta": {
      "report_type": "twelve_tasks",
      "title": "Twelve Tasks",
      "period": {
        "start": "2025-02-01",
        "end": "2025-02-28",
        "label": "February 2025",
        "token": "2025-02"
      },
      "campus": null,
      "generated_at": "2026-10-05T12:06:48.615219Z"
    },
    "kpis": [
      {
        "key": "completed_tasks",
        "label": "Completed Tasks",
        "value": 4,
        "delta": null,
        "delta_label": null,
        "format": "number",
        "hint": null
      },
      {
        "key": "tracked_people",
        "label": "Tracked People",
        "value": 1,
        "delta": null,
        "delta_label": null,
        "format": "number",
        "hint": null
      }
    ],
    "tables": [
      {
        "key": "twelve_tasks",
        "title": "Twelve Tasks",
        "columns": [
          {
            "key": "name",
            "label": "Name",
            "format": null
          },
          {
            "key": "last_week",
            "label": "Last Week",
            "format": "number"
          },
          {
            "key": "target",
            "label": "Target",
            "format": "number"
          },
          {
            "key": "this_week",
            "label": "This Week",
            "format": "number"
          }
        ],
        "rows": [
          {
            "person_id": "person-1",
            "name": "Alex Leader",
            "last_week": 2,
            "target": 3,
            "this_week": 4
          }
        ]
      }
    ],
    "series": []
  },
  "generated_by": "admin-1",
  "created_at": "2026-10-05T12:00:00Z"
}
```

**Errors:** `401`, `403`, `422` invalid id format not enforced (missing IDs return `404`), `501` N/A, `500` history query failure; `404` missing or out-of-scope run.

## Compatibility and Setup

Existing `/stats/*` contracts and `/report-schedules` remain available. Schedule IDs are translated to the established stored report type names. Apply `migration_reporting_core.sql` in the Supabase project's **SQL Editor** (or run it with `psql` against that same project's database) before calling generate/history. The migration is idempotent and is not applied by this generator.
