from supabase_helpers.twelve_tasks import sb_get_twelve_tasks_report, _period_range
from unittest.mock import MagicMock, patch
from datetime import timedelta

with patch("supabase_helpers.twelve_tasks.supabase") as mock_supabase:
    start, end = _period_range("thisWeek")
    print(f"start={start}, end={end}")
    
    # Check what the query looks like
    task_q = (
        mock_supabase.table("Tasks")
        .select("_id, name, taskType, status, followup_date, completedAt, created_at, assignedfor, assigned_to_email")
        .or_(
            f"followup_date.gte.{start.isoformat()},"
            f"completedAt.gte.{start.isoformat()},"
            f"created_at.gte.{start.isoformat()}"
        )
    )
    print(f"task_q type: {type(task_q)}")
    print(f"task_q.columns: {task_q.columns if hasattr(task_q, 'columns') else 'N/A'}")
    
    # Try executing
    result = task_q.execute()
    print(f"execute result: {result}")
    print(f"execute result data: {result.data if hasattr(result, 'data') else 'N/A'}")
    
    # Now try with our mock data approach
    mock_cursor_current = MagicMock()
    mock_cursor_current.execute.return_value.data = [
        {"assignedfor": "leader1@example.com", "followup_date": start.isoformat()},
    ]
    
    mock_cursor_previous = MagicMock()
    mock_cursor_previous.execute.return_value.data = []
    
    with patch.object(
        mock_supabase.table.return_value.select.return_value.or_,
        "execute",
        side_effect=[mock_cursor_current, mock_cursor_previous],
    ):
        result2 = sb_get_twelve_tasks_report(period="thisWeek")
    
    print(f"result2 leaders: {result2.get('leaders', [])}")