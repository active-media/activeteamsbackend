"""
Comprehensive pytest suite for stats_service.py

Tests cover:
- Period range calculations
- Dashboard comprehensive stats
- Dashboard quick stats
- Outstanding items
- People capture stats
- Error handling and edge cases
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
import asyncio

# Import functions to test
from services.stats_service import (
    get_period_range,
    get_dashboard_comprehensive,
    get_dashboard_quick_stats,
    get_outstanding_items,
    get_people_capture_stats,
    _fetch_overdue_cells,
    _fetch_tasks_by_user,
    _fetch_users,
    _fetch_task_types,
    _format_cells,
    _create_user_map,
    _process_task_groups,
    _build_overview,
    _fetch_task_counts,
    _fetch_consolidation_counts,
    _fetch_overdue_cells_count,
    _fetch_task_type_breakdown,
    EXCLUDED_TASK_TYPES_FROM_COMPLETED
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def mock_current_user():
    """Mock authenticated user"""
    return {
        "_id": "user123",
        "email": "test@example.com",
        "name": "Test",
        "surname": "User"
    }


@pytest.fixture
def sample_tasks():
    """Sample task data for testing"""
    return [
        {
            "_id": "task1",
            "name": "Follow up call",
            "taskType": "consolidation",
            "status": "completed",
            "assignedfor": "user1@example.com",
            "followup_date": datetime(2024, 12, 15),
            "completedAt": datetime(2024, 12, 16),
            "createdAt": datetime(2024, 12, 10),
            "priority": 1,
            "is_excluded_type": False
        },
        {
            "_id": "task2",
            "name": "No answer follow up",
            "taskType": "no answer",
            "status": "completed",
            "assignedfor": "user1@example.com",
            "followup_date": datetime(2024, 12, 14),
            "completedAt": datetime(2024, 12, 15),
            "createdAt": datetime(2024, 12, 10),
            "priority": 1,
            "is_excluded_type": True
        },
        {
            "_id": "task3",
            "name": "Pending task",
            "taskType": "consolidation",
            "status": "pending",
            "assignedfor": "user2@example.com",
            "followup_date": datetime(2024, 12, 18),
            "createdAt": datetime(2024, 12, 10),
            "priority": 2,
            "is_excluded_type": False
        }
    ]


@pytest.fixture
def sample_users():
    """Sample user data for testing"""
    return [
        {
            "_id": "user1",
            "email": "user1@example.com",
            "name": "John",
            "surname": "Doe"
        },
        {
            "_id": "user2",
            "email": "user2@example.com",
            "name": "Jane",
            "surname": "Smith"
        }
    ]


@pytest.fixture
def sample_cells():
    """Sample cell event data for testing"""
    return [
        {
            "_id": "cell1",
            "Event Name": "Prayer Cell",
            "Event Type": "Cells",
            "Leader": "John Doe",
            "Email": "john@example.com",
            "date": datetime(2024, 12, 10),
            "status": "incomplete",
            "attendees": ["person1", "person2", "person3"]
        },
        {
            "_id": "cell2",
            "eventName": "Bible Study",
            "eventType": "Cells",
            "eventLeaderName": "Jane Smith",
            "eventLeaderEmail": "jane@example.com",
            "date": datetime(2024, 12, 12),
            "Status": "Incomplete",
            "attendees": ["person4", "person5"]
        }
    ]


@pytest.fixture
def sample_task_groups():
    """Sample grouped task data from aggregation"""
    return [
        {
            "_id": "user1@example.com",
            "tasks": [
                {
                    "_id": "task1",
                    "name": "Task 1",
                    "taskType": "consolidation",
                    "status": "completed",
                    "is_completed": True,
                    "is_due_in_period": True,
                    "completed_in_period": True,
                    "is_excluded_type": False,
                    "followup_date": datetime(2024, 12, 15)
                },
                {
                    "_id": "task2",
                    "name": "Task 2",
                    "taskType": "no answer",
                    "status": "completed",
                    "is_completed": False,
                    "is_due_in_period": True,
                    "completed_in_period": False,
                    "is_excluded_type": True,
                    "followup_date": datetime(2024, 12, 14)
                }
            ],
            "total_tasks": 2,
            "completed_tasks": 1,
            "completed_in_period": 1,
            "due_in_period": 2
        }
    ]


# ============================================================================
# TEST: Period Range Calculations
# ============================================================================

class TestGetPeriodRange:
    """Test date range calculations for different periods"""
    
    def test_today_period(self):
        """Test 'today' period returns correct date range"""
        start, end = get_period_range("today")
        
        now = datetime.utcnow()
        assert start.date() == now.date()
        assert end.date() == now.date()
        assert start.hour == 0
        assert end.hour == 23
    
    def test_this_week_period(self):
        """Test 'thisWeek' period returns Monday to Sunday"""
        start, end = get_period_range("thisWeek")
        
        # Start should be Monday
        assert start.weekday() == 0
        
        # End should be Sunday
        assert end.weekday() == 6
        
        # Should be same week
        assert (end - start).days == 6
    
    def test_this_month_period(self):
        """Test 'thisMonth' period returns first to last day of month"""
        start, end = get_period_range("thisMonth")
        
        now = datetime.utcnow()
        assert start.day == 1
        assert start.month == now.month
        assert start.year == now.year
        
        # End should be last day of month
        if now.month == 12:
            expected_end = datetime(now.year + 1, 1, 1) - timedelta(microseconds=1)
        else:
            expected_end = datetime(now.year, now.month + 1, 1) - timedelta(microseconds=1)
        
        assert end.date() == expected_end.date()
    
    def test_previous_7_days(self):
        """Test 'previous7' period returns last 7 days"""
        start, end = get_period_range("previous7")
        
        now = datetime.utcnow()
        yesterday = now - timedelta(days=1)
        
        # End should be yesterday
        assert end.date() == yesterday.date()
        
        # Should span 7 days
        assert (end.date() - start.date()).days == 6
    
    def test_previous_week_period(self):
        """Test 'previousWeek' period returns last week Monday to Sunday"""
        start, end = get_period_range("previousWeek")
        
        # Should be Monday to Sunday
        assert start.weekday() == 0
        assert end.weekday() == 6
        
        # Should be 7 days
        assert (end - start).days == 6
        
        # Should be in the past
        now = datetime.utcnow()
        assert end < now
    
    def test_previous_month_period(self):
        """Test 'previousMonth' period returns last month"""
        start, end = get_period_range("previousMonth")
        
        now = datetime.utcnow()
        expected_month = now.month - 1 if now.month > 1 else 12
        expected_year = now.year if now.month > 1 else now.year - 1
        
        assert start.month == expected_month
        assert start.year == expected_year
        assert start.day == 1
    
    def test_invalid_period_raises_error(self):
        """Test that invalid period raises ValueError"""
        with pytest.raises(ValueError, match="Invalid period"):
            get_period_range("invalid_period")


# ============================================================================
# TEST: Data Fetching Functions
# ============================================================================

class TestFetchOverdueCells:
    """Test fetching overdue cell events"""
    
    @pytest.mark.asyncio
    async def test_fetch_overdue_cells_success(self, sample_cells):
        """Test successful fetch of overdue cells"""
        with patch('services.stats_service.events_collection') as mock_collection:
            # Mock the aggregation pipeline
            mock_cursor = AsyncMock()
            mock_cursor.to_list = AsyncMock(return_value=sample_cells)
            mock_collection.aggregate.return_value = mock_cursor
            
            result = await _fetch_overdue_cells(datetime(2024, 12, 18))
            
            assert len(result) == 2
            assert result == sample_cells
    
    @pytest.mark.asyncio
    async def test_fetch_overdue_cells_empty(self):
        """Test fetch when no overdue cells exist"""
        with patch('services.stats_service.events_collection') as mock_collection:
            mock_cursor = AsyncMock()
            mock_cursor.to_list = AsyncMock(return_value=[])
            mock_collection.aggregate.return_value = mock_cursor
            
            result = await _fetch_overdue_cells(datetime(2024, 12, 18))
            
            assert result == []
    
    @pytest.mark.asyncio
    async def test_fetch_overdue_cells_error_handling(self):
        """Test that errors are caught and empty list returned"""
        with patch('services.stats_service.events_collection') as mock_collection:
            mock_collection.aggregate.side_effect = Exception("Database error")
            
            result = await _fetch_overdue_cells(datetime(2024, 12, 18))
            
            # Should return empty list instead of raising
            assert result == []


class TestFetchTasksByUser:
    """Test fetching tasks grouped by user"""
    
    @pytest.mark.asyncio
    async def test_fetch_tasks_by_user_success(self, sample_task_groups):
        """Test successful fetch of tasks by user"""
        with patch('services.stats_service.tasks_collection') as mock_collection:
            mock_cursor = AsyncMock()
            mock_cursor.to_list = AsyncMock(return_value=sample_task_groups)
            mock_collection.aggregate.return_value = mock_cursor
            
            start = datetime(2024, 12, 1)
            end = datetime(2024, 12, 31)
            result = await _fetch_tasks_by_user(start, end)
            
            assert len(result) == 1
            assert result[0]["_id"] == "user1@example.com"
            assert result[0]["total_tasks"] == 2
    
    @pytest.mark.asyncio
    async def test_fetch_tasks_by_user_error_handling(self):
        """Test error handling returns empty list"""
        with patch('services.stats_service.tasks_collection') as mock_collection:
            mock_collection.aggregate.side_effect = Exception("Database error")
            
            start = datetime(2024, 12, 1)
            end = datetime(2024, 12, 31)
            result = await _fetch_tasks_by_user(start, end)
            
            assert result == []


class TestFetchUsers:
    """Test fetching user details"""
    
    @pytest.mark.asyncio
    async def test_fetch_users_success(self, sample_users):
        """Test successful user fetch"""
        with patch('services.stats_service.users_collection') as mock_collection:
            mock_cursor = AsyncMock()
            mock_cursor.to_list = AsyncMock(return_value=sample_users)
            mock_cursor.limit = MagicMock(return_value=mock_cursor)
            mock_collection.find.return_value = mock_cursor
            
            result = await _fetch_users(100)
            
            assert len(result) == 2
            assert result[0]["email"] == "user1@example.com"
    
    @pytest.mark.asyncio
    async def test_fetch_users_error_handling(self):
        """Test error handling returns empty list"""
        with patch('services.stats_service.users_collection') as mock_collection:
            mock_collection.find.side_effect = Exception("Database error")
            
            result = await _fetch_users(100)
            
            assert result == []


class TestFetchTaskTypes:
    """Test fetching task type names"""
    
    @pytest.mark.asyncio
    async def test_fetch_task_types_success(self):
        """Test successful fetch of task types"""
        mock_task_types = [
            {"name": "consolidation"},
            {"name": "follow up"},
            {"name": "no answer"}
        ]
        
        with patch('services.stats_service.tasktypes_collection') as mock_collection:
            mock_cursor = AsyncMock()
            mock_cursor.to_list = AsyncMock(return_value=mock_task_types)
            mock_collection.find.return_value = mock_cursor
            
            result = await _fetch_task_types()
            
            assert len(result) == 3
            assert "consolidation" in result
            assert "no answer" in result
    
    @pytest.mark.asyncio
    async def test_fetch_task_types_error_handling(self):
        """Test error handling returns empty list"""
        with patch('services.stats_service.tasktypes_collection') as mock_collection:
            mock_collection.find.side_effect = Exception("Database error")
            
            result = await _fetch_task_types()
            
            assert result == []


# ============================================================================
# TEST: Data Processing Functions
# ============================================================================

class TestFormatCells:
    """Test cell data formatting"""
    
    def test_format_cells_success(self, sample_cells):
        """Test successful formatting of cells"""
        result = _format_cells(sample_cells)
        
        assert len(result) == 2
        assert isinstance(result[0]["_id"], str)
        assert isinstance(result[0]["date"], str)
    
    def test_format_cells_handles_malformed_data(self):
        """Test that malformed cells are skipped"""
        cells = [
            {"_id": "valid1", "date": datetime(2024, 12, 10)},
            {"_id": None},  # Malformed - no valid _id
            {"_id": "valid2", "date": datetime(2024, 12, 11)}
        ]
        
        result = _format_cells(cells)
        
        # Should skip the malformed cell
        assert len(result) == 2


class TestCreateUserMap:
    """Test user map creation"""
    
    def test_create_user_map_success(self, sample_users):
        """Test successful user map creation"""
        result = _create_user_map(sample_users)
        
        # Should have entries for both email and ID
        assert "user1@example.com" in result
        assert "user1" in result
        assert result["user1@example.com"]["fullName"] == "John Doe"
    
    def test_create_user_map_handles_missing_email(self):
        """Test that users without email are skipped"""
        users = [
            {"_id": "user1", "email": "valid@example.com", "name": "Valid"},
            {"_id": "user2", "name": "No Email"},  # Missing email
        ]
        
        result = _create_user_map(users)
        
        # Only valid user should be in map
        assert "valid@example.com" in result
        assert "user2" not in result


class TestProcessTaskGroups:
    """Test task group processing"""
    
    def test_process_task_groups_success(self, sample_task_groups, sample_users):
        """Test successful processing of task groups"""
        user_map = _create_user_map(sample_users)
        
        grouped_tasks, task_type_stats, global_stats = _process_task_groups(
            sample_task_groups, user_map
        )
        
        assert len(grouped_tasks) == 1
        assert grouped_tasks[0]["totalCount"] == 2
        assert grouped_tasks[0]["completedCount"] == 1
        
        # Check global stats
        assert global_stats["total_tasks"] == 2
        assert global_stats["completed_tasks"] == 1
        
        # Check task type stats
        assert "consolidation" in task_type_stats
        assert "no answer" in task_type_stats
    
    def test_process_task_groups_excluded_types(self, sample_task_groups, sample_users):
        """Test that excluded task types are handled correctly"""
        user_map = _create_user_map(sample_users)
        
        grouped_tasks, task_type_stats, global_stats = _process_task_groups(
            sample_task_groups, user_map
        )
        
        # "no answer" should be marked as excluded
        assert task_type_stats["no answer"]["is_excluded"] == True
        assert task_type_stats["consolidation"]["is_excluded"] == False


class TestBuildOverview:
    """Test overview statistics building"""
    
    def test_build_overview_success(self, sample_cells, sample_users):
        """Test successful overview building"""
        formatted_cells = _format_cells(sample_cells)
        user_map = _create_user_map(sample_users)
        
        global_stats = {
            "total_tasks": 10,
            "completed_tasks": 7,
            "completed_in_period": 5,
            "due_in_period": 8,
            "incomplete_due": 3
        }
        
        task_type_stats = {
            "consolidation": {
                "total": 5,
                "completed": 4,
                "completed_in_period": 3
            }
        }
        
        grouped_tasks = []
        
        result = _build_overview(
            formatted_cells=formatted_cells,
            global_stats=global_stats,
            task_type_stats=task_type_stats,
            grouped_tasks=grouped_tasks,
            users=sample_users,
            all_task_types=["consolidation", "follow up"]
        )
        
        assert result["total_tasks_in_period"] == 10
        assert result["tasks_completed_in_period"] == 5
        assert result["completion_rate_due_tasks"] == 62.5  # 5/8 * 100
        assert result["outstanding_cells"] == 2
    
    def test_build_overview_handles_division_by_zero(self):
        """Test that division by zero is handled gracefully"""
        result = _build_overview(
            formatted_cells=[],
            global_stats={
                "total_tasks": 0,
                "completed_tasks": 0,
                "completed_in_period": 0,
                "due_in_period": 0,
                "incomplete_due": 0
            },
            task_type_stats={},
            grouped_tasks=[],
            users=[],
            all_task_types=[]
        )
        
        # Should return 0 instead of raising division error
        assert result["completion_rate_due_tasks"] == 0
        assert result["completion_rate_overall"] == 0


# ============================================================================
# TEST: Dashboard Comprehensive
# ============================================================================

class TestGetDashboardComprehensive:
    """Test comprehensive dashboard statistics"""
    
    @pytest.mark.asyncio
    async def test_dashboard_comprehensive_success(
        self, mock_current_user, sample_cells, sample_task_groups, 
        sample_users
    ):
        """Test successful comprehensive dashboard fetch"""
        with patch('services.stats_service._fetch_overdue_cells', new_callable=AsyncMock) as mock_cells, \
             patch('services.stats_service._fetch_tasks_by_user', new_callable=AsyncMock) as mock_tasks, \
             patch('services.stats_service._fetch_users', new_callable=AsyncMock) as mock_users, \
             patch('services.stats_service._fetch_task_types', new_callable=AsyncMock) as mock_types:
            
            mock_cells.return_value = sample_cells
            mock_tasks.return_value = sample_task_groups
            mock_users.return_value = sample_users
            mock_types.return_value = ["consolidation", "follow up"]
            
            result = await get_dashboard_comprehensive(
                period="today",
                limit=100,
                current_user=mock_current_user
            )
            
            assert "overview" in result
            assert "overdueCells" in result
            assert "groupedTasks" in result
            assert "allUsers" in result
            assert result["period"] == "today"
    
    @pytest.mark.asyncio
    async def test_dashboard_comprehensive_handles_errors(self, mock_current_user):
        """Test that dashboard handles partial failures gracefully"""
        with patch('services.stats_service._fetch_overdue_cells', new_callable=AsyncMock) as mock_cells, \
             patch('services.stats_service._fetch_tasks_by_user', new_callable=AsyncMock) as mock_tasks, \
             patch('services.stats_service._fetch_users', new_callable=AsyncMock) as mock_users, \
             patch('services.stats_service._fetch_task_types', new_callable=AsyncMock) as mock_types:
            
            # One fetch fails, others succeed
            mock_cells.side_effect = Exception("Cell fetch failed")
            mock_tasks.return_value = []
            mock_users.return_value = []
            mock_types.return_value = []
            
            result = await get_dashboard_comprehensive(
                period="today",
                limit=100,
                current_user=mock_current_user
            )
            
            # Should still return valid response with empty data
            assert "overview" in result
            assert result["overdueCells"] == []


# ============================================================================
# TEST: Dashboard Quick Stats
# ============================================================================

class TestGetDashboardQuickStats:
    """Test quick dashboard statistics"""
    
    @pytest.mark.asyncio
    async def test_quick_stats_success(self, mock_current_user):
        """Test successful quick stats fetch"""
        with patch('services.stats_service._fetch_task_counts', new_callable=AsyncMock) as mock_counts, \
             patch('services.stats_service._fetch_consolidation_counts', new_callable=AsyncMock) as mock_consol, \
             patch('services.stats_service._fetch_overdue_cells_count', new_callable=AsyncMock) as mock_cells, \
             patch('services.stats_service._fetch_task_type_breakdown', new_callable=AsyncMock) as mock_breakdown, \
             patch('services.stats_service._fetch_excluded_task_counts', new_callable=AsyncMock) as mock_excluded:
            
            mock_counts.return_value = {
                "total_tasks": 50,
                "tasks_due": 20,
                "tasks_completed_in_period": 15,
                "total_completed": 30
            }
            mock_consol.return_value = {
                "total": 10,
                "completed": 8,
                "completed_in_period": 6
            }
            mock_cells.return_value = 5
            mock_breakdown.return_value = {
                "consolidation": {"total": 10, "completed": 8}
            }
            mock_excluded.return_value = {"no_answer": 2, "awaiting_call": 1}
            
            result = await get_dashboard_quick_stats(
                period="today",
                current_user=mock_current_user
            )
            
            assert result["taskCount"] == 50
            assert result["tasksDueInPeriod"] == 20
            assert result["tasksCompletedInPeriod"] == 15
            assert result["completionRateDueTasks"] == 75.0  # 15/20 * 100
            assert result["overdueCells"] == 5
    
    @pytest.mark.asyncio
    async def test_quick_stats_handles_errors(self, mock_current_user):
        """Test that quick stats handles errors gracefully"""
        with patch('services.stats_service._fetch_task_counts', new_callable=AsyncMock) as mock_counts:
            mock_counts.side_effect = Exception("Fetch failed")
            
            # Should raise HTTPException for critical errors
            with pytest.raises(HTTPException):
                await get_dashboard_quick_stats(
                    period="today",
                    current_user=mock_current_user
                )


# ============================================================================
# TEST: Outstanding Items
# ============================================================================

class TestGetOutstandingItems:
    """Test outstanding items fetch"""
    
    @pytest.mark.asyncio
    async def test_outstanding_items_success(self):
        """Test successful fetch of outstanding items"""
        sample_cells = [
            {
                "eventLeader": "John Doe",
                "location": "Location A",
                "eventName": "Cell Meeting",
                "date": datetime(2024, 12, 15),
                "status": "pending"
            }
        ]
        
        sample_tasks = [
            {
                "assignedTo": "Jane Smith",
                "email": "jane@example.com",
                "taskName": "Follow up",
                "priority": 2,
                "dueDate": datetime(2024, 12, 20),
                "status": "pending"
            }
        ]
        
        with patch('services.stats_service.events_collection') as mock_events, \
             patch('services.stats_service.tasks_collection') as mock_tasks_coll:
            
            mock_events_cursor = AsyncMock()
            mock_events_cursor.to_list = AsyncMock(return_value=sample_cells)
            mock_events.find.return_value = mock_events_cursor
            
            mock_tasks_cursor = AsyncMock()
            mock_tasks_cursor.to_list = AsyncMock(return_value=sample_tasks)
            mock_tasks_coll.find.return_value = mock_tasks_cursor
            
            result = await get_outstanding_items()
            
            assert "outstanding_cells" in result
            assert "outstanding_tasks" in result
            assert len(result["outstanding_cells"]) == 1
            assert len(result["outstanding_tasks"]) == 1
    
    @pytest.mark.asyncio
    async def test_outstanding_items_error_handling(self):
        """Test error handling in outstanding items"""
        with patch('services.stats_service.events_collection') as mock_events:
            mock_events.find.side_effect = Exception("Database error")
            
            with pytest.raises(HTTPException) as exc_info:
                await get_outstanding_items()
            
            assert exc_info.value.status_code == 500


# ============================================================================
# TEST: People Capture Stats
# ============================================================================

class TestGetPeopleCaptureStats:
    """Test people capture statistics"""
    
    @pytest.mark.asyncio
    async def test_people_capture_stats_success(self):
        """Test successful fetch of capture statistics"""
        mock_results = [
            {
                "capturer_id": "user1",
                "capturer_name": "John Doe",
                "capturer_email": "john@example.com",
                "people_captured_count": 5,
                "captured_people": [
                    {"name": "Person 1", "email": "p1@example.com"},
                    {"name": "Person 2", "email": "p2@example.com"}
                ]
            }
        ]
        
        with patch('services.stats_service.db') as mock_db:
            mock_cursor = AsyncMock()
            mock_cursor.to_list = AsyncMock(return_value=mock_results)
            mock_db.people.aggregate.return_value = mock_cursor
            
            result = await get_people_capture_stats()
            
            assert "capture_stats" in result
            assert result["total_capturers"] == 1
            assert result["total_people_captured"] == 5
    
    @pytest.mark.asyncio
    async def test_people_capture_stats_no_data(self):
        """Test when no capture data exists"""
        with patch('services.stats_service.db') as mock_db:
            mock_cursor = AsyncMock()
            mock_cursor.to_list = AsyncMock(return_value=[])
            mock_db.people.aggregate.return_value = mock_cursor
            
            result = await get_people_capture_stats()
            
            assert result["total_capturers"] == 0
            assert result["total_people_captured"] == 0
            assert result["message"] == "No capture data found"
    
    @pytest.mark.asyncio
    async def test_people_capture_stats_error_handling(self):
        """Test error handling in capture stats"""
        with patch('services.stats_service.db') as mock_db:
            mock_db.people.aggregate.side_effect = Exception("Database error")
            
            with pytest.raises(HTTPException) as exc_info:
                await get_people_capture_stats()
            
            assert exc_info.value.status_code == 500


# ============================================================================
# TEST: Excluded Task Types
# ============================================================================

class TestExcludedTaskTypes:
    """Test that excluded task types are handled correctly"""
    
    def test_excluded_types_constant(self):
        """Test that excluded types constant is defined correctly"""
        assert "no answer" in EXCLUDED_TASK_TYPES_FROM_COMPLETED
        assert "Awaiting Call" in EXCLUDED_TASK_TYPES_FROM_COMPLETED
    
    @pytest.mark.asyncio
    async def test_excluded_types_not_counted_in_completed(self, sample_task_groups, sample_users):
        """Test that excluded types don't count toward completion"""
        user_map = _create_user_map(sample_users)
        
        grouped_tasks, task_type_stats, global_stats = _process_task_groups(
            sample_task_groups, user_map
        )
        
        # "no answer" task should be excluded from completed count
        # Even though it has status "completed", is_completed should be False
        no_answer_stats = task_type_stats.get("no answer", {})
        assert no_answer_stats.get("is_excluded") == True


# ============================================================================
# TEST: Integration Tests
# ============================================================================

class TestIntegration:
    """Integration tests for complete workflows"""
    
    @pytest.mark.asyncio
    async def test_full_dashboard_workflow(self, mock_current_user):
        """Test complete dashboard data flow"""
        # This would be a more complex test that mocks the entire chain
        # from endpoint to database and back
        pass
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self, mock_current_user):
        """Test handling multiple concurrent requests"""
        with patch('services.stats_service._fetch_task_counts', new_callable=AsyncMock) as mock_counts, \
             patch('services.stats_service._fetch_consolidation_counts', new_callable=AsyncMock) as mock_consol, \
             patch('services.stats_service._fetch_overdue_cells_count', new_callable=AsyncMock) as mock_cells, \
             patch('services.stats_service._fetch_task_type_breakdown', new_callable=AsyncMock) as mock_breakdown, \
             patch('services.stats_service._fetch_excluded_task_counts', new_callable=AsyncMock) as mock_excluded:
            
            mock_counts.return_value = {
                "total_tasks": 50,
                "tasks_due": 20,
                "tasks_completed_in_period": 15,
                "total_completed": 30
            }
            mock_consol.return_value = {"total": 10, "completed": 8, "completed_in_period": 6}
            mock_cells.return_value = 5
            mock_breakdown.return_value = {"consolidation": {"total": 10, "completed": 8}}
            mock_excluded.return_value = {"no_answer": 2, "awaiting_call": 1}
            
            # Make 5 concurrent requests
            tasks = [
                get_dashboard_quick_stats(period="today", current_user=mock_current_user)
                for _ in range(5)
            ]
            
            results = await asyncio.gather(*tasks)
            
            # All should succeed
            assert len(results) == 5
            for result in results:
                assert result["taskCount"] == 50
                assert result["period"] == "today"


# ============================================================================
# TEST: Edge Cases and Boundary Conditions
# ============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions"""
    
    def test_empty_task_list(self):
        """Test processing empty task list"""
        grouped_tasks, task_type_stats, global_stats = _process_task_groups([], {})
        
        assert len(grouped_tasks) == 0
        assert len(task_type_stats) == 0
        assert global_stats["total_tasks"] == 0
    
    def test_task_without_type(self, sample_users):
        """Test handling tasks without taskType field"""
        task_groups = [
            {
                "_id": "user1@example.com",
                "tasks": [
                    {
                        "_id": "task1",
                        "name": "Unnamed task",
                        "taskType": None,  # No type
                        "status": "pending",
                        "is_completed": False,
                        "is_due_in_period": True,
                        "completed_in_period": False,
                        "is_excluded_type": False,
                        "followup_date": datetime(2024, 12, 15)
                    }
                ],
                "total_tasks": 1,
                "completed_tasks": 0,
                "completed_in_period": 0,
                "due_in_period": 1
            }
        ]
        
        user_map = _create_user_map(sample_users)
        grouped_tasks, task_type_stats, global_stats = _process_task_groups(
            task_groups, user_map
        )
        
        # Should be categorized as "Uncategorized"
        assert "Uncategorized" in task_type_stats
    
    def test_user_without_name(self):
        """Test handling users without name fields"""
        users = [
            {
                "_id": "user1",
                "email": "noname@example.com"
                # Missing name and surname
            }
        ]
        
        user_map = _create_user_map(users)
        
        # Should use email prefix as name
        assert "noname@example.com" in user_map
        assert "noname" in user_map["noname@example.com"]["fullName"]
    
    def test_cell_with_no_attendees(self):
        """Test handling cells with no attendees"""
        cells = [
            {
                "_id": "cell1",
                "Event Name": "Empty Cell",
                "date": datetime(2024, 12, 10),
                "attendees": []  # No attendees
            }
        ]
        
        formatted = _format_cells(cells)
        
        assert len(formatted) == 1
        assert formatted[0]["attendees"] == []
    
    def test_task_with_future_completion_date(self):
        """Test handling tasks with future completion dates"""
        # This shouldn't normally happen but we should handle it gracefully
        task_groups = [
            {
                "_id": "user1@example.com",
                "tasks": [
                    {
                        "_id": "task1",
                        "name": "Future completed",
                        "taskType": "consolidation",
                        "status": "completed",
                        "completedAt": datetime(2025, 1, 1),  # Future date
                        "is_completed": True,
                        "is_due_in_period": False,
                        "completed_in_period": False,
                        "is_excluded_type": False
                    }
                ],
                "total_tasks": 1,
                "completed_tasks": 1,
                "completed_in_period": 0,
                "due_in_period": 0
            }
        ]
        
        grouped_tasks, task_type_stats, global_stats = _process_task_groups(
            task_groups, {}
        )
        
        # Should still process correctly
        assert global_stats["total_tasks"] == 1
        assert global_stats["completed_tasks"] == 1
    
    @pytest.mark.asyncio
    async def test_invalid_period_in_endpoint(self, mock_current_user):
        """Test that invalid period is rejected by Query validation"""
        # This would be caught by FastAPI's Query validation before reaching the function
        # But we test the underlying function
        with pytest.raises(ValueError):
            get_period_range("invalid")
    
    def test_very_large_task_count(self):
        """Test handling large numbers of tasks"""
        # Create a large task group
        large_tasks = [
            {
                "_id": f"task{i}",
                "name": f"Task {i}",
                "taskType": "consolidation",
                "status": "completed" if i % 2 == 0 else "pending",
                "is_completed": i % 2 == 0,
                "is_due_in_period": True,
                "completed_in_period": i % 2 == 0,
                "is_excluded_type": False,
                "followup_date": datetime(2024, 12, 15)
            }
            for i in range(1000)
        ]
        
        task_groups = [
            {
                "_id": "user1@example.com",
                "tasks": large_tasks,
                "total_tasks": 1000,
                "completed_tasks": 500,
                "completed_in_period": 500,
                "due_in_period": 1000
            }
        ]
        
        grouped_tasks, task_type_stats, global_stats = _process_task_groups(
            task_groups, {}
        )
        
        assert global_stats["total_tasks"] == 1000
        assert global_stats["completed_tasks"] == 500


# ============================================================================
# TEST: Date and Time Handling
# ============================================================================

class TestDateTimeHandling:
    """Test date and time handling edge cases"""
    
    def test_period_range_year_boundary(self):
        """Test period calculations across year boundaries"""
        with patch('stats_service.datetime') as mock_datetime:
            # Mock current date as Dec 31, 2024
            mock_datetime.utcnow.return_value = datetime(2024, 12, 31, 12, 0, 0)
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            
            start, end = get_period_range("thisMonth")
            
            # Should be December 2024
            assert start.month == 12
            assert start.year == 2024
    
    def test_period_range_leap_year(self):
        """Test period calculations in leap year"""
        with patch('services.stats_service.datetime') as mock_datetime:
            # Mock current date as Feb 29, 2024 (leap year)
            mock_datetime.utcnow.return_value = datetime(2024, 2, 29, 12, 0, 0)
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            
            start, end = get_period_range("thisMonth")
            
            # Should handle leap year correctly
            assert start.month == 2
            assert end.day == 29
    
    def test_timezone_handling(self):
        """Test that UTC is used consistently"""
        start, end = get_period_range("today")
        
        # Both should be timezone-naive UTC times
        assert start.tzinfo is None
        assert end.tzinfo is None


# ============================================================================
# TEST: Task Type Statistics
# ============================================================================

class TestTaskTypeStatistics:
    """Test task type statistics calculations"""
    
    @pytest.mark.asyncio
    async def test_task_type_breakdown_excludes_types(self):
        """Test that excluded types are properly marked"""
        mock_results = [
            {
                "_id": "consolidation",
                "total": 10,
                "completed": 8,
                "completed_in_period": 6,
                "due_in_period": 9,
                "is_excluded": False
            },
            {
                "_id": "no answer",
                "total": 5,
                "completed": 0,  # Should be 0 because excluded
                "completed_in_period": 0,
                "due_in_period": 5,
                "is_excluded": True
            }
        ]
        
        with patch('services.stats_service.tasks_collection') as mock_collection:
            mock_cursor = AsyncMock()
            mock_cursor.to_list = AsyncMock(return_value=mock_results)
            mock_collection.aggregate.return_value = mock_cursor
            
            start = datetime(2024, 12, 1)
            end = datetime(2024, 12, 31)
            result = await _fetch_task_type_breakdown(start, end)
            
            assert result["consolidation"]["is_excluded"] == False
            assert result["no answer"]["is_excluded"] == True
            assert result["consolidation"]["completion_rate"] == 80.0
    
    def test_update_task_type_stats(self):
        """Test updating task type statistics"""
        from services.stats_service import _update_task_type_stats
        
        task_type_stats = {}
        
        task = {
            "taskType": "consolidation",
            "is_excluded_type": False,
            "is_completed": True,
            "completed_in_period": True,
            "is_due_in_period": True
        }
        
        _update_task_type_stats(task, task_type_stats)
        
        assert "consolidation" in task_type_stats
        assert task_type_stats["consolidation"]["total"] == 1
        assert task_type_stats["consolidation"]["completed"] == 1
        assert task_type_stats["consolidation"]["is_excluded"] == False
    
    def test_update_task_type_stats_multiple_calls(self):
        """Test accumulating statistics across multiple tasks"""
        from services.stats_service import _update_task_type_stats
        
        task_type_stats = {}
        
        tasks = [
            {
                "taskType": "consolidation",
                "is_excluded_type": False,
                "is_completed": True,
                "completed_in_period": True,
                "is_due_in_period": True
            },
            {
                "taskType": "consolidation",
                "is_excluded_type": False,
                "is_completed": False,
                "completed_in_period": False,
                "is_due_in_period": True
            },
            {
                "taskType": "consolidation",
                "is_excluded_type": False,
                "is_completed": True,
                "completed_in_period": False,
                "is_due_in_period": False
            }
        ]
        
        for task in tasks:
            _update_task_type_stats(task, task_type_stats)
        
        assert task_type_stats["consolidation"]["total"] == 3
        assert task_type_stats["consolidation"]["completed"] == 2
        assert task_type_stats["consolidation"]["completed_in_period"] == 1
        assert task_type_stats["consolidation"]["due_in_period"] == 2


# ============================================================================
# TEST: Completion Rate Calculations
# ============================================================================

class TestCompletionRates:
    """Test completion rate calculations"""
    
    def test_completion_rate_100_percent(self):
        """Test 100% completion rate"""
        overview = _build_overview(
            formatted_cells=[],
            global_stats={
                "total_tasks": 10,
                "completed_tasks": 10,
                "completed_in_period": 10,
                "due_in_period": 10,
                "incomplete_due": 0
            },
            task_type_stats={},
            grouped_tasks=[],
            users=[],
            all_task_types=[]
        )
        
        assert overview["completion_rate_due_tasks"] == 100.0
        assert overview["completion_rate_overall"] == 100.0
    
    def test_completion_rate_zero_percent(self):
        """Test 0% completion rate"""
        overview = _build_overview(
            formatted_cells=[],
            global_stats={
                "total_tasks": 10,
                "completed_tasks": 0,
                "completed_in_period": 0,
                "due_in_period": 10,
                "incomplete_due": 10
            },
            task_type_stats={},
            grouped_tasks=[],
            users=[],
            all_task_types=[]
        )
        
        assert overview["completion_rate_due_tasks"] == 0.0
        assert overview["completion_rate_overall"] == 0.0
    
    def test_completion_rate_partial(self):
        """Test partial completion rate"""
        overview = _build_overview(
            formatted_cells=[],
            global_stats={
                "total_tasks": 10,
                "completed_tasks": 7,
                "completed_in_period": 3,
                "due_in_period": 5,
                "incomplete_due": 2
            },
            task_type_stats={},
            grouped_tasks=[],
            users=[],
            all_task_types=[]
        )
        
        assert overview["completion_rate_due_tasks"] == 60.0  # 3/5
        assert overview["completion_rate_overall"] == 70.0  # 7/10
    
    def test_completion_rate_rounding(self):
        """Test that completion rates are properly rounded"""
        overview = _build_overview(
            formatted_cells=[],
            global_stats={
                "total_tasks": 3,
                "completed_tasks": 2,
                "completed_in_period": 1,
                "due_in_period": 3,
                "incomplete_due": 2
            },
            task_type_stats={},
            grouped_tasks=[],
            users=[],
            all_task_types=[]
        )
        
        # Should round to 2 decimal places
        assert overview["completion_rate_due_tasks"] == 33.33  # 1/3
        assert overview["completion_rate_overall"] == 66.67  # 2/3


# ============================================================================
# TEST: Error Messages and Logging
# ============================================================================

class TestErrorHandling:
    """Test error handling and logging"""
    
    @pytest.mark.asyncio
    async def test_fetch_with_database_timeout(self):
        """Test handling database timeout errors"""
        with patch('services.stats_service.events_collection') as mock_collection:
            mock_collection.aggregate.side_effect = asyncio.TimeoutError("Timeout")
            
            result = await _fetch_overdue_cells(datetime(2024, 12, 18))
            
            # Should return empty list instead of raising
            assert result == []
    
    @pytest.mark.asyncio
    async def test_fetch_with_connection_error(self):
        """Test handling database connection errors"""
        with patch('stats_service.tasks_collection') as mock_collection:
            mock_collection.count_documents.side_effect = ConnectionError("Connection failed")
            
            with pytest.raises(ConnectionError):
                await _fetch_task_counts(
                    datetime(2024, 12, 1),
                    datetime(2024, 12, 31)
                )
    
    def test_format_cells_with_invalid_objectid(self):
        """Test formatting cells with invalid ObjectIds"""
        cells = [
            {"_id": None, "date": datetime(2024, 12, 10)},
            {"_id": "", "date": datetime(2024, 12, 11)}
        ]
        
        # Should handle gracefully and skip invalid entries
        result = _format_cells(cells)
        
        # Both should be skipped
        assert len(result) == 0


# ============================================================================
# TEST: Response Format Validation
# ============================================================================

class TestResponseFormat:
    """Test that API responses have correct format"""
    
    @pytest.mark.asyncio
    async def test_comprehensive_dashboard_response_structure(self, mock_current_user):
        """Test that comprehensive dashboard has all required fields"""
        with patch('stats_service._fetch_overdue_cells', new_callable=AsyncMock) as mock_cells, \
             patch('stats_service._fetch_tasks_by_user', new_callable=AsyncMock) as mock_tasks, \
             patch('stats_service._fetch_users', new_callable=AsyncMock) as mock_users, \
             patch('stats_service._fetch_task_types', new_callable=AsyncMock) as mock_types:
            
            mock_cells.return_value = []
            mock_tasks.return_value = []
            mock_users.return_value = []
            mock_types.return_value = []
            
            result = await get_dashboard_comprehensive(
                period="today",
                limit=100,
                current_user=mock_current_user
            )
            
            # Check required top-level keys
            required_keys = [
                "overview", "overdueCells", "groupedTasks", 
                "allTasks", "allUsers", "period", "date_range"
            ]
            for key in required_keys:
                assert key in result, f"Missing required key: {key}"
            
            # Check overview structure
            overview_keys = [
                "total_attendance", "outstanding_cells", "outstanding_tasks",
                "completion_rate_due_tasks", "completion_rate_overall"
            ]
            for key in overview_keys:
                assert key in result["overview"], f"Missing overview key: {key}"
    
    @pytest.mark.asyncio
    async def test_quick_stats_response_structure(self, mock_current_user):
        """Test that quick stats has all required fields"""
        with patch('stats_service._fetch_task_counts', new_callable=AsyncMock) as mock_counts, \
             patch('stats_service._fetch_consolidation_counts', new_callable=AsyncMock) as mock_consol, \
             patch('stats_service._fetch_overdue_cells_count', new_callable=AsyncMock) as mock_cells, \
             patch('stats_service._fetch_task_type_breakdown', new_callable=AsyncMock) as mock_breakdown, \
             patch('stats_service._fetch_excluded_task_counts', new_callable=AsyncMock) as mock_excluded:
            
            mock_counts.return_value = {
                "total_tasks": 0, "tasks_due": 0,
                "tasks_completed_in_period": 0, "total_completed": 0
            }
            mock_consol.return_value = {"total": 0, "completed": 0, "completed_in_period": 0}
            mock_cells.return_value = 0
            mock_breakdown.return_value = {}
            mock_excluded.return_value = {"no_answer": 0, "awaiting_call": 0}
            
            result = await get_dashboard_quick_stats(
                period="today",
                current_user=mock_current_user
            )
            
            # Check required keys
            required_keys = [
                "period", "date_range", "taskCount", "tasksDueInPeriod",
                "tasksCompletedInPeriod", "completionRateDueTasks",
                "overdueCells", "timestamp"
            ]
            for key in required_keys:
                assert key in result, f"Missing required key: {key}"


# ============================================================================
# TEST: Performance and Optimization
# ============================================================================

class TestPerformance:
    """Test performance-related aspects"""
    
    @pytest.mark.asyncio
    async def test_parallel_fetch_faster_than_sequential(self):
        """Test that parallel fetching is used"""
        # This is more of a code review test - we verify gather is used
        with patch('stats_service.asyncio.gather', new_callable=AsyncMock) as mock_gather:
            mock_gather.return_value = ([], [], [], [], [])
            
            try:
                await get_dashboard_comprehensive(
                    period="today",
                    limit=100,
                    current_user={"email": "test@example.com"}
                )
            except:
                pass
            
            # Verify gather was called (parallel execution)
            assert mock_gather.called
    
    @pytest.mark.asyncio
    async def test_aggregation_pipeline_efficiency(self):
        """Test that aggregation pipelines are used instead of Python loops"""
        # Verify that _fetch_tasks_by_user uses aggregation
        with patch('stats_service.tasks_collection') as mock_collection:
            mock_cursor = AsyncMock()
            mock_cursor.to_list = AsyncMock(return_value=[])
            mock_collection.aggregate.return_value = mock_cursor
            
            await _fetch_tasks_by_user(
                datetime(2024, 12, 1),
                datetime(2024, 12, 31)
            )
            
            # Verify aggregate was called (not find)
            assert mock_collection.aggregate.called
            assert not mock_collection.find.called


# ============================================================================
# RUN CONFIGURATION
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])