# Refactoring Guide

This document shows how to refactor `main.py` to use the service layer.

## Structure

All business logic has been extracted into service files in the `services/` folder:
- `services/people_service.py` - People CRUD, search, cache
- `services/tasks_service.py` - Tasks and task types
- `services/auth_service.py` - Authentication, signup, login, profile
- `services/events_service.py` - Events, event types, attendance (placeholder - needs extraction)
- `services/stats_service.py` - Statistics and dashboard (placeholder - needs extraction)
- `services/consolidation_service.py` - Consolidations (placeholder - needs extraction)
- `services/admin_service.py` - Admin operations (placeholder - needs extraction)
- `services/service_checkin_service.py` - Service checkin (placeholder - needs extraction)
- `services/leader_service.py` - Leader operations (placeholder - needs extraction)
- `services/utils.py` - Shared utilities

## How to Refactor main.py

### Before (Current Structure):
```python
@app.get("/people")
async def get_people(
    page: int = Query(1, ge=1),
    perPage: int = Query(100, ge=0),
    name: Optional[str] = None,
    # ... more params
):
    try:
        query = {}
        # ... 50+ lines of business logic
        return result
    except Exception as e:
        raise HTTPException(...)
```

### After (Refactored Structure):
```python
from services.people_service import get_people as get_people_service

@app.get("/people")
async def get_people(
    page: int = Query(1, ge=1),
    perPage: int = Query(100, ge=0),
    name: Optional[str] = None,
    # ... more params
):
    return await get_people_service(page, perPage, name, ...)
```

## Example Refactored Routes

### People Routes
```python
from services.people_service import (
    get_people,
    get_person_by_id,
    create_person,
    update_person,
    delete_person,
    search_people,
    search_people_fast,
    get_all_people_minimal,
    get_leaders_only,
    get_cached_people,
    get_people_simple,
    refresh_people_cache,
    get_cache_status,
    background_load_all_people
)

@app.get("/people")
async def get_people_route(
    page: int = Query(1, ge=1),
    perPage: int = Query(100, ge=0),
    name: Optional[str] = None,
    gender: Optional[str] = None,
    dob: Optional[str] = None,
    location: Optional[str] = None,
    leader: Optional[str] = None,
    stage: Optional[str] = None
):
    return await get_people(page, perPage, name, gender, dob, location, leader, stage)

@app.get("/people/{person_id}")
async def get_person_by_id_route(person_id: str = Path(...)):
    return await get_person_by_id(person_id)

@app.post("/people")
async def create_person_route(person_data: PersonCreate):
    return await create_person(person_data)

# ... etc for all people routes
```

### Auth Routes
```python
from services.auth_service import (
    signup,
    login,
    forgot_password,
    reset_password,
    refresh_token,
    logout,
    get_profile,
    update_profile,
    upload_avatar,
    change_password
)

@app.post("/signup")
async def signup_route(user: UserCreate):
    return await signup(user)

@app.post("/login")
async def login_route(user: UserLogin):
    return await login(user)

# ... etc for all auth routes
```

### Tasks Routes
```python
from services.tasks_service import (
    create_task,
    get_user_tasks,
    get_task_types,
    create_task_type,
    update_task,
    get_all_tasks,
    get_leader_tasks
)

@app.post("/tasks")
async def create_task_route(task: TaskModel, current_user: dict = Depends(get_current_user)):
    return await create_task(task.dict(), current_user)

@app.get("/tasks")
async def get_user_tasks_route(
    email: str = Query(None),
    userId: str = Query(None),
    view_all: bool = Query(False),
    current_user: dict = Depends(get_current_user)
):
    return await get_user_tasks(email, userId, view_all, current_user)

# ... etc for all task routes
```

## Next Steps

1. **Extract remaining service logic**: The placeholder services (events, stats, consolidation, admin, service_checkin, leader) need their logic extracted from main.py

2. **Refactor main.py**: Replace route handlers to import from services instead of containing business logic

3. **Update imports**: Ensure all imports are correct and dependencies are resolved

4. **Test**: Test all endpoints to ensure functionality is preserved

5. **Clean up**: Remove duplicate code and unused functions from main.py

## Notes

- Keep route definitions in main.py
- Move all business logic to service files
- Service functions should be async and return data (not HTTP responses)
- HTTPException should be raised in service functions when needed
- Route handlers should be thin wrappers that call service functions

