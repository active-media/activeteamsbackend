"""
Example of refactored main.py structure
This shows how main.py should be structured after refactoring
Only route definitions should remain in main.py, all business logic in services/
"""

import os
from datetime import datetime
from fastapi import Body, FastAPI, HTTPException, Query, Path, Depends, BackgroundTasks, File, UploadFile
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
import asyncio

# Import models
from auth.models import (
    EventCreate, UserProfile, ConsolidationCreate, UserProfileUpdate, 
    CheckIn, UncaptureRequest, UserCreate, UserCreater, UserLogin,
    RefreshTokenRequest, ForgotPasswordRequest, ResetPasswordRequest,
    TaskModel, PersonCreate, EventTypeCreate, TaskTypeIn, TaskTypeOut,
    LeaderStatusResponse
)

# Import utilities
from auth.utils import get_current_user
from database import db, events_collection, people_collection, users_collection, tasks_collection, tasktypes_collection

# Import services
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
from services.tasks_service import (
    create_task,
    get_user_tasks,
    get_task_types,
    create_task_type,
    update_task,
    get_all_tasks,
    get_leader_tasks
)
from services.events_service import (
    create_event,
    get_cell_events,
    get_other_events,
    get_global_events,
    update_event,
    delete_event,
    get_event_by_id,
    submit_attendance,
    create_event_type,
    get_event_types,
    update_event_type,
    delete_event_type
)
from services.stats_service import (
    get_stats_overview,
    get_dashboard_comprehensive,
    get_dashboard_quick_stats,
    get_outstanding_items,
    get_people_capture_stats
)
from services.consolidation_service import (
    create_consolidation,
    get_consolidations,
    update_consolidation,
    get_consolidation_stats,
    get_person_consolidation_history,
    get_event_consolidations
)
from services.admin_service import (
    create_user,
    get_all_users,
    update_user_role,
    delete_user,
    update_role_permissions,
    get_role_permissions,
    get_activity_logs
)
from services.service_checkin_service import (
    get_service_checkin_real_time_data,
    service_checkin_person,
    remove_from_service_checkin,
    update_service_checkin_person
)
from services.leader_service import (
    get_all_leaders,
    get_leader_cells,
    check_leader_status
)
from services.utils import sanitize_document

app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://teams.theactivechurch.org",
        "http://localhost:8000",
        "http://localhost:5173",
        "https://new-active-teams.netlify.app",
        "https://activeteams.netlify.app",
        "https://activeteamsbackend2.0.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept", "Origin", "X-Requested-With", "Access-Control-Allow-Origin"],
    expose_headers=["*"],
    max_age=3600,
)

# ==================== ROOT & HEALTH ====================

@app.get("/")
def root():
    return {"message": "App is live on Render!"}

@app.get("/health")
async def health_check():
    """Simple health check endpoint"""
    from services.people_service import get_people_cache
    people_cache = get_people_cache()
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "cache_status": {
            "has_data": len(people_cache["data"]) > 0,
            "data_count": len(people_cache["data"]),
            "is_loading": people_cache["is_loading"],
            "last_updated": people_cache["last_updated"]
        }
    }

@app.get("/ping")
async def ping():
    return JSONResponse(content={"message": "Server is alive"}, status_code=200)

# ==================== STARTUP EVENTS ====================

@app.on_event("startup")
async def startup_event():
    """Start background loading of all people on startup"""
    print("Starting background load of ALL people...")
    asyncio.create_task(background_load_all_people())

@app.on_event("startup")
async def create_indexes_on_startup():
    """Create MongoDB indexes for faster queries"""
    print("Creating MongoDB indexes for faster queries...")
    try:
        await events_collection.create_index(
            [("Event Type", 1), ("Email", 1), ("Day", 1), ("Event Name", 1)],
            name="fast_lookup_idx"
        )
        await events_collection.create_index(
            [("Leader", 1), ("Leader at 12", 1)],
            name="leader_search_idx"
        )
        await people_collection.create_index(
            [("Name", 1), ("Surname", 1), ("Gender", 1)],
            name="people_lookup_idx"
        )
        print("Indexes created successfully")
    except Exception as e:
        print(f"Error creating indexes: {e}")

# ==================== PEOPLE ROUTES ====================

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

@app.patch("/people/{person_id}")
async def update_person_route(person_id: str = Path(...), update_data: dict = Body(...)):
    return await update_person(person_id, update_data)

@app.delete("/people/{person_id}")
async def delete_person_route(person_id: str = Path(...)):
    return await delete_person(person_id)

@app.get("/people/search")
async def search_people_route(
    query: str = Query("", min_length=2),
    limit: int = Query(50, ge=1, le=200)
):
    return await search_people(query, limit)

@app.get("/people/search-fast")
async def search_people_fast_route(
    query: str = Query(..., min_length=2),
    limit: int = Query(25, le=50)
):
    return await search_people_fast(query, limit)

@app.get("/people/all-minimal")
async def get_all_people_minimal_route():
    return await get_all_people_minimal()

@app.get("/people/leaders-only")
async def get_leaders_only_route():
    return await get_leaders_only()

@app.get("/people/simple")
async def get_people_simple_route(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=1000)
):
    return await get_people_simple(page, per_page)

@app.get("/cache/people")
async def get_cached_people_route():
    return await get_cached_people()

@app.post("/cache/people/refresh")
async def refresh_people_cache_route():
    return await refresh_people_cache()

@app.get("/cache/people/status")
async def get_cache_status_route():
    return await get_cache_status()

# ==================== AUTH ROUTES ====================

@app.post("/signup")
async def signup_route(user: UserCreate):
    return await signup(user)

@app.post("/login")
async def login_route(user: UserLogin):
    return await login(user)

@app.post("/forgot-password")
async def forgot_password_route(payload: ForgotPasswordRequest, background_tasks: BackgroundTasks):
    return await forgot_password(payload, background_tasks)

@app.post("/reset-password")
async def reset_password_route(data: ResetPasswordRequest):
    return await reset_password(data)

@app.post("/refresh-token")
async def refresh_token_route(payload: RefreshTokenRequest = Body(...)):
    return await refresh_token(payload)

@app.post("/logout")
async def logout_route(user_id: str = Body(..., embed=True)):
    return await logout(user_id)

@app.get("/profile/{user_id}", response_model=UserProfile)
async def get_profile_route(user_id: str, current_user: dict = Depends(get_current_user)):
    return await get_profile(user_id)

@app.put("/profile/{user_id}")
async def update_profile_route(
    user_id: str,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    from fastapi import Request
    update_data = await request.json()
    return await update_profile(user_id, update_data)

@app.post("/users/{user_id}/avatar")
async def upload_avatar_route(
    user_id: str,
    avatar: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    avatar_data = await avatar.read()
    return await upload_avatar(user_id, avatar_data)

@app.put("/users/{user_id}/password")
async def change_password_route(
    user_id: str,
    password_data: dict,
    current_user: dict = Depends(get_current_user)
):
    return await change_password(user_id, password_data, current_user)

# ==================== TASKS ROUTES ====================

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

@app.get("/tasks/all")
async def get_all_tasks_route(current_user: dict = Depends(get_current_user)):
    return await get_all_tasks(current_user)

@app.get("/tasks/leader/{leader_email}")
async def get_leader_tasks_route(
    leader_email: str,
    current_user: dict = Depends(get_current_user)
):
    return await get_leader_tasks(leader_email, current_user)

@app.put("/tasks/{task_id}")
async def update_task_route(task_id: str, updated_task: dict):
    return await update_task(task_id, updated_task)

@app.get("/tasktypes", response_model=List[TaskTypeOut])
async def get_task_types_route():
    return await get_task_types()

@app.post("/tasktypes", response_model=TaskTypeOut)
async def create_task_type_route(task: TaskTypeIn):
    return await create_task_type(task)

# ==================== EVENTS ROUTES ====================
# Note: These need to be implemented in events_service.py first

@app.post("/events")
async def create_event_route(event: EventCreate):
    return await create_event(event)

@app.get("/events/cells")
async def get_cell_events_route(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    personal: Optional[bool] = Query(False),
    start_date: Optional[str] = Query(None),
    **kwargs
):
    return await get_cell_events(current_user, page, limit, status, search, event_type, personal, start_date, **kwargs)

# ... Add more event routes as needed

# ==================== STATS ROUTES ====================

@app.get("/stats/overview")
async def get_stats_overview_route(period: str = "monthly"):
    return await get_stats_overview(period)

@app.get("/stats/dashboard-comprehensive")
async def get_dashboard_comprehensive_route(
    period: str = Query("today", regex="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$"),
    limit: int = Query(100, ge=1, le=1000),
    current_user: dict = Depends(get_current_user)
):
    return await get_dashboard_comprehensive(period, limit, current_user)

@app.get("/stats/dashboard-quick")
async def get_dashboard_quick_stats_route(
    period: str = Query("today", regex="^(today|thisWeek|thisMonth|previous7|previousWeek|previousMonth)$"),
    current_user: dict = Depends(get_current_user)
):
    return await get_dashboard_quick_stats(period, current_user)

@app.get("/stats/outstanding-items")
async def get_outstanding_items_route():
    return await get_outstanding_items()

@app.get("/stats/people-with-tasks")
async def get_people_capture_stats_route():
    return await get_people_capture_stats()

# ==================== CONSOLIDATION ROUTES ====================

@app.post("/consolidations")
async def create_consolidation_route(
    consolidation: ConsolidationCreate,
    current_user: dict = Depends(get_current_user)
):
    return await create_consolidation(consolidation, current_user)

@app.get("/consolidations")
async def get_consolidations_route(
    assigned_to: Optional[str] = None,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    perPage: int = Query(50, ge=1),
    current_user: dict = Depends(get_current_user)
):
    return await get_consolidations(assigned_to, status, page, perPage, current_user)

# ... Add more consolidation routes as needed

# ==================== ADMIN ROUTES ====================

@app.post("/admin/users", response_model=MessageResponse)
async def create_user_route(
    user_data: UserCreater,
    current_user: dict = Depends(get_current_user)
):
    return await create_user(user_data, current_user)

@app.get("/admin/users", response_model=UserList)
async def get_all_users_route(current_user: dict = Depends(get_current_user)):
    return await get_all_users(current_user)

# ... Add more admin routes as needed

# ==================== LEADER ROUTES ====================

@app.get("/leaders")
async def get_all_leaders_route():
    return await get_all_leaders()

@app.get("/leaders/cells-for/{email}")
async def get_leader_cells_route(email: str):
    return await get_leader_cells(email)

@app.get("/check-leader-status", response_model=LeaderStatusResponse)
async def check_leader_status_route(current_user: dict = Depends(get_current_user)):
    return await check_leader_status(current_user)

# ==================== SERVICE CHECKIN ROUTES ====================

@app.get("/service-checkin/real-time-data")
async def get_service_checkin_real_time_data_route(
    event_id: str = Query(..., description="Event ID to get real-time data for"),
    current_user: dict = Depends(get_current_user)
):
    return await get_service_checkin_real_time_data(event_id, current_user)

@app.post("/service-checkin/checkin")
async def service_checkin_person_route(
    checkin_data: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    return await service_checkin_person(checkin_data, current_user)

# ... Add more service checkin routes as needed

# Note: This is an example structure. The actual main.py should have ALL routes refactored in this way.
# All business logic should be in service files, and routes should just be thin wrappers.

