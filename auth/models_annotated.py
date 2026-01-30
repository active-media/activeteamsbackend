# Import FastAPI class used to create app instances
from fastapi import FastAPI
# Import HTTP-related exceptions and request types used by APIs
from fastapi import HTTPException, Request, APIRouter, Body, Path
# Import JSON response helper used to return JSON payloads
from fastapi.responses import JSONResponse
# Import the validation error used to handle request validation exceptions
from fastapi.exceptions import RequestValidationError
# Import Motor async MongoDB client for database operations
from motor.motor_asyncio import AsyncIOMotorClient
# Import dotenv loader to read environment variables from a .env file
from dotenv import load_dotenv
# Import Pydantic base model, field helpers and email type used in models
from pydantic import BaseModel, Field, EmailStr, field_validator
# Import Enum to create enumerated types
from enum import Enum
# Import typing helpers for optional and list types
from typing import Optional, List, Literal
# Import date/time types used in model fields
from datetime import datetime, date
# Import BSON ObjectId for MongoDB document id typing
from bson import ObjectId
# Import uuid module for generating UUIDs where needed
import uuid
# Import URL decode helper used when parsing request parameters
from urllib.parse import unquote

# Create a FastAPI app instance; often present in model modules for testing
app = FastAPI()

### User creation models
# Define Pydantic model for user creation input with required fields
class UserCreate(BaseModel):
    # First name of the user
    name: str
    # Last name of the user
    surname: str
    # Date of birth as a string
    date_of_birth: str
    # Home address string
    home_address: str
    # Who invited this user (full name)
    invited_by: str
    # Phone number as string
    phone_number: str
    # Email address validated by Pydantic
    email: EmailStr
    # Gender string
    gender: str
    # Plain text password (will be hashed before storage)
    password: str
    # Optional role, default None
    role: Optional[str] = None

# Define Pydantic model for login payload (email + password)
class UserLogin(BaseModel):
    # Email for login
    email: EmailStr
    # Password for login
    password: str

### Attendee model used in attendance payloads
# Define an attendee record with optional metadata fields
class Attendee(BaseModel):
    # Optional identifier string
    id: Optional[str] = None
    # Optional display name
    name: Optional[str] = None
    # Optional fullName field
    fullName: Optional[str] = None
    # Optional leader at 12 field
    leader12: Optional[str] = None
    # Optional leader at 144 field
    leader144: Optional[str] = None
    # Optional timestamp for check-in
    time: Optional[datetime] = None
    # Optional email address
    email: Optional[str] = None
    # Optional phone number
    phone: Optional[str] = None
    # Optional decision field (e.g., first-time decision)
    decision: Optional[str] = None
    # Optional pricing related fields follow
    priceTier: Optional[str] = None
    price: Optional[float] = None
    ageGroup: Optional[str] = None
    memberType: Optional[str] = None
    paymentMethod: Optional[str] = None
    paid: Optional[float] = None
    owing: Optional[float] = None

    # Validator to fill fullName from name when fullName not provided
    @field_validator("fullName", mode="before")
    def set_fullname(cls, v, info):
        # If fullName is missing but name exists, use name as fullName
        if not v and info.data.get("name"):
            return info.data.get("name")
        return v

### Attendance submission model
# Model describing the payload when submitting attendance for an event
class AttendanceSubmission(BaseModel):
    # List of attendee objects
    attendees: List[Attendee]
    # Leader's email submitting the attendance
    leaderEmail: str
    # Leader's display name
    leaderName: str
    # Flag indicating the group did not meet
    did_not_meet: bool = False
    # Flag indicating if event is ticketed
    isTicketed: bool = False

    # Validator warns if did_not_meet is true but attendees list is non-empty
    @field_validator("attendees", mode="before")
    def validate_attendance(cls, v, info):
        # Print a warning when inconsistent data is submitted
        if info.data.get("did_not_meet") and v:
            print(f" Warning: did_not_meet is True but attendees list is not empty: {len(v)} attendees")
        return v

### Person creation model used on Event screen
# Model for adding a new person via the event UI
class PersonCreate(BaseModel):
    # Who invited the person
    invitedBy: str
    # Given name
    name: str
    # Family name
    surname: str
    # Gender string
    gender: str
    # Email address
    email: str
    # Phone number
    number: str
    # Date of birth string
    dob: str
    # Postal or home address
    address: str
    # List of leaders (hierarchy)
    leaders: list[str]
    # Stage (literal constrained to "Win")
    stage: Literal["Win"]

### Event type definition
# Model used to create or describe event types
class EventTypeCreate(BaseModel):
    # Name of the event type
    name: str
    # Human-readable description
    description: str
    # Whether the event type is ticketed
    isTicketed: Optional[bool] = False
    # Whether type is global
    isGlobal: Optional[bool] = None
    # Whether the event type involves person-step tracking
    hasPersonSteps: Optional[bool] = None
    # Optional creation timestamp
    createdAt: Optional[datetime] = None
    # Flag to indicate this document represents an event type
    isEventType: Optional[bool] = True

### Attendance helper models
# Simple check-in payload model
class CheckIn(BaseModel):
    # Event id being checked into
    event_id: str
    # Name of the attendee
    name: str

# Request payload model to uncapture attendance
class UncaptureRequest(BaseModel):
    # Event id for which to uncapture
    event_id: str
    # Person name to uncapture
    name: str

### Token models
# Token response model returned to clients after auth
class TokenResponse(BaseModel):
    # JWT access token
    access_token: str
    # Token type, typically "bearer"
    token_type: str

# Token data model representing decoded token payload
class TokenData(BaseModel):
    # Subject (e.g., user id or email)
    sub: Optional[str] = None
    # Role information from token
    role: Optional[str] = None

### Event base model
# Base class for events containing shared fields
class EventBase(BaseModel):
    # Name of the event type
    eventTypeName: str
    # Friendly event name
    eventName: str
    # Optional scheduled date/time
    date: Optional[datetime] = None
    # Optional time string
    time: Optional[str] = None
    # Recurring days list with default empty list
    recurring_day: List[str] = Field(default_factory=list)
    # Location string
    location: str
    # Optional leader name
    eventLeader: Optional[str] = None
    # Optional description field
    description: Optional[str] = None
    # Whether the event requires tickets
    isTicketed: bool = False
    # Fallback price field
    price: Optional[float] = None
    # Email of user creating event
    userEmail: Optional[str] = None
    # Whether event is global in scope
    isGlobal: Optional[bool] = None  
    # Whether event tracks person steps (cells)
    hasPersonSteps: Optional[bool] = None  
    # Leader email for the event
    eventLeaderEmail: Optional[str] = None  
    # Leader hierarchy fields
    leader1: Optional[str] = None
    leader12: Optional[str] = None
    # Active flag for event
    is_active: bool = True
    # Optional deactivation window and reason
    deactivation_start: Optional[datetime] = None
    deactivation_end: Optional[datetime] = None
    deactivation_reason: Optional[str] = None

# Event creation model inherits all EventBase fields
class EventCreate(EventBase):
    pass

# Event update model where all fields are optional for partial updates
class EventUpdate(BaseModel):
    eventType: Optional[str] = None
    eventName: Optional[str] = None
    date: Optional[datetime] = None
    time: Optional[str] = None
    recurring_day: Optional[List[str]] = None
    location: Optional[str] = None
    eventLeader: Optional[str] = None
    description: Optional[str] = None
    userEmail: Optional[str] = None
    status: Optional[str] = None
    Status: Optional[str] = None
    attendees: Optional[List[dict]] = None
    did_not_meet: Optional[bool] = None
    total_attendance: Optional[int] = None
    isTicketed: Optional[bool] = None
    isGlobal: Optional[bool] = None
    hasPersonSteps: Optional[bool] = None
    priceTiers: Optional[List[dict]] = None
    leader1: Optional[str] = None
    leader12: Optional[str] = None
    price: Optional[float] = None
    is_active: Optional[bool] = None
    deactivation_start: Optional[datetime] = None
    deactivation_end: Optional[datetime] = None
    deactivation_reason: Optional[str] = None

# Request model to deactivate a cell for a given number of weeks
class CellDeactivateRequest(BaseModel):
    # Weeks to deactivate (default 1, constrained 1-12)
    weeks: int = Field(1, ge=1, le=12, description="Number of weeks to deactivate (1-12)")
    # Optional reason string up to 200 characters
    reason: Optional[str] = Field(None, max_length=200, description="Reason for deactivation")

# Response model returned when deactivating a cell
class CellDeactivateResponse(BaseModel):
    # Whether the operation succeeded
    success: bool
    # Human-readable message
    message: str
    # Number of weeks deactivated
    weeks: int
    # Computed deactivation end datetime
    deactivation_end: datetime
    # Number of cells affected
    cell_count: int

# Event representation stored in the database including attendees
class EventInDB(EventBase):
    # Document id as string
    _id: str
    # Attendees list stored as dicts
    attendees: List[dict] = []
    # Cached total attendance integer
    total_attendance: int = 0

### User profile models
# Model returned to clients representing a user profile
class UserProfile(BaseModel):
    # User id as string
    id: str
    # Given name
    name: str
    # Family name
    surname: str
    # DOB string
    date_of_birth: str
    # Home address
    home_address: str
    # Optional invited by
    invited_by: Optional[str]
    # Phone number
    phone_number: str
    # Email (validated)
    email: EmailStr
    # Gender
    gender: str
    # Role defaulting to "user"
    role: Optional[str] = "user"

# Model for updating user profile where fields are optional
class UserProfileUpdate(BaseModel):
    name: Optional[str]
    surname: Optional[str]
    date_of_birth: Optional[str]
    home_address: Optional[str]
    invited_by: Optional[str]
    phone_number: Optional[str]
    email: Optional[EmailStr]
    gender: Optional[str]

### Cell event models
# Model for creating a cell (recurring small-group) event
class CellEventCreate(BaseModel):
    service_name: str
    leader_id: str
    start_date: datetime
    start_time: str
    recurring: bool
    recurring_day: Optional[
        Literal[
            "monday", "tuesday", "wednesday", "thursday",
            "friday", "saturday", "sunday"
        ]
    ] = None
    # Optional list of member names or ids
    members: Optional[List[str]] = []

    # Normalize recurring_day to lowercase before Pydantic validation
    @field_validator("recurring_day", mode="before")
    def normalize_day(cls, v):
        if v:
            return v.lower()
        return v

    # When converting model to dict, capitalize recurring_day for presentation
    def model_dump(self, **kwargs):
        data = super().model_dump(**kwargs)
        if data.get("recurring_day"):
            data["recurring_day"] = data["recurring_day"].capitalize()
        return data

# Request model to add a member by name to a cell
class AddMemberNamesRequest(BaseModel):
    name: str

# Request model to remove a member by name from a cell
class RemoveMemberRequest(BaseModel):
    name: str

### Password reset models
# Payload for requesting a password reset (forgot password)
class ForgotPasswordRequest(BaseModel):
    email: str

# Payload for submitting a reset token and new password
class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str

### Refresh token model
# Payload used to rotate refresh tokens
class RefreshTokenRequest(BaseModel):
    refresh_token_id: str
    refresh_token: str

### Task-related models
# Contacted person information used in tasks
class ContactedPerson(BaseModel):
    name: str
    phone: str
    email: EmailStr

# Full task creation model used by the API
class TaskModel(BaseModel):
    memberID: str
    name: str
    taskType: str
    contacted_person: ContactedPerson
    followup_date: datetime
    status: str
    type: str
    assignedfor: str

    # Pydantic config for task model
    class Config:
        validate_by_name = True
        arbitrary_types_allowed = True

# Simple task type input model (name only)
class TaskTypeIn(BaseModel):
    name: str

# Task type output model including id and name
class TaskTypeOut(BaseModel):
    id: str
    name: str

# PersonInfo used within task update payloads
class PersonInfo(BaseModel):
    name: Optional[str]
    phone: Optional[str]
    email: Optional[str]

# Partial task update model where fields are optional
class TaskUpdate(BaseModel):
    name: Optional[str]
    taskType: Optional[str]
    contacted_person: Optional[PersonInfo]
    followup_date: Optional[str]
    status: Optional[str]
    type: Optional[str]

### User management models
# Model representing a user in list responses
class UserListResponse(BaseModel):
    id: str
    name: str
    surname: str
    email: str
    role: str
    date_of_birth: Optional[str] = None
    phone_number: Optional[str] = None
    address: Optional[str] = None
    gender: Optional[str] = None
    invitedBy: Optional[str] = None
    leader12: Optional[str] = None
    leader144: Optional[str] = None
    leader1728: Optional[str] = None
    stage: Optional[str] = None
    created_at: Optional[datetime] = None

# Wrapper for a list of users returned by the API
class UserList(BaseModel):
    users: List[UserListResponse]

# Model to update a user's role
class RoleUpdate(BaseModel):
    role: str

# Model to toggle a permission on or off
class PermissionUpdate(BaseModel):
    permission: str
    enabled: bool

# Generic single-message response model
class MessageResponse(BaseModel):
    message: str

# Response model describing leadership-related status flags
class LeaderStatusResponse(BaseModel):
    isLeader: bool
    hasCell: bool
    canAccessEvents: bool

# Admin-facing user creation model with many optional fields
class UserCreater(BaseModel):
    name: str
    surname: str
    email: str
    password: str
    phone_number: Optional[str] = None
    date_of_birth: Optional[date] = None
    address: Optional[str] = None
    gender: Optional[str] = None
    invitedBy: Optional[str] = None
    leader12: Optional[str] = None
    leader144: Optional[str] = None
    leader1728: Optional[str] = None
    stage: Optional[str] = "Win"
    role: str = "user"

# DecisionType enum used when recording consolidation decisions
class DecisionType(str, Enum):
    FIRST_TIME = "first_time"
    RECOMMITMENT = "recommitment"

# Model used to create a consolidation task (decision follow-up)
class ConsolidationCreate(BaseModel):
    person_name: str
    person_surname: str
    person_email: Optional[str] = None
    person_phone: Optional[str] = None
    decision_type: DecisionType
    decision_date: str
    assigned_to: str
    assigned_to_email: str
    notes: Optional[str] = None
    event_id: Optional[str] = None
    leaders: List[str] = []

# ConsolidationTask extends TaskModel and adds consolidation-specific fields
class ConsolidationTask(TaskModel):
    consolidation_id: str
    person_name: str
    person_surname: str
    decision_type: str
