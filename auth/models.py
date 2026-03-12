from fastapi import FastAPI, HTTPException, Request, APIRouter, Body, Path
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pydantic import BaseModel, Field, EmailStr, field_validator
from enum import Enum
from typing import Optional, List, Literal
from datetime import datetime, date
from bson import ObjectId
import uuid
from urllib.parse import unquote

app = FastAPI()

# ===== User Creation =====
class UserCreate(BaseModel):
    name: str
    surname: str
    date_of_birth: str
    home_address: str
    invited_by: str
    invited_by_id: Optional[str] = None  # People._id (stringified ObjectId) when selected from autocomplete
    phone_number: str
    email: EmailStr
    gender: str
    password: str
    role: Optional[str] = None
    organization: Optional[str] = None  # e.g. "Active Church", "City Church"
    org_tag: Optional[str] = None       # auto-resolved from organization if omitted

class UserLogin(BaseModel):
    email: EmailStr
    password: str

# ===== FIXED Attendee Model =====
class Attendee(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    fullName: Optional[str] = None
    leader12: Optional[str] = None
    leader144: Optional[str] = None
    time: Optional[datetime] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    decision: Optional[str] = None
    priceName: Optional[str] = None
    priceTier: Optional[str] = None
    price: Optional[float] = None
    ageGroup: Optional[str] = None
    memberType: Optional[str] = None
    paymentMethod: Optional[str] = None
    paid: Optional[float] = None
    owing: Optional[float] = None

    @field_validator("priceName", mode="before")
    def set_price_name(cls, v, info):
        if not v and info.data.get("priceTier"):
            return info.data.get("priceTier")
        return v

# ===== IMPROVED AttendanceSubmission Model =====
class AttendanceSubmission(BaseModel):
    attendees: List[Attendee]
    leaderEmail: str
    leaderName: str
    did_not_meet: bool = False
    isTicketed: bool = False

    @field_validator("attendees", mode="before")
    def validate_attendance(cls, v, info):
        if info.data.get("did_not_meet") and v:
            print(f" Warning: did_not_meet is True but attendees list is not empty: {len(v)} attendees")
        return v

# Adding new Person in the Event screen
class PersonCreate(BaseModel):
    invitedBy: str
    name: str
    surname: str
    gender: str
    email: str
    number: str
    dob: str
    address: str
    leaders: list[str]
    stage: Literal["Win"]

# ===== EventTypes - FIXED =====
class EventTypeCreate(BaseModel):
    name: str
    description: str
    isTicketed: Optional[bool] = False
    isGlobal: Optional[bool] = None
    hasPersonSteps: Optional[bool] = None
    createdAt: Optional[datetime] = None
    isEventType: Optional[bool] = True

# ===== Attendance =====
class CheckIn(BaseModel):
    event_id: str
    name: str

class UncaptureRequest(BaseModel):
    event_id: str
    name: str

# ===== Auth Tokens =====
class TokenResponse(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    sub: Optional[str] = None
    role: Optional[str] = None

# ===== Event Models =====
class EventBase(BaseModel):
    eventTypeName: str
    eventName: str
    date: Optional[datetime] = None
    time: Optional[str] = None
    recurring_day: List[str] = Field(default_factory=list)
    location: str
    eventLeader: Optional[str] = None
    description: Optional[str] = None
    isTicketed: bool = False
    priceTiers: List[dict] = Field(default_factory=list)
    userEmail: Optional[str] = None
    isGlobal: Optional[bool] = None  
    hasPersonSteps: Optional[bool] = None  
    eventLeaderEmail: Optional[str] = None  
    leader1: Optional[str] = None
    leader12: Optional[str] = None
    is_active: bool = True
    deactivation_start: Optional[datetime] = None
    deactivation_end: Optional[datetime] = None
    deactivation_reason: Optional[str] = None

class EventCreate(EventBase):
    pass

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
class CellDeactivateRequest(BaseModel):
    weeks: int = Field(1, ge=1, le=12, description="Number of weeks to deactivate (1-12)")
    reason: Optional[str] = Field(None, max_length=200, description="Reason for deactivation")

class CellDeactivateResponse(BaseModel):
    success: bool
    message: str
    weeks: int
    deactivation_end: datetime
    cell_count: int
    
class EventInDB(EventBase):
    _id: str
    attendees: List[dict] = []
    total_attendance: int = 0

# ============= Profile Update =============
class UserProfile(BaseModel):
    id: str
    name: str
    surname: str
    date_of_birth: str
    home_address: str
    invited_by: Optional[str]
    phone_number: str
    email: EmailStr
    gender: str
    role: Optional[str] = "user"
    organization: Optional[str] = None
    org_tag: Optional[str] = None
    profile_picture: Optional[str] = None

class UserProfileUpdate(BaseModel):
    name: Optional[str] = None
    surname: Optional[str] = None
    date_of_birth: Optional[str] = None
    home_address: Optional[str] = None
    invited_by: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[EmailStr] = None
    gender: Optional[str] = None
    organization: Optional[str] = None  # updating org also re-derives org_tag

# ===== Cell Events =====
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
    members: Optional[List[str]] = []

    @field_validator("recurring_day", mode="before")
    def normalize_day(cls, v):
        if v:
            return v.lower()
        return v

    def model_dump(self, **kwargs):
        data = super().model_dump(**kwargs)
        if data.get("recurring_day"):
            data["recurring_day"] = data["recurring_day"].capitalize()
        return data

class AddMemberNamesRequest(BaseModel):
    name: str

class RemoveMemberRequest(BaseModel):
    name: str

# ===== Password Reset =====
class ForgotPasswordRequest(BaseModel):
    email: str

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str

# ===== Refresh Token =====
class RefreshTokenRequest(BaseModel):
    refresh_token_id: str
    refresh_token: str

# ===== Task Models =====
class ContactedPerson(BaseModel):
    name: str
    phone: str
    email: EmailStr

class TaskModel(BaseModel):
    memberID: str
    name: str
    taskType: str
    contacted_person: ContactedPerson
    followup_date: datetime
    status: str
    type: str
    assignedfor: str

    class Config:
        validate_by_name = True
        arbitrary_types_allowed = True

class TaskTypeIn(BaseModel):
    name: str

class TaskTypeOut(BaseModel):
    id: str
    name: str

class PersonInfo(BaseModel):
    name: Optional[str]
    phone: Optional[str]
    email: Optional[str]

class TaskUpdate(BaseModel):
    name: Optional[str]
    taskType: Optional[str]
    contacted_person: Optional[PersonInfo]
    followup_date: Optional[str]
    status: Optional[str]
    type: Optional[str]

# ===== User Management =====
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

class UserList(BaseModel):
    users: List[UserListResponse]

class RoleUpdate(BaseModel):
    role: str

class PermissionUpdate(BaseModel):
    permission: str
    enabled: bool

class MessageResponse(BaseModel):
    message: str

class LeaderStatusResponse(BaseModel):
    isLeader: bool
    hasCell: bool
    canAccessEvents: bool

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

class DecisionType(str, Enum):
    FIRST_TIME = "first_time"
    RECOMMITMENT = "recommitment"


class ConsolidationSource(str, Enum):
    MANUAL = "manual"
    SERVICE = "service_consolidation"
    EVENT = "event_consolidation"

class ConsolidationCreate(BaseModel):
    person_name: str
    person_surname: str
    person_email: Optional[str] = ""
    person_phone: Optional[str] = ""
    decision_type: DecisionType
    decision_date: str
    assigned_to: str
    assigned_to_email: Optional[str] = None
    leaders: list = Field(default_factory=list)
    event_id: Optional[str] = None
    is_check_in: bool = False
    attendance_status: str = "checked_in"
    notes: Optional[str] = ""
    source: ConsolidationSource = ConsolidationSource.MANUAL

class ConsolidationTask(TaskModel):
    consolidation_id: str
    person_name: str
    person_surname: str
    decision_type: str

