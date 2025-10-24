from fastapi import FastAPI, HTTPException, Request, APIRouter
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pydantic import BaseModel, Field, EmailStr, field_validator
from enum import Enum
from typing import Optional, List, Literal
from datetime import datetime
from bson import ObjectId

app = FastAPI()

# ===== User Creation =====
class UserCreate(BaseModel):
    name: str
    surname: str
    date_of_birth: str
    home_address: str
    invited_by: str
    phone_number: str
    email: EmailStr
    gender: str
    password: str
    role: Optional[str] = None  # Optional; default to 'user' in logic

class UserLogin(BaseModel):
    email: EmailStr
    password: str

# ===== Event Models =====
class EventBase(BaseModel):
    eventType: str
    eventName: str
    date: Optional[datetime] = None
    time: Optional[str] = None
    recurring_day: List[str] = Field(default_factory=list)
    location: str
    eventLeader: Optional[str] = None
    description: Optional[str] = None
    userEmail: Optional[str] = None
    email: Optional[str] = None
    
    # 🔥 CRITICAL: Add these fields
    isTicketed: Optional[bool] = False
    isGlobal: Optional[bool] = False
    hasPersonSteps: Optional[bool] = False
    priceTiers: Optional[List[dict]] = Field(default_factory=list)
    leader1: Optional[str] = None
    leader12: Optional[str] = None
    price: Optional[float] = None  # For backward compatibility

class EventCreate(EventBase):
    """Schema for creating events (inherits from EventBase)."""
    pass

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
    
    # 🔥 NEW: Ticketed event payment fields
    priceTier: Optional[str] = None
    price: Optional[float] = None
    ageGroup: Optional[str] = None
    memberType: Optional[str] = None
    paymentMethod: Optional[str] = None
    paid: Optional[float] = None
    owing: Optional[float] = None

    @field_validator("fullName", mode="before")
    def set_fullname(cls, v, info):
        """If fullName is missing, use name"""
        if not v and info.data.get("name"):
            return info.data.get("name")
        return v


from pydantic import BaseModel, Field, model_validator
from typing import List, Optional

# ===== FIXED AttendanceSubmission Model =====
# ===== IMPROVED AttendanceSubmission Model =====
class AttendanceSubmission(BaseModel):
    attendees: List[Attendee]
    leaderEmail: str
    leaderName: str
    did_not_meet: bool = False
    isTicketed: bool = False

    @model_validator(mode="after")
    def validate_attendance(self):
        """
        ✅ IMPROVED: More flexible validation
        - If `did_not_meet` is True: attendees should be empty (but don't block if not)
        - If `did_not_meet` is False: allow empty attendees (frontend might send empty array)
        """
        if self.did_not_meet and self.attendees:
            print(f"⚠️ Warning: did_not_meet is True but attendees list is not empty: {len(self.attendees)} attendees")
            # Don't raise error, just log it
        return self
    
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


# ===== EventTypes =====
class EventTypeCreate(BaseModel):
    name: str
    isTicketed: Optional[bool] = False
    isGlobal: Optional[bool] = False
    hasPersonSteps: Optional[bool] = False
    description: str
    createdAt: Optional[datetime] = None


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
    
    # 🔥 CRITICAL: Add these fields
    isTicketed: Optional[bool] = None
    isGlobal: Optional[bool] = None
    hasPersonSteps: Optional[bool] = None
    priceTiers: Optional[List[dict]] = None
    leader1: Optional[str] = None
    leader12: Optional[str] = None
    price: Optional[float] = None

class EventInDB(EventBase):
    _id: str  # MongoDB ObjectId as string
    attendees: List[dict] = []
    total_attendance: int = 0

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
    eventType: str
    eventName: str
    date: Optional[datetime] = None
    time: Optional[str] = None
    recurring_day: List[str] = Field(default_factory=list)  # Fixed field name
    location: str
    eventLeader: Optional[str] = None
    description: Optional[str] = None
    isTicketed: bool = False
    price: Optional[float] = None  # Allow null values
    userEmail: Optional[str] = None  # Add this field your frontend sends
    # Add any other fields your frontend might send
class EventCreate(EventBase):
    """Schema for creating events (inherits from EventBase)."""
    pass

async def validation_exception_handler(request: Request, exc: RequestValidationError):
    formatted = [
        {"field": ".".join(err["loc"][1:]), "message": err["msg"]}
        for err in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content={"errors": formatted}
    )
# ============= PProfile Update =============

class UserProfile(BaseModel):
    id: str  # stringified ObjectId
    name: str
    surname: str
    date_of_birth: str
    home_address: str
    invited_by: Optional[str]
    phone_number: str
    email: EmailStr
    gender: str
    role: Optional[str] = "user"


class UserProfileUpdate(BaseModel):
    name: Optional[str]
    surname: Optional[str]
    date_of_birth: Optional[str]  # or date/datetime if you're parsing it
    home_address: Optional[str]
    invited_by: Optional[str]
    phone_number: Optional[str]
    email: Optional[EmailStr]
    gender: Optional[str]


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



# Nested contacted_person model
class ContactedPerson(BaseModel):
    name: str
    phone: str
    email: EmailStr


# Main task model
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

# --- Pydantic Model ---
class TaskTypeIn(BaseModel):
    name: str

class TaskTypeOut(BaseModel):
    id: str
    name: str

# --- Pydantic model for task update ---
class TaskUpdate(BaseModel):
    name: str | None = None
    taskType: str | None = None
    contacted_person: dict | None = None
    followup_date: str | None = None
    status: str | None = None
    type: str | None = None
    assignedfor: str | None = None

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

# Models for profile- dont modify
class UserProfile(BaseModel):
    id: str
    name: str
    surname: str
    date_of_birth: Optional[str] = ""
    home_address: Optional[str] = ""
    invited_by: Optional[str] = ""
    phone_number: Optional[str] = ""
    email: str
    gender: Optional[str] = ""
    role: str = "user"
    profile_picture: Optional[str] = ""

class UserProfileUpdate(BaseModel):
    name: Optional[str] = None
    surname: Optional[str] = None
    date_of_birth: Optional[str] = None
    home_address: Optional[str] = None
    invited_by: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None
    gender: Optional[str] = None
    profile_picture: Optional[str] = None
    

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


class UserCreater(BaseModel):
    name: str
    surname: str
    email: EmailStr
    password: str
    phone_number: str
    date_of_birth: str  # Keep as string for flexibility
    address: str
    gender: str
    invitedBy: Optional[str] = ""
    leader12: Optional[str] = ""
    leader144: Optional[str] = ""
    leader1728: Optional[str] = ""
    stage: Optional[str] = "Win"
    role: str
class DecisionType(str, Enum):
    FIRST_TIME = "first_time"
    RECOMMITMENT = "recommitment"

class ConsolidationCreate(BaseModel):
    person_name: str
    person_surname: str
    person_email: Optional[str] = None
    person_phone: Optional[str] = None
    decision_type: DecisionType
    decision_date: str
    assigned_to: str
    notes: Optional[str] = None
    event_id: Optional[str] = None
    leaders: List[str] = []
class ConsolidationTask(TaskModel):
    consolidation_id: str
    person_name: str
    person_surname: str
    decision_type: str