from fastapi import FastAPI, HTTPException, Request, APIRouter
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

from pydantic import BaseModel, Field, EmailStr, field_validator

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
    recurring_day: Optional[List[str]] = Field(default_factory=list)
    location: str
    eventLeader: Optional[str] = None
    description: Optional[str] = None
    isTicketed: Optional[bool] = False
    price: Optional[float] = 0.0
    userEmail: Optional[str] = None
    leader1: Optional[str] = None
    leader12: Optional[str] = None
    email: Optional[str] = None

class EventCreate(EventBase):
    """Schema for creating events (inherits from EventBase)."""
    pass


class Attendee(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None  # for basic events
    fullName: Optional[str] = None  # for cell events
    leader12: Optional[str] = None
    leader144: Optional[str] = None
    time: Optional[datetime] = None

    @field_validator("name", mode="before")
    def set_name_if_fullName_missing(cls, v, values):
        # If "name" is missing but "fullName" exists, use that
        return v or values.get("fullName")

class AttendanceSubmission(BaseModel):
    attendees: List[Attendee]
    leaderEmail: str  # ✅ required
    leaderName: str   # ✅ required
    did_not_meet: Optional[bool] = False


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    formatted = [
        {"field": ".".join(err["loc"][1:]), "message": err["msg"]}
        for err in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content={"errors": formatted}
    )

# ===== EventTypes =====
class EventTypeCreate(BaseModel):
    name: str
    isTicketed: Optional[bool] = False
    isGlobal: Optional[bool] = False
    hasPersonSteps: Optional[bool] = False
    description: str
    createdAt: Optional[datetime] = None



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

    class Config:
        validate_by_name = True
        arbitrary_types_allowed = True


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