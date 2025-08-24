from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, EmailStr, field_validator, Field
from typing import Optional, Literal, List
from datetime import datetime
from fastapi.exceptions import RequestValidationError
from bson import ObjectId
from fastapi.responses import JSONResponse


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
    recurringDays: List[str] = Field(default_factory=list)
    location: str
    eventLeader: Optional[str] = None
    description: Optional[str] = None
    isTicketed: Optional[bool] = False
    price: Optional[float] = 0.0

class EventCreate(EventBase):
    # No need to redefine fields unless overriding defaults
    pass # override default None with empty list


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    formatted = [
        {"field": ".".join(err["loc"][1:]), "message": err["msg"]}
        for err in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content={"errors": formatted}
    )

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

# ===== Cell Member Management =====
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
