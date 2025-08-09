from pydantic import BaseModel, EmailStr, field_validator
from typing import Optional, Literal, List
from datetime import datetime

# User creation model with role
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
    role: Optional[str] = None  # Optional role; default to 'user' in signup logic

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class Event(BaseModel):
    eventType: str
    service_name: str
    date: str 
    location: str
    total_attendance: int = 0
    attendees: list[dict] = []

class CheckIn(BaseModel):
    event_id: str
    name: str

class UncaptureRequest(BaseModel):
    event_id: str
    name: str

# Authentication models
class TokenResponse(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    sub: Optional[str] = None
    role: Optional[str] = None


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
        """Override output to capitalize the day when returning data."""
        data = super().model_dump(**kwargs)
        if data.get("recurring_day"):
            data["recurring_day"] = data["recurring_day"].capitalize()
        return data
    
class RefreshTokenRequest(BaseModel):
    refresh_token_id: str
    refresh_token: str

class AddMemberNamesRequest(BaseModel):
     name: str

class RemoveMemberRequest(BaseModel):
    name: str
