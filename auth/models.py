from pydantic import BaseModel, EmailStr
from datetime import datetime
from bson import ObjectId


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
    
class UserLogin(BaseModel):
    email: EmailStr
    password: str

# Nested contacted_person model
class ContactedPerson(BaseModel):
    name: str
    phone: str
    email: EmailStr


# Main task model
class TaskModel(BaseModel):
    
    memberID: str
    name: str
    contacted_person: ContactedPerson
    followup_date: datetime
    status: str

    class Config:
        validate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
