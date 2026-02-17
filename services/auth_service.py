"""Auth service - handles authentication and user profile logic"""
from datetime import datetime, timedelta
from bson import ObjectId
from typing import Optional, Dict, Any
from fastapi import HTTPException, BackgroundTasks
import secrets
import logging
import base64
from database import users_collection, people_collection, events_collection, db
from services.utils import hash_password, verify_password
from services.people_service import get_people_cache, people_cache  
from auth.utils import create_access_token, decode_access_token 
from auth.email_utils import send_reset_email
from auth.models import UserCreate, UserLogin, ForgotPasswordRequest, ResetPasswordRequest, RefreshTokenRequest, UserProfile
from fastapi import Body, Request
from fastapi import Depends, Query, HTTPException, File, UploadFile, BackgroundTasks
from fastapi.security import HTTPBearer
from auth.utils import get_current_user
import json

logger = logging.getLogger("auth")

# These should be imported from main.py or env
JWT_EXPIRE_MINUTES = 1440
REFRESH_TOKEN_EXPIRE_DAYS = 30


async def signup(user: UserCreate):
    logger.info(f"Signup attempt: {user.email}")
   
    # Normalize email
    email = user.email.lower().strip()
   
    # Check if user already exists in Users collection ONLY
    existing = await db["Users"].find_one({"email": email})
    if existing:
        logger.warning(f"Signup failed - email already registered: {email}")
        raise HTTPException(status_code=400, detail="Email already registered")

    # Hash password
    hashed = hash_password(user.password)
   
    # Create user document
    user_dict = {
        "name": user.name,
        "surname": user.surname,
        "date_of_birth": user.date_of_birth,
        "home_address": user.home_address,
        "invited_by": user.invited_by,
        "phone_number": user.phone_number,
        "email": email,
        "gender": user.gender,
        "password": hashed,
        "confirm_password": hashed,
        "role": "user",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }
   
    # Insert user into Users collection
    user_result = await db["Users"].insert_one(user_dict)
    logger.info(f"User created successfully: {email}")
   
    # USE BACKGROUND-LOADED CACHE FOR LEADER ASSIGNMENT
    inviter_full_name = user.invited_by.strip()
    leader1 = ""
    leader12 = ""
    leader144 = ""
    leader1728 = ""
   
    if inviter_full_name:
        print(f"Looking for inviter in background cache: '{inviter_full_name}'")
       
        # Search in background-loaded cache (contains ALL people)
        cached_inviter = None
        for person in get_people_cache["data"]:
            full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            if (full_name.lower() == inviter_full_name.lower() or
                person.get('Name', '').lower() == inviter_full_name.lower()):
                cached_inviter = person
                break
       
        if cached_inviter:
            print(f"Found inviter in background cache: {cached_inviter.get('FullName')}")
            # checking if gender is matching so it's not it can just assign them a leader at 12
            isGenderMatching = cached_inviter.get("Gender", "") == user.gender.capitalize()

            print(cached_inviter)
            print(isGenderMatching)
            print(cached_inviter.get("Gender", ""),user.gender.capitalize())

            if  not isGenderMatching:
                if user.gender == "male":
                    leader1 = "Gavin Enslin"
                else:
                    leader1 = "Vicky Enslin"
                leader12 = ""  
                leader144 = ""
                leader1728 = ""    
            else: #if gender is matching should go on as usual
                # Get the inviter's leader hierarchy from cache
                inviter_leader1 = cached_inviter.get("Leader @1", "")
                inviter_leader12 = cached_inviter.get("Leader @12", "")
                inviter_leader144 = cached_inviter.get("Leader @144", "")
                inviter_leader1728 = cached_inviter.get("Leader @1728", "")
                print(cached_inviter,inviter_leader1,inviter_leader12,inviter_leader144,inviter_leader1728)
               
               
                # Determine what level the inviter is at and set leaders accordingly
                if inviter_leader1728:
                    print("1")
                    leader1 = inviter_leader1
                    leader12 = inviter_leader12
                    leader144 = inviter_leader144
                    leader1728 = inviter_full_name
                elif inviter_leader144:
                    print("2")
                    leader1 = inviter_leader1
                    leader12 = inviter_leader12
                    leader144 = inviter_leader144
                    leader1728 = inviter_full_name
                elif inviter_leader12:
                    print("3")
                    leader1 = inviter_leader1
                    leader12 = inviter_leader12
                    leader144 = inviter_full_name
                    leader1728 = ""
                elif inviter_leader1:
                    print("4")
                    leader1 = inviter_leader1
                    leader12 = ""
                    leader144 = ""
                    leader1728 = ""
                else:
                    print("5")
                    leader1 = inviter_leader1
                    leader12 = ""
                    leader144 = ""
                    leader1728 = ""
               
                logger.info(f"Leader hierarchy set for {email}: L1={leader1}, L12={leader12}, L144={leader144}, L1728={leader1728}")
        else:
            print("6")
            print(f"Inviter '{inviter_full_name}' not found in background cache")
            # Fallback: set inviter as Leader @1
            leader1 = inviter_full_name
   
    # Create corresponding person record in People collection
    person_doc = {
        "Name": user.name.strip(),
        "Surname": user.surname.strip(),
        "Email": email,
        "Number": user.phone_number.strip(),
        "Address": user.home_address.strip(),
        "Gender": user.gender.strip(),
        "Birthday": user.date_of_birth,
        "InvitedBy": inviter_full_name,
        "Leader @1": leader1,
        "Leader @12": leader12,
        "Leader @144": leader144,
        "Leader @1728": leader1728,
        "Stage": "Win",
        "Date Created": datetime.utcnow().isoformat(),
        "UpdatedAt": datetime.utcnow().isoformat(),
        "user_id": str(user_result.inserted_id)
    }
   
    try:
        person_result = await people_collection.insert_one(person_doc)
        logger.info(f"Person record created successfully for: {email} (ID: {person_result.inserted_id})")
       
        # ADD THE NEW PERSON TO BACKGROUND CACHE
        new_person_cache_entry = {
            "_id": str(person_result.inserted_id),
            "Name": user.name.strip(),
            "Surname": user.surname.strip(),
            "Email": email,
            "Number": user.phone_number.strip(),
            "Leader @1": leader1,
            "Leader @12": leader12,
            "Leader @144": leader144,
            "Leader @1728": leader1728,
            "FullName": f"{user.name.strip()} {user.surname.strip()}".strip()
        }
        people_cache["data"].append(new_person_cache_entry)
        print(f"Added new person to background cache: {new_person_cache_entry['FullName']}")
       
    except Exception as e:
        logger.error(f"Failed to create person record for {email}: {e}")
   
    return {"message": "User created successfully"}


async def login(user: UserLogin):
    logger.info(f"Login attempt: {user.email}")
    existing = await users_collection.find_one({"email": user.email})
    if not existing or not verify_password(user.password, existing["password"]):
        logger.warning(f"Login failed: {user.email}")
        raise HTTPException(status_code=401, detail="Invalid credentials")
    person = await people_collection.find_one({"Email":user.email}) or {}


    full_name = f"{person.get('Name') or ''} {person.get('Surname') or ''}"
    print("FULL NAME",full_name)
    is_Leader = await events_collection.find_one({"$or":[{"Email":user.email,"Event Type":"Cells"},{"Leader":full_name,"Event Type":"Cells"}]})
    is_Leader = bool(is_Leader)
    if not person:
        person = await people_collection.find_one({"Name":existing["name"], "Surname":existing["surname"]}) or {}
   

    access_token = create_access_token(
        {"user_id": str(existing["_id"]), "email": existing["email"], "role": existing.get("role", "user")},
        expires_delta=timedelta(minutes=JWT_EXPIRE_MINUTES)
    )

    refresh_token_id = secrets.token_urlsafe(16)
    refresh_plain = secrets.token_urlsafe(32)
    refresh_hash = hash_password(refresh_plain)
    refresh_expires = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    await users_collection.update_one(
        {"_id": existing["_id"]},
        {"$set": {
            "refresh_token_id": refresh_token_id,
            "refresh_token_hash": refresh_hash,
            "refresh_token_expires": refresh_expires,
        }}
    )

    logger.info(f"Login successful: {user.email}")
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "refresh_token_id": refresh_token_id,
        "refresh_token": refresh_plain,
        "user": {
        "id": str(existing["_id"]),
        "email": existing["email"],
        "name": existing.get("name", ""),
        "surname": existing.get("surname", ""),
        "role": existing.get("role", "registrant"),
        "date_of_birth": existing.get("date_of_birth", ""),
        "home_address": existing.get("home_address", ""),
        "phone_number": existing.get("phone_number", ""),
        "gender": existing.get("gender", ""),
        "invited_by": existing.get("invited_by", "")
    },
    "leaders":{
        'leaderAt1':person.get("Leader @1",""),
        'leaderAt12':person.get("Leader @12",""),
        'leaderAt144':person.get("Leader @144",""),
    },
    "isLeader": is_Leader
    }


async def forgot_password(payload: ForgotPasswordRequest, background_tasks: BackgroundTasks):
    logger.info(f"Forgot password requested for email: {payload.email}")
   
    # Find the user by email
    user = await users_collection.find_one({"email": payload.email})
   
    if not user:
        logger.info(f"Forgot password - email not found: {payload.email}")
        return {"message": "If your email exists, a reset link has been sent."}

    # Create a reset token valid for 1 hour
    reset_token = create_access_token(
        {"user_id": str(user["_id"])},
        expires_delta=timedelta(hours=1)
    )
   
    reset_link = f"https://teams.theactivechurch.org/reset-password?token={reset_token}"
    recipient_name = user.get("name", "there")  # Default to "there" if name missing

    logger.info(f"Reset link generated for {payload.email}")

    # Add background task with all required arguments
    background_tasks.add_task(send_reset_email, payload.email, recipient_name, reset_link)
    logger.info(f"Reset email task scheduled for {payload.email}")

    return {"message": "If your email exists, a reset link has been sent."}


async def reset_password(data: ResetPasswordRequest):
    try:
        payload = decode_access_token(data.token)
    except Exception:
        logger.warning("Invalid or expired reset token")
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    user_id = payload.get("user_id")
    if not user_id:
        logger.warning("Invalid token payload")
        raise HTTPException(status_code=400, detail="Invalid token payload")

    hashed_pw = hash_password(data.new_password)
    result = await users_collection.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"password": hashed_pw, "confirm_password": hashed_pw}}
    )

    if result.modified_count == 0:
        logger.warning(f"Reset password failed - user not found or unchanged: {user_id}")
        raise HTTPException(status_code=404, detail="User not found or password unchanged")

    user = await users_collection.find_one({"_id": ObjectId(user_id)})
    access_token = create_access_token(
        {"user_id": str(user["_id"]), "email": user["email"], "role": user.get("role", "user")},
        expires_delta=timedelta(minutes=JWT_EXPIRE_MINUTES)
    )

    logger.info(f"Password reset successful for {user['email']}")
    return {
        "message": "Password has been reset successfully.",
        "access_token": access_token,
        "token_type": "bearer"
    }


async def refresh_token(payload: RefreshTokenRequest = Body(...)):
    logger.info(f"Refresh token requested: {payload.refresh_token_id}")
    user = await users_collection.find_one({"refresh_token_id": payload.refresh_token_id})
    if (
        not user
        or not user.get("refresh_token_hash")
        or not verify_password(payload.refresh_token, user["refresh_token_hash"])
        or not user.get("refresh_token_expires")
        or user["refresh_token_expires"] < datetime.utcnow()
    ):
        logger.warning(f"Refresh token invalid/expired: {payload.refresh_token_id}")
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    token = create_access_token(
        {"user_id": str(user["_id"]), "email": user["email"], "role": user.get("role", "user")},
        expires_delta=timedelta(minutes=JWT_EXPIRE_MINUTES)
    )

    new_refresh_token_id = secrets.token_urlsafe(16)
    new_refresh_plain = secrets.token_urlsafe(32)
    new_refresh_hash = hash_password(new_refresh_plain)
    new_refresh_expires = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    await users_collection.update_one(
        {"_id": user["_id"]},
        {"$set": {
            "refresh_token_id": new_refresh_token_id,
            "refresh_token_hash": new_refresh_hash,
            "refresh_token_expires": new_refresh_expires,
        }},
    )

    logger.info(f"Refresh token rotated for user: {user['email']}")
    return {
        "access_token": token,
        "token_type": "bearer",
        "refresh_token_id": new_refresh_token_id,
        "refresh_token": new_refresh_plain,
    }


async def logout(user_id: str = Body(..., embed=True)):
    await users_collection.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {
            "refresh_token_id": None,
            "refresh_token_hash": None,
            "refresh_token_expires": None,
        }},
    )
    logger.info(f"User logged out: {user_id}")
    return {"message": "Logged out successfully"}


def normalize_gender_value(gender):
    """Normalize gender values to consistent format"""
    if not gender:
        return gender
   
    gender = str(gender).strip()
    gender_map = {
        'male': 'Male',
        'female': 'Female',
        'm': 'Male',
        'f': 'Female',
        'Male': 'Male',
        'Female': 'Female',
        'Other': 'Other',
        'Prefer not to say': 'Prefer not to say'
    }
   
    return gender_map.get(gender, gender)


def format_user_response(user):
    """Format user document for response"""
    return {
        "id": str(user["_id"]),
        "name": user.get("name", ""),
        "surname": user.get("surname", ""),
        "date_of_birth": user.get("date_of_birth", ""),
        "home_address": user.get("home_address", ""),
        "invited_by": user.get("invited_by", ""),
        "phone_number": user.get("phone_number", ""),
        "email": user.get("email", ""),
        "gender": normalize_gender_value(user.get("gender", "")),
        "role": user.get("role", "user"),
        "profile_picture": user.get("profile_picture", ""),
    }


async def get_profile(user_id: str, current_user: dict = Depends(get_current_user)):
    """Get user profile - uses consistent authentication"""
    # Verify user owns this account
    token_user_id = current_user.get("user_id")
   
    if not token_user_id or token_user_id != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to access this profile")

    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=400, detail="Invalid user ID")

    user = await users_collection.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "id": str(user["_id"]),
        "name": user.get("name", ""),
        "surname": user.get("surname", ""),
        "date_of_birth": user.get("date_of_birth", ""),
        "home_address": user.get("home_address", ""),
        "invited_by": user.get("invited_by", ""),
        "phone_number": user.get("phone_number", ""),
        "email": user.get("email", ""),
        "gender": user.get("gender", ""),
        "role": user.get("role", "user"),
        "profile_picture": user.get("profile_picture", ""),
    }


async def update_profile(
    user_id: str,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    """Complete working profile update endpoint"""
    try:
        print(f"PROFILE UPDATE ENDPOINT CALLED")
        print(f"User ID from URL: {user_id}")
        print(f"Current User ID from Token: {current_user.get('user_id')}")
       
        # Check authorization
        token_user_id = current_user.get("user_id")
        if not token_user_id or token_user_id != user_id:
            print(f"AUTHORIZATION FAILED: {token_user_id} != {user_id}")
            raise HTTPException(status_code=403, detail="Not authorized to update this profile")

        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user ID")

        # Get and parse request body
        body = await request.body()
        body_str = body.decode('utf-8')
        print(f"RAW REQUEST BODY: {body_str}")
       
        try:
            update_data = json.loads(body_str)
        except json.JSONDecodeError as e:
            print(f"JSON PARSE ERROR: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

        print(f"PARSED UPDATE DATA: {update_data}")

        # Check if user exists
        existing_user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not existing_user:
            print(f"USER NOT FOUND: {user_id}")
            raise HTTPException(status_code=404, detail="User not found")

        # Build update payload - handle all possible field names
        update_payload = {}
       
        # Field mapping from frontend to database
        field_mapping = {
            # Direct mappings
            "name": "name",
            "surname": "surname",
            "email": "email",
            "date_of_birth": "date_of_birth",
            "home_address": "home_address",
            "phone_number": "phone_number",
            "invited_by": "invited_by",
            "gender": "gender",
            "profile_picture": "profile_picture",
           
            # Alternative field names from frontend
            "dob": "date_of_birth",
            "address": "home_address",
            "invitedBy": "invited_by",
            "phone": "phone_number"
        }
       
        # Map all fields
        for frontend_field, db_field in field_mapping.items():
            if frontend_field in update_data:
                value = update_data[frontend_field]
                if value is not None and value != "":
                    # Normalize gender values
                    if db_field == "gender":
                        value = normalize_gender_value(value)
                   
                    update_payload[db_field] = value
                    print(f"Mapping {frontend_field} -> {db_field}: {value}")

        # Add update timestamp
        update_payload["updated_at"] = datetime.utcnow().isoformat()
       
        print(f" FINAL UPDATE PAYLOAD: {update_payload}")

        if not update_payload:
            print("No fields to update")
            return {
                "message": "No changes to update",
                "user": format_user_response(existing_user)
            }

        # Perform the update
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": update_payload}
        )

        print(f"UPDATE RESULT - matched: {result.matched_count}, modified: {result.modified_count}")

        # Fetch and return updated user
        updated_user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not updated_user:
            raise HTTPException(status_code=404, detail="User not found after update")

        response_data = format_user_response(updated_user)
        print(f"UPDATE SUCCESSFUL: {response_data}")

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        print(f"UNEXPECTED ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


async def upload_avatar(
    user_id: str,
    avatar: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    """Upload profile picture - uses consistent authentication"""
    try:
        # Verify user owns this account
        token_user_id = current_user.get("user_id")
       
        if not token_user_id or token_user_id != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to update this profile")
       
        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user ID")

        # Validate file type
        if not avatar.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File must be an image")

        # Validate file size (e.g., max 5MB)
        contents = await avatar.read()
        if len(contents) > 5 * 1024 * 1024:  # 5MB
            raise HTTPException(status_code=400, detail="File too large. Maximum size is 5MB")
       
        # Convert to base64 for storage
        image_base64 = base64.b64encode(contents).decode('utf-8')
        image_data_url = f"data:{avatar.content_type};base64,{image_base64}"

        # Update user with profile picture
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"profile_picture": image_data_url}}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="User not found")

        return {"message": "Avatar uploaded successfully", "avatarUrl": image_data_url}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading avatar: {str(e)}")


async def change_password(
    user_id: str,
    password_data: dict,
    current_user: dict = Depends(get_current_user)
):
    """Change user password - uses consistent authentication"""
    try:
        # Verify user owns this account
        token_user_id = current_user.get("user_id")
       
        if not token_user_id or token_user_id != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to update this profile")

        if not ObjectId.is_valid(user_id):
            raise HTTPException(status_code=400, detail="Invalid user ID")

        current_password = password_data.get("currentPassword")
        new_password = password_data.get("newPassword")

        if not current_password or not new_password:
            raise HTTPException(status_code=400, detail="Current password and new password are required")

        # Basic password validation
        if len(new_password) < 8:
            raise HTTPException(status_code=400, detail="New password must be at least 8 characters long")

        # Get user and verify current password
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Verify current password
        if not verify_password(current_password, user["password"]):
            raise HTTPException(status_code=400, detail="Current password is incorrect")

        # Hash new password and update
        hashed_new_password = hash_password(new_password)
       
        result = await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"password": hashed_new_password}}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update password")

        return {"message": "Password updated successfully"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error changing password: {str(e)}")

