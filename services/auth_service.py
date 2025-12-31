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
from services.people_service import get_people_cache
from auth.utils import create_access_token
from auth.email_utils import send_reset_email

logger = logging.getLogger("auth")

# These should be imported from main.py or env
JWT_EXPIRE_MINUTES = 1440
REFRESH_TOKEN_EXPIRE_DAYS = 30


async def signup(user_data):
    """Handle user signup"""
    logger.info(f"Signup attempt: {user_data.email}")
    
    email = user_data.email.lower().strip()
    
    existing = await db["Users"].find_one({"email": email})
    if existing:
        logger.warning(f"Signup failed - email already registered: {email}")
        raise HTTPException(status_code=400, detail="Email already registered")

    hashed = hash_password(user_data.password)
    
    user_dict = {
        "name": user_data.name,
        "surname": user_data.surname,
        "date_of_birth": user_data.date_of_birth,
        "home_address": user_data.home_address,
        "invited_by": user_data.invited_by,
        "phone_number": user_data.phone_number,
        "email": email,
        "gender": user_data.gender,
        "password": hashed,
        "confirm_password": hashed,
        "role": "user",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }
    
    user_result = await db["Users"].insert_one(user_dict)
    logger.info(f"User created successfully: {email}")
    
    # Get people cache for leader assignment
    people_cache = get_people_cache()
    inviter_full_name = user_data.invited_by.strip()
    leader1 = ""
    leader12 = ""
    leader144 = ""
    leader1728 = ""
    
    if inviter_full_name:
        print(f"Looking for inviter in background cache: '{inviter_full_name}'")
        
        cached_inviter = None
        for person in people_cache["data"]:
            full_name = f"{person.get('Name', '')} {person.get('Surname', '')}".strip()
            if (full_name.lower() == inviter_full_name.lower() or
                person.get('Name', '').lower() == inviter_full_name.lower()):
                cached_inviter = person
                break
        
        if cached_inviter:
            print(f"Found inviter in background cache: {cached_inviter.get('FullName')}")
            isGenderMatching = cached_inviter.get("Gender", "") == user_data.gender.capitalize()

            if not isGenderMatching:
                if user_data.gender == "male":
                    leader1 = "Gavin Enslin"
                else:
                    leader1 = "Vicky Enslin"
                leader12 = ""  
                leader144 = ""
                leader1728 = ""    
            else:
                inviter_leader1 = cached_inviter.get("Leader @1", "")
                inviter_leader12 = cached_inviter.get("Leader @12", "")
                inviter_leader144 = cached_inviter.get("Leader @144", "")
                inviter_leader1728 = cached_inviter.get("Leader @1728", "")
                
                if inviter_leader1728:
                    leader1 = inviter_leader1
                    leader12 = inviter_leader12
                    leader144 = inviter_leader144
                    leader1728 = inviter_full_name
                elif inviter_leader144:
                    leader1 = inviter_leader1
                    leader12 = inviter_leader12
                    leader144 = inviter_leader144
                    leader1728 = inviter_full_name
                elif inviter_leader12:
                    leader1 = inviter_leader1
                    leader12 = inviter_leader12
                    leader144 = inviter_full_name
                    leader1728 = ""
                elif inviter_leader1:
                    leader1 = inviter_leader1
                    leader12 = ""
                    leader144 = ""
                    leader1728 = ""
                else:
                    leader1 = inviter_leader1
                    leader12 = ""
                    leader144 = ""
                    leader1728 = ""
                
                logger.info(f"Leader hierarchy set for {email}: L1={leader1}, L12={leader12}, L144={leader144}, L1728={leader1728}")
        else:
            print(f"Inviter '{inviter_full_name}' not found in background cache")
            leader1 = inviter_full_name
    
    # Create person record
    person_doc = {
        "Name": user_data.name.strip(),
        "Surname": user_data.surname.strip(),
        "Email": email,
        "Number": user_data.phone_number.strip(),
        "Address": user_data.home_address.strip(),
        "Gender": user_data.gender.strip(),
        "Birthday": user_data.date_of_birth,
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
        
        # Add to cache
        new_person_cache_entry = {
            "_id": str(person_result.inserted_id),
            "Name": user_data.name.strip(),
            "Surname": user_data.surname.strip(),
            "Email": email,
            "Number": user_data.phone_number.strip(),
            "Leader @1": leader1,
            "Leader @12": leader12,
            "Leader @144": leader144,
            "Leader @1728": leader1728,
            "FullName": f"{user_data.name.strip()} {user_data.surname.strip()}".strip()
        }
        people_cache["data"].append(new_person_cache_entry)
        print(f"Added new person to background cache: {new_person_cache_entry['FullName']}")
        
    except Exception as e:
        logger.error(f"Failed to create person record for {email}: {e}")
    
    return {"message": "User created successfully"}


async def login(user_data):
    """Handle user login"""
    logger.info(f"Login attempt: {user_data.email}")
    existing = await users_collection.find_one({"email": user_data.email})
    if not existing or not verify_password(user_data.password, existing["password"]):
        logger.warning(f"Login failed: {user_data.email}")
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    person = await people_collection.find_one({"Email": user_data.email}) or {}
    
    full_name = f"{person.get('Name') or ''} {person.get('Surname') or ''}"
    print("FULL NAME", full_name)
    is_Leader = await events_collection.find_one({"$or": [{"Email": user_data.email, "Event Type": "Cells"}, {"Leader": full_name, "Event Type": "Cells"}]})
    is_Leader = bool(is_Leader)
    if not person:
        person = await people_collection.find_one({"Name": existing["name"], "Surname": existing["surname"]}) or {}
    
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

    logger.info(f"Login successful: {user_data.email}")
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
        "leaders": {
            'leaderAt1': person.get("Leader @1", ""),
            'leaderAt12': person.get("Leader @12", ""),
            'leaderAt144': person.get("Leader @144", ""),
        },
        "isLeader": is_Leader
    }


async def forgot_password(payload, background_tasks: BackgroundTasks):
    """Handle forgot password request"""
    logger.info(f"Forgot password requested for email: {payload.email}")
    
    user = await users_collection.find_one({"email": payload.email})
    
    if not user:
        logger.info(f"Forgot password - email not found: {payload.email}")
        return {"message": "If your email exists, a reset link has been sent."}

    reset_token = create_access_token(
        {"user_id": str(user["_id"])},
        expires_delta=timedelta(hours=1)
    )
    
    reset_link = f"https://teams.theactivechurch.org/reset-password?token={reset_token}"
    recipient_name = user.get("name", "there")
    
    background_tasks.add_task(send_reset_email, payload.email, recipient_name, reset_link)
    
    return {"message": "If your email exists, a reset link has been sent."}


async def reset_password(data):
    """Handle password reset"""
    try:
        from auth.utils import decode_access_token
        
        decoded = decode_access_token(data.token)
        user_id = decoded.get("user_id")
        
        if not user_id:
            raise HTTPException(status_code=400, detail="Invalid token")
        
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        hashed = hash_password(data.new_password)
        await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"password": hashed, "updated_at": datetime.utcnow().isoformat()}}
        )
        
        return {"message": "Password reset successfully"}
    except Exception as e:
        logger.error(f"Error resetting password: {e}")
        raise HTTPException(status_code=400, detail="Invalid or expired token")


async def refresh_token(payload):
    """Handle token refresh"""
    try:
        from auth.utils import decode_access_token
        
        user = await users_collection.find_one({"refresh_token_id": payload.refresh_token_id})
        if not user:
            raise HTTPException(status_code=401, detail="Invalid refresh token")
        
        if not verify_password(payload.refresh_token, user.get("refresh_token_hash", "")):
            raise HTTPException(status_code=401, detail="Invalid refresh token")
        
        if datetime.utcnow() > user.get("refresh_token_expires", datetime.min):
            raise HTTPException(status_code=401, detail="Refresh token expired")
        
        access_token = create_access_token(
            {"user_id": str(user["_id"]), "email": user["email"], "role": user.get("role", "user")},
            expires_delta=timedelta(minutes=JWT_EXPIRE_MINUTES)
        )
        
        return {"access_token": access_token, "token_type": "bearer"}
    except Exception as e:
        logger.error(f"Error refreshing token: {e}")
        raise HTTPException(status_code=401, detail="Invalid refresh token")


async def logout(user_id: str):
    """Handle user logout"""
    try:
        await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$unset": {"refresh_token_id": "", "refresh_token_hash": "", "refresh_token_expires": ""}}
        )
        return {"message": "Logged out successfully"}
    except Exception as e:
        logger.error(f"Error logging out: {e}")
        raise HTTPException(status_code=500, detail="Error logging out")


def normalize_gender_value(gender):
    """Normalize gender value"""
    if not gender:
        return None
    gender_lower = gender.lower().strip()
    if gender_lower in ["male", "m"]:
        return "Male"
    elif gender_lower in ["female", "f"]:
        return "Female"
    return gender.capitalize()


def format_user_response(user):
    """Format user response"""
    return {
        "_id": str(user["_id"]),
        "email": user.get("email", ""),
        "name": user.get("name", ""),
        "surname": user.get("surname", ""),
        "role": user.get("role", "user"),
        "date_of_birth": user.get("date_of_birth", ""),
        "home_address": user.get("home_address", ""),
        "phone_number": user.get("phone_number", ""),
        "gender": user.get("gender", ""),
        "invited_by": user.get("invited_by", ""),
        "created_at": user.get("created_at", ""),
        "updated_at": user.get("updated_at", "")
    }


async def get_profile(user_id: str):
    """Get user profile"""
    try:
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        person = await people_collection.find_one({"Email": user.get("email", "")}) or {}
        
        return {
            "user": format_user_response(user),
            "person": {
                "_id": str(person.get("_id", "")),
                "Name": person.get("Name", ""),
                "Surname": person.get("Surname", ""),
                "Email": person.get("Email", ""),
                "Number": person.get("Number", ""),
                "Address": person.get("Address", ""),
                "Gender": person.get("Gender", ""),
                "Birthday": person.get("Birthday", ""),
                "Leader @1": person.get("Leader @1", ""),
                "Leader @12": person.get("Leader @12", ""),
                "Leader @144": person.get("Leader @144", ""),
                "Leader @1728": person.get("Leader @1728", ""),
            } if person else None
        }
    except Exception as e:
        logger.error(f"Error getting profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def update_profile(user_id: str, update_data: dict):
    """Update user profile"""
    try:
        user_updates = {}
        person_updates = {}
        
        # Map fields to appropriate collections
        field_mapping = {
            "name": ("user", "name"),
            "surname": ("user", "surname"),
            "date_of_birth": ("user", "date_of_birth"),
            "home_address": ("user", "home_address"),
            "phone_number": ("user", "phone_number"),
            "gender": ("user", "gender"),
            "Name": ("person", "Name"),
            "Surname": ("person", "Surname"),
            "Number": ("person", "Number"),
            "Address": ("person", "Address"),
            "Gender": ("person", "Gender"),
            "Birthday": ("person", "Birthday"),
        }
        
        for key, value in update_data.items():
            if key in field_mapping:
                collection, field = field_mapping[key]
                if collection == "user":
                    user_updates[field] = value
                else:
                    person_updates[field] = value
        
        if user_updates:
            user_updates["updated_at"] = datetime.utcnow().isoformat()
            await users_collection.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": user_updates}
            )
        
        if person_updates:
            user = await users_collection.find_one({"_id": ObjectId(user_id)})
            if user:
                email = user.get("email", "")
                person_updates["UpdatedAt"] = datetime.utcnow().isoformat()
                await people_collection.update_one(
                    {"Email": email},
                    {"$set": person_updates}
                )
        
        return await get_profile(user_id)
    except Exception as e:
        logger.error(f"Error updating profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def upload_avatar(user_id: str, avatar_data: bytes):
    """Upload user avatar"""
    try:
        avatar_base64 = base64.b64encode(avatar_data).decode('utf-8')
        await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"avatar": avatar_base64, "updated_at": datetime.utcnow().isoformat()}}
        )
        return {"message": "Avatar uploaded successfully"}
    except Exception as e:
        logger.error(f"Error uploading avatar: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def change_password(user_id: str, password_data: dict, current_user: dict):
    """Change user password"""
    try:
        if str(current_user.get("_id", "")) != user_id and current_user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Not authorized")
        
        user = await users_collection.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if not verify_password(password_data.get("current_password", ""), user.get("password", "")):
            raise HTTPException(status_code=400, detail="Current password is incorrect")
        
        hashed = hash_password(password_data.get("new_password", ""))
        await users_collection.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"password": hashed, "updated_at": datetime.utcnow().isoformat()}}
        )
        
        return {"message": "Password changed successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error changing password: {e}")
        raise HTTPException(status_code=500, detail=f"Error changing password: {str(e)}")

