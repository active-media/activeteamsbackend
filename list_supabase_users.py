#!/usr/bin/env python3
"""List all users in Supabase Auth using service role"""

import os
from dotenv import load_dotenv

load_dotenv()

try:
    from supabase_helpers.supabase_connection import supabase_admin
    
    print("=" * 60)
    print("SUPABASE USERS LIST (Using Service Role)")
    print("=" * 60 + "\n")
    
    # Get all users using service role (admin access)
    response = supabase_admin.auth.admin.list_users()
    
    users = response.users if hasattr(response, 'users') else response
    
    if users:
        print(f"Found {len(users)} user(s):\n")
        for i, user in enumerate(users, 1):
            print(f"{i}. Email: {user.email}")
            print(f"   ID: {user.id}")
            print(f"   Confirmed: {user.email_confirmed_at is not None}")
            print(f"   Last Sign In: {user.last_sign_in_at}")
            print()
    else:
        print("No users found")
        
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
