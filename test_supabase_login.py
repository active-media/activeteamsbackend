#!/usr/bin/env python3
"""Test Supabase authentication"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("=" * 60)
print("SUPABASE CONNECTION TEST")
print("=" * 60)

# Check environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

print(f"\n✓ SUPABASE_URL: {SUPABASE_URL}")
print(f"✓ SUPABASE_KEY: {SUPABASE_KEY[:20]}..." if SUPABASE_KEY else "✗ SUPABASE_KEY: NOT SET")
print(f"✓ SUPABASE_SERVICE_ROLE_KEY: {SUPABASE_SERVICE_ROLE_KEY[:20]}..." if SUPABASE_SERVICE_ROLE_KEY else "✗ SUPABASE_SERVICE_ROLE_KEY: NOT SET")

# Test connection
try:
    from supabase_helpers.supabase_connection import supabase as supabase_anon
    print("\n✓ Supabase anon client imported successfully")
    
    # Try to authenticate
    print("\n" + "=" * 60)
    print("ATTEMPTING LOGIN")
    print("=" * 60)
    
    email = "madlanganhlaka@gmail.com"
    password = "Nhl@kanipho99"
    
    print(f"\nEmail: {email}")
    print(f"Password: {'*' * len(password)}")
    
    auth_result = supabase_anon.auth.sign_in_with_password({
        "email": email,
        "password": password,
    })
    
    if auth_result.session and auth_result.user:
        print("\n✅ LOGIN SUCCESSFUL!")
        print(f"User ID: {auth_result.user.id}")
        print(f"Email: {auth_result.user.email}")
        print(f"Access Token: {auth_result.session.access_token[:30]}...")
    else:
        print("\n❌ LOGIN FAILED - No session or user")
        print(f"Auth result: {auth_result}")
        
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
