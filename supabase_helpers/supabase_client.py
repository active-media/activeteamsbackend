import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

print("SUPABASE URL:", SUPABASE_URL)
print("SUPABASE ANON KEY loaded:", bool(SUPABASE_KEY))
print("SUPABASE SERVICE KEY loaded:", bool(SUPABASE_SERVICE_KEY))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError('SUPABASE_URL and SUPABASE_KEY must be set in environment')

# ⚠️ RLS NOTE: see supabase_connection.py — this project's RLS only permits
# authenticated user JWTs, so the anon client returns 0 rows for the core
# tables (People, Tasks, events, Task Types...). The backend is trusted
# server-side code, so `supabase` is the service-role client (kept under the
# same name so all importing modules keep working). Org scoping for
# user-facing data is applied in application code.
_service_key = SUPABASE_SERVICE_KEY or SUPABASE_KEY

supabase: Client = create_client(SUPABASE_URL, _service_key)
supabase_admin: Client = create_client(SUPABASE_URL, _service_key)