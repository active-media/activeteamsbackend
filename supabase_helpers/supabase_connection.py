import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

load_dotenv()

print("========== SUPABASE DEBUG ==========")
print("SUPABASE URL:", os.getenv("SUPABASE_URL"))
print("SUPABASE ANON KEY LOADED:", bool(os.getenv("SUPABASE_KEY")))
print("SUPABASE SERVICE KEY LOADED:", bool(os.getenv("SUPABASE_SERVICE_ROLE_KEY")))
print("=====================================")

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')


if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError('SUPABASE_URL and SUPABASE_KEY must be set in environment')

# ⚠️ RLS NOTE: this project enforces row-level security that only permits
# authenticated user JWTs. The anon key returns "200 OK" with ZERO rows for
# People / Tasks / events / Task Types etc., which made every server-side
# cache and list endpoint appear empty. The backend is trusted server-side
# code, so `supabase` is the SERVICE-role client; the `supabase` name is kept
# so every importing module keeps working unchanged. Org scoping for
# user-facing data is enforced in application code (org_id filters), and
# `supabase_admin` remains exported for explicit admin use.
_service_key = SUPABASE_SERVICE_KEY or SUPABASE_KEY

supabase: Client = create_client(SUPABASE_URL, _service_key)
supabase_admin: Client = create_client(SUPABASE_URL, _service_key)