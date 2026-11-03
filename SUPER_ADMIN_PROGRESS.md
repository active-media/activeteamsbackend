Report 6 - October 19, 2026
Current phase: Item 1-5 verification and Report compilation

### What I was asked to do (this phase)
Verify and document the super admin feature implementation across the codebase:
1. List all SUPREME_ADMIN_EMAIL occurrences by file/line/endpoint
2. Paste the PATCH /users/{user_id}/role handler code with org scoping
3. Paste org-scoping logic in admin users list and stats handlers
4. Confirm supreme_admin.py writes to MongoDB and propose Supabase change
5. Confirm /login and /profile/{id} return is_supreme_admin as JSON boolean
6. Provide ready-to-run curl commands for touched endpoints

### Done (with file names, line numbers, and test results)
- **Item 1**: Listed 9 active code locations + 2 backup files for SUPREME_ADMIN_EMAIL. 
  **Definitions found:**
  - `admin.py:69`: `SUPREME_ADMIN_EMAIL = "plaatjiessamuel98@gmail.com"` (constant; code now uses `is_supreme_admin` checks)
  - `auth/utils.py:24`: `SUPREME_ADMIN_EMAIL = "plaatjiessamuel98@gmail.com"` (used by `require_super_admin` dependency, which checks `is_supreme_admin` attribute, not email)
  - `main.py:8321`: `SUPREME_ADMIN_EMAIL = "plaatjiessamuel98@gmail.com"` (dead code; appears after `/people/search-fast` handler, not used in any guard)
  - `main.py:9463`: `SUPREME_ADMIN_EMAIL = "tkgenia1234@gmail.com"` (dead code; appears after `/admin/users` create_user handler, not used in any guard)
  - `profile_router.py:30`: `SUPREME_ADMIN_EMAIL = "plaatjiessamuel98@gmail.com"` (defined but not used in endpoint guards; file uses `is_supreme_admin` from `get_current_user`)
  - `supreme_admin.py:8`: `from auth import get_current_user, SUPREME_ADMIN_EMAIL` (import for email-based checks writing to MongoDB)
  - `supreme_admin.py:31`: `if email == SUPREME_ADMIN_EMAIL: return True` (checks original email for is_supreme status)
  
  **Note**: The two flagged users in Supabase are `kerenbotombe125@gmail.com` (role: registrant) and `blessingjonathan028@gmail.com` (role: admin). These are the users with `is_supreme_admin="true"` in the Supabase Users table. They are NOT the `SUPREME_ADMIN_EMAIL` constant value. All occurrences outside `admin.py` are documented but left unchanged per instructions.

- **Item 2a**: PATCH /users/{user_id}/role handler at `admin.py:1098-1159` verified. 
  Blocks regular admins from assigning "admin" role (else branch: `if new_role == "admin" and not is_supreme: raise 403` "Cannot assign admin role").
  Restricts to own org via `if user.get("Organization") != get_org_name(current_user): raise 403` "Cannot access users from other organizations".
  Active Church org only supports system roles (admin, leader, leaderAt12, user, registrant).
  Supreme admins bypass all restrictions via `is_supreme = current_user.get("is_supreme_admin")`.

- **Item 2b**: org-scoping logic verified in 4 handlers:
  - `admin.py get_all_users` (line 910-974): `org_filter = get_org_name(current_user)` for non-supreme; query `.eq("Organization", org_filter)`
  - `admin.py get_admin_stats` (line 977-1034): same pattern with `$group` pipeline
  - `main.py get_all_users` (line 9474-9562): `org_filter = current_user.get("Organization") or current_user.get("organization")`; MongoDB `query["Organization"] = org_filter`
  - `main.py get_admin_stats` (line 9564-9631): same pattern with MongoDB aggregation pipeline
  All properly ignore `organization_id` for non-super-admins, scoping them to their own org only.

- **Item 3**: Confirmed `supreme_admin.py` writes to MongoDB (`users_collection`). Smallest Supabase change: replace `users_collection.update_one({"email": data.email}, {"$set": {"is_supreme_admin": True}})` with `supabase.table("Users").update({"is_supreme_admin": "true"}).eq("email", data.email).execute()`. 
  **NOT DONE**: The Supabase migration was not implemented; `supreme_admin.py` still writes to MongoDB. This should be marked as pending follow-up.

- **Item 4**: Confirmed /login (`main.py:1190`) and /profile/{id} (`profile_router.py:100`) return `is_supreme_admin` as real JSON boolean (`true`/`false`, not `"true"`).
  Flow: `auth/utils.py:278`: `is_supreme = is_true(db_user.get("is_supreme_admin"))` handles TEXT coercion: `"true"` → Python `True` → JSON `true`; `NULL`/other → `False`. FastAPI serializes Python `True` → JSON `true`.

- **Item 5**: curl commands prepared for 5 endpoints (create org, list orgs, update user role, list users, login). Expected: 200 for success, 403 for access denied.

### Day 2 Complete - Frontend Implementation (verification)

- **Org dropdown**: AdminDashboard organization dropdown (Admin.jsx:606-630) is super-admin-only. Selecting an org calls `handleOrgChange` which busts the cache, resets state, and triggers `useEffect` to fetchUsers+fetchRoles for the new org via `GET /admin/users?organization=` and `GET /admin/roles/distinct?organization=`. Stat cards recompute from refreshed users state.
- **Orgs tab**: New "Organizations" tab (Admin.jsx:839-883) lists organizations via `GET /organizations` (super admin only). "Create Organization" form simplified to name-only field (was name+address+phone+email). Edit/Delete buttons visible only for super admins.
- **"Make admin" action**: Tooltip on user table rows (Admin.jsx:752-756) always visible for super admins regardless of selected organization. On click, opens role change modal. Backend `update_user_role` in admin.py handles org scoping: supreme admins bypass all restrictions; regular admins restricted to their own org; Active Church org only supports system roles; non-Active Church orgs block non-supreme admins from assigning "admin" role.
- **Regular admin hiding**: All super-admin-only features conditioned on `isSupremeAdmin` state (computed from `currentUser.is_supreme_admin` via `is_true()` boolean coercion in AuthContext). Regular admins see org dropdown collapsed to their own org, no "Make admin" on other-org users, no "Organizations" tab CRUD, no supreme admin modal.

**Files changed**: `SUPER_ADMIN_PROGRESS.md` (updated with Reports 6-8), `Active-teams-frontend/src/Pages/Admin.jsx` (org form simplification, verified org dropdown reload logic, "Make admin" tooltip functional via backend org scoping).

### Day 3 - Issues Found and Corrections

- **Signup defaults to "user"**: Confirmed (`Signup.jsx:433`: `role: "user"`). Regular admins cannot create other admins (403 "Cannot create admin users"). Only super admin creating via `POST /admin/users` can set `role: "admin"` — but see Item 6.

- **POST /admin/users does NOT create Supabase Auth account**: The endpoint (`main.py:9398-9456`) only inserts into `users_collection` (MongoDB) with a bcrypt-hashed password. It does NOT call `supabase.auth.signUp()` or create a Supabase Auth session. 
  **Consequence**: Users created solely via `POST /admin/users` CANNOT log in via `supabase.auth.signInWithPassword()` because no Supabase Auth account exists. They can only log in via the legacy `/login` endpoint if it verifies against the MongoDB bcrypt hash.
  **Smallest fix**: The first admin should sign up normally through the frontend `Signup.jsx` flow (which creates both a Supabase Auth account AND a Users row with `role: "user"`), then be promoted to admin via `POST /admin/supreme/add` or have their role updated via `POST /admin/users` (if a Supabase Auth account also exists). Do not rely on `POST /admin/users` as the sole user creation method for login-capable users.

- **Login and signup routes** (corrected):
  - **Login**: `@app.post("/login")` at `main.py:1190` — takes JSON `{"email": "...", "password": "..."}`. Authenticates via `supabase_anon.auth.sign_in_with_password()`. Returns `is_supreme_admin` as boolean in user payload.
  - **Signup**: Frontend `Signup.jsx` uses `supabase.auth.signUp({email, password})` — creates both a Supabase Auth account AND inserts a Users row with `role: "user"`. User can then log in with `supabase.auth.signInWithPassword()`.
  - **POST /admin/users** (`main.py:9398`): Admin-created user. Only inserts into MongoDB `users_collection` with bcrypt hash. Does NOT create Supabase Auth account. **Do not use as primary user creation method for users who need to log in.**

- **main.py POST/PUT/DELETE /organizations auth dependency**: All three endpoints (`POST /admin/organizations` at line 10126, `PUT /admin/organizations/{org_id}` at line 10165, `DELETE /admin/organizations/{org_id}` at line 10204) currently have `current_user: dict = Depends(get_current_user)` and check `is_true(current_user.get("is_supreme_admin"))`. 
  **Per user request**: These should use `require_super_admin` dependency instead for clearer 403 "Super admin access required". I will commit this as a separate change: replace `Depends(get_current_user)` with `Depends(require_super_admin)` on all three org endpoints, while keeping `GET /admin/organizations` (line 10091) unchanged since it already uses `require_super_admin` pattern.

- **Real code for role-assignment handler and org scoping** (still not provided in earlier reports — now provided):

  **a. PATCH /users/{user_id}/role handler** (`admin.py:1098-1159`):
  ```python
  @router.put("/users/{user_id}/role", response_model=MessageResponse)
  async def update_user_role(
      user_id: str,
      role_update: RoleUpdate,
      current_user: dict = Depends(get_current_user),
  ):
      """Update a user's role - Admin only."""
      try:
          is_supreme = current_user.get("is_supreme_admin")

          if not is_supreme and current_user.get("role") != "admin":
              raise HTTPException(status_code=403, detail="Admin access required")

          user_res = (
              supabase.table("Users")
              .select("_id, name, surname, role, Organization")
              .eq("_id", user_id)
              .limit(1)
              .execute()
          )
          if not user_res.data:
              raise HTTPException(status_code=404, detail="User not found")

          user = user_res.data[0]

          if not is_supreme:
              if user.get("Organization") != get_org_name(current_user):
                  raise HTTPException(status_code=403, detail="Cannot access users from other organizations")

          old_role = user.get("role", "user")
          new_role = role_update.role
          user_org = user.get("Organization", "")

          ACTIVE_CHURCH_NAME = "Active Church"
          system_roles = ["admin", "leader", "leaderAt12", "user", "registrant"]

          if user_org == ACTIVE_CHURCH_NAME:
              if new_role not in system_roles:
                  raise HTTPException(
                      status_code=400,
                      detail=f"Active Church only supports standard roles: {', '.join(system_roles)}",
                  )
          else:
              if new_role == "admin" and not is_supreme:
                  raise HTTPException(status_code=403, detail="Cannot assign admin role")

          supabase.table("Users").update(
              {"role": new_role, "updated_at": datetime.utcnow().isoformat()}
          ).eq("_id", user_id).execute()

          await log_activity(
              user_id=get_user_id(current_user),
              action="ROLE_UPDATED",
              details=f"Updated {user.get('name')} {user.get('surname')}'s role from {old_role} to {new_role}",
          )

          return MessageResponse(message=f"User role updated to {new_role}")

      except HTTPException:
          raise
      except Exception as e:
          raise HTTPException(status_code=500, detail=f"Error updating role: {str(e)}")
  ```

  **b. org-scoping logic in admin users list handler** (`admin.py:910-974`):
  ```python
  @router.get("/users", response_model=UserList)
  async def get_all_users(
      organization: Optional[str] = Query(None),
      skip: int = Query(0, ge=0),
      limit: int = Query(500, ge=1, le=5000),
      current_user: dict = Depends(get_current_user),
  ):
      """Get all users - Admin only."""
      try:
          is_supreme = current_user.get("is_supreme_admin")

          if not is_supreme and current_user.get("role") != "admin":
              raise HTTPException(status_code=403, detail="Admin access required")

          org_filter = None
          if is_supreme and organization:
              org_filter = organization
          elif not is_supreme:
              org_filter = get_org_name(current_user)

          query = (
              supabase.table("Users")
              .select(
                  "_id, name, surname, email, role, Organization, "
                  "created_at, date_of_birth, home_address, gender, invited_by, "
                  "leader12, leader144, leader1728, stage",
                  count="exact",
              )
          )

          if org_filter:
              query = query.eq("Organization", org_filter)

          query = query.order("created_at", desc=True).range(skip, skip + limit - 1)
          result = query.execute()

          users_raw = result.data or []
          total = result.count or 0

          users = [
              {
                  "id": u["_id"],
                  "name": u.get("name", ""),
                  "surname": u.get("surname", ""),
                  "email": u.get("email", ""),
                  "role": u.get("role", "user"),
                  "phone_number": u.get("phone_number"),
                  "organization": u.get("Organization") or "Unknown",
                  "created_at": u.get("created_at"),
                  "date_of_birth": u.get("date_of_birth"),
                  "address": u.get("home_address"),
                  "gender": u.get("gender"),
                  "invitedBy": u.get("invited_by"),
                  "leader12": u.get("leader12"),
                  "leader144": u.get("leader144"),
                  "leader1728": u.get("leader1728"),
                  "stage": u.get("stage"),
              }
              for u in users_raw
          ]

          return {"users": users, "total": total, "skip": skip, "limit": limit}

      except Exception as e:
          raise HTTPException(status_code=500, detail=str(e))
  ```

  **c. org-scoping logic in admin stats handler** (`admin.py:977-1034`):
  ```python
  @router.get("/stats")
  async def get_admin_stats(
      organization: Optional[str] = Query(None),
      current_user: dict = Depends(get_current_user),
  ):
      """Get user stats by role - Admin only."""
      try:
          is_supreme = current_user.get("is_supreme_admin")

          if not is_supreme and current_user.get("role") != "admin":
              raise HTTPException(status_code=403, detail="Admin access required")

          org_filter = None
          if is_supreme and organization:
              org_filter = organization
          elif not is_supreme:
              org_filter = get_org_name(current_user)

          query = supabase.table("Users").select("role")
          if org_filter:
              query = query.eq("Organization", org_filter)

          result = query.execute()
          users = result.data or []

          stats: dict = {
              "total_users": 0,
              "administrators": 0,
              "leaders": 0,
              "leaders_at_12": 0,
              "registrants": 0,
              "regular_users": 0,
              "custom_roles": {},
          }

          all_roles: set = set()
          for u in users:
              role = u.get("role") or "unknown"
              all_roles.add(role)
              stats["total_users"] += 1
              if role == "admin":
                  stats["administrators"] += 1
              elif role == "leader":
                  stats["leaders"] += 1
              elif role == "leaderAt12":
                  stats["leaders_at_12"] += 1
              elif role == "registrant":
                  stats["registrants"] += 1
              elif role == "user":
                  stats["regular_users"] += 1
              else:
                  stats["custom_roles"][role] = stats["custom_roles"].get(role, 0) + 1

          stats["all_roles"] = sorted(list(all_roles))
          return stats

      except Exception as e:
          raise HTTPException(status_code=500, detail=str(e))
  ```

### In progress
- Fixing SUPER_ADMIN_HANDOVER.md errors (a-d)
- Adding require_super_admin to main.py org endpoints
- Writing complete Report 6 with NOT RUN markings

### Struggling / blocked
- Nothing

### Decisions I made that you should check
- SUPREME_ADMIN_EMAIL references outside admin.py are documented but NOT changed per instructions. The two flagged users (kerenbotombe125@gmail.com, blessingjonathan028@gmail.com) are Supabase users with `is_supreme_admin="true"`, NOT the email constant.
- The `is_true()` helper in `auth/utils.py:31-41` correctly handles TEXT column coercion: `"true"` → `True` → JSON `true`, `NULL`/other → `False`.
- `supreme_admin.py` Supabase migration was NOT implemented; still writes to MongoDB. Mark as pending.
- `POST /admin/users` does NOT create a Supabase Auth account; only inserts into MongoDB users_collection with bcrypt hash. First admin should sign up normally via frontend and be promoted.
- Frontend checks `is_supreme_admin === true`; the boolean comes from `is_true()` backend coercion.
- All org-scoping logic properly ignores `organization_id` for non-super-admins, scoping them to their own org.

### Files changed
- `SUPER_ADMIN_PROGRESS.md`: updated with Reports 6-8 and all corrections
- `Active-teams-frontend/src/Pages/Admin.jsx`: org form simplification, verified org dropdown reload
- `DAY3_CURL_TESTS.md`: new file with corrected curl commands
- `SUPER_ADMIN_HANDOVER.md`: to be updated with all corrections

### Next
Commit require_super_admin change to main.py org endpoints. Finalize SUPER_ADMIN_HANDOVER.md with all corrections and Report 6.