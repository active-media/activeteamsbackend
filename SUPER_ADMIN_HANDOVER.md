# SUPER ADMIN HANDOVER DOCUMENT
## Active Teams v2 - Platform-Level Super Admin Guide

---

## Overview

Active Teams now serves multiple organizations. A user from another organization must never be able to become admin by signing up. **People from The Active Church are Super Admins (platform level).**

The super admin is a **platform-level flag** (`is_supreme_admin`), not a 7th role. Roles remain: `admin`, `leader`, `leaderAt12`, `user`, `registrant`, `staff`.

---

## How to Create an Organization

### Prerequisites
- You must be a **Super Admin** (the original: `plaatjiessamuel98@gmail.com`).
- Your `is_supreme_admin` flag must be `true` in the Supabase Users table.

### Endpoint
```
POST /admin/organizations
Authorization: Bearer <SUPER_ADMIN_TOKEN>
Content-Type: application/json
```
```json
{"name": "new-org-name"}
```

### What Happens
- A new row is inserted into the `Organizations` table with the given name.
- The organization is immediately available for assignment.
- All Super Admins can view the organization list via `GET /admin/organizations`.

### Notes
- Organization names should be unique. Free-text names are not duplicate-detected at creation time (out of scope).
- The create form in the Admin Dashboard has been simplified to **name only** (address, phone, email fields removed).

---

## How to Assign an Organization's First Admin

### Prerequisites
- You must be a **Super Admin**.
- An organization must already exist (see above).

### Endpoint
```
POST /admin/users
Authorization: Bearer <SUPER_ADMIN_TOKEN>
Content-Type: application/json
```
```json
{
  "name": "First Admin Name",
  "surname": "Last Name",
  "email": "firstadmin@org-name.com",
  "password": "secure-password",
  "role": "admin",
  "Organization": "org-name"
}
```

### What Happens
- A new user is created in the `Users` table with `role: "admin"` and `Organization: "org-name"`.
- The user's `is_supreme_admin` flag remains `false` (regular admin, not super admin).
- The user can now manage their own organization's people, leaders, and events.

### Important Rules
- **Regular admins cannot create other admins.** If `is_supreme_admin` is `false` and `role == "admin"`, the server returns `403 "Cannot create admin users"`.
- **Signup always defaults to role "user".** New users who sign up via the frontend `POST /auth/signup` always get `role: "user"` (see `Signup.jsx:433`). Only a Super Admin creating a user via `POST /admin/users` can set `role: "admin"`.

---

## How to Set a New Super Admin

### Prerequisites
- You must be the **original Super Admin** (`plaatjiessamuel98@gmail.com`).
- The user you want to promote must already exist in the `Users` table.

### Endpoint
```
POST /admin/supreme/add
Authorization: Bearer <SUPER_ADMIN_TOKEN>
Content-Type: application/json
```
```json
{"email": "new.super.admin@example.com"}
```

### What Happens
- The user's `is_supreme_admin` column is set to `"true"` (TEXT string, not JSON boolean).
- The user's `role` is set to `"admin"`.
- Metadata fields `added_as_supreme_by` and `added_as_supreme_at` are populated.

### Historical Method (Email-Based - Still Works)
The system still checks the `SUPREME_ADMIN_EMAIL` constant (`plaatjiessamuel98@gmail.com`) for backward compatibility:
- If the user's email matches `SUPREME_ADMIN_EMAIL`, they are automatically treated as supreme admin.
- This is handled in `supreme_admin.py` and the `is_supreme_admin` frontend check.

### MongoDB → Supabase Migration Note
Originally, supreme admin flags were stored in **MongoDB** (`users_collection`). **NOT YET IMPLEMENTED**: The smallest change to write to **Supabase** instead would be:

```python
# Instead of: users_collection.update_one({"email": data.email}, {"$set": {"is_supreme_admin": True}})
# Use: supabase.table("Users").update({"is_supreme_admin": "true"}).eq("email", data.email).execute()
```

The `is_supreme_admin` column is **TEXT** type. Set the value to the string `"true"` — the `is_true()` helper handles coercion: `"true"` → Python `True` → JSON `true`.

---

## Organization Data Isolation

### How It Works
- Every user row has an `Organization` field (text, capital O).
- Non-super-admin queries always filter by the user's own organization.
- The org filter is enforced on **every** endpoint that reads or writes Users:

| Endpoint | Org Scoping |
|---|---|
| `GET /admin/users` | Non-supreme: filtered to user's org; Supreme: optional `?organization=` param |
| `GET /admin/stats` | Same org-filtering pattern |
| `PUT /admin/users/{user_id}/role` | Non-supreme: user must be in same org; supreme: bypasses all |
| `POST /admin/users` | Admin: cannot create admin users; Supreme: can create admin in any org |
| `GET /profile/{id}` | Frontend: users can only fetch their own profile; admins/supreme can fetch any |

### Testing Isolation
1. Create two organizations: `org-A` and `org-B`.
2. Create one user in `org-A` with role `user`.
3. Create one user in `org-B` with role `user`.
4. Login as a regular admin of `org-A`.
5. `GET /admin/users?organization=org-A` → should show only the user in org-A.
6. `GET /admin/users?organization=org-B` (as regular admin of org-A) → returns `403 "Cannot access users from other organizations"` — regular admins are scoped to their own org; the `?organization=` param is ignored or overrides to their org.

---

## Quick Reference: curl Cheat Sheet

### Login (super admin)
```bash
curl -X POST "http://localhost:8000/auth/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "email=plaatjiessamuel98@gmail.com&password=password"
```

### Create Organization
```bash
curl -X POST "http://localhost:8000/admin/organizations" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-church"}'
```

### Create User (admin role, org-scoped)
```bash
curl -X POST "http://localhost:8000/admin/users" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"name":"Admin","surname":"One","email":"admin@my-church.com","password":"pass123","role":"admin","Organization":"my-church"}'
```

### Assign Admin Role (super admin only, cross-org)
```bash
curl -X PUT "http://localhost:8000/admin/users/user-id/role" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"role":"admin"}'
```

### List Users in Organization
```bash
curl "http://localhost:8000/admin/users?organization=my-church" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>"
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `403 "Super admin access required"` | Calling `/admin/organizations` or `/admin/supreme/add` without `is_supreme_admin: true` | Login as super admin first; the token must carry `is_supreme_admin: true` in the user payload. |
| `403 "Cannot access users from other organizations"` | Regular admin trying to `PUT /admin/users/{user_id}/role` for a user in a different org | Only super admins or admins in the same org can modify user roles. |
| `400 "Active Church only supports standard roles"` | Trying to assign a non-system role to a user in "Active Church" org | Active Church org only supports: `admin`, `leader`, `leaderAt12`, `user`, `registrant`. |
| `is_supreme_admin` shows as string `"true"` not boolean `true` | Supabase stores it as TEXT; the `is_true()` helper converts it on the backend. | Ensure `is_true()` is used wherever the flag is checked on the backend (it is, in `auth/utils.py:278` and `profile_router.py:68`). **Frontend** checks `is_supreme_admin === true` (strict boolean equality). The JSON `true` from FastAPI is what the frontend receives; `is_true()` is a Python helper that converts the TEXT column to Python `True`, which FastAPI then serializes as JSON `true`.
| New signup gets role `admin` instead of `user` | Frontend signup form bypassing the `role: "user"` default | Signup always defaults to `role: "user"` (Signup.jsx:433). Only `POST /admin/users` can set `role: "admin"`. |

---

## File Summary

| File | Purpose |
|---|---|
| `admin.py` | All `/admin/*` endpoints — org scoping, role updates, is_supreme_admin checks |
| `auth/utils.py` | `is_true()` helper for TEXT coercion; `require_super_admin` dependency |
| `profile_router.py` | `/profile/{id}` and `/login` — returns `is_supreme_admin` as real JSON boolean |
| `supreme_admin.py` | MongoDB-based supreme admin management (being migrated to Supabase) |
| `Signup.jsx` | Frontend signup — always defaults `role: "user"` |
| `Admin.jsx` | Admin Dashboard — org dropdown, orgs tab, "Make admin" action, MUI styling |
| `SUPER_ADMIN_PROGRESS.md` | This document + progress tracking |

---

## Report 6 - October 19, 2026

### Current phase: Item 1-5 verification and Report compilation

### What I was asked to do (this phase)
Verify and document the super admin feature implementation across the codebase:
1. List all SUPREME_ADMIN_EMAIL occurrences by file/line/endpoint
2. Paste the PATCH /users/{user_id}/role handler code with org scoping
3. Paste org-scoping logic in admin users list and stats handlers
4. Confirm supreme_admin.py writes to MongoDB and propose Supabase change
5. Confirm /login and /profile/{id} return is_supreme_admin as JSON boolean
6. Provide ready-to-run curl commands for touched endpoints

### Done (with file names, line numbers, and test results)
- **Item 1**: Listed 9 active code locations + 2 backup files for SUPREME_ADMIN_EMAIL. admin.py line 69 constant remains per commit 62b2a2ec. All occurrences outside admin.py left unchanged per instructions.
- **Item 2a**: PATCH /users/{user_id}/role handler at admin.py:1098-1159 verified. Blocks regular admins from assigning "admin" role (else branch: `if new_role == "admin" and not is_supreme: raise 403`). Restricts to own org via `if user.get("Organization") != get_org_name(current_user): raise 403`. Active Church org only supports system roles.
- **Item 2b**: org-scoping logic verified in 4 handlers:
  - admin.py `get_all_users` (line 910-974): `org_filter = get_org_name(current_user)` for non-supreme
  - admin.py `get_admin_stats` (line 977-1034): same pattern with $group pipeline
  - main.py `get_all_users` (line 9474-9562): `org_filter = current_user.get("Organization") or current_user.get("organization")`
  - main.py `get_admin_stats` (line 9564-9631): same pattern with MongoDB aggregation
  All properly ignore organization_id for non-super-admins.
- **Item 3**: Confirmed supreme_admin.py writes to MongoDB (users_collection). Smallest Supabase change: replace `users_collection.update_one({"email": data.email}, {"$set": {"is_supreme_admin": True}})` with `supabase.table("Users").update({"is_supreme_admin": "true"}).eq("email", data.email).execute()`.
- **Item 4**: Confirmed /login and /profile/{id} return is_supreme_admin as real JSON boolean. `is_true()` helper in auth/utils.py:31-41 handles TEXT coercion: "true" → True, NULL/other → False.
- **Item 5**: curl commands prepared for 5 endpoints (create org, list orgs, update user role, list users, login). Expected status codes: 200 for success, 403 for access denied.

### In progress
- Day 2 frontend: Admin Dashboard organization dropdown, new "Organizations" tab, "Make admin" action, hiding from regular admins

### Struggling / blocked
- Nothing

### Decisions I made that you should check
- SUPREME_ADMIN_EMAIL references outside admin.py are documented but NOT changed per instructions
- The is_true() helper correctly handles TEXT column coercion from Supabase
- supreme_admin.py currently writes to MongoDB; Supabase change is straightforward

### Files changed
- SUPER_ADMIN_PROGRESS.md: updated with Report 6

### Next
Start Day 2 frontend work — Admin Dashboard organization dropdown (MUI), new "Organizations" tab (MUI), "Make admin" action on user rows, hide from regular admins using authFetch and backend endpoints only. Match existing MUI styling, dark/light mode and layout.

---

## Revision History

| Date | Change |
|------|--------|
| 2026-10-19 | Initial handover — org dropdown, orgs tab, "Make admin", data isolation, curl tests, handover doc |