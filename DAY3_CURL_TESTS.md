# Day 3 - Curl Tests (NOT RUN)

**Prerequisite:** Server running on `http://localhost:8000` with current code. Test users must be created via the signup flow (or a Supabase Auth account), not POST /admin/users, so they can log in.

---

## 1. Login as Super Admin

```bash
curl -X POST "http://localhost:8000/login" \
  -H "Content-Type: application/json" \
  -d '{"email": "plaatjiessamuel98@gmail.com", "password": "password"}'
```

**Expected:** `200` with JSON body containing access_token, user with `is_supreme_admin: true`

**NOT RUN**
---

## 2. Signup a New User

```bash
curl -X POST "http://localhost:8000/signup" \
  -H "Content-Type: application/json" \
  -d '{"email": "testuser1@activechurch.com", "password": "password123"}'
```

**Expected:** `200` `{"message": "User created successfully", "Organization": "Active Church"}`

**NOT RUN**
---

## 3. Signup Another User

```bash
curl -X POST "http://localhost:8000/signup" \
  -H "Content-Type: application/json" \
  -d '{"email": "testuser2@activechurch.com", "password": "password123"}'
```

**Expected:** `200` `{"message": "User created successfully", "Organization": "Active Church"}`

**NOT RUN**
---

## 4. Create Organization 'test-org-1'

```bash
curl -X POST "http://localhost:8000/admin/organizations" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"name": "test-org-1"}'
```

**Expected:** `200` `{"message": "Organization test-org-1 created successfully"}`

**NOT RUN**
---

## 5. Create Organization 'test-org-2'

```bash
curl -X POST "http://localhost:8000/admin/organizations" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"name": "test-org-2"}'
```

**Expected:** `200` `{"message": "Organization test-org-2 created successfully"}`

**NOT RUN**
---

## 6. List Users in test-org-1 (Data Isolation Test)

```bash
curl "http://localhost:8000/admin/users?organization=test-org-1" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>"
```

**Expected:** `200` with users array containing **only** users from test-org-1. Users from other orgs must NOT appear.

**NOT RUN**
---

## 7. List Users in test-org-2 (Data Isolation Test)

```bash
curl "http://localhost:8000/admin/users?organization=test-org-2" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>"
```

**Expected:** `200` with users array containing **only** users from test-org-2. Users from other orgs must NOT appear.

**NOT RUN**
---

## 8. Assign Admin Role to testuser1 (Super Admin)

```bash
curl -X PUT "http://localhost:8000/admin/users/testuser1/role" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"role": "admin"}'
```

**Expected:** `200` `{"message": "User role updated to admin"}`

**NOT RUN**
*(Note: Supreme admins bypass all org restrictions. The user's Organization stays as "test-org-1" but their role becomes "admin".)*

---

## 9. Verify testuser1 Now Has admin Role

```bash
curl "http://localhost:8000/admin/users?organization=test-org-1" \
  -H "Authorization: Bearer <SUPER_ADMIN_TOKEN>"
```

**Expected:** `200` with the user previously known as testuser1 now showing `"role": "admin"`.

**NOT RUN**
---

## 10. Assign Admin Role as Regular Admin (Should Fail - Cross-Org)

**First, login as the regular admin:**

```bash
curl -X POST "http://localhost:8000/login" \
  -H "Content-Type: application/json" \
  -d '{"email": "testuser1@activechurch.com", "password": "password123"}'
```

**Expected:** `200` with `is_supreme_admin: false, role: admin`

**NOT RUN**

**Then, try assigning admin to another user:**

```bash
curl -X PUT "http://localhost:8000/admin/users/testuser2/role" \
  -H "Authorization: Bearer <ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"role": "admin"}'
```

**Expected:** `403` `{"detail": "Cannot assign admin role"}`

**NOT RUN**
*(Note: Regular admins can only manage users in their own organization, and cannot assign the "admin" role in a non-Active-Church org.)*

---

## 11. Verify Data Isolation with Regular Admin

```bash
curl "http://localhost:8000/admin/users?organization=test-org-1" \
  -H "Authorization: Bearer <ADMIN_TOKEN>"
```

**Expected:** `200` showing only users in test-org-1. The regular admin (now acting as testuser1) should NOT see testuser2 or any users from other organizations.

**NOT RUN**
---

## Summary of Expected Results

| Step | Endpoint | Status | Purpose |
|------|----------|--------|---------|
| 1 | POST /login (super admin) | NOT RUN | Login as super admin |
| 2 | POST /signup | NOT RUN | Create test user 1 via auth |
| 3 | POST /signup | NOT RUN | Create test user 2 via auth |
| 4 | POST /admin/organizations | NOT RUN | Create two test orgs |
| 5 | POST /admin/organizations | NOT RUN | Create two test orgs |
| 6 | GET /admin/users?org=test-org-1 | NOT RUN | Isolate: only user1 visible |
| 7 | GET /admin/users?org=test-org-2 | NOT RUN | Isolate: only user2 visible |
| 8 | PUT /admin/users/testuser1/role | NOT RUN | Super admin assigns admin role |
| 9 | GET /admin/users?org=test-org-1 | NOT RUN | Verify: user1 now has role=admin |
| 10 | PUT /admin/users/testuser2/role (as admin) | NOT RUN | Regular admin blocked from cross-org admin assign |
| 11 | GET /admin/users?org=test-org-1 (as admin) | NOT RUN | Regular admin sees only their org |

**Data isolation verification:** After steps 6 and 7, each organization's users are independently scoped. After step 8, the assigned user gains platform‑level admin rights but remains org‑scoped for management operations. Regular admins (step 10‑11) are strictly limited to their own org and cannot promote other users to admin.