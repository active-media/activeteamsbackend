# activeteamsbackend

# ActiveTeams Backend

This is a FastAPI backend server for the ActiveTeams site.  
It integrates with:

- Firebase Authentication (via Firebase Admin SDK)
- MongoDB (using Motor async driver)
- AWS (via boto3, e.g., S3)
- FastAPI for API routing

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd activeteamsbackend
````

### 2. Create and Activate Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install fastapi uvicorn python-dotenv motor firebase-admin boto3
```

### 4. Create `.env` File

```env
MONGO_URI=mongodb+srv://activeteams:helloactiveteams@active-teams.ykghvqr.mongodb.net/
AWS_REGION=your-region
AWS_ACCESS_KEY=your-access-key
AWS_SECRET_KEY=your-secret-key
```

> Make sure to **never commit** your `.env` file or Firebase service key!

### 5. Add Firebase Admin Credentials

Place your downloaded Firebase Admin SDK file (from your Firebase console) in the root of this project:

```
firebaseServiceAccountKey.json
```

> Add it to `.gitignore` for safety.

---

## Run the Server

```bash
uvicorn main:app --reload
```

* Visit: [http://127.0.0.1:8000](http://127.0.0.1:8000)
* Docs: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

## 🔐 Authenticated Route Example

To test protected Firebase-authenticated routes, send a `GET` request to:

```
GET /protected
```

With a valid Firebase ID token in the `Authorization` header:

```
Authorization: Bearer <ID_TOKEN>
```

---

## 🗂 Tech Stack

* [FastAPI](https://fastapi.tiangolo.com/)
* [MongoDB + Motor](https://motor.readthedocs.io/)
* [Firebase Admin SDK](https://firebase.google.com/docs/admin/setup)
* [AWS SDK for Python (Boto3)](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)

```

---# Active-teams-backend
