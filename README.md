# Resume-Parser

# Resume Intelligence API — Testing Guide

## Architecture Overview

```
Postman
  │
  ├── Auth endpoints  ──────────────► resume-service (port 8081)
  │
  └── Resume endpoints ────────────► API Gateway (port 8080)
                                          │
                                          └── resume-service (port 8081)
```

---

## Prerequisites

Before hitting any endpoint, make sure all services are running:

```powershell
mvn clean package -DskipTests
docker compose up --build
```

Verify containers are up:
```powershell
docker ps
```

You should see: `postgres`, `eureka-server`, `resume-service`, `api-gateway`

---

## Step 1 — Register

> Hit resume-service directly (auth is not routed through the gateway)

- **Method:** `POST`
- **URL:** `http://localhost:8081/auth/register`
- **Headers:** `Content-Type: application/json`
- **Body:** `raw` → `JSON`

```json
{
    "email": "test@example.com",
    "password": "123456"
}
```

**Expected Response:**
```
User registered successfully
```

---

## Step 2 — Login

- **Method:** `POST`
- **URL:** `http://localhost:8081/auth/login`
- **Headers:** `Content-Type: application/json`
- **Body:** `raw` → `JSON`

```json
{
    "email": "test@example.com",
    "password": "123456"
}
```

**Expected Response:**
```
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0QGV4YW1...
```

> ⚠️ **Copy this token** — you will need it for all resume endpoints.

---

## Step 3 — Upload Resume

> All resume endpoints go through the API Gateway on port 8080

- **Method:** `POST`
- **URL:** `http://localhost:8080/api/resumes/upload`
- **Headers:**
  - `Authorization: Bearer <paste_token_here>`
- **Body:** `form-data`
  - Key: `file` | Type: **File** | Value: select a PDF file

**Expected Response:**
```json
{
    "id": 1,
    "status": "COMPLETED",
    "rawText": "...",
    "parsedJson": "{...}"
}
```

> ℹ️ The upload flow: PDF is parsed → sent to FastAPI on port 8000 → parsed JSON stored in DB

---

## Step 4 — Get Resume

- **Method:** `GET`
- **URL:** `http://localhost:8080/api/resumes/1`
- **Headers:**
  - `Authorization: Bearer <paste_token_here>`

**Expected Response:**
```json
{
    "id": 1,
    "status": "COMPLETED",
    "data": "raw text from PDF...",
    "parsed": "{...}"
}
```

> ⚠️ You can only access resumes that belong to your account. Others return an error.

---

## Step 5 — Update Resume

- **Method:** `PUT`
- **URL:** `http://localhost:8080/api/resumes/1`
- **Headers:**
  - `Authorization: Bearer <paste_token_here>`
  - `Content-Type: application/json`
- **Body:** `raw` → `JSON`

```json
{
    "name": "John Doe",
    "skills": ["Java", "Python", "Spring Boot"]
}
```

**Expected Response:** `200 OK` (empty body)

---

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `403 Forbidden - CSRF token not found` | SecurityConfig not loaded | Add `@EnableWebSecurity` + exclude auto-config, rebuild |
| `WeakKeyException: 192 bits` | JWT secret too short | Use 32+ char secret with `.getBytes()` in JwtUtil |
| `401 Unauthorized` | Missing or expired token | Re-login and use fresh token |
| `FastAPI unreachable` | Python server not running | Run `python -m uvicorn main:app --reload --port 8000` |
| `User not found` | Token email not in DB | Register first, then login |

---

## Quick Reference

| Endpoint | Method | Port | Auth Required |
|----------|--------|------|---------------|
| `/auth/register` | POST | 8081 | No |
| `/auth/login` | POST | 8081 | No |
| `/api/resumes/upload` | POST | 8080 | Yes |
| `/api/resumes/{id}` | GET | 8080 | Yes |
| `/api/resumes/{id}` | PUT | 8080 | Yes |

---

## Service Ports

| Service | Port |
|---------|------|
| API Gateway | 8080 |
| Resume Service | 8081 |
| Eureka Server | 8761 |
| PostgreSQL | 5433 |
| FastAPI (Python) | 8000 |
