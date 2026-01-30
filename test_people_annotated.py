# test_people_annotated.py - annotated explanations for test file
import pytest
import asyncio
from datetime import datetime
from bson import ObjectId
from httpx import AsyncClient, ASGITransport
from fastapi import FastAPI
from unittest.mock import AsyncMock, MagicMock, patch

# Import your people router and dependencies
from people import router as people_router
from database import people_collection

# Create test app and include the people router
app = FastAPI()
app.include_router(people_router)

# ========== FIXTURES ==========
# Fixture providing sample person document structure for tests
@pytest.fixture
def mock_person_data():
    """Sample person data for testing"""
    return {
        "Name": "John",
        "Surname": "Doe",
        "Email": "john.doe@example.com",
        "Number": "+27123456789",
        "Address": "123 Main St, Johannesburg",
        "Gender": "Male",
        "Birthday": "1990-01-15",
        "InvitedBy": "Jane Smith",
        "Leader @1": "Gavin Enslin",
        "Leader @12": "Sarah Johnson",
        "Leader @144": "Mike Williams",
        "Leader @1728": "Lisa Brown",
        "Stage": "Win",
        "Date Created": datetime.utcnow().isoformat(),
        "UpdatedAt": datetime.utcnow().isoformat()
    }

@pytest.fixture
def mock_person_create_data():
    """Sample PersonCreate data"""
    return {
        "name": "Alice",
        "surname": "Wonder",
        "email": "alice.wonder@example.com",
        "number": "+27987654321",
        "address": "456 Oak Ave, Pretoria",
        "gender": "Female",
        "dob": "1995-05-20",
        "invitedBy": "Bob Builder",
        "leaders": ["Vicky Enslin", "Tom Hardy", "Emma Watson", "Chris Evans"],
        "stage": "Consolidate"  # Test with a non-default stage
    }

@pytest.fixture
def mock_object_id():
    """Generate a mock ObjectId"""
    return ObjectId()

# ========== TEST GET /people ==========
# Tests use patching to mock the database collection behavior
@pytest.mark.asyncio
async def test_get_people_success(mock_person_data):
    """Test successful retrieval of people list"""
    with patch('people.people_collection') as mock_collection:
        # Setup mock cursor - use MagicMock for synchronous cursor methods
        mock_cursor = MagicMock()
        mock_cursor.skip.return_value = mock_cursor
        mock_cursor.limit.return_value = mock_cursor
        mock_cursor.__aiter__.return_value = iter([
            {**mock_person_data, "_id": ObjectId()},
            {**mock_person_data, "_id": ObjectId(), "Name": "Jane"}
        ])
        mock_collection.find.return_value = mock_cursor
        mock_collection.count_documents = AsyncMock(return_value=2)
        
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/people?page=1&perPage=10")
        
        assert response.status_code == 200
        data = response.json()
        assert data["page"] == 1
        assert data["perPage"] == 10
        assert data["total"] == 2
        assert len(data["results"]) == 2

# Additional tests annotated similarly follow...
