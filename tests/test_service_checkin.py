import pytest
from unittest.mock import patch, AsyncMock
from bson import ObjectId
from fastapi import HTTPException

from services import service_checkin_service as scs


@pytest.fixture
def fake_user():
    return {"email": "test@test.com", "id": "user123", "role": "admin"}


@pytest.mark.asyncio
async def test_get_service_checkin_invalid_event_id(fake_user):
    # Invalid ObjectId should raise HTTP 400
    with pytest.raises(HTTPException) as exc:
        await scs.get_service_checkin_real_time_data(event_id="invalid_id", current_user=fake_user)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_get_service_checkin_not_found(fake_user):
    # Patch events_collection to return None
    with patch.object(scs.events_collection, "find_one", new_callable=AsyncMock) as mock_find:
        mock_find.return_value = None
        with pytest.raises(HTTPException) as exc:
            await scs.get_service_checkin_real_time_data(event_id=str(ObjectId()), current_user=fake_user)
        assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_get_service_checkin_success(fake_user):
    fake_event = {
        "_id": ObjectId(),
        "eventName": "Sunday Service",
        "attendees": [{"id": "1", "checked_in": True}],
        "new_people": [{"id": "np1"}],
        "consolidations": [{"id": "c1"}]
    }

    with patch.object(scs.events_collection, "find_one", new_callable=AsyncMock) as mock_find:
        mock_find.return_value = fake_event
        result = await scs.get_service_checkin_real_time_data(event_id=str(fake_event["_id"]), current_user=fake_user)

    assert result["success"] is True
    assert result["present_count"] == 1
    assert result["new_people_count"] == 1
    assert result["consolidation_count"] == 1



@pytest.mark.asyncio
async def test_service_checkin_attendee_success(fake_user):
    fake_event_id = str(ObjectId())
    fake_person_id = str(ObjectId())

    # Use a mutable dict to simulate DB event
    fake_event = {
        "_id": ObjectId(fake_event_id),
        "attendees": [],
        "total_attendance": 0
    }

    fake_person = {
        "_id": ObjectId(fake_person_id),
        "Name": "Keren",
        "Surname": "Botombe",
        "Email": "kerenbotombe125@gmail.com",
        "Number": "0684605059"
    }

    async def mock_find_one(query):
        if "attendees.id" in query:
            # Check for duplicate attendee
            for a in fake_event["attendees"]:
                if a["id"] == str(fake_person["_id"]):
                    return a
            return None
        elif "_id" in query:
            return fake_event
        return None

    async def mock_update_one(filter_query, update_query):
        # Simulate $push and $inc for attendees
        if "$push" in update_query and "attendees" in update_query["$push"]:
            attendee_record = update_query["$push"]["attendees"]
            fake_event["attendees"].append(attendee_record)
        if "$inc" in update_query and "total_attendance" in update_query["$inc"]:
            fake_event["total_attendance"] += update_query["$inc"]["total_attendance"]

    with patch.object(scs.events_collection, "find_one", side_effect=mock_find_one), \
         patch("services.service_checkin_service.people_collection", new_callable=AsyncMock) as mock_people, \
         patch.object(scs.events_collection, "update_one", side_effect=mock_update_one):

        mock_people.find_one.return_value = fake_person

        checkin_data = {
            "event_id": fake_event_id,
            "person_data": {"id": fake_person_id},
            "type": "attendee"
        }

        result = await scs.service_checkin_person(
            checkin_data=checkin_data,
            current_user=fake_user
        )

        assert result["success"] is True
        assert result["attendee"]["id"] == fake_person_id
        assert result["present_count"] == 1
        assert result["attendee"]["checked_in"] is True
        assert result["attendee"]["name"] == "Keren"

@pytest.mark.asyncio
async def test_remove_from_service_checkin_success(fake_user):
    fake_event_id = str(ObjectId())
    fake_person_id = "1"
    fake_event = {
        "_id": ObjectId(fake_event_id),
        "attendees": [{"id": fake_person_id, "checked_in": True}],
        "new_people": [],
        "consolidations": []
    }

    with patch.object(scs.events_collection, "update_one", new_callable=AsyncMock) as mock_update, \
         patch.object(scs.events_collection, "find_one", new_callable=AsyncMock) as mock_find:

        mock_find.return_value = {**fake_event, "attendees": []}
        mock_update.return_value.modified_count = 1

        removal_data = {
            "event_id": fake_event_id,
            "person_id": fake_person_id,
            "type": "attendees"
        }

        result = await scs.remove_from_service_checkin(removal_data=removal_data, current_user=fake_user)

    assert result["success"] is True
    assert result["updated_counts"]["present_count"] == 0
