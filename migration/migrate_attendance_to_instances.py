from datetime import datetime
from bson import ObjectId

async def migrate_attendance_to_instances(
    events_collection,
    cells_instances_collection
):
    events = await events_collection.find({}).to_list(None)

    created = 0

    for event in events:
        attendance = event.get("attendance", {})

        for _, record in attendance.items():
            instance_date = (
                record.get("event_date_exact")
                or record.get("event_date_iso")
            )

            if not instance_date:
                continue

            raw_status = record.get("status", "pending")

            if raw_status == "complete":
                status = "completed"
            elif raw_status == "did_not_meet":
                status = "did_not_meet"
            else:
                status = "pending"

            result = await cells_instances_collection.update_one(
                {
                    "event_id": event["_id"],
                    "date": instance_date
                },
                {
                    "$setOnInsert": {
                        "event_id": event["_id"],
                        "date": instance_date,
                        "status": status,
                        "created_at": record.get(
                            "submitted_at", datetime.utcnow()
                        ),
                        "resolved_at": record.get("submitted_at"),
                        "resolved_by": record.get("submitted_by")
                    }
                },
                upsert=True
            )

            if result.upserted_id:
                created += 1

    return {"created_instances": created}
