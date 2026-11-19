import json
import logging
import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException,BackgroundTasks, Header
from pydantic import BaseModel, EmailStr, Field, HttpUrl, field_validator
try:
    from pywebpush import WebPushException, webpush
except ImportError:
    class WebPushException(Exception):
        pass

    def webpush(*args, **kwargs):
        raise RuntimeError("pywebpush is required to send push notifications")
import secrets
from supabase import Client
from supabase_helpers.supabase_connection import supabase

from auth.utils import get_current_user
from supabase_helpers.supabase_connection import supabase_admin
from datetime import datetime
from zoneinfo import ZoneInfo


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/push", tags=["Push notifications"])


class PushSubscriptionKeys(BaseModel):
    p256dh: str = Field(min_length=1)
    auth: str = Field(min_length=1)


class PushSubscription(BaseModel):
    endpoint: HttpUrl
    expirationTime: Optional[int] = None
    keys: PushSubscriptionKeys


class PushSubscriptionDelete(BaseModel):
    endpoint: HttpUrl


class PushNotificationRequest(BaseModel):
    recipient_emails: list[EmailStr] = Field(min_length=1, max_length=20)
    title: str = Field(min_length=1, max_length=120)
    body: str = Field(min_length=1, max_length=500)
    url: str = Field(default="/", max_length=2048)

    @field_validator("url")
    @classmethod
    def validate_internal_url(cls, value: str) -> str:
        if not value.startswith("/") or value.startswith("//"):
            raise ValueError("Notification URLs must be paths on this site.")
        return value


def _organization_id(user: dict) -> str:
    org_id = user.get("org_id") or user.get("Organization") or user.get("organization") or ""
    normalized = str(org_id).strip().lower().replace(" ", "-")
    if normalized in {"active-church", "active church"}:
        return "active-teams"
    return normalized


def _get_delivery_configuration() -> tuple[str, str, str]:
    public_key = os.getenv("VAPID_PUBLIC_KEY")
    private_key = os.getenv("VAPID_PRIVATE_KEY")
    subject = os.getenv("VAPID_SUBJECT")
    if not public_key or not private_key or not subject:
        raise HTTPException(
            status_code=503,
            detail="Push notifications are not configured on the server.",
        )
    return public_key, private_key, subject


@router.get("/public-key")
def get_push_public_key():
    public_key, _, _ = _get_delivery_configuration()
    return {"publicKey": public_key}


@router.post("/subscribe")
def save_push_subscription(
    subscription: PushSubscription,
    current_user: dict = Depends(get_current_user),
):
    email = str(current_user.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=401, detail="Authenticated user has no email address.")

    payload = subscription.model_dump(mode="json")
    supabase_admin.table("web_push_subscriptions").upsert(
        {
            "user_email": email,
            "endpoint": payload["endpoint"],
            "subscription": payload,
        },
        on_conflict="endpoint",
    ).execute()
    return {"subscribed": True}


@router.delete("/subscribe")
def delete_push_subscription(
    request: PushSubscriptionDelete,
    current_user: dict = Depends(get_current_user),
):
    email = str(current_user.get("email") or "").strip().lower()
    supabase_admin.table("web_push_subscriptions").delete().eq(
        "user_email", email
    ).eq("endpoint", str(request.endpoint)).execute()
    return {"unsubscribed": True}


def send_push_notification(
    request: PushNotificationRequest,
    # current_user: dict = Depends(get_current_user),
):
    _, private_key, subject = _get_delivery_configuration()
    recipient_emails = request.recipient_emails
    
    subscriptions = (
        supabase_admin.table("web_push_subscriptions")
        .select("id, user_email, subscription")
        .in_("user_email", sorted(recipient_emails))
        .execute()
        .data
        or []
    )

    payload = json.dumps(
        {"title": request.title, "body": request.body, "url": request.url}
    )
    sent = 0
    failed = 0
    expired = 0

    for saved_subscription in subscriptions:
        try:
            webpush(
                subscription_info=saved_subscription["subscription"],
                data=payload,
                vapid_private_key=private_key,
                vapid_claims={"sub": subject},
                ttl=3600,
            )
            sent += 1
        except WebPushException as error:
            response = getattr(error, "response", None)
            status_code = getattr(response, "status_code", None)
            if status_code in {404, 410}:
                supabase_admin.table("web_push_subscriptions").delete().eq(
                    "id", saved_subscription["id"]
                ).execute()
                expired += 1
            else:
                logger.warning(
                    "Push delivery failed for a subscription (HTTP %s).",
                    status_code,
                )
                failed += 1

    return {"sent": sent, "failed": failed, "expired": expired}


def send_event_notification():
    now = datetime.now(ZoneInfo("Africa/Johannesburg"))
    today_name = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][now.weekday()]

    morning = now.hour < 12

    if morning:
        rows = (
            supabase.table("events")
            .select("reminder_emails,event_name,event_type_name")
            .not_.is_("reminder_emails", "null")
            .eq("is_active", True)
            .ilike("recurring_day", f"%{today_name}%")
            .lt("time", "12:00:00")
            .execute()
            .data
        )
    else:
        rows = (
            supabase.table("events")
            .select("reminder_emails,event_name,event_type_name")
            .not_.is_("reminder_emails", "null")
            .eq("is_active", True)
            .ilike("recurring_day", f"%{today_name}%")
            .gte("time", "12:00:00")
            .execute()
            .data
        )
    emails = [email for row in rows for email in row["reminder_emails"]]
    if emails:
        send_push_notification(
            PushNotificationRequest(
                recipient_emails=emails,
                title="Event Capture Reminder",
                body=f"Don't forget to capture your {rows[0]['event_type_name']} event today ({today_name})!",
                url="/events",
            )
        )
    print(emails)
    print(f"Sent notifications to {len(emails)} users for {len(rows)} events.")
    return   
                          
@router.post("/run-job/send-notifications")
def run_send_notifications_job(background_tasks: BackgroundTasks, x_cron_secret: str = Header(None)):
    expected = os.environ["CRON_SECRET"]
    # if not x_cron_secret or not secrets.compare_digest(x_cron_secret, expected):
    #     raise HTTPException(status_code=401, detail="Unauthorized")
    print("Running send notifications job...")
    background_tasks.add_task(send_event_notification)  # your existing job function
    return {"status": "started"}