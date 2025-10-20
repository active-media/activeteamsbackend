import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import logging

logger = logging.getLogger(__name__)

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_FROM")

def send_reset_email(to_email: str, reset_link: str):
    if not SENDGRID_API_KEY or not EMAIL_FROM:
        logger.error("SendGrid API key or sender email not configured")
        return 500

    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=to_email,
        subject="Reset Your Active Teams Password",
        html_content=f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2>Password Reset Request</h2>
            <p>Hello,</p>
            <p>You requested to reset your password. Click below to reset it:</p>
            <a href="{reset_link}" 
               style="background-color: #007bff; color: white; padding: 12px 24px; 
                      text-decoration: none; border-radius: 4px; display: inline-block;">
               Reset Password
            </a>
            <p>This link will expire in 1 hour.</p>
            <hr>
            <p style="font-size: 12px; color: #666;">
                If the button above doesn’t work, copy and paste this link:<br>
                <a href="{reset_link}">{reset_link}</a>
            </p>
        </div>
        """
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logger.info(f"Password reset email sent to {to_email}, status: {response.status_code}")
        return response.status_code
    except Exception as e:
        logger.error(f"Error sending password reset email to {to_email}: {e}")
        return 500
