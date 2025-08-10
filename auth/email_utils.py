import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

load_dotenv()

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_FROM")

def send_reset_password_email(to_email: str, reset_link: str):
    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=to_email,
        subject="Password Reset Request",
        html_content=f"""
        <p>Hello,</p>
        <p>You requested to reset your password. Click the link below to reset it:</p>
        <p><a href="{reset_link}">Reset Password</a></p>
        <p>If you did not request this, please ignore this email.</p>
        """
    )
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        return response.status_code
    except Exception as e:
        print(f"Error sending email: {e}")
        return None
