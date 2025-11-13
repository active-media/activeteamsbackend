# auth/email_utils.py
import smtplib
from email.message import EmailMessage
import os

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 465))
SMTP_USER = os.getenv("SMTP_USER", "your_email@gmail.com")
SMTP_PASS = os.getenv("SMTP_PASS", "your_app_password")  # use app password

def send_reset_email(to_email: str, reset_link: str):
    msg = EmailMessage()
    msg['Subject'] = "Password Reset Request"
    msg['From'] = SMTP_USER
    msg['To'] = to_email
    msg.set_content(f"""
Hello,

You requested a password reset. Click the link below to reset your password:

{reset_link}

If you did not request this, please ignore this email.

Thanks.
""")
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)
