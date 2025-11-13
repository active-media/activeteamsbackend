# auth/email_utils.py
import smtplib
from email.message import EmailMessage
import os

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))  # use 587 for STARTTLS
SMTP_USER = os.getenv("SMTP_USER", "your_email@gmail.com")
SMTP_PASS = os.getenv("SMTP_PASS", "your_app_password")  # use app password

def send_reset_email(to_email: str, reset_link: str):
    msg = EmailMessage()
    msg['Subject'] = "Password Reset Request"
    msg['From'] = SMTP_USER
    msg['To'] = to_email
    msg.set_content(f"""
Dear User,

We received a request to reset the password associated with this email address. 
Please click the link below to securely reset your password:

{reset_link}

If you did not request a password reset, please disregard this message. 
Your account remains secure and no changes will be made.

Thank you for using our service.

Best regards,
The Active Teams Support Team
""")

    # Use plain SMTP with STARTTLS
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.ehlo()
        smtp.starttls()  # upgrade connection to secure
        smtp.ehlo()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)
