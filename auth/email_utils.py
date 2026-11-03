import os
import smtplib
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders as email_encoders
from dotenv import load_dotenv
from pathlib import Path

# Load .env file
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Your Gmail account (use YOUR email, not activeteams10)
GMAIL_USER = os.getenv("GMAIL_USER", "your-email@gmail.com")  # CHANGE THIS to your email
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
FROM_EMAIL = GMAIL_USER
FROM_NAME = "Active Teams"

print(f" Gmail User: {GMAIL_USER}")
print(f" App Password configured: {' YES' if GMAIL_APP_PASSWORD else ' NO'}")


def send_report_email(to_email: str, subject: str, body: str,
                      attachment_bytes: bytes | None = None,
                      filename: str | None = None,
                      mimetype: str | None = None) -> bool:
    """
    Send a report email using Gmail SMTP.

    Supports attaching a report file (bytes) with a given filename and mimetype.
    Sets a socket timeout to avoid hanging on Render.

    Returns True on success, False on failure.
    """
    if not GMAIL_APP_PASSWORD:
        print(" No App Password configured!")
        return False

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{FROM_NAME} <{FROM_EMAIL}>"
        msg['To'] = to_email

        msg.attach(MIMEText(body, 'html'))

        # Attachment handling
        if attachment_bytes and filename and mimetype:
            attachment_mime = MIMEBase('application', 'octet-stream')
            attachment_mime.set_payload(attachment_bytes)
            email_encoders.encode_base64(attachment_mime)
            attachment_mime.add_header(
                'Content-Disposition',
                f'attachment; filename="{filename}"'
            )
            msg.attach(attachment_mime)

        print(f" Connecting to Gmail SMTP (timeout=30s)...")
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=30) as server:
            print(" Logging in...")
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            print(" Sending...")
            server.send_message(msg)

        print(f" Email sent successfully to {to_email}")
        return True
    except socket.timeout:
        print(f" SMTP connection timed out to {to_email}")
        return False
    except Exception as e:
        print(f" Error sending report email to {to_email}: {e}")
        return False


def send_reset_email(to_email: str, recipient_name: str, reset_link: str):
    """Send password reset email using Gmail SMTP"""
    
    print(f"\n Sending reset email to: {to_email}")
    print(f" Reset link: {reset_link}")
    
    if not GMAIL_APP_PASSWORD:
        print(" No App Password configured!")
        return False
    
    html_content = f"""
    <html>
      <body style="font-family: Arial, sans-serif;">
        <div style="max-width:600px;margin:auto;background:white;padding:30px;border-radius:10px;">
          <h2 style="color:#4A90E2;">Active Teams</h2>
          <p>Dear {recipient_name},</p>
          <p>We received a request to reset your password.</p>
          <div style="text-align:center;margin:30px 0;">
            <a href="{reset_link}" 
               style="background:#4A90E2;color:white;padding:14px 28px;text-decoration:none;border-radius:6px;">
               Reset Password
            </a>
          </div>
          <p>This link expires in 1 hour.</p>
          <p>If you didn't request this, ignore this email.</p>
          <p>Blessings,<br/>Active Teams</p>
        </div>
      </body>
    </html>
    """

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Reset Your Password - Active Teams"
        msg['From'] = f"{FROM_NAME} <{FROM_EMAIL}>"
        msg['To'] = to_email
        msg.attach(MIMEText(html_content, 'html'))
        
        print(" Connecting to Gmail SMTP...")
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            print(" Logging in...")
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            print(" Sending...")
            server.send_message(msg)
        
        print(f" Email sent successfully to {to_email}")
        return True
    except Exception as e:
        print(f" Error: {e}")
        return False