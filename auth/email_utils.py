from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import os

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL", "activeteams10@gmail.com")  # default fallback

def send_reset_email(to_email: str, reset_link: str):
    subject = "Reset Your Password"
    html_content = f"""
    <html>
      <body>
        <p>Hi,</p>
        <p>We received a request to reset your password. Click the link below to set a new password:</p>
        <p><a href="{reset_link}" target="_blank">Reset Password</a></p>
        <p>If you did not request this, you can safely ignore this email.</p>
        <br>
        <p>Active Teams Team</p>
      </body>
    </html>
    """

    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=to_email,
        subject=subject,
        html_content=html_content
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"Reset email sent to {to_email}: {response.status_code}")
        return response.status_code
    except Exception as e:
        print(f"Error sending reset email to {to_email}: {e}")
        return None
