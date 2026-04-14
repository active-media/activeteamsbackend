import os
import resend

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL")

if not RESEND_API_KEY:
    raise ValueError("RESEND_API_KEY not set in environment variables")

resend.api_key = RESEND_API_KEY


def send_reset_email(to_email: str, recipient_name: str, reset_link: str):

    html_content = f"""
    <html>
      <body style="font-family: Arial, sans-serif; background-color: #f4f6f8; padding: 20px;">
        <div style="max-width:600px;margin:auto;background:white;padding:30px;border-radius:10px;">
          <h2 style="color:#4A90E2;">Active Teams</h2>

          <p>Dear {recipient_name},</p>

          <p>We received a request to reset your password.</p>

          <div style="text-align:center;margin:30px 0;">
            <a href="{reset_link}" 
               style="background:#4A90E2;color:white;padding:14px 28px;text-decoration:none;border-radius:6px;font-weight:bold;">
               Reset Password
            </a>
          </div>

          <p>This link will expire soon for security reasons.</p>

          <p>If you did not request this, you may ignore this email.</p>

          <p>Blessings,<br/>Active Teams</p>
        </div>
      </body>
    </html>
    """

    try:
        response = resend.Emails.send({
            "from": "Active MI <support@activemi.co.za>",
            "to": to_email,
            "subject": "Reset Your Password - Active Teams",
            "html": html_content,
        })

        print(f"Reset email sent to {to_email}")
        return True

    except Exception as e:
        print(f"Error sending reset email: {e}")
        return False