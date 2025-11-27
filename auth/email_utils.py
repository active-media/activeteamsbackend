from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import os

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL", "activeteams10@gmail.com")

def send_reset_email(to_email: str, recipient_name: str, reset_link: str):
    subject = "Reset Your Password - Active Teams Church"
    html_content = f"""
    <html>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
        <style>
          body {{ margin: 0; padding: 0; font-family: 'Segoe UI', Arial, sans-serif; background-color: #f0f4f8; }}
          .container {{ max-width: 600px; margin: auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
          .header {{ text-align: center; padding: 30px; background: linear-gradient(90deg, #4A90E2, #50E3C2); color: #ffffff; }}
          .header h1 {{ margin: 0; font-size: 28px; }}
          .header p {{ margin: 5px 0 0 0; font-size: 16px; }}
          .content {{ padding: 30px 25px; color: #333333; line-height: 1.7; }}
          .button {{ background-color: #4A90E2; color: #ffffff; text-decoration: none; padding: 14px 28px; border-radius: 6px; font-weight: bold; font-size: 16px; display: inline-block; }}
          .footer {{ text-align: center; padding: 20px; font-size: 12px; color: #777777; background-color: #f7f7f7; }}
          @media screen and (max-width: 600px) {{
            .content {{ padding: 20px 15px; }}
            .header h1 {{ font-size: 24px; }}
            .header p {{ font-size: 14px; }}
            .button {{ padding: 12px 22px; font-size: 15px; }}
          }}
        </style>
      </head>
      <body>
        <div class="container">
          
          <!-- Header -->
          <div class="header">
            <h1>Active Teams</h1>
            <p>Connecting Faith & Community</p>
          </div>
          
          <!-- Main Content -->
          <div class="content">
            <p>Dear {recipient_name},</p>
            <p>We have received a request to reset your password. To continue your journey with us, please click the button below:</p>
            <p style="text-align: center; margin: 30px 0;">
              <a href="{reset_link}" target="_blank" class="button">Reset Password</a>
            </p>
            <p>If you did not request this, you can safely ignore this email. Your account remains secure.</p>
            <p>May your day be blessed and filled with peace.</p>
            <br>
            <p>With regards,</p>
            <p><strong>Active Teams</strong></p>
          </div>
          
          <!-- Footer -->
          <div class="footer">
            <p>Active Teams Church | Faith, Fellowship & Service</p>
            <p>Bringing people closer to God, one step at a time</p>
          </div>
          
        </div>
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
