from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import os

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL", "activeteams10@gmail.com")

def send_reset_email(to_email: str, reset_link: str):
    subject = "✝️ Reset Your Password - Active Teams Church ✝️"
    html_content = f"""
    <html>
      <body style="font-family: 'Segoe UI', Arial, sans-serif; background-color: #f0f4f8; margin: 0; padding: 0;">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td align="center" style="padding: 20px 0;">
              <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); overflow: hidden;">
                
                <!-- Header with gradient and cross icon -->
                <tr>
                  <td style="text-align: center; padding: 30px; background: linear-gradient(90deg, #4A90E2, #50E3C2); color: #ffffff;">
                    <h1 style="margin: 0; font-size: 28px;">✝️ Active Teams Church ✝️</h1>
                    <p style="margin: 5px 0 0 0; font-size: 16px;">Connecting Faith & Community</p>
                  </td>
                </tr>

                <!-- Main message -->
                <tr>
                  <td style="padding: 30px 25px; color: #333333; line-height: 1.7;">
                    <p>Dear Beloved,</p>
                    <p>We have received a request to reset your password. To continue your journey with us, click the button below:</p>
                    <p style="text-align: center; margin: 30px 0;">
                      <a href="{reset_link}" target="_blank" style="background-color: #4A90E2; color: #ffffff; text-decoration: none; padding: 14px 28px; border-radius: 6px; font-weight: bold; font-size: 16px; display: inline-block;">Reset Password</a>
                    </p>
                    <p>If you did not request this, simply ignore this message. Your account remains secure.</p>
                    <p>May the Lord bless your day and keep you in His peace.</p>
                    <br>
                    <p>With love and prayers,</p>
                    <p><strong>The Active Teams Church Team</strong></p>
                  </td>
                </tr>

                <!-- Footer -->
                <tr>
                  <td style="text-align: center; padding: 20px; font-size: 12px; color: #777777; background-color: #f7f7f7;">
                    <p>Active Teams Church | Faith, Fellowship & Service</p>
                    <p>✝️ Bringing people closer to God, one step at a time ✝️</p>
                  </td>
                </tr>

              </table>
            </td>
          </tr>
        </table>
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
