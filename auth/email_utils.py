import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

load_dotenv()

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_FROM")

def send_reset_password_email(to_email: str, reset_link: str):
    """
    Send password reset email using SendGrid
    Returns HTTP status code (200 for success, 500 for failure)
    """
    
    # Validate configuration
    if not SENDGRID_API_KEY:
        print("Error: SENDGRID_API_KEY not configured")
        return 500
        
    if not EMAIL_FROM:
        print("Error: EMAIL_FROM not configured")
        return 500
    
    message = Mail(
        from_email=EMAIL_FROM,
        to_emails=to_email,
        subject="Password Reset Request",
        html_content=f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2>Password Reset Request</h2>
            <p>Hello,</p>
            <p>You requested to reset your password. Click the button below to reset it:</p>
            <div style="text-align: center; margin: 30px 0;">
                <a href="{reset_link}" 
                   style="background-color: #007bff; color: white; padding: 12px 24px; 
                          text-decoration: none; border-radius: 4px; display: inline-block;">
                    Reset Password
                </a>
            </div>
            <p><strong>Important:</strong> This link will expire in 1 hour for security reasons.</p>
            <p>If you did not request this password reset, please ignore this email. Your password will not be changed.</p>
            <hr>
            <p style="font-size: 12px; color: #666;">
                If the button above doesn't work, copy and paste this link into your browser:<br>
                <a href="{reset_link}">{reset_link}</a>
            </p>
        </div>
        """
    )
    
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        
        print(f"SendGrid response status: {response.status_code}")
        print(f"Password reset email sent successfully to: {to_email}")
        
        return response.status_code
        
    except Exception as e:
        print(f"Error sending email to {to_email}: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        
        # Return 500 instead of None to indicate server error
        return 500

def test_email_service():
    """
    Test function to verify SendGrid configuration
    """
    print("Testing SendGrid configuration...")
    print(f"SENDGRID_API_KEY configured: {'Yes' if SENDGRID_API_KEY else 'No'}")
    print(f"EMAIL_FROM configured: {EMAIL_FROM if EMAIL_FROM else 'No'}")
    
    if not SENDGRID_API_KEY or not EMAIL_FROM:
        print("❌ Email service not properly configured")
        return False
    
    # Test with a dummy email (won't actually send)
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        print("✅ SendGrid API client created successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to create SendGrid client: {e}")
        return False

if __name__ == "__main__":
    # Run this file directly to test your configuration
    test_email_service()