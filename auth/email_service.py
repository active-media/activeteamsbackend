# auth/email_service.py
import os
import resend
from datetime import datetime

# Initialize Resend
resend.api_key = os.getenv("RESEND_API_KEY")

def send_reset_email_resend(to_email: str, reset_link: str) -> bool:
    """
    Send password reset email using Resend with the actual reset link
    """
    try:
        params = {
            "from": os.getenv("EMAIL_FROM", "Active Teams <noreply@activeteams.com>"),
            "to": [to_email],
            "subject": "Reset Your Password - Active Teams",
            "html": f"""
            <!DOCTYPE html>
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                    .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                    .header {{ background: #4F46E5; color: white; padding: 20px; text-align: center; }}
                    .content {{ padding: 30px; background: #f9f9f9; }}
                    .button {{ display: inline-block; padding: 12px 24px; background: #4F46E5; 
                             color: white; text-decoration: none; border-radius: 5px; margin: 20px 0; }}
                    .footer {{ text-align: center; padding: 20px; color: #666; font-size: 14px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>Active Teams</h1>
                    </div>
                    <div class="content">
                        <h2>Password Reset Request</h2>
                        <p>You requested to reset your password for your Active Teams account.</p>
                        <p>Click the button below to create a new password:</p>
                        
                        <div style="text-align: center;">
                            <a href="{reset_link}" class="button" style="color: white; text-decoration: none;">
                                Reset Your Password
                            </a>
                        </div>
                        
                        <p>If the button doesn't work, copy and paste this link into your browser:</p>
                        <p style="word-break: break-all; background: #eee; padding: 10px; border-radius: 4px; font-family: monospace;">
                            {reset_link}
                        </p>
                        
                        <p><strong>Important:</strong> This link will expire in 1 hour for security reasons.</p>
                        
                        <p>If you didn't request this password reset, please ignore this email and your password will remain unchanged.</p>
                        
                        <p>Need help? Contact our support team or reply to this email.</p>
                    </div>
                    <div class="footer">
                        <p>&copy; 2024 Active Teams. All rights reserved.</p>
                    </div>
                </div>
            </body>
            </html>
            """,
            # Optional: Add text version for email clients that don't support HTML
            "text": f"""
            Password Reset Request - Active Teams
            
            You requested to reset your password for your Active Teams account.
            
            To reset your password, click this link:
            {reset_link}
            
            This link will expire in 1 hour for security reasons.
            
            If you didn't request this password reset, please ignore this email and your password will remain unchanged.
            
            Need help? Contact our support team.
            
            © 2024 Active Teams. All rights reserved.
            """
        }
        
        email = resend.Emails.send(params)
        print(f" Password reset email sent via Resend to {to_email}")
        print(f"📧 Email ID: {email['id']}")
        print(f"🔗 Reset link sent: {reset_link}")
        return True
        
    except Exception as e:
        print(f"❌ Resend error for {to_email}: {e}")
        return False

def send_welcome_email_resend(to_email: str, user_name: str) -> bool:
    """
    Send welcome email using Resend
    """
    try:
        params = {
            "from": os.getenv("EMAIL_FROM", "Active Teams <noreply@activeteams.com>"),
            "to": [to_email],
            "subject": "Welcome to Active Teams!",
            "html": f"""
            <!DOCTYPE html>
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                    .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                    .header {{ background: #10B981; color: white; padding: 20px; text-align: center; }}
                    .content {{ padding: 30px; background: #f9f9f9; }}
                    .footer {{ text-align: center; padding: 20px; color: #666; font-size: 14px; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>Welcome to Active Teams! 🎉</h1>
                    </div>
                    <div class="content">
                        <h2>Hello {user_name},</h2>
                        <p>Your account has been successfully created and you're now part of our community!</p>
                        
                        <h3>What you can do:</h3>
                        <ul>
                            <li>Manage your cell groups and events</li>
                            <li>Track attendance and follow-ups</li>
                            <li>Connect with your team members</li>
                            <li>Access leadership resources</li>
                        </ul>
                        
                        <p>We're excited to have you on board!</p>
                        
                        <p><strong>Need help?</strong> Reply to this email or contact your team leader.</p>
                    </div>
                    <div class="footer">
                        <p>&copy; 2024 Active Teams. All rights reserved.</p>
                    </div>
                </div>
            </body>
            </html>
            """
        }
        
        email = resend.Emails.send(params)
        print(f" Welcome email sent via Resend to {to_email}: {email['id']}")
        return True
        
    except Exception as e:
        print(f"❌ Resend welcome email error for {to_email}: {e}")
        return False