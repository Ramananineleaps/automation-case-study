import os
import base64
import sqlite3
import pandas as pd
from datetime import datetime
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

def send_email(subject, html_content, recipient):
    """Send email via Gmail API"""
    try:
        creds = Credentials.from_authorized_user_file(
            "token.json", ["https://www.googleapis.com/auth/gmail.send"]
        )
        service = build("gmail", "v1", credentials=creds)

        # Create MIME message
        message = MIMEText(html_content, "html")
        message["to"] = recipient
        message["subject"] = subject

        # ✅ Proper Base64 encoding for Gmail API
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        send_body = {"raw": encoded_message}
        service.users().messages().send(userId="me", body=send_body).execute()

        print(f"✅ Sent to {recipient}")
        with open("logs/newsletter_log.txt", "a") as log:
            log.write(f"{datetime.now()} | SUCCESS | {recipient}\n")

    except Exception as e:
        print(f"❌ Failed to send to {recipient}: {str(e)}")
        with open("logs/newsletter_log.txt", "a") as log:
            log.write(f"{datetime.now()} | FAILURE | {recipient} | {str(e)}\n")


def build_email_html(articles):
    """Build HTML email content"""
    html = """
    <html>
    <body style="font-family:Arial, sans-serif; color:#333;">
        <div style="background:#f5f5f5; padding:20px; border-radius:8px;">
            <h2 style="color:#0078D7;">🗞 Weekly Tech News Digest</h2>
            <p>Hello!</p>
            <p>Here are this week’s top technology articles:</p>
            <ul style="line-height:1.6;">
    """
    for _, row in articles.iterrows():
        html += f"""
            <li>
                <a href="{row['url']}" target="_blank" style="color:#0078D7; text-decoration:none;">
                    {row['title']}
                </a><br>
                <small>By {row['author']} — {row['pub_date']}</small>
            </li>
        """
    html += """
            </ul>
            <p>Best,<br>The Automated News Bot 🤖</p>
        </div>
    </body>
    </html>
    """
    return html


def main():
    """Main delivery function"""
    # Resolve DB path relative to this script and ensure directory exists
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(script_dir, "..", "data")
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, "news_articles.db")
    
    # Debugging: Print the resolved database path
    print(f"Resolved database path: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.OperationalError as e:
        print(f"❌ Unable to open database file at {db_path}: {e}")
        return