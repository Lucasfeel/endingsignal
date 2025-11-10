# services/smtp_service.py
import os
import smtplib
from email.mime.text import MIMEText
import config
from .base_email_service import BaseEmailService

class SmtpService(BaseEmailService):
    """
    표준 SMTP(Gmail 등)를 사용하여 이메일을 발송하는 서비스입니다.
    """
    def __init__(self):
        self.smtp_server = config.SMTP_SERVER
        self.smtp_port = config.SMTP_PORT
        self.sender_email = os.getenv('EMAIL_ADDRESS')
        self.sender_password = os.getenv('EMAIL_PASSWORD')
        if not self.sender_email or not self.sender_password:
            raise ValueError("SMTP 서비스에 필요한 EMAIL_ADDRESS 또는 EMAIL_PASSWORD가 설정되지 않았습니다.")

    def send_mail(self, to_email: str, subject: str, body: str) -> bool:
        msg = MIMEText(body, _charset='utf-8')
        msg['Subject'] = subject
        msg['From'] = self.sender_email
        msg['To'] = to_email

        try:
            # 매번 새로운 연결을 생성합니다. (단순 구현)
            # 대량 발송 시 성능 최적화가 필요할 수 있습니다.
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, to_email, msg.as_string())
            return True
        except Exception as e:
            print(f"오류: [SmtpService] {to_email}에게 이메일 발송 실패 - {e}")
            return False
