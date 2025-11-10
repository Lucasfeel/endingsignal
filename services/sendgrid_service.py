# services/sendgrid_service.py
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from .base_email_service import BaseEmailService

class SendGridService(BaseEmailService):
    """
    SendGrid API를 사용하여 이메일을 발송하는 서비스입니다.
    """
    def __init__(self):
        self.api_key = os.getenv('SENDGRID_API_KEY')
        self.sender_email = os.getenv('EMAIL_ADDRESS') # SendGrid에 등록된 발신자
        if not self.api_key or not self.sender_email:
            raise ValueError("SendGrid 서비스에 필요한 SENDGRID_API_KEY 또는 EMAIL_ADDRESS가 설정되지 않았습니다.")
        self.sg = SendGridAPIClient(self.api_key)

    def send_mail(self, to_email: str, subject: str, body: str) -> bool:
        message = Mail(
            from_email=self.sender_email,
            to_emails=to_email,
            subject=subject,
            plain_text_content=body
        )
        try:
            response = self.sg.send(message)
            # 2xx 응답 코드는 성공으로 간주
            return 200 <= response.status_code < 300
        except Exception as e:
            print(f"오류: [SendGridService] {to_email}에게 이메일 발송 실패 - {e}")
            return False
