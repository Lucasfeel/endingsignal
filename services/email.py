# services/email.py
import config
from .base_email_service import BaseEmailService
from .smtp_service import SmtpService
from .sendgrid_service import SendGridService

def get_email_service() -> BaseEmailService:
    """
    설정(config.EMAIL_PROVIDER)에 따라 적절한 이메일 서비스 인스턴스를 반환합니다.
    """
    if config.EMAIL_PROVIDER == 'sendgrid':
        print("LOG: [EmailService] SendGridService를 사용합니다.")
        return SendGridService()

    # 기본값은 'smtp'
    print("LOG: [EmailService] SmtpService를 사용합니다.")
    return SmtpService()

# ----------------------------------------------------------------------
# [중요] 프로젝트 전역에서 사용할 단일 이메일 서비스 인스턴스
# ----------------------------------------------------------------------
# 🚨 [수정] 아래 코드를 제거하여 인스턴스 즉시 생성을 방지합니다.
# email_service: BaseEmailService = get_email_service()
# ----------------------------------------------------------------------
