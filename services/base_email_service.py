# services/base_email_service.py
from abc import ABC, abstractmethod

class BaseEmailService(ABC):
    """
    모든 이메일 발송 서비스가 상속받아야 할 추상 기본 클래스입니다.
    이메일 발송에 필요한 'send_mail' 메서드를 정의합니다.
    """

    @abstractmethod
    def send_mail(self, to_email: str, subject: str, body: str) -> bool:
        """
        단일 수신자에게 이메일을 발송합니다.

        Args:
            to_email (str): 수신자 이메일 주소
            subject (str): 이메일 제목
            body (str): 이메일 본문 (text)

        Returns:
            bool: 발송 성공 여부
        """
        pass
