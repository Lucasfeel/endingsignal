import os
from typing import Any, Dict, Optional

from services.mtls_http import AppsInTossApiError, request_json

SEND_MESSAGE_PATH = "/api-partner/v1/apps-in-toss/messenger/send-message"


class AppsInTossMessageError(RuntimeError):
    def __init__(self, code: str, message: str, *, status_code: int = 400, payload: Any = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.payload = payload


def _mock_enabled() -> bool:
    raw = (os.getenv("AIT_MESSAGE_MOCK_ENABLED") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "t", "yes", "y", "on"}
    return os.getenv("FLASK_ENV") in {"development", "test"} or bool(os.getenv("PYTEST_CURRENT_TEST"))


def send_message(
    *,
    user_key: str,
    template_set_code: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    if not template_set_code:
        raise AppsInTossMessageError(
            "TEMPLATE_CODE_MISSING",
            "Set AIT_COMPLETION_TEMPLATE_CODE before sending Apps-in-Toss messages.",
            status_code=500,
        )

    if _mock_enabled():
        return {
            "resultType": "SUCCESS",
            "result": {
                "msgCount": 1,
                "sentPushCount": 1,
                "detail": {
                    "sentPush": [{"contentId": f"mock:{user_key}:{template_set_code}"}],
                },
            },
        }

    try:
        response_payload = request_json(
            "POST",
            SEND_MESSAGE_PATH,
            headers={"X-Toss-User-Key": str(user_key)},
            json_body={
                "templateSetCode": template_set_code,
                "context": context,
            },
        )
    except AppsInTossApiError as exc:
        raise AppsInTossMessageError(
            "MESSAGE_SEND_FAILED",
            "Apps-in-Toss message send failed.",
            status_code=exc.status_code,
            payload=exc.payload,
        ) from exc

    if response_payload.get("resultType") != "SUCCESS":
        raise AppsInTossMessageError(
            "MESSAGE_SEND_FAILED",
            "Apps-in-Toss message send was rejected.",
            payload=response_payload,
        )

    return response_payload


def send_completion_message(
    *,
    user_key: str,
    content_title: str,
    source_name: str,
    content_path: str,
    template_set_code: Optional[str] = None,
) -> Dict[str, Any]:
    resolved_template = (
        template_set_code
        or (os.getenv("AIT_COMPLETION_TEMPLATE_CODE") or "").strip()
    )
    return send_message(
        user_key=user_key,
        template_set_code=resolved_template,
        context={
            "contentTitle": content_title,
            "sourceName": source_name,
            "contentPath": content_path,
        },
    )
