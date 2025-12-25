"""gcal_sync.py
KFIT Google Calendar Sync (Local OAuth)

- 로컬 PC(설계사 PC)에서 OAuth 동의 후 token 저장 방식.
- Streamlit에서 호출할 수 있도록 예외를 조용히 처리하며, 실패해도 DB 저장은 유지.

필요 패키지:
  pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Optional, Tuple

# 구글 라이브러리는 미설치일 수 있으니 늦게 import
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
except Exception:  # pragma: no cover
    Credentials = None  # type: ignore
    InstalledAppFlow = None  # type: ignore
    Request = None  # type: ignore
    build = None  # type: ignore

SCOPES = ["https://www.googleapis.com/auth/calendar"]

USER_DATA_DIR = os.path.join(os.path.expanduser("~"), "KFIT_Data")
CREDENTIALS_PATH = os.path.join(USER_DATA_DIR, "gcal_credentials.json")
TOKEN_PATH = os.path.join(USER_DATA_DIR, "gcal_token.json")


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")

def _ensure_dir() -> None:
    os.makedirs(USER_DATA_DIR, exist_ok=True)

def is_google_lib_ready() -> bool:
    return all(x is not None for x in [Credentials, InstalledAppFlow, Request, build])

def get_service(*, interactive: bool = False):
    """Google Calendar service 생성.
    interactive=True면 최초 1회 OAuth 동의를 진행(run_local_server).
    """
    if not is_google_lib_ready():
        raise RuntimeError("Google Calendar 라이브러리가 설치되어 있지 않습니다.")
    _ensure_dir()

    creds = None
    if os.path.exists(TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        except Exception:
            creds = None

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception:
            creds = None

    if not creds:
        if not os.path.exists(CREDENTIALS_PATH):
            raise FileNotFoundError(
                f"gcal_credentials.json 파일이 없습니다: {CREDENTIALS_PATH}\n"
                "Google Cloud Console에서 OAuth Client를 만든 뒤 다운로드한 JSON을 이 경로에 저장하세요."
            )
        if not interactive:
            raise RuntimeError("OAuth 인증이 필요합니다. 설정 화면에서 '구글 인증'을 먼저 진행하세요.")
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
        # 로컬 PC에서만 정상 동작(브라우저 열림)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    return build("calendar", "v3", credentials=creds)


def parse_due_datetime(due_str: str, *, tz: str = "Asia/Seoul") -> Tuple[datetime, datetime]:
    """tasks.due_date(문자열)를 datetime 범위(start/end)로 변환.
    - 'YYYY-MM-DD HH:MM' -> 해당 시간 시작, 1시간짜리 이벤트
    - 'YYYY-MM-DD' -> 오전 9시 시작(1시간)
    """
    s = (due_str or "").strip()
    if not s:
        start = datetime.now().replace(second=0, microsecond=0)
        return start, start + timedelta(hours=1)

    try:
        # 'YYYY-MM-DD HH:MM'
        if ":" in s:
            start = datetime.fromisoformat(s)
        else:
            start = datetime.fromisoformat(s + " 09:00")
    except Exception:
        start = datetime.now().replace(second=0, microsecond=0)

    start = start.replace(second=0, microsecond=0)
    end = start + timedelta(hours=1)
    return start, end


def create_event(*, calendar_id: str, summary: str, start_dt: datetime, end_dt: datetime,
                 description: str = "", location: str = "", timezone: str = "Asia/Seoul",
                 interactive: bool = False) -> Tuple[str, str]:
    """이벤트 생성 -> (event_id, htmlLink)"""
    service = get_service(interactive=interactive)

    body = {
        "summary": summary,
        "description": description or "",
        "location": location or "",
        "start": {"dateTime": start_dt.isoformat(), "timeZone": timezone},
        "end": {"dateTime": end_dt.isoformat(), "timeZone": timezone},
    }
    ev = service.events().insert(calendarId=calendar_id, body=body).execute()
    return str(ev.get("id")), str(ev.get("htmlLink") or "")


def get_event(*, calendar_id: str, event_id: str, interactive: bool = False) -> dict:
    service = get_service(interactive=interactive)
    return service.events().get(calendarId=calendar_id, eventId=event_id).execute()


def update_event_summary(*, calendar_id: str, event_id: str, new_summary: str, interactive: bool = False) -> bool:
    """이벤트 제목(summary) 수정"""
    try:
        service = get_service(interactive=interactive)
        ev = service.events().get(calendarId=calendar_id, eventId=event_id).execute()
        ev["summary"] = new_summary
        service.events().update(calendarId=calendar_id, eventId=event_id, body=ev).execute()
        return True
    except Exception:
        return False


def mark_event_done(*, calendar_id: str, event_id: str, interactive: bool = False) -> bool:
    """제목 앞에 ✅를 붙여 완료 표시"""
    try:
        service = get_service(interactive=interactive)
        ev = service.events().get(calendarId=calendar_id, eventId=event_id).execute()
        cur = str(ev.get("summary") or "").strip()
        if cur.startswith("✅"):
            return True
        ev["summary"] = "✅ " + cur
        service.events().update(calendarId=calendar_id, eventId=event_id, body=ev).execute()
        return True
    except Exception:
        return False


def delete_event(*, calendar_id: str, event_id: str, interactive: bool = False) -> bool:
    try:
        service = get_service(interactive=interactive)
        service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
        return True
    except Exception:
        return False
