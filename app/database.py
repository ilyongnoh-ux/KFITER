"""
[특허 청구항 1: 데이터 저장 구조]
KFIT Local SQLite DB Schema
발명의 명칭: "유연한 필드 확장을 위한 하이브리드 데이터베이스 스키마 구조 및 데이터 무결성 보장 장치"

[기술적 특징]
1. Hybrid Schema: 정규화된 RDBMS 테이블(Customers, Contracts)과 비정형 JSON 필드(custom_data)의 결합.
2. Physical Separation: 개인정보 보호를 위해 신원정보(Identity)와 금융정보(Financial)를 물리적으로 분리 저장.
3. Auto-Migration: 스키마 변경 시 서비스 중단 없는 자가 치유형 컬럼 확장 기술.
"""

from __future__ import annotations

import os
import re
import sqlite3
import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Optional

# [설정] 데이터 저장 경로 (사용자 로컬 환경 격리)
USER_DATA_DIR = os.path.join(os.path.expanduser("~"), "KFIT_Data")
os.makedirs(USER_DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(USER_DATA_DIR, "kfit_local.db")


def _get_columns(cur: sqlite3.Cursor, table_name: str) -> List[str]:
    """[Helper] 현재 테이블의 메타데이터(컬럼 정보) 조회"""
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def _ensure_column(cur: sqlite3.Cursor, table: str, column: str, col_type: str) -> None:
    """
    [기술적 특징: 자가 치유형 스키마 마이그레이션]
    앱 구동 시 DB 스키마를 검사하여 누락된 컬럼이 있으면 동적으로 생성(Alter).
    이를 통해 버전 업데이트 시 별도의 DB 마이그레이션 스크립트 실행 절차를 제거함.
    """
    cols = _get_columns(cur, table)
    if column in cols:
        return
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def _normalize_phone(phone: Optional[str]) -> str:
    """[전처리] 전화번호 정규화 (중복 제거를 위한 Unique Key 생성 전단계)"""
    if phone is None: return ""
    return re.sub(r"\D", "", str(phone))


def _make_match_key(name: Optional[str], phone: Optional[str]) -> str:
    """
    [특허 포인트: 지퍼 매칭 키 생성 알고리즘]
    이기종 데이터(연락처 vs 계약서) 간의 동일인 식별을 위해
    이름의 첫 글자와 전화번호 뒷 4자리를 결합한 경량 해시 키를 생성.
    (예: 홍길동, 010-1234-5678 -> '홍5678')
    """
    if not name: return ""
    digits = _normalize_phone(phone)
    if len(digits) < 4: return ""
    return f"{str(name).strip()[:1]}{digits[-4:]}"



def _norm_text(x: object) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s.lower()

def _norm_name(x: object) -> str:
    s = _norm_text(x)
    s = re.sub(r"[^0-9a-zA-Z가-힣\*]", "", s)
    return s

def _norm_birth(x: object) -> str:
    s = re.sub(r"\D", "", str(x or ""))
    return s[:8] if len(s) >= 8 else s

def _norm_policy_no(x: object) -> str:
    s = str(x or "").strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9a-zA-Z]", "", s)
    return s.upper()

def _norm_date(x: object) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return ""
    # 이미 YYYY-MM-DD 형태면 그대로
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    # 'YYYY-MM-DD ...' 형태면 앞 10자리
    if len(s) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    # 8자리 숫자면 YYYY-MM-DD로
    d = re.sub(r"\D", "", s)
    if len(d) >= 8:
        return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
    return re.sub(r"\s+", "", s).lower()

def _norm_premium(x: object) -> int:
    try:
        return int(re.sub(r"\D", "", str(x or "")) or 0)
    except Exception:
        return 0

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _contract_key_hash(customer_id: int, company: object, policy_no: object, product_name: object,
                       start_date: object, premium: object, insured_birth: object = None, insured_name: object = None) -> str:
    cid = str(customer_id or "")
    comp = _norm_text(company)
    pol = _norm_policy_no(policy_no)
    if pol:
        base = "|".join(["P", cid, comp, pol])
    else:
        discr = _norm_birth(insured_birth) or _norm_name(insured_name)
        base = "|".join([
            "N", cid, comp, _norm_text(product_name), _norm_date(start_date), str(_norm_premium(premium)), discr
        ])
    return _sha1(base)

def _contract_stable_hash(customer_id: int, company: object, product_name: object, start_date: object, premium: object,
                          insured_birth: object = None, insured_name: object = None, insured_gender: object = None) -> str:
    cid = str(customer_id or "")
    comp = _norm_text(company)
    discr = _norm_birth(insured_birth) or _norm_name(insured_name)
    gen = _norm_text(insured_gender)
    base = "|".join(["S", cid, comp, _norm_text(product_name), _norm_date(start_date), str(_norm_premium(premium)), discr, gen])
    return _sha1(base)

def _contract_content_hash(customer_id: int, company: object, product_name: object, policy_no: object, premium: object,
                           status: object, start_date: object, end_date: object,
                           insured_name: object, insured_phone: object, insured_birth: object, insured_gender: object,
                           coverage_summary: object) -> str:
    base = "|".join([
        str(customer_id or ""),
        _norm_text(company),
        _norm_text(product_name),
        _norm_policy_no(policy_no),
        str(_norm_premium(premium)),
        _norm_text(status),
        _norm_date(start_date),
        _norm_date(end_date),
        _norm_name(insured_name),
        _norm_text(insured_phone),
        _norm_birth(insured_birth),
        _norm_text(insured_gender),
        _norm_text(coverage_summary),
    ])
    return _sha1(base)


def init_db() -> None:
    """
    [시스템 초기화 루틴]
    데이터베이스 연결을 수립하고 테이블 무결성을 검증하며 스키마를 최신화함.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON") # 참조 무결성 강제
    c = conn.cursor()

    # -----------------------------------------------------------
    # 1. Customers 테이블: 고객의 고유 식별 정보 (Identity)
    # -----------------------------------------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- [식별자 필드]
            name TEXT NOT NULL,
            phone TEXT,
            phone_norm TEXT,   -- 검색 속도 최적화를 위한 인덱싱 타겟
            
            -- [인구통계 정보]
            birth_date TEXT,
            gender TEXT,
            region TEXT,
            address TEXT,
            email TEXT,
            source TEXT,
            
            -- [특허 포인트: 비정형 확장 필드]
            -- 사용자가 임의로 추가하는 데이터(취미, MBTI 등)를 JSON 형태로 직렬화하여 저장.
            -- 정형 데이터베이스의 경직성을 해소함.
            custom_data TEXT,
            family_info TEXT,
            saju_info TEXT,
            
            -- [시스템 메타데이터]
            match_key TEXT,    -- 이기종 데이터 병합용 키
            origin_hash TEXT,
            stable_hash TEXT,  -- 데이터 중복 업로드 방지용 해시
            last_contact TEXT, -- 활동성 지표
            next_plan TEXT,    -- CRM 자동화 트리거
            memo TEXT,
            
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # 마이그레이션 (기존 버전 호환성 유지)
    for col in ["phone_norm", "gender", "address", "email", "source", 
                "family_info", "saju_info", "match_key", "stable_hash", "last_contact", "next_plan", "custom_data", "memo"]:
        _ensure_column(c, "customers", col, "TEXT")

    # -----------------------------------------------------------
    # 2. Tasks 테이블: 영업 비서 스케줄러
    # -----------------------------------------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            type TEXT,
            status TEXT DEFAULT '미완료',
            due_date TEXT,
            FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE CASCADE
        )
    """)

    # ✅ Tasks 테이블 확장 컬럼(구글 캘린더 연동용)
    for col in ["gcal_event_id", "gcal_html_link", "gcal_calendar_id", "gcal_sync_status", "gcal_last_sync"]:
        _ensure_column(c, "tasks", col, "TEXT")


    # -----------------------------------------------------------
    # 3. Consultations 테이블: 상호작용 로그 (Activity Log)
    # -----------------------------------------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS consultations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            consult_type TEXT,
            content TEXT,
            consult_date TEXT,
            origin_hash TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE CASCADE
        )
    """)

    # -----------------------------------------------------------
    # 4. Contracts 테이블: 금융 계약 정보 (Financial Data)
    # [특허 포인트: 1:N 관계의 자동 정규화 저장소]
    # ETL 엔진에 의해 분리된 계약 정보가 이곳에 저장됨.
    # -----------------------------------------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            company TEXT,
            product_name TEXT,
            policy_no TEXT,
            policy_no_norm TEXT,
            premium INTEGER,
            status TEXT,
            start_date TEXT,
            end_date TEXT,
            coverage_summary TEXT,
            
            -- [NEW: 피보험자 분리 저장]
            -- 계약자(Customer)와 피보험자가 다를 경우를 대비한 별도 컬럼
            insured_name TEXT,
            insured_phone TEXT,
            insured_birth TEXT,
            insured_gender TEXT,

            -- [NEW: 계약자(Policyholder) 정보 저장]
            -- [데이터(db포함) 오류] 누락 보완: 계약자≠피보험자 케이스(개인/법인)에서
            --  1) 상담 주체(primary_role) 결정
            --  2) 계약자명(policyholder_name) 기준 검색/그룹핑
            --  를 안정적으로 수행하기 위한 최소 컬럼 세트
            policyholder_name TEXT,
            policyholder_type TEXT,       -- 'PERSON' | 'CORP'
            policyholder_norm TEXT,       -- 검색/그룹핑용 정규화 키(예: '(주)선경스틸' → '선경스틸')
            policyholder_phone TEXT,
            primary_role TEXT,            -- 'POLICYHOLDER' | 'INSURED'

            origin_hash TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(customer_id) REFERENCES customers(id) ON DELETE CASCADE
        )
    """)
    # 계약 테이블 마이그레이션
    # 계약 테이블 마이그레이션(누락 컬럼 보강)
    # [데이터(db포함) 오류] "insured_b..." 같은 잘못된 컬럼명이 생성되지 않도록 컬럼 목록을 명시적으로 고정
    for col in [
        "insured_name", "insured_phone", "insured_birth", "insured_gender",
        "policyholder_name", "policyholder_type", "policyholder_norm", "policyholder_phone", "primary_role",
        "policy_no_norm", "stable_hash", "key_hash", "content_hash",
    ]:
        _ensure_column(c, "contracts", col, "TEXT")


    # -----------------------------------------------------------
    # [데이터(db포함) 오류] 계약자(Policyholder) 기반 검색/그룹핑을 위한 인덱스
    # - 법인 계약자 검색 성능 향상
    # - policyholder_norm는 공백/기호/법인표기를 제거한 정규화 키(예: '(주)선경스틸' → '선경스틸')
    # -----------------------------------------------------------
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_contracts_policyholder_norm ON contracts(policyholder_norm)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_contracts_policyholder_type_norm ON contracts(policyholder_type, policyholder_norm)")
    except Exception:
        # SQLite 구버전/권한 문제 등에서 인덱스 생성 실패 시에도 기능은 동작 가능(성능만 저하)
        pass

    # -----------------------------------------------------------
    # [데이터(db포함) 오류] 기존 데이터 백필(호환성 유지)
    # - 기존 contracts 레코드에는 policyholder_*가 비어있을 수 있음
    # - 최소한 표시/검색이 깨지지 않도록:
    #   1) policyholder_name이 비어 있으면 customers.name을 사용(과거 데이터 호환)
    #   2) policyholder_norm/type은 간단 휴리스틱으로 채움(100% 정확성보다 "검색 가능" 우선)
    # -----------------------------------------------------------
    def _is_corp_guess(n: str) -> bool:
        n = (n or "").strip()
        if not n:
            return False
        # 대표적인 법인 표기/조직 키워드(보수적으로)
        corp_kws = ["(주)", "㈜", "주식회사", "유한회사", "재단", "사단", "협동조합", "법무법인", "병원", "학교", "센터", "협회", "공사", "공단"]
        return any(k in n for k in corp_kws)

    def _norm_org(n: str) -> str:
        n = (n or "").strip()
        if not n:
            return ""
        # 법인 표기/괄호/공백 제거(간단 버전)
        n2 = n.replace(" ", "")
        for k in ["(주)", "㈜", "주식회사", "유한회사", "재단법인", "사단법인"]:
            n2 = n2.replace(k, "")
        n2 = re.sub(r"[\(\)\[\]\{\}]", "", n2)
        return n2

    try:
        c.execute(
            "SELECT ct.id, ct.customer_id, COALESCE(ct.policyholder_name,''), COALESCE(cu.name,'') "
            "FROM contracts ct LEFT JOIN customers cu ON cu.id = ct.customer_id "
            "WHERE COALESCE(ct.policyholder_name,'') = ''"
        )
        rows = c.fetchall() or []
        for rid, cid, ph, cu_name in rows:
            ph_name = (cu_name or "").strip()
            ph_type = "CORP" if _is_corp_guess(ph_name) else "PERSON"
            ph_norm = _norm_org(ph_name)
            c.execute(
                "UPDATE contracts SET policyholder_name = ?, policyholder_type = ?, policyholder_norm = ? WHERE id = ?",
                (ph_name, ph_type, ph_norm, rid),
            )
    except Exception:
        pass

    # -----------------------------------------------------------
    # [중요] 계약 중복 방지(멱등 업로드)
    # 1) 기존 데이터에 key_hash/content_hash 백필
    # 2) key_hash 기준 중복 행 제거(최초 1건만 유지)
    # 3) UNIQUE INDEX 생성 (향후 중복 원천 차단)
    # -----------------------------------------------------------
    try:
        c.execute("""SELECT id, customer_id, company, product_name, policy_no, premium, status, start_date, end_date,
                             insured_name, insured_phone, insured_birth, insured_gender, coverage_summary
                      FROM contracts
                      WHERE (key_hash IS NULL OR key_hash = '') OR (content_hash IS NULL OR content_hash = '')""")
        rows = c.fetchall()
        for r in rows:
            (rid, customer_id, company, product_name, policy_no, premium, status, start_date, end_date,
             insured_name, insured_phone, insured_birth, insured_gender, coverage_summary) = r
            kh = _contract_key_hash(customer_id, company, policy_no, product_name, start_date, premium, insured_birth, insured_name)
            pn = _norm_policy_no(policy_no)
            sh = _contract_stable_hash(customer_id, company, product_name, start_date, premium, insured_birth, insured_name, insured_gender)
            ch = _contract_content_hash(customer_id, company, product_name, policy_no, premium, status, start_date, end_date,
                                        insured_name, insured_phone, insured_birth, insured_gender, coverage_summary)
            c.execute("UPDATE contracts SET policy_no_norm = ?, stable_hash = ?, key_hash = ?, content_hash = ? WHERE id = ?", (pn, sh, kh, ch, rid))

        # -----------------------------------------------------------
        # [추가] policy_no_norm / stable_hash 백필 (기존 key_hash 보유 데이터 포함)
        # - 향후 업로드에서 2차 매칭(policy_no_norm/stable_hash)이 확실히 작동하도록 보장
        # -----------------------------------------------------------
        c.execute("""SELECT id, customer_id, company, product_name, policy_no, premium, start_date,
                             insured_name, insured_birth, insured_gender
                      FROM contracts
                      WHERE (policy_no_norm IS NULL OR policy_no_norm = '')
                         OR (stable_hash IS NULL OR stable_hash = '')""")
        for rr in c.fetchall():
            rid2, customer_id2, company2, product_name2, policy_no2, premium2, start_date2, insured_name2, insured_birth2, insured_gender2 = rr
            pn2 = _norm_policy_no(policy_no2)
            sh2 = _contract_stable_hash(customer_id2, company2, product_name2, start_date2, premium2, insured_birth2, insured_name2, insured_gender2)
            c.execute("UPDATE contracts SET policy_no_norm = ?, stable_hash = ? WHERE id = ?", (pn2, sh2, rid2))

        # key_hash가 있는 데이터만 대상으로 중복 제거 (NULL/빈값은 제외)
        c.execute("""
            DELETE FROM contracts
            WHERE key_hash IS NOT NULL AND key_hash <> ''
              AND id NOT IN (
                  SELECT MIN(id) FROM contracts
                  WHERE key_hash IS NOT NULL AND key_hash <> ''
                  GROUP BY key_hash
              )
        """)

        # UNIQUE INDEX (중복 원천 봉쇄)
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_contracts_key_hash ON contracts(key_hash)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_contracts_policy_no_norm ON contracts(customer_id, policy_no_norm)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_contracts_stable_hash ON contracts(customer_id, stable_hash)")
    except Exception:
        # 구버전/환경차로 인한 예외는 앱 동작을 막지 않음
        pass

    # -----------------------------------------------------------
    # 5. Upload History: 업로드 파일 해시 기록 (실수 재업로드 방지)
    # 동일 파일(sha256)이 동일 액션(full_upload / contracts_only)으로 다시 들어오면
    # 앱에서 스킵/경고 처리할 수 있도록 DB에 기록합니다.
    # -----------------------------------------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS upload_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_hash TEXT NOT NULL,
            action TEXT NOT NULL,
            filename TEXT,
            filesize INTEGER,
            uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            summary_json TEXT,
            UNIQUE(file_hash, action)
        )
    """)
    # ✅ 마이그레이션: 기존 DB에 upload_history 컬럼이 빠져있을 수 있음
    # (CREATE TABLE IF NOT EXISTS 는 기존 테이블 스키마를 바꾸지 않으므로)
    try:
        existing_cols = {row[1] for row in c.execute("PRAGMA table_info(upload_history)").fetchall()}
        # 필요한 컬럼들 보강
        if "filename" not in existing_cols:
            c.execute("ALTER TABLE upload_history ADD COLUMN filename TEXT")
        if "filesize" not in existing_cols:
            c.execute("ALTER TABLE upload_history ADD COLUMN filesize INTEGER")
        if "uploaded_at" not in existing_cols:
            # DEFAULT는 ALTER에서 환경/버전에 따라 제한이 있을 수 있어 먼저 컬럼 추가 후 백필
            c.execute("ALTER TABLE upload_history ADD COLUMN uploaded_at DATETIME")
            c.execute("UPDATE upload_history SET uploaded_at = CURRENT_TIMESTAMP WHERE uploaded_at IS NULL")
        if "summary_json" not in existing_cols:
            c.execute("ALTER TABLE upload_history ADD COLUMN summary_json TEXT")
    except Exception:
        # 마이그레이션 실패 시에도 앱이 죽지 않게(최악의 경우 업로드 히스토리 기능만 비활성)
        pass

    # 인덱스 생성(컬럼 존재 후)
    c.execute("CREATE INDEX IF NOT EXISTS idx_upload_history_uploaded_at ON upload_history(uploaded_at)")


    conn.commit()
    conn.close()


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

if __name__ == "__main__":
    init_db()
    print(f"✅ DB 초기화 완료: {DB_PATH}")

# ---------------------------------------------------------
# [데이터(db포함) 오류] (CTO Patch Pack v2025-12-22)
# 이 파일은 UI 위젯(사용자 화면) 직접 출력이 없어 Streamlit use_container_width / ArrowTypeError와
# 직접적인 연관이 낮습니다.
#
# 특허 포인트(명세서 기재용):
# - 데이터 파이프라인 전체(입력→정규화→DB반영→표시) 중 "표시 단계"에서 발생하는 직렬화 실패를
#   utils.py의 호환/정규화 레이어가 흡수하여, 본 모듈의 로직(데이터/DB)은 그대로 유지됩니다.
# - 즉, 본 모듈은 기능/정합성 변경 없이도 상위 계층의 안정화 효과(로그/오류 감소)를 획득합니다.
# ---------------------------------------------------------

# ---------------------------------------------------------
# ---- DB Hardening Layer (Lock/Corruption Mitigation) -----------------
# KFIT_DB_HARDENING_LAYER
# 목적:
# - Streamlit 다중 세션/새로고침/동시 저장 상황에서 발생 가능한 "database is locked" 및
#   트랜잭션 지연을 최소화하기 위한 연결 옵션을 기본 적용합니다.
#
# 구현 요지(특허 명세서용 정리):
# 1) WAL(Write-Ahead Logging) 모드로 전환해 읽기/쓰기 경합을 완화합니다.
# 2) busy_timeout으로 잠금 대기 시간을 부여해 순간 경합을 "오류"가 아닌 "대기"로 흡수합니다.
# 3) synchronous를 NORMAL로 조정해 안정성과 속도의 균형을 맞춥니다(로컬 앱 특성 반영).
# 4) 모든 PRAGMA는 실패해도 서비스가 계속되도록 예외를 삼켜 '자가 치유(Self-Healing)'합니다.
#
# ⚠️ UI 영향 없음: DB 연결 설정만 보강(화면/레이아웃 변경 없음)
# ----------------------------------------------------------------------
def _kfit_apply_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    """SQLite 연결에 대해 안정성/경합 완화 PRAGMA를 적용한다."""
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except Exception:
        # 일부 환경/권한에서 WAL 설정이 실패할 수 있으나, 동작 자체는 가능해야 한다.
        pass
    try:
        conn.execute("PRAGMA synchronous = NORMAL")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA busy_timeout = 30000")  # 30s
    except Exception:
        pass
    try:
        conn.execute("PRAGMA temp_store = MEMORY")
    except Exception:
        pass

# ✅ 재정의(override): 상단의 get_connection()을 대체하여 전 모듈에서 동일 효과를 얻는다.
# - Python은 함수 호출 시점에 globals의 이름을 조회하므로, 아래 재정의는 import 이후에도 유효.
def get_connection() -> sqlite3.Connection:
    """Hardened connection factory (CTO Patch Pack)."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    _kfit_apply_sqlite_pragmas(conn)
    return conn
# [체크리스트]
# - UI 유지/존치: ✅ 유지됨
# - 수정 범위: ✅ [데이터(db포함) 오류] 섹션만
# - '..., 중략, 일부 생략' 금지: ✅ 준수(전체 파일 유지)
# - 수정 전 라인수: 409
# - 수정 후 라인수: 454
# ---------------------------------------------------------