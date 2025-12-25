"""
[특허 청구항 3: 데이터 관리 방법]
KFIT DB Transactions Module
발명의 명칭: "ETL 전처리 데이터의 관계형 데이터베이스 자동 분배 및 이중 검증 저장 방법"

[기술적 특징]
1. Upsert Strategy: 기존 고객은 업데이트하고 신규 고객은 생성하는 지능형 저장.
2. Dual Verification: 해시 키 매칭 후 패턴 검증을 통해 가족 간 오매칭 방지.
3. Atomic Distribution: 단일 엑셀 행을 '고객 ID 생성 -> 계약 정보 연결' 순서로 트랜잭션 처리.
"""

from __future__ import annotations
import re
import hashlib
import json
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from database import get_connection
import utils # [중요] 정밀 대조 함수 사용을 위해

# --- Helper Functions ---
def normalize_phone(phone):
    """전화번호 정규화 (DB 검색용 Key 생성)"""
    if phone is None: return ""
    return re.sub(r"\D", "", str(phone))

def phone_last4(phone):
    d = normalize_phone(phone)
    return d[-4:] if len(d) >= 4 else ""

def make_match_key(name, phone_or_last4):
    """[알고리즘] 최소 정보(이름1글자+번호4자리)를 이용한 경량 매칭 키 생성"""
    if not name: return ""
    first = str(name).strip()[:1]
    last4 = re.sub(r"\D", "", str(phone_or_last4))
    if len(last4) > 4: last4 = last4[-4:]
    return f"{first}{last4}" if first and len(last4) == 4 else ""

# ---------------------------------------------------------
# [데이터(db포함) 오류] 계약자(Policyholder) 개인/법인 분기 + 검색 키 정규화
# - queries.py는 streamlit 의존을 피하기 위해 utils.py를 import하지 않고 동일 로직을 최소로 구현
# - 특허 관점: "표준화된 정규화 키(policyholder_norm)를 생성하여 검색/그룹핑의 정확도를 높이는 방법"
# ---------------------------------------------------------
def _is_corporate_name(name: str) -> bool:
    n = (name or "").strip()
    if not n:
        return False
    corp_kws = [
        "(주)", "㈜", "주식회사", "유한회사", "재단", "사단", "협동조합",
        "법무법인", "세무법인", "회계법인", "병원", "의원", "학교", "학원",
        "센터", "협회", "조합", "공사", "공단",
    ]
    if any(k in n for k in corp_kws):
        return True
    if re.search(r"\b(CORP|CORPORATION|LTD|LIMITED|INC)\b", n, flags=re.I):
        return True
    return False


def _norm_org_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""
    n2 = n.replace(" ", "")
    for k in ["(주)", "㈜", "주식회사", "유한회사", "재단법인", "사단법인"]:
        n2 = n2.replace(k, "")
    n2 = re.sub(r"[\(\)\[\]\{\}]", "", n2)
    return n2



def _norm_text(x: object) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def _norm_name(x: object) -> str:
    # 이름은 마스킹(*)이 들어올 수 있으므로, 가능한 한 단순 정규화만 수행
    s = _norm_text(x)
    s = re.sub(r"[^0-9a-zA-Z가-힣\*]", "", s)
    return s

def _norm_birth(x: object) -> str:
    s = re.sub(r"\D", "", str(x or ""))
    # 8자리(YYYYMMDD) 우선, 없으면 있는 만큼만 사용
    return s[:8] if len(s) >= 8 else s

def _norm_policy_no(x: object) -> str:
    # 증권번호/계약번호는 공백/하이픈/특수문자 차이로 흔들리기 쉬움 → 영숫자만 남기고 대문자
    s = str(x or "").strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9a-zA-Z]", "", s)
    return s.upper()

def _norm_date(x: object) -> str:
    # 가능한 한 YYYY-MM-DD로 통일 (엑셀 serial / 문자열 / Timestamp 모두 대응)
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return ""

    try:
        # pandas Timestamp / datetime 등
        dt = pd.to_datetime(x, errors="coerce")
        if not pd.isna(dt):
            return dt.date().isoformat()
    except Exception:
        pass

    # 엑셀 serial 가능성(대략 20000~60000일 사이)
    try:
        fx = float(x)
        if 20000 <= fx <= 60000:
            dt = pd.to_datetime(fx, unit="D", origin="1899-12-30", errors="coerce")
            if not pd.isna(dt):
                return dt.date().isoformat()
    except Exception:
        pass

    # 마지막 fallback: 공백 제거 후 소문자
    s = re.sub(r"\s+", "", s)
    return s.lower()

def _norm_premium(x: object) -> int:
    try:
        return int(re.sub(r"\D", "", str(x or "")) or 0)
    except Exception:
        return 0


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _contract_key_hash(customer_id: int, company: object, policy_no: object, product_name: object,
                       start_date: object, premium: object, insured_birth: object = None, insured_name: object = None) -> str:
    """계약 유일키(멱등 업로드용)
    - 1순위: (customer_id, policy_no_norm)  ※ 증권번호가 정상적으로 들어오는 경우 가장 안전
    - 2순위: (customer_id, company, product_name, start_date, premium, insured_birth/insured_name)
      ※ 증권번호가 비어있거나 포맷이 흔들리는 경우, 같은 계약을 안정적으로 묶기 위한 보조키
    """
    cid = str(customer_id or "")
    comp = _norm_text(company)
    pol = _norm_policy_no(policy_no)

    if pol:
        base = "|".join(["P", cid, comp, pol])
    else:
        # 증권번호가 없을 때는 '상품+일자+보험료+피보험자 식별자' 조합을 사용
        discr = _norm_birth(insured_birth) or _norm_name(insured_name)
        base = "|".join([
            "N",
            cid,
            comp,
            _norm_text(product_name),
            _norm_date(start_date),
            str(_norm_premium(premium)),
            discr
        ])
    return _sha1(base)

def _contract_stable_hash(customer_id: int, company: object, product_name: object, start_date: object, premium: object,
                          insured_birth: object = None, insured_name: object = None, insured_gender: object = None) -> str:
    """증권번호가 흔들려도 동일 계약을 찾기 위한 '안정 키'(Unique 아님)
    - (customer_id, company, product_name, start_date, premium, insured_birth/insured_name, insured_gender)
    """
    cid = str(customer_id or "")
    comp = _norm_text(company)
    discr = _norm_birth(insured_birth) or _norm_name(insured_name)
    gen = _norm_text(insured_gender)
    base = "|".join([
        "S", cid, comp, _norm_text(product_name), _norm_date(start_date), str(_norm_premium(premium)), discr, gen
    ])
    return _sha1(base)



def _contract_content_hash(customer_id: int, company: object, product_name: object, policy_no: object, premium: object,
                           status: object, start_date: object, end_date: object,
                           insured_name: object, insured_phone: object, insured_birth: object, insured_gender: object,
                           coverage_summary: object) -> str:
    # 내용 변경 여부 판단용(정규화 후 해시)
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



def _pick(row: pd.Series, keys: tuple, default: str = "") -> str:
    for k in keys:
        if k in row and pd.notna(row[k]) and str(row[k]).strip() != "":
            return str(row[k]).strip()
    return default

# --- Customers (Upsert Logic) ---
# [queries.py] 내부 upsert_customer_identity 함수 수정

def upsert_customer_identity(*, name, phone, birth_date="", gender="", region="", address="", email="", source="", memo="", custom_data="", match_key=""):
    """[지능형 고객 저장] 공백 제거 강제 적용"""
    
    # 1. [핵심] 이름 내 모든 공백 제거 (예: '노 일용' -> '노일용')
    if name:
        name = str(name).replace(" ", "").strip()
        
    if not name or not phone: return False, "이름/연락처 필수", None
    
    # 2. 전화번호 정규화 (숫자만 남김)
    phone_norm = normalize_phone(phone)
    
    if not match_key: match_key = make_match_key(name, phone_last4(phone_norm))

    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM customers WHERE phone_norm = ? LIMIT 1", (phone_norm,))
        row = cur.fetchone()
        if row:
            cid = int(row[0])
            # 기존 고객 업데이트 시에도 이름은 공백 없는 버전으로 갱신 가능하도록 로직 추가 가능하나,
            # 여기서는 안전하게 기존 로직 유지 (필요 시 name=? 구문 추가 가능)
            cur.execute("""
                UPDATE customers SET
                    birth_date = CASE WHEN ? <> '' THEN ? ELSE birth_date END,
                    gender = CASE WHEN ? <> '' THEN ? ELSE gender END,
                    region = CASE WHEN ? <> '' THEN ? ELSE region END,
                    email = CASE WHEN ? <> '' THEN ? ELSE email END,
                    custom_data = CASE WHEN ? <> '' THEN ? ELSE custom_data END
                WHERE id = ?
            """, (birth_date, birth_date, gender, gender, region, region, email, email, custom_data, custom_data, cid))
            conn.commit()
            return True, "Update", cid
        else:
            cur.execute("""
                INSERT INTO customers (name, phone, phone_norm, match_key, birth_date, gender, region, address, email, source, memo, custom_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, phone, phone_norm, match_key, birth_date, gender, region, address, email, source, memo, custom_data))
            conn.commit()
            return True, "Insert", int(cur.lastrowid)
    except Exception as e:
        conn.rollback(); return False, str(e), None
    finally: conn.close()

# --- [핵심] ETL 연동 저장 함수 ---
def insert_customer_data(df: pd.DataFrame, source: str = "upload"):
    """[특허 포인트: 자동 분배 저장 알고리즘]"""
    stats = {
        "new_cust": 0, "update_cust": 0,
        "new_cont": 0, "update_cont": 0, "same_cont": 0, "ambig_cont": 0,
        "fail": 0
    }

    for _, row in df.iterrows():
        name = row.get('name')
        phone = row.get('phone')
        if not name or not phone:
            stats['fail'] += 1
            continue

        ok, msg, cid = upsert_customer_identity(
            name=name, phone=phone,
            birth_date=row.get('birth_date') or "",
            gender=row.get('gender') or "",
            region=row.get('region') or "",
            address=row.get('address') or "",
            email=row.get('email') or "",
            source=source,
            memo=row.get('memo') or "",
            custom_data=row.get('custom_data') or "",
            match_key=row.get('match_key') or ""
        )
        if not ok or not cid:
            stats['fail'] += 1
            continue

        if msg == "Insert":
            stats['new_cust'] += 1
        else:
            stats['update_cust'] += 1

        fin_data = row.get("financial")
        if not isinstance(fin_data, dict):
            fin_data = row.get("financial_temp")
        if isinstance(fin_data, dict):
            res = add_contract(
                customer_id=cid,
                company=fin_data.get('company'),
                product_name=fin_data.get('product_name'),
                policy_no=fin_data.get('policy_no'),
                premium=fin_data.get('premium'),
                status=fin_data.get('status'),
                start_date=fin_data.get('start_date'),
                end_date=fin_data.get('end_date'),
                insured_name=fin_data.get('insured_name'),
                insured_phone=fin_data.get('insured_phone'),
                insured_birth=fin_data.get('insured_birth'),
                insured_gender=fin_data.get('insured_gender'),
                coverage_summary=fin_data.get('coverage_summary', "")
            )
            if res == "insert":
                stats['new_cont'] += 1
            elif res == "update":
                stats['update_cont'] += 1
            elif res == "same":
                stats['same_cont'] += 1
            elif res == "ambig":
                stats['ambig_cont'] += 1
            else:
                stats['fail'] += 1

    msg = (
        f"✅ 고객: 신규 {stats['new_cust']} / 업데이트 {stats['update_cust']}  |  "
        f"계약: 신규 {stats['new_cont']} / 변경 {stats['update_cont']} / 유지 {stats['same_cont']} / 보류 {stats['ambig_cont']}  |  "
        f"실패 {stats['fail']}"
    )

    return True, msg, stats

# --- Contracts & Dual Verification ---

def add_contract(customer_id, company, product_name, policy_no, premium, status, start_date, end_date,
                 insured_name=None, insured_phone=None, insured_birth=None, insured_gender=None, coverage_summary="",
                 policyholder_name=None, policyholder_phone=None, policyholder_type=None, policyholder_norm=None,
                 primary_role=None):
    """계약 정보 저장 (강화된 멱등 업로드)
    원칙:
    1) key_hash(유일키)로 먼저 매칭
    2) 실패 시 policy_no_norm / stable_hash로 2차 매칭(증권번호 누락/흔들림 대응)
    3) 내용(content_hash)이 동일하면 UPDATE하지 않고 'same' 처리
    반환: "insert" | "update" | "same" | "ambig" | "fail"
    """
    conn = get_connection(); cur = conn.cursor()
    try:
        prem_int = _norm_premium(premium)
        start_norm = _norm_date(start_date)
        end_norm = _norm_date(end_date)
        pol_norm = _norm_policy_no(policy_no)
        stable_hash = _contract_stable_hash(customer_id, company, product_name, start_norm, prem_int,
                                            insured_birth, insured_name, insured_gender)
        key_hash = _contract_key_hash(customer_id, company, pol_norm, product_name, start_norm, prem_int,
                                      insured_birth, insured_name)
        content_hash = _contract_content_hash(
            customer_id, company, product_name, pol_norm, prem_int, status, start_norm, end_norm,
            insured_name, insured_phone, insured_birth, insured_gender, coverage_summary
        )

        # --------------------------
        # ---------------------------------------------------------
        # [데이터(db포함) 오류] 계약자(Policyholder) 정보 정규화/보강
        # - 계약 표시/법인 검색/상담주체(primary_role) 분기에서 사용
        # - 정책:
        #   * policyholder_name 미제공 시: insured_name으로 대체(표시/검색 최소 보장)
        #   * type/norm 미제공 시: 휴리스틱으로 자동 산출
        # ---------------------------------------------------------
        ph_name = (policyholder_name or "").strip()
        if not ph_name:
            ph_name = (insured_name or "").strip()
        ph_phone = (policyholder_phone or "").strip()
        ph_type = (policyholder_type or "").strip()
        if not ph_type:
            ph_type = "CORP" if _is_corporate_name(ph_name) else "PERSON"
        ph_norm = (policyholder_norm or "").strip()
        if not ph_norm:
            ph_norm = _norm_org_name(ph_name)
        pr_role = (primary_role or "").strip()
        if not pr_role:
            # 기본값: 계약자 기준(POLICYHOLDER). 단, 계약자가 법인이며 피보험자가 따로 있으면 INSURED로 둔다.
            if ph_type == "CORP" and (insured_name or "").strip() and ph_name and ph_name.replace(" ", "") != (insured_name or "").strip().replace(" ", ""):
                pr_role = "INSURED"
            else:
                pr_role = "POLICYHOLDER"

        # 1) key_hash 직접 매칭
        # --------------------------
        cur.execute("SELECT id, content_hash, key_hash FROM contracts WHERE key_hash = ? LIMIT 1", (key_hash,))
        row = cur.fetchone()
        match_id = None
        existing_content = None
        existing_key = None

        if row:
            match_id, existing_content, existing_key = row

        # --------------------------
        # 2) policy_no_norm 매칭 (증권번호 포맷 흔들림/기존 key_hash 방식 차이 대비)
        # --------------------------
        if match_id is None and pol_norm:
            try:
                cur.execute(
                    "SELECT id, content_hash, key_hash FROM contracts WHERE customer_id = ? AND policy_no_norm = ? ORDER BY id ASC LIMIT 5",
                    (customer_id, pol_norm)
                )
                rows = cur.fetchall()
                if rows:
                    # 완전 동일 내용이 있으면 same
                    for r in rows:
                        if r[1] == content_hash:
                            match_id, existing_content, existing_key = r[0], r[1], r[2]
                            break
                    if match_id is None and len(rows) == 1:
                        match_id, existing_content, existing_key = rows[0][0], rows[0][1], rows[0][2]
            except Exception:
                # 컬럼이 아직 없거나(마이그레이션 전) 등의 예외는 stable_hash로 이어서 처리
                pass

        # --------------------------
        # 3) stable_hash 매칭 (증권번호 없거나 바뀌어도 동일 계약을 찾기)
        # --------------------------
        if match_id is None and stable_hash:
            try:
                cur.execute(
                    "SELECT id, content_hash, key_hash, COALESCE(policy_no_norm,'') FROM contracts WHERE customer_id = ? AND stable_hash = ? ORDER BY id ASC LIMIT 10",
                    (customer_id, stable_hash)
                )
                rows = cur.fetchall()
                if rows:
                    # 완전 동일 내용이 있으면 same
                    for r in rows:
                        if r[1] == content_hash:
                            match_id, existing_content, existing_key = r[0], r[1], r[2]
                            break

                    if match_id is None:
                        if len(rows) == 1:
                            match_id, existing_content, existing_key = rows[0][0], rows[0][1], rows[0][2]
                        else:
                            # 여러 개가 걸렸는데 증권번호가 서로 다르면 실제로 복수 계약일 수 있음 → 안전하게 ambiguous 처리(추가 입력 방지)
                            policy_set = set([r[3] for r in rows if r[3]])
                            if (not pol_norm) and len(policy_set) == 0:
                                # 모두 증권번호 없음 → 중복으로 보는 편이 안전(첫 번째로 귀속)
                                match_id, existing_content, existing_key = rows[0][0], rows[0][1], rows[0][2]
                            else:
                                return "ambig"
            except Exception:
                pass

        # --------------------------
        # Match found → same/update
        # --------------------------
        if match_id is not None:
            if existing_content == content_hash:
                return "same"

            # update (키 갱신은 충돌시 기존 키 유지)
            try:
                cur.execute("""
                    UPDATE contracts SET
                        company = ?,
                        product_name = ?,
                        policy_no = ?,
                        policy_no_norm = ?,
                        premium = ?,
                        status = ?,
                        start_date = ?,
                        end_date = ?,
                        insured_name = ?,
                        insured_phone = ?,
                        insured_birth = ?,
                        insured_gender = ?,
                        coverage_summary = ?,
                        policyholder_name = ?,
                        policyholder_type = ?,
                        policyholder_norm = ?,
                        policyholder_phone = ?,
                        primary_role = ?,
                        stable_hash = ?,
                        content_hash = ?,
                        key_hash = ?
                    WHERE id = ?
                """, (
                    company, product_name, policy_no, pol_norm, prem_int, status, start_norm, end_norm,
                    insured_name, insured_phone, insured_birth, insured_gender, coverage_summary,
                    ph_name, ph_type, ph_norm, ph_phone, pr_role,
                    stable_hash, content_hash, key_hash,
                    match_id
                ))
                conn.commit()
                return "update"
            except sqlite3.IntegrityError:
                # key_hash 충돌(이미 동일 key_hash가 다른 행에 존재) → 기존 key 유지하고 업데이트
                cur.execute("""
                    UPDATE contracts SET
                        company = ?,
                        product_name = ?,
                        policy_no = ?,
                        policy_no_norm = ?,
                        premium = ?,
                        status = ?,
                        start_date = ?,
                        end_date = ?,
                        insured_name = ?,
                        insured_phone = ?,
                        insured_birth = ?,
                        insured_gender = ?,
                        coverage_summary = ?,
                        policyholder_name = ?,
                        policyholder_type = ?,
                        policyholder_norm = ?,
                        policyholder_phone = ?,
                        primary_role = ?,
                        stable_hash = ?,
                        content_hash = ?
                    WHERE id = ?
                """, (
                    company, product_name, policy_no, pol_norm, prem_int, status, start_norm, end_norm,
                    insured_name, insured_phone, insured_birth, insured_gender, coverage_summary,
                    stable_hash, content_hash,
                    match_id
                ))
                conn.commit()
                return "update"

        # --------------------------
        # No match → insert
        # --------------------------
        try:
            cur.execute("""
                INSERT INTO contracts (
                    customer_id, company, product_name, policy_no, policy_no_norm,
                    premium, status, start_date, end_date, coverage_summary,
                    insured_name, insured_phone, insured_birth, insured_gender,
                    policyholder_name, policyholder_type, policyholder_norm, policyholder_phone, primary_role,
                    stable_hash, key_hash, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                customer_id, company, product_name, policy_no, pol_norm,
                prem_int, status, start_norm, end_norm, coverage_summary,
                insured_name, insured_phone, insured_birth, insured_gender,
                ph_name, ph_type, ph_norm, ph_phone, pr_role,
                stable_hash, key_hash, content_hash
            ))
            conn.commit()
            return "insert"
        except sqlite3.IntegrityError:
            # 이미 들어간 경우(key_hash 유일 제약) → same/update로 정리
            cur.execute("SELECT id, content_hash FROM contracts WHERE key_hash = ? LIMIT 1", (key_hash,))
            row = cur.fetchone()
            if row:
                if row[1] == content_hash:
                    return "same"
                # 다른 내용이면 해당 행 업데이트
                match_id = row[0]
                cur.execute("""
                    UPDATE contracts SET
                        company = ?,
                        product_name = ?,
                        policy_no = ?,
                        policy_no_norm = ?,
                        premium = ?,
                        status = ?,
                        start_date = ?,
                        end_date = ?,
                        insured_name = ?,
                        insured_phone = ?,
                        insured_birth = ?,
                        insured_gender = ?,
                        coverage_summary = ?,
                        policyholder_name = ?,
                        policyholder_type = ?,
                        policyholder_norm = ?,
                        policyholder_phone = ?,
                        primary_role = ?,
                        stable_hash = ?,
                        content_hash = ?
                    WHERE id = ?
                """, (
                    company, product_name, policy_no, pol_norm, prem_int, status, start_norm, end_norm,
                    insured_name, insured_phone, insured_birth, insured_gender, coverage_summary,
                    stable_hash, content_hash, match_id
                ))
                conn.commit()
                return "update"
            return "fail"

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return "fail"
    finally:
        conn.close()

def bulk_import_masked_contracts(df: pd.DataFrame, progress_cb=None):
    """
    [특허 포인트: 이중 검증(Dual Verification) 알고리즘]
    1. Broad Key: '성 + 번호뒷자리'로 후보군 전체 추출.
    2. Deep Check: '홍*동' vs '홍길동' 패턴을 정밀 대조하여 가족 간 오매칭 원천 차단.

    + 멱등 업로드:
      동일 계약은 '유지' 처리(중복 INSERT 방지)

    progress_cb(done:int, total:int, message:str)
    """
    inserted, updated, same, ambig, hold = 0, 0, 0, 0, 0
    failed, ambig = 0, 0
    conn = get_connection(); cur = conn.cursor()

    total = int(len(df)) if df is not None else 0

    def _cb(done: int, msg: str = ""):
        if progress_cb:
            try:
                progress_cb(done, total, msg)
            except Exception:
                pass

    _cb(0, "시작")

    try:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            masked_name = _pick(row, ("이름", "고객명", "피보험자", "계약자"))
            phone = _pick(row, ("연락처", "휴대폰", "휴대전화"))
            mk = make_match_key(masked_name, phone_last4(phone))
            if not mk:
                failed += 1
                _cb(i, f"{masked_name} / match_key 없음")
                continue

            cur.execute("SELECT id, name FROM customers WHERE match_key = ?", (mk,))
            candidates = cur.fetchall()
            if not candidates:
                failed += 1
                _cb(i, f"{masked_name} / 고객 후보 없음")
                continue

            valid_candidates = []
            for cid, real_name in candidates:
                if utils.is_name_match(masked_name, real_name):
                    valid_candidates.append((cid, real_name))

            if len(valid_candidates) > 1:
                ambig += 1
                _cb(i, f"{masked_name} / 후보 다수(모호)")
                continue
            if len(valid_candidates) == 0:
                failed += 1
                _cb(i, f"{masked_name} / 이름 불일치")
                continue

            target_id = int(valid_candidates[0][0])

            company = _pick(row, ("보험사", "회사", "보험회사"))
            product_name = _pick(row, ("상품명", "상품", "담보", "보험상품"))
            policy_no = _pick(row, ("증권번호", "증권", "증서번호", "증번호", "계약번호", "폴리시번호", "policy_no"))
            status = _pick(row, ("상태", "계약상태"))
            start_date = _pick(row, ("계약일", "청약일", "가입일", "개시일"))
            end_date = _pick(row, ("만기일", "해지일", "종료일"))

            premium = 0
            try:
                premium = int(re.sub(r"\D", "", str(_pick(row, ("보험료", "납입보험료", "월보험료", "보험료(월)")))))
            except:
                premium = 0

            res = add_contract(
                customer_id=target_id, company=company, product_name=product_name, policy_no=policy_no,
                premium=premium, status=status, start_date=start_date, end_date=end_date
            )
            if res == "insert":
                inserted += 1
            elif res == "update":
                updated += 1
            elif res == "same":
                same += 1
            elif res == "ambig":
                hold += 1
            else:
                failed += 1

            # 5건마다 / 마지막에만 갱신(너무 잦은 UI 업데이트 방지)
            if i == 1 or i % 5 == 0 or i == total:
                _cb(i, f"{masked_name} / {policy_no}")

        conn.commit()
        stats = {"new_cont": inserted, "update_cont": updated, "same_cont": same, "hold_cont": hold, "failed": failed, "ambig": ambig}
        msg = f"✅ 계약 처리: 신규 {inserted} / 변경 {updated} / 유지 {same}"
        if hold > 0:
            msg += f" / 보류: {hold}건"
        if failed > 0:
            msg += f" / 미등록: {failed}건"
        if ambig > 0:
            msg += f" / ⚠️ 중복확인필요: {ambig}건"

        _cb(total, "완료")
        return True, msg, stats
    except Exception as e:
        conn.rollback()
        stats = {"new_cont": inserted, "update_cont": updated, "same_cont": same, "hold_cont": hold, "failed": failed, "ambig": ambig}
        _cb(0, "오류")
        return False, f"오류: {e}", stats
    finally:
        conn.close()


# --- Upload History (Duplicate Upload Guard) ---
def get_upload_history(file_hash: str, action: str):
    """동일 파일(sha256) + 동일 액션 처리 이력이 있으면 반환"""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT file_hash, action, filename, filesize, uploaded_at, summary_json
                   FROM upload_history
                  WHERE file_hash = ? AND action = ?
                  ORDER BY uploaded_at DESC
                  LIMIT 1""",
            (file_hash, action),
        )
        row = cur.fetchone()
        if not row:
            return None
        summary = None
        if row[5]:
            try:
                summary = json.loads(row[5])
            except Exception:
                summary = {"raw": row[5]}
        return {
            "file_hash": row[0],
            "action": row[1],
            "filename": row[2],
            "filesize": row[3],
            "uploaded_at": row[4],
            "summary": summary,
        }
    finally:
        conn.close()

def upsert_upload_history(file_hash: str, action: str, filename: str, filesize: int, summary: dict):
    """처리 결과를 업로드 이력에 기록. (동일 file_hash+action이면 최신으로 갱신)"""
    conn = get_connection()
    try:
        cur = conn.cursor()
        summary_json = json.dumps(summary or {}, ensure_ascii=False)
        # SQLite 3.24+ ON CONFLICT DO UPDATE 지원
        cur.execute(
            """INSERT INTO upload_history (file_hash, action, filename, filesize, uploaded_at, summary_json)
                   VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                   ON CONFLICT(file_hash, action) DO UPDATE SET
                        filename=excluded.filename,
                        filesize=excluded.filesize,
                        uploaded_at=CURRENT_TIMESTAMP,
                        summary_json=excluded.summary_json""",
            (file_hash, action, filename, int(filesize or 0), summary_json),
        )
        conn.commit()
        return True
    except Exception:
        # 환경차로 UPSERT가 실패하면, 최소한 INSERT 시도 후 무시
        try:
            cur = conn.cursor()
            cur.execute(
                """INSERT OR IGNORE INTO upload_history (file_hash, action, filename, filesize, uploaded_at, summary_json)
                       VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)""",
                (file_hash, action, filename, int(filesize or 0), summary_json),
            )
            conn.commit()
            return True
        except Exception:
            return False
    finally:
        conn.close()





# ---------------------------------------------------------
# [데이터(db포함) 오류] Upload Hold Store (hold_store / decision / approval / audit)
# ---------------------------------------------------------
# 목적:
# - 업로드 과정에서 발생하는 모호/충돌 데이터를 즉시 DB 반영하지 않고 '보류'로 저장하여
#   데이터 정합성을 보호한다.
# - 대표님이 사후에 (1) 기존 고객 매핑 (2) 신규 고객 생성 (3) 스킵 을 명시적으로 결정할 수 있도록
#   근거(원본/정규화/정정/후보/결정/감사)를 1:1 매핑 구조로 보존한다.
#
# 특허 포인트(명세서 용어 1:1):
# - hold_store   => upload_holds
# - decision     => hold_decisions
# - approval     => approval_proofs
# - audit        => audit_logs
# ---------------------------------------------------------

def _json_dumps_safe(obj) -> str:
    """JSON 직렬화 안전 래퍼(특허: 입력 다양성에 대한 견고성)."""
    try:
        return json.dumps(obj or {}, ensure_ascii=False)
    except Exception:
        try:
            return json.dumps({"repr": repr(obj)}, ensure_ascii=False)
        except Exception:
            return "{}"


def audit_log(event_type: str, ref_table: str, ref_id: int | None, payload: dict | None = None) -> None:
    """감사 로그 기록(실패해도 앱이 죽지 않도록 방어)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO audit_logs (event_type, ref_table, ref_id, payload_json)
                   VALUES (?, ?, ?, ?)""",
            (event_type, ref_table, int(ref_id) if ref_id is not None else None, _json_dumps_safe(payload)),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def create_customer_direct(
    name: str,
    phone: str,
    birth_date: str = "",
    gender: str = "",
    region: str = "",
    address: str = "",
    email: str = "",
    source: str = "",
    custom_data: dict | None = None,
    memo: str = "",
) -> tuple[bool, str, int | None]:
    """고객을 '무조건 신규'로 생성한다(전화번호 중복 허용).

    [왜 필요한가]
    - upsert_customer_identity는 phone_norm 기준으로 기존 고객을 업데이트(흡수)한다.
    - '동일 전화번호 + 다른 이름'은 정합성 위험이 커서 대표님이 명시적으로 신규 생성할 수 있어야 함.
    - 따라서 'create_new' 결정은 이 함수로만 생성하여 자동 흡수를 차단한다.
    """
    nm = (name or "").strip()
    ph = (phone or "").strip()
    pn = normalize_phone(ph)
    mk = make_match_key(nm, phone_last4(ph))

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO customers (name, phone, phone_norm, birth_date, gender, region, address, email, source,
                                      custom_data, match_key, memo, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (
                nm,
                ph,
                pn,
                (birth_date or "").strip(),
                (gender or "").strip(),
                (region or "").strip(),
                (address or "").strip(),
                (email or "").strip(),
                (source or "").strip(),
                _json_dumps_safe(custom_data or {}),
                mk,
                (memo or "").strip(),
            ),
        )
        cid = int(cur.lastrowid)
        conn.commit()
        audit_log("CUSTOMER_CREATE_DIRECT", "customers", cid, {"name": nm, "phone_norm": pn, "birth_date": birth_date})
        return True, "created", cid
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"오류: {e}", None
    finally:
        conn.close()


def find_customer_candidates(name: str = "", phone: str = "", birth_date: str = "", limit: int = 30):
    """보류 해결용 후보 고객 리스트를 생성한다.

    매칭 신뢰도 순서(특허: 다중 기준 스코어링):
    1) phone_norm 정확 일치
    2) match_key(이름1글자+번호4자리) 일치 후 이름 패턴 검증
    3) name + birth_date 일치(공백 제거/정규화)

    반환: [{id,name,phone,birth_date,score,reason}, ...]
    """
    nm = (name or "").strip()
    ph = (phone or "").strip()
    pn = normalize_phone(ph)
    mk = make_match_key(nm, phone_last4(ph))
    bd = (birth_date or "").strip()

    conn = get_connection()
    try:
        cur = conn.cursor()
        seen = {}

        # 1) phone_norm
        if pn:
            cur.execute(
                """SELECT id, name, phone, birth_date FROM customers
                       WHERE phone_norm = ?
                       ORDER BY id DESC
                       LIMIT ?""",
                (pn, limit),
            )
            for cid, cname, cphone, cbd in cur.fetchall():
                seen[int(cid)] = {"id": int(cid), "name": cname, "phone": cphone, "birth_date": cbd, "score": 100, "reason": "phone"}

        # 2) match_key
        if mk:
            cur.execute(
                """SELECT id, name, phone, birth_date FROM customers
                       WHERE match_key = ?
                       ORDER BY id DESC
                       LIMIT ?""",
                (mk, limit),
            )
            for cid, cname, cphone, cbd in cur.fetchall():
                cid = int(cid)
                # 이름 패턴 검증(마스킹/초성 등)
                try:
                    ok = utils.is_name_match(nm, cname) if nm else True
                except Exception:
                    ok = True
                if not ok:
                    continue
                if cid not in seen:
                    seen[cid] = {"id": cid, "name": cname, "phone": cphone, "birth_date": cbd, "score": 70, "reason": "match_key"}

        # 3) name+birth
        if nm and bd:
            cur.execute(
                """SELECT id, name, phone, birth_date FROM customers
                       WHERE REPLACE(name, ' ', '') = REPLACE(?, ' ', '')
                         AND birth_date = ?
                       ORDER BY id DESC
                       LIMIT ?""",
                (nm, bd, limit),
            )
            for cid, cname, cphone, cbd in cur.fetchall():
                cid = int(cid)
                if cid not in seen:
                    seen[cid] = {"id": cid, "name": cname, "phone": cphone, "birth_date": cbd, "score": 80, "reason": "name_birth"}

        # 정렬
        out = sorted(seen.values(), key=lambda r: (-int(r.get("score", 0)), -int(r.get("id", 0))))
        return out[:limit]
    finally:
        conn.close()


def _reason_code_from_row(r: dict) -> str:
    """분석 row(dict)로부터 보류 사유 코드를 산출."""
    cr = (r.get("customer_reason") or "")
    rr = (r.get("row_reason") or "")
    tr = (r.get("contract_reason") or "")
    msg = f"{cr} {rr} {tr}".strip()

    if "파일 내부" in msg and "연락처" in msg and "서로 다른" in msg:
        return "PHONE_NAME_CONFLICT_FILE"
    if "동일 연락처" in msg and "이름" in msg and "불일치" in msg:
        return "PHONE_NAME_MISMATCH_DB"
    if "동일 연락처 고객이 2명 이상" in msg:
        return "PHONE_DUP_DB"
    if "필수" in msg or "없음" in msg:
        return "REQUIRED_MISSING"
    return "HOLD"


def sync_upload_holds(file_hash: str, filename: str, rows: list[dict]) -> int:
    """분석 결과의 보류 행을 upload_holds로 동기화한다.

    - UNIQUE(file_hash,row_no)로 중복을 방지한다.
    - 이미 RESOLVED 된 항목은 덮어쓰지 않는다(재업로드해도 해결 상태 유지).

    반환: 동기화된(INSERT/UPDATE 시도) 건수
    """
    if not rows:
        return 0

    conn = get_connection()
    try:
        cur = conn.cursor()
        synced = 0
        for r in rows:
            if (r.get("row_status") or "") != "보류":
                continue
            row_no = int(r.get("seq") or r.get("row_no") or 0)
            if row_no <= 0:
                continue

            reason_code = _reason_code_from_row(r)
            reason_msg = (r.get("customer_reason") or r.get("contract_reason") or r.get("row_reason") or "보류").strip()

            name = r.get("name") or ""
            phone = r.get("phone") or ""
            birth_date = r.get("birth_date") or ""
            normalized = {
                "name_norm": (str(name).strip().replace(" ", "")),
                "phone_norm": normalize_phone(phone),
                "birth_date": str(birth_date).strip(),
            }

            candidates = r.get("customer_candidates")
            if not candidates:
                # 파일 내부 충돌/신규 보류인 경우에도 후보를 생성해둔다(사후 UI 편의)
                candidates = find_customer_candidates(name=name, phone=phone, birth_date=birth_date, limit=20)

            # 반영용 최소 payload (원본 보존)
            row_payload = {
                "seq": r.get("seq"),
                "name": name,
                "phone": phone,
                "birth_date": birth_date,
                "gender": r.get("gender"),
                "region": r.get("region"),
                "address": r.get("address"),
                "email": r.get("email"),
                "source": r.get("source"),
                "memo": r.get("memo"),
                "custom_data": r.get("custom_data"),
                "policyholder_name": r.get("policyholder_name"),
                "policyholder_phone": r.get("policyholder_phone"),
                "policyholder_type": r.get("policyholder_type"),
                "policyholder_norm": r.get("policyholder_norm"),
                "primary_role": r.get("primary_role"),
                "financial": r.get("financial") or {},
            }

            # 이미 RESOLVED면 덮어쓰지 않음
            cur.execute(
                """SELECT id, status FROM upload_holds WHERE file_hash = ? AND row_no = ?""",
                (file_hash, row_no),
            )
            ex = cur.fetchone()
            if ex and (ex[1] or "") == "RESOLVED":
                continue

            raw_json = _json_dumps_safe(r)
            normalized_json = _json_dumps_safe(normalized)
            candidates_json = _json_dumps_safe(candidates)
            row_payload_json = _json_dumps_safe(row_payload)

            if not ex:
                cur.execute(
                    """INSERT INTO upload_holds (file_hash, row_no, filename, reason_code, reason_msg, status,
                                                  raw_json, normalized_json, corrected_json, candidates_json, row_payload_json,
                                                  created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, 'OPEN', ?, ?, '', ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
                    (file_hash, row_no, filename, reason_code, reason_msg, raw_json, normalized_json, candidates_json, row_payload_json),
                )
                hold_id = int(cur.lastrowid)
                audit_log("HOLD_CREATE", "upload_holds", hold_id, {"file_hash": file_hash, "row_no": row_no, "reason": reason_code})
                synced += 1
            else:
                hold_id = int(ex[0])
                cur.execute(
                    """UPDATE upload_holds
                          SET filename=?, reason_code=?, reason_msg=?, status=COALESCE(NULLIF(status,''),'OPEN'),
                              raw_json=?, normalized_json=?, candidates_json=?, row_payload_json=?, updated_at=CURRENT_TIMESTAMP
                        WHERE id=?""",
                    (filename, reason_code, reason_msg, raw_json, normalized_json, candidates_json, row_payload_json, hold_id),
                )
                synced += 1

        conn.commit()
        return synced
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        conn.close()


def list_upload_hold_reason_codes() -> list[str]:
    """upload_holds에 존재하는 사유코드 목록을 반환한다.

    [데이터(db포함) 오류] 운영 중 사유코드는 추가/변경될 수 있으므로, UI에서 하드코딩하지 않고
    DB에서 현재 사용 중인 사유코드를 가져와 필터로 제공할 수 있도록 한다.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT DISTINCT reason_code
                   FROM upload_holds
                  WHERE COALESCE(reason_code,'') <> ''
                  ORDER BY reason_code ASC"""
        )
        return [r[0] for r in cur.fetchall() if r and r[0]]
    except Exception:
        return []
    finally:
        conn.close()


def list_upload_hold_batches(limit: int = 50) -> list[dict]:
    """업로드 배치(=file_hash) 단위로 보류 건수를 집계한다.

    [데이터(db포함) 오류] '업로드보류(관리)' 화면에서 특정 업로드(파일)만 골라 보류를 처리할 수 있어야 한다.
    - upload_id: 내부적으로 file_hash를 사용(명세서의 업로드 배치 식별자와 1:1 매핑)
    - open_count: 아직 해결되지 않은 보류(OPEN)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT
                    file_hash AS upload_id,
                    MAX(filename) AS filename,
                    MIN(created_at) AS created_at,
                    SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) AS open_count,
                    SUM(CASE WHEN status='RESOLVED' THEN 1 ELSE 0 END) AS resolved_count,
                    SUM(CASE WHEN status='SKIPPED' THEN 1 ELSE 0 END) AS skipped_count
                FROM upload_holds
                GROUP BY file_hash
                ORDER BY created_at DESC
                LIMIT ?""",
            (int(limit),),
        )
        rows = cur.fetchall() or []
        out = []
        for r in rows:
            out.append(
                {
                    "upload_id": r[0],
                    "filename": r[1],
                    "created_at": r[2],
                    "open_count": int(r[3] or 0),
                    "resolved_count": int(r[4] or 0),
                    "skipped_count": int(r[5] or 0),
                }
            )
        return out
    except Exception:
        return []
    finally:
        conn.close()


def list_upload_holds(
    statuses: list[str] | None = None,
    keyword: str | None = None,
    upload_id: str | None = None,
    reason_codes: list[str] | None = None,
    limit: int = 200,
    offset: int = 0,
):
    """upload_holds 목록 조회(업로드보류 관리용).

    [데이터(db포함) 오류] main.py(업로드보류(관리) 탭)가 기대하는 구조로 '정규화/정정/후보/계약힌트'를 함께 제공한다.

    반환 dict 스키마(주요):
    - id, row_no, status, reason_code, reason_msg
    - normalized: dict
    - corrected: dict
    - candidates: list[dict]
    - contract_hint: dict(보험사/상품/증권/상태 등)
    - display_name/display_phone: UI 표시용(정정 > 정규화 > 원본 순)
    """

    def _loads(s: str, default):
        try:
            return json.loads(s) if s else default
        except Exception:
            return default

    # 상태 기본값
    st_list = [s.upper() for s in (statuses or ["OPEN"]) if s]
    if "ALL" in st_list:
        st_list = []

    kw = (keyword or "").strip()
    upload_id = (upload_id or "").strip() or None
    rcodes = [c for c in (reason_codes or []) if c]

    where = ["1=1"]
    params: list = []

    if st_list:
        where.append(f"status IN ({','.join(['?']*len(st_list))})")
        params.extend(st_list)

    if upload_id:
        # upload_id는 file_hash와 동일 취급
        where.append("file_hash=?")
        params.append(upload_id)

    if rcodes:
        where.append(f"reason_code IN ({','.join(['?']*len(rcodes))})")
        params.extend(rcodes)

    if kw:
        like = f"%{kw}%"
        # JSON 컬럼을 대상으로 단순 LIKE 검색(속도는 느릴 수 있으나 운영 규모(로컬)에서 충분)
        where.append("(row_payload_json LIKE ? OR normalized_json LIKE ? OR corrected_json LIKE ? OR candidates_json LIKE ? OR reason_msg LIKE ?)")
        params.extend([like, like, like, like, like])

    sql = f"""
        SELECT id, file_hash, row_no, filename, reason_code, reason_msg, status,
               normalized_json, corrected_json, candidates_json, row_payload_json,
               created_at, updated_at
          FROM upload_holds
         WHERE {' AND '.join(where)}
         ORDER BY created_at DESC, id DESC
         LIMIT ? OFFSET ?
    """
    params.extend([int(limit), int(offset)])

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall() or []
    except Exception:
        rows = []
    finally:
        conn.close()

    out = []
    for r in rows:
        hid = int(r[0])
        file_hash = r[1]
        row_no = int(r[2])
        filename = r[3]
        reason_code = r[4]
        reason_msg = r[5]
        status = r[6]
        normalized = _loads(r[7] or "", {})
        corrected = _loads(r[8] or "", {})
        candidates = _loads(r[9] or "", [])
        payload = _loads(r[10] or "", {})
        created_at = r[11]
        updated_at = r[12]

        # 계약 힌트(행 payload에 저장해둔 financial 요약을 그대로 사용)
        fin = payload.get("financial") if isinstance(payload.get("financial"), dict) else {}
        contract_hint = {
            "policy_no": fin.get("policy_no"),
            "company": fin.get("company"),
            "product_name": fin.get("product_name"),
            "status": fin.get("status"),
            "start_date": fin.get("start_date"),
            "end_date": fin.get("end_date"),
            "insured_name": fin.get("insured_name"),
            "policyholder_name": fin.get("policyholder_name") or payload.get("policyholder_name"),
        }

        display_name = (corrected.get("name") or normalized.get("name") or payload.get("name") or "").strip() or "-"
        display_phone = (corrected.get("phone") or normalized.get("phone") or payload.get("phone") or "").strip() or "-"

        out.append(
            {
                "id": hid,
                "file_hash": file_hash,
                "upload_id": file_hash,
                "row_no": row_no,
                "filename": filename,
                "reason_code": reason_code,
                "reason_msg": reason_msg,
                "status": status,
                "normalized": normalized,
                "corrected": corrected,
                "candidates": candidates,
                "payload": payload,
                "contract_hint": contract_hint,
                "display_name": display_name,
                "display_phone": display_phone,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )
    return out


def get_upload_hold(hold_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, file_hash, row_no, filename, reason_code, reason_msg, status,
                      raw_json, normalized_json, corrected_json, candidates_json, row_payload_json,
                      created_at, updated_at
                   FROM upload_holds
                  WHERE id=?""",
            (int(hold_id),),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {
            "id": int(r[0]),
            "file_hash": r[1],
            "row_no": int(r[2]),
            "filename": r[3],
            "reason_code": r[4],
            "reason_msg": r[5],
            "status": r[6],
            "raw_json": r[7],
            "normalized_json": r[8],
            "corrected_json": r[9],
            "candidates_json": r[10],
            "row_payload_json": r[11],
            "created_at": r[12],
            "updated_at": r[13],
        }
    finally:
        conn.close()


def get_upload_hold_by_file_row(file_hash: str, row_no: int):
    """(file_hash, row_no) 기준으로 보류(hold_store) 1건을 조회한다.

    [데이터(db포함) 오류]
    - 업로드 반영(apply_import) 단계에서, 보류건에 대해 '결정/승인/감사' 로그를 남기려면
      해당 행의 hold_id가 필요하다.
    - smart_import.py는 (file_hash, seq=row_no)로 보류를 추적하므로, 동일 키로 조회하는
      보조 함수를 제공한다.

    반환 형식:
    - get_upload_hold()와 동일한 dict 구조(주요 키: id, file_hash, row_no, status ...)를 반환한다.
    - 없으면 None 반환.
    """
    if not file_hash or not row_no:
        return None

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, file_hash, row_no, filename, reason_code, reason_msg, status,
                      raw_json, normalized_json, corrected_json, candidates_json, row_payload_json,
                      created_at, updated_at
                   FROM upload_holds
                  WHERE file_hash=? AND row_no=?""",
            (str(file_hash), int(row_no)),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {
            "id": int(r[0]),
            "file_hash": r[1],
            "row_no": int(r[2]),
            "filename": r[3],
            "reason_code": r[4],
            "reason_msg": r[5],
            "status": r[6],
            "raw_json": r[7],
            "normalized_json": r[8],
            "corrected_json": r[9],
            "candidates_json": r[10],
            "row_payload_json": r[11],
            "created_at": r[12],
            "updated_at": r[13],
        }
    finally:
        conn.close()



def update_upload_hold_corrected(
    hold_id: int,
    corrected: dict | None = None,
    *,
    name: str | None = None,
    phone: str | None = None,
    birth_date: str | None = None,
    decided_by: str = "",
):
    """보류 항목의 정정값(corrected) 저장 + 후보 재탐색.

    [데이터(db포함) 오류] '업로드보류(관리)' 화면에서 대표님이 이름/전화/생년월일을 수정하면
    그 즉시 후보 고객(candidates)을 다시 계산해 보여줘야 한다.

    main.py 호환:
    - update_upload_hold_corrected(hid, name=..., phone=..., birth_date=...) 형태를 지원한다.
    - 반환: (ok:bool, msg:str, extra:dict|None)
    """
    # corrected 딕셔너리 구성(직접 전달된 dict 우선, 키워드 인자는 덮어쓰기)
    corr = corrected.copy() if isinstance(corrected, dict) else {}
    if name is not None:
        corr["name"] = name
    if phone is not None:
        corr["phone"] = phone
    if birth_date is not None:
        corr["birth_date"] = birth_date

    corr_name = (corr.get("name") or "").strip()
    corr_phone = normalize_phone(corr.get("phone") or "")
    corr_birth = normalize_birth(corr.get("birth_date") or "")

    # 후보 재탐색(정정값 기반)
    candidates = find_customer_candidates(corr_name, corr_phone, corr_birth, limit=30)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """UPDATE upload_holds
                   SET corrected_json=?,
                       candidates_json=?,
                       updated_at=CURRENT_TIMESTAMP
                 WHERE id=?""",
            (_json_dumps_safe({"name": corr_name, "phone": corr_phone, "birth_date": corr_birth}),
             _json_dumps_safe(candidates),
             int(hold_id)),
        )
        conn.commit()
        audit_log(
            "HOLD_CORRECTED_UPDATED",
            "upload_holds",
            int(hold_id),
            {"corrected": {"name": corr_name, "phone": corr_phone, "birth_date": corr_birth}, "cand_count": len(candidates)},
            decided_by,
        )
        return True, "정정 저장 완료", {"candidates": candidates}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"정정 저장 실패: {e}", None
    finally:
        conn.close()


def set_upload_hold_status_by_file_row(file_hash: str, row_no: int, status: str) -> None:
    """(file_hash,row_no)로 보류 상태를 변경한다."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """UPDATE upload_holds
                  SET status=?, updated_at=CURRENT_TIMESTAMP
                WHERE file_hash=? AND row_no=?""",
            ((status or "OPEN").upper(), file_hash, int(row_no)),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def resolve_upload_hold_by_file_row(file_hash: str, row_no: int) -> None:
    set_upload_hold_status_by_file_row(file_hash, row_no, "RESOLVED")

def set_upload_hold_status(hold_id: int, status: str, *, decided_by: str = ""):
    """보류 항목 1건의 상태를 변경한다.

    [데이터(db포함) 오류] UI에서 '해결됨 표시'를 누르면 해당 보류는 RESOLVED로 전환되고,
    목록에서 자동 제거(필터 OPEN 기준)된다.

    반환: (ok:bool, msg:str, extra:dict|None)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """UPDATE upload_holds
                   SET status=?,
                       updated_at=CURRENT_TIMESTAMP
                 WHERE id=?""",
            (str(status).upper(), int(hold_id)),
        )
        conn.commit()
        audit_log(
            "HOLD_STATUS_UPDATED",
            "upload_holds",
            int(hold_id),
            {"status": str(status).upper()},
            decided_by,
        )
        return True, "상태 변경 완료", get_upload_hold(int(hold_id))
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"상태 변경 실패: {e}", None
    finally:
        conn.close()



def insert_hold_decision(hold_id: int, decision: str, target_customer_id: int | None = None, decision_json: dict | None = None, decided_by: str = "") -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO hold_decisions (hold_id, decision, target_customer_id, decision_json, decided_by)
                   VALUES (?, ?, ?, ?, ?)""",
            (int(hold_id), (decision or "").upper(), int(target_customer_id) if target_customer_id else None, _json_dumps_safe(decision_json), (decided_by or "").strip()),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def insert_approval_proof(hold_id: int, approval: str, approval_json: dict | None = None, approved_by: str = "") -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO approval_proofs (hold_id, approval, approval_json, approved_by)
                   VALUES (?, ?, ?, ?)""",
            (int(hold_id), (approval or "AUTO").upper(), _json_dumps_safe(approval_json), (approved_by or "").strip()),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


def apply_upload_hold_decision(
    hold_id: int,
    decision: str,
    target_customer_id: int | None = None,
    corrected: dict | None = None,
    decided_by: str = "",
):
    """업로드보류(관리)에서 선택한 결정을 실제 DB에 반영한다.

    [데이터(db포함) 오류] 보류 처리의 핵심은 'hold_store → decision → approval → audit' 흐름이다.
    - hold_store: upload_holds에 원본행 + 정규화/정정/후보를 보관
    - decision: 대표님이 선택(MAP_EXISTING / CREATE_NEW / SKIP)
    - approval: 승인 증적(approval_proofs) 저장
    - audit: audit_log 기록(사후 검증/특허 방어력)

    main.py 호환:
    - 반환: (ok:bool, msg:str, extra:dict|None)
    """
    hold = get_upload_hold(int(hold_id))
    if not hold:
        return False, "보류 항목을 찾을 수 없음", None

    dec = (decision or "").upper().strip()

    # 정정 저장(선택) - UI에서 입력한 정정값을 hold_store에 즉시 반영
    if corrected is not None:
        update_upload_hold_corrected(int(hold_id), corrected, decided_by=decided_by)
        hold = get_upload_hold(int(hold_id)) or hold

    # SKIP(보류 유지 또는 스킵 처리)
    if dec == "SKIP":
        set_upload_hold_status_by_file_row(hold["file_hash"], hold["row_no"], "SKIPPED")
        insert_hold_decision(int(hold_id), dec, None, {"note": "skipped"}, decided_by)
        insert_approval_proof(int(hold_id), "APPROVED", {"decision": dec}, decided_by)
        audit_log("HOLD_SKIP", "upload_holds", int(hold_id), {"by": decided_by})
        return True, "스킵 처리됨(SKIPPED)", {"decision": dec}

    # payload
    try:
        payload = json.loads(hold.get("row_payload_json") or "{}")
    except Exception:
        payload = {}

    # effective customer fields
    try:
        corr = json.loads(hold.get("corrected_json") or "{}")
    except Exception:
        corr = {}
    if corrected is not None:
        # 바로 전달된 정정값을 우선
        corr = corrected or corr

    eff_name = (corr.get("name") or payload.get("name") or "").strip()
    eff_phone = (corr.get("phone") or payload.get("phone") or "").strip()
    eff_birth = (corr.get("birth_date") or payload.get("birth_date") or "").strip()

    # 결정에 따른 customer_id
    cid = None
    if dec == "MAP_EXISTING":
        if not target_customer_id:
            return False, "기존 고객 매핑을 위해 대상 고객을 선택해야 함", {"decision": dec}
        cid = int(target_customer_id)
    elif dec == "CREATE_NEW":
        ok, msg, cid = create_customer_direct(
            eff_name,
            eff_phone,
            birth_date=eff_birth,
            gender=payload.get("gender") or "",
            region=payload.get("region") or "",
            address=payload.get("address") or "",
            email=payload.get("email") or "",
            source=payload.get("source") or "upload_hold",
            custom_data=payload.get("custom_data") if isinstance(payload.get("custom_data"), dict) else None,
            memo=payload.get("memo") or "",
        )
        if not ok or not cid:
            return False, f"고객 신규 생성 실패: {msg}", {"decision": dec, "name": eff_name, "phone": eff_phone}
    else:
        return False, "알 수 없는 결정", {"decision": dec}

    fin = payload.get("financial") or {}

    # 계약 반영
    c_ok, c_action = add_contract(
        customer_id=cid,
        company=fin.get("company"),
        product_name=fin.get("product_name"),
        policy_no=fin.get("policy_no"),
        premium=fin.get("premium"),
        status=fin.get("status"),
        start_date=fin.get("start_date"),
        end_date=fin.get("end_date"),
        coverage_summary=fin.get("coverage_summary"),
        insured_name=fin.get("insured_name"),
        insured_phone=fin.get("insured_phone"),
        insured_birth=fin.get("insured_birth"),
        insured_gender=fin.get("insured_gender"),
        insured_ssn=fin.get("insured_ssn"),
        policyholder_name=payload.get("policyholder_name"),
        policyholder_phone=payload.get("policyholder_phone"),
        policyholder_type=payload.get("policyholder_type"),
        policyholder_norm=payload.get("policyholder_norm"),
        primary_role=payload.get("primary_role"),
    )

    # decision 기록(증적용)
    insert_hold_decision(int(hold_id), dec, cid, {"contract_action": c_action}, decided_by)
    insert_approval_proof(int(hold_id), "APPROVED", {"decision": dec, "customer_id": cid, "contract_action": c_action}, decided_by)

    if not c_ok or c_action in ("ambig", "fail"):
        # 계약 반영이 실패/모호면 hold를 OPEN으로 유지(대표님 재확인 필요)
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """UPDATE upload_holds
                      SET reason_msg=?, status='OPEN', updated_at=CURRENT_TIMESTAMP
                    WHERE id=?""",
                (f"{hold.get('reason_msg') or ''} | 계약반영:{c_action}", int(hold_id)),
            )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn.close()
        audit_log(
            "HOLD_DECISION_APPLIED_BUT_CONTRACT_NOT_OK",
            "upload_holds",
            int(hold_id),
            {"decision": dec, "customer_id": cid, "contract_action": c_action},
        )
        return False, f"계약 반영이 완료되지 않음: {c_action}", {"decision": dec, "customer_id": cid, "contract_action": c_action}

    # 성공 -> RESOLVED
    set_upload_hold_status_by_file_row(hold["file_hash"], hold["row_no"], "RESOLVED")
    audit_log("HOLD_RESOLVED", "upload_holds", int(hold_id), {"decision": dec, "customer_id": cid, "contract_action": c_action})
    return True, f"반영 완료: customer_id={cid}, contract={c_action}", {"decision": dec, "customer_id": cid, "contract_action": c_action}


# [데이터(db포함) 오류] 계약자/피보험자 분기 + 법인 계약자 검색 지원(명세서 반영)
def get_customer_contracts(customer_id):
    """고객(상담 주체) 기준 계약 조회.

    [표시 규칙(대표님 최종 합의)]
    1) 계약자 = 피보험자 → 피보험자만 표시
    2) 계약자 ≠ 피보험자 → 계약자만 표시(개인/법인 동일)

    구현 방식(=UI 변경 최소화):
    - main.py는 기존처럼 r.get('insured_name')만 출력한다.
    - 조회 단계에서 insured_name을 '표시용 이름(display_party)'으로 치환해 반환한다.
    """
    conn = get_connection()
    sql = """
        SELECT
            id, customer_id,
            company, product_name, policy_no, policy_no_norm,
            premium, status, start_date, end_date, coverage_summary,
            insured_phone, insured_birth, insured_gender,
            policyholder_name, policyholder_type, policyholder_norm, policyholder_phone, primary_role,
            CASE
                WHEN COALESCE(policyholder_name,'') <> ''
                 AND COALESCE(insured_name,'') <> ''
                 AND REPLACE(policyholder_name,' ','') <> REPLACE(insured_name,' ','')
                THEN policyholder_name
                ELSE insured_name
            END AS insured_name,
            CASE
                WHEN COALESCE(policyholder_name,'') <> ''
                 AND COALESCE(insured_name,'') <> ''
                 AND REPLACE(policyholder_name,' ','') <> REPLACE(insured_name,' ','')
                THEN '계'
                ELSE '피'
            END AS display_party_label
        FROM contracts
        WHERE customer_id = ?
        ORDER BY start_date DESC, id DESC
    """
    try:
        return pd.read_sql(sql, conn, params=(customer_id,))
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()




def search_corporate_contracts(policyholder_query: str, *, limit: int = 200) -> pd.DataFrame:
    """법인 계약자 기준 전체 계약 조회(검색용).
    - UI: '법인 계약자 검색' 입력창에서 사용
    - 정책: policyholder_type='CORP' AND policyholder_norm LIKE %query%
    - 반환 DataFrame은 get_customer_contracts와 동일한 컬럼 스키마(insured_name=표시용 이름 치환)를 유지
    """
    q = (policyholder_query or "").strip()
    if not q:
        return pd.DataFrame()
    qn = _norm_org_name(q)
    # 정규화가 너무 공격적일 수 있으므로, norm이 비면 원문 기반으로도 한번 더 조회 가능하도록 한다.
    like_val = f"%{qn or q.replace(' ', '')}%"

    conn = get_connection()
    sql = """
        SELECT
            id, customer_id,
            company, product_name, policy_no, policy_no_norm,
            premium, status, start_date, end_date, coverage_summary,
            insured_phone, insured_birth, insured_gender,
                        insured_name AS insured_name_raw,
policyholder_name, policyholder_type, policyholder_norm, policyholder_phone, primary_role,
            -- 표시 규칙 동일 적용(2/3은 계약자만, 1은 피보험자만)
            CASE
                WHEN COALESCE(policyholder_name,'') <> ''
                 AND COALESCE(insured_name,'') <> ''
                 AND REPLACE(policyholder_name,' ','') <> REPLACE(insured_name,' ','')
                THEN policyholder_name
                ELSE insured_name
            END AS insured_name,
            CASE
                WHEN COALESCE(policyholder_name,'') <> ''
                 AND COALESCE(insured_name,'') <> ''
                 AND REPLACE(policyholder_name,' ','') <> REPLACE(insured_name,' ','')
                THEN '계'
                ELSE '피'
            END AS display_party_label
        FROM contracts
        WHERE COALESCE(policyholder_type,'') = 'CORP'
          AND COALESCE(policyholder_norm,'') LIKE ?
        ORDER BY policyholder_norm ASC, start_date DESC, id DESC
        LIMIT ?
    """
    try:
        return pd.read_sql(sql, conn, params=(like_val, int(limit)))
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def add_task(customer_id, type, due_date):
    conn = get_connection()
    try: conn.execute("INSERT INTO tasks (customer_id, type, due_date) VALUES (?, ?, ?)", (customer_id, type, due_date)); conn.commit(); return True
    except: return False
    finally: conn.close()

def complete_task(tid: int, *, sync_gcal: bool = True) -> bool:
    """Task 완료 처리 + (선택) 구글 캘린더 이벤트도 같이 처리
    - utils.load_app_config()의 gcal_done_action에 따라:
        * 'prefix' : 제목 앞에 ✅ 붙이기
        * 'delete' : 구글 캘린더 이벤트 삭제
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, type, due_date, gcal_event_id, gcal_calendar_id FROM tasks WHERE id=?", (int(tid),))
        row = cur.fetchone()
        if not row:
            return False

        task_id, title, due_date, event_id, cal_id = row
        cur.execute("UPDATE tasks SET status='완료' WHERE id=?", (int(task_id),))
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()

    # --- 구글 캘린더 동기화 (DB 저장과 분리: 실패해도 '완료'는 유지) ---
    if not sync_gcal:
        return True

    cfg = {}
    try:
        cfg = utils.load_app_config()
    except Exception:
        cfg = {}

    if not cfg.get("gcal_enabled", False):
        return True

    if not event_id:
        # 이벤트 ID가 없으면(예전 데이터 등) 여기서는 건드리지 않음
        _set_task_gcal_sync(task_id, sync_status="NO_EVENT_ID")
        return True

    cal_id = (cal_id or cfg.get("gcal_calendar_id") or "primary")
    done_action = (cfg.get("gcal_done_action") or "prefix").strip().lower()
    tz = cfg.get("gcal_timezone") or "Asia/Seoul"

    try:
        import gcal_sync
        ok = False
        if done_action == "delete":
            ok = gcal_sync.delete_event(calendar_id=cal_id, event_id=str(event_id))
            _set_task_gcal_sync(task_id, sync_status=("DONE_DELETED" if ok else "DONE_DELETE_FAIL"))
        else:
            ok = gcal_sync.mark_event_done(calendar_id=cal_id, event_id=str(event_id))
            _set_task_gcal_sync(task_id, sync_status=("DONE_PREFIXED" if ok else "DONE_PREFIX_FAIL"))
        return True
    except Exception:
        _set_task_gcal_sync(task_id, sync_status="DONE_SYNC_EXCEPTION")
        return True


def _set_task_gcal_info(task_id: int, *, calendar_id: str | None, event_id: str | None, html_link: str | None, sync_status: str) -> None:
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE tasks SET gcal_calendar_id=?, gcal_event_id=?, gcal_html_link=?, gcal_sync_status=?, gcal_last_sync=? WHERE id=?",
            (calendar_id, event_id, html_link, sync_status, datetime.now().isoformat(sep=' '), int(task_id)),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def _set_task_gcal_sync(task_id: int, *, sync_status: str) -> None:
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE tasks SET gcal_sync_status=?, gcal_last_sync=? WHERE id=?",
            (sync_status, datetime.now().isoformat(sep=' '), int(task_id)),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def get_dashboard_todos(
    days_task_lookahead: int = 7,
    days_renewal_lookahead: int = 30,
    *,
    # ✅ v1.6 패치 호환: main.py에서 days_lookahead / include_overdue 키워드로 호출해도 동작
    days_lookahead: int | None = None,
    include_overdue: bool = True,
):
    """대시보드용 할일/갱신 알림
    - 할일(tasks): 미완료 + (include_overdue=True면 연체 포함)
    - 갱신(contracts): 정상 계약 중 만기일 알림

    호환 규칙:
    - days_lookahead가 들어오면(메인에서 D-7 기준 사용) tasks/renewal 모두 동일 값으로 override
    - 기존 코드(포지션 인자)도 그대로 동작
    반환 DF 컬럼: customer_id, name, type, date, source, msg
    """
    conn = get_connection()
    todos = []
    now = datetime.now()

    # ✅ days_lookahead(키워드) 우선 적용
    if days_lookahead is not None:
        try:
            d = int(days_lookahead)
            days_task_lookahead = d
            days_renewal_lookahead = d
        except Exception:
            pass
    # tasks 상한(오늘+N일 23:59)
    upper_task = (now + timedelta(days=int(days_task_lookahead))).replace(hour=23, minute=59, second=0, microsecond=0)
    upper_task_s = upper_task.strftime("%Y-%m-%d %H:%M")
    today_s = now.strftime("%Y-%m-%d")

    try:
        cur = conn.cursor()

        # 1) 다음 일정(미완료) : 연체 포함, 상한일까지
        # 1) 다음 일정(미완료)
        #  - include_overdue=True : 과거(연체) + 미래(상한일까지)
        #  - include_overdue=False: 오늘~상한일까지(미래만)
        if include_overdue:
            cur.execute(
                """
                SELECT t.id, t.customer_id, c.name, t.type, t.due_date
                  FROM tasks t
                  JOIN customers c ON t.customer_id = c.id
                 WHERE t.status = '미완료'
                   AND COALESCE(t.due_date,'') <> ''
                   AND datetime(t.due_date) <= datetime(?)
                 ORDER BY datetime(t.due_date) ASC
                """,
                (upper_task_s,),
            )
        else:
            cur.execute(
                """
                SELECT t.id, t.customer_id, c.name, t.type, t.due_date
                  FROM tasks t
                  JOIN customers c ON t.customer_id = c.id
                 WHERE t.status = '미완료'
                   AND COALESCE(t.due_date,'') <> ''
                   AND date(t.due_date) >= date(?)
                   AND datetime(t.due_date) <= datetime(?)
                 ORDER BY datetime(t.due_date) ASC
                """,
                (today_s, upper_task_s),
            )
        for r in cur.fetchall():
            todos.append({
                "id": r[0],
                "customer_id": r[1],
                "name": r[2],
                "type": r[3],
                "date": r[4],
                "source": "task",
                "msg": f"{r[2]} {r[3]}",
            })

        # 2) 갱신 알림(정상 계약, 만기일)
        upper_renew = (now + timedelta(days=int(days_renewal_lookahead))).date().isoformat()
        cur.execute(
            """
            SELECT con.id, con.customer_id, c.name, con.product_name, con.end_date
              FROM contracts con
              JOIN customers c ON con.customer_id = c.id
             WHERE con.status = '정상'
               AND COALESCE(con.end_date,'') <> ''
               AND con.end_date BETWEEN ? AND ?
             ORDER BY con.end_date ASC
            """,
            (today_s, upper_renew),
        )
        for r in cur.fetchall():
            todos.append({
                "id": r[0],
                "customer_id": r[1],
                "name": r[2],
                "type": "갱신",
                "date": r[4],
                "source": "renewal",
                "msg": f"{r[2]} {r[3]} 만기",
            })

    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

    return pd.DataFrame(todos).sort_values("date") if todos else pd.DataFrame()

def get_all_customers():
    """전체 고객 목록을 반환합니다.

    - customers 테이블 기본 컬럼 + 고객별 상담(consultations) 건수(consult_count)를 포함합니다.
    - consult_count는 상담 이력이 없는 고객도 0으로 반환됩니다.
    """
    conn = get_connection()
    try:
        q = """
            SELECT
                c.*,
                COALESCE(x.consult_count, 0) AS consult_count
            FROM customers c
            LEFT JOIN (
                SELECT customer_id, COUNT(*) AS consult_count
                FROM consultations
                GROUP BY customer_id
            ) x
            ON c.id = x.customer_id
            ORDER BY c.created_at DESC
        """
        return pd.read_sql(q, conn)
    except:
        return pd.DataFrame()
    finally:
        conn.close()


def add_interaction_log(cid, type, content, date):
    conn = get_connection()
    try:
        conn.execute("INSERT INTO consultations (customer_id, consult_type, content, consult_date) VALUES (?,?,?,?)", (cid, type, content, date))
        conn.execute("UPDATE customers SET last_contact=? WHERE id=?", (date, cid)) 
        conn.commit(); return True
    except: return False
    finally: conn.close()

def get_customer_logs(cid):
    conn = get_connection()
    try: return pd.read_sql("SELECT id, consult_date as '날짜', consult_type as '방법', content as '내용' FROM consultations WHERE customer_id=? ORDER BY consult_date DESC", conn, params=(cid,))
    except: return pd.DataFrame()
    finally: conn.close()



def delete_consultations(ids):
    """선택한 상담(consultations) 기록을 id 기준으로 삭제

    Args:
        ids: consultations.id 리스트

    Returns:
        bool: 성공 여부
    """
    if not ids:
        return True
    conn = get_connection()
    try:
        ids = [int(x) for x in ids if str(x).strip() != '']
        if not ids:
            return True
        qmarks = ','.join(['?'] * len(ids))
        cur = conn.cursor()
        cur.execute(f"DELETE FROM consultations WHERE id IN ({qmarks})", ids)
        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()

def get_recent_activities(limit=5):
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT c.name, l.consult_date as date, l.consult_type as type, l.content FROM consultations l JOIN customers c ON l.customer_id=c.id ORDER BY l.consult_date DESC LIMIT ?", conn, params=(limit,))
        df.columns = ['name', 'date', 'type', 'content']; return df
    except: return pd.DataFrame()
    finally: conn.close()

def get_monthly_consultation_count():
    conn = get_connection()
    try:
        m = datetime.now().strftime('%Y-%m')
        cur = conn.cursor(); cur.execute("SELECT COUNT(*) FROM consultations WHERE strftime('%Y-%m', consult_date)=?", (m,))
        return cur.fetchone()[0]
    except: return 0
    finally: conn.close()

def delete_customer(cid):
    conn = get_connection()
    try:
        for t in ["contracts", "consultations", "tasks", "customers"]:
            col = 'id' if t=='customers' else 'customer_id'
            conn.execute(f"DELETE FROM {t} WHERE {col}=?", (cid,))
        conn.commit()
    except: pass
    finally: conn.close()

# ---------------------------------------------------------
# [Auto-added] Dashboard helpers (anniversaries / tasks txn)
# ---------------------------------------------------------

def get_contract_brief_map(customer_ids: list[int]):
    """고객별 대표 계약(보험사/증권번호) 1개를 뽑아오는 간단 맵
    반환: {customer_id: (company, policy_no)}
    """
    if not customer_ids:
        return {}

    conn = get_connection()
    try:
        # IN 절 파라미터
        ph = ",".join(["?"] * len(customer_ids))
        sql = f"""
            SELECT customer_id,
                   COALESCE(company,'') AS company,
                   COALESCE(policy_no,'') AS policy_no,
                   COALESCE(start_date,'') AS start_date,
                   id
              FROM contracts
             WHERE customer_id IN ({ph})
               AND COALESCE(company,'') <> ''
             ORDER BY customer_id ASC, start_date DESC, id DESC
        """
        cur = conn.cursor()
        cur.execute(sql, tuple(customer_ids))
        rows = cur.fetchall()

        out = {}
        for cid, comp, pol, _, _id in rows:
            cid = int(cid)
            if cid not in out:
                out[cid] = (comp, pol)
        return out
    except Exception:
        return {}
    finally:
        conn.close()


def get_upcoming_policy_anniversaries(days_ahead: int = 7):
    """청약(계약) 기념일(개시일 기준) D-day 리스트
    반환: list[dict] (대시보드에서 사용)
    dict keys:
      customer_id, name, company, policy_no, start_date, next_anniv, years, d_day
    """
    conn = get_connection()
    try:
        df = pd.read_sql(
            """
            SELECT con.customer_id,
                   c.name,
                   COALESCE(con.company,'') AS company,
                   COALESCE(con.policy_no,'') AS policy_no,
                   COALESCE(con.start_date,'') AS start_date,
                   COALESCE(con.status,'') AS status
              FROM contracts con
              JOIN customers c ON con.customer_id = c.id
             WHERE COALESCE(con.start_date,'') <> ''
            """,
            conn,
        )
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if df.empty:
        return []

    now = datetime.now().date()
    out = []
    for _, r in df.iterrows():
        start_raw = str(r.get("start_date") or "").strip()
        if not start_raw:
            continue

        try:
            sd = pd.to_datetime(start_raw, errors="coerce")
            if pd.isna(sd):
                continue
            sd = sd.date()
        except Exception:
            continue

        # 다음 기념일: 올해 mm/dd, 지났으면 내년
        mm = sd.month
        dd = sd.day
        try:
            cand = datetime(now.year, mm, dd).date()
        except ValueError:
            # 2/29 같은 케이스 → 2/28로 보정
            cand = datetime(now.year, mm, 28).date()

        if cand < now:
            try:
                cand = datetime(now.year + 1, mm, dd).date()
            except ValueError:
                cand = datetime(now.year + 1, mm, 28).date()

        d_day = (cand - now).days
        if d_day < 0 or d_day > int(days_ahead):
            continue

        years = cand.year - sd.year
        out.append({
            "customer_id": int(r.get("customer_id") or 0),
            "name": str(r.get("name") or ""),
            "company": str(r.get("company") or ""),
            "policy_no": str(r.get("policy_no") or ""),
            "start_date": sd.isoformat(),
            "next_anniv": cand.isoformat(),
            "years": int(years),
            "d_day": int(d_day),
        })

    return out


def add_consultation_with_optional_task(*, customer_id: int, consult_type: str, content: str, consult_date: str,
                                       task_title: str | None = None, task_due: str | None = None) -> bool:
    """상담이력 + (선택)다음일정 한 번에 저장(트랜잭션)
    - task_due: 'YYYY-MM-DD HH:MM' 또는 'YYYY-MM-DD' 문자열 권장
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO consultations (customer_id, consult_type, content, consult_date) VALUES (?,?,?,?)",
            (int(customer_id), consult_type, content, consult_date),
        )
        cur.execute("UPDATE customers SET last_contact=? WHERE id=?", (consult_date, int(customer_id)))

        if task_title and task_due:
            cur.execute(
                "INSERT INTO tasks (customer_id, type, due_date) VALUES (?,?,?)",
                (int(customer_id), task_title, task_due),
            )

        conn.commit()
        return True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()


def add_consultation_with_optional_task_v2(*, customer_id: int, consult_type: str, content: str, consult_date: str,
                                          task_title: str | None = None, task_due: str | None = None) -> dict:
    """상담이력 + (선택)다음일정 저장 + (선택)구글 캘린더 이벤트 생성

    return:
      {"ok": bool, "task_id": int|None, "gcal_ok": bool|None, "gcal_event_id": str|None}
    """
    conn = get_connection()
    task_id = None
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO consultations (customer_id, consult_type, content, consult_date) VALUES (?,?,?,?)",
            (int(customer_id), consult_type, content, consult_date),
        )
        cur.execute("UPDATE customers SET last_contact=? WHERE id=?", (consult_date, int(customer_id)))

        if task_title and task_due:
            cur.execute(
                "INSERT INTO tasks (customer_id, type, due_date) VALUES (?,?,?)",
                (int(customer_id), task_title, task_due),
            )
            task_id = int(cur.lastrowid)

        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"ok": False, "task_id": None, "gcal_ok": None, "gcal_event_id": None}
    finally:
        conn.close()

    # 다음 일정이 없으면 여기서 종료
    if not task_id:
        return {"ok": True, "task_id": None, "gcal_ok": None, "gcal_event_id": None}

    # --- 구글 캘린더 연동(설정 ON + 인증 완료일 때만) ---
    try:
        cfg = utils.load_app_config()
    except Exception:
        cfg = {}

    if not cfg.get("gcal_enabled", False):
        _set_task_gcal_sync(task_id, sync_status="DISABLED")
        return {"ok": True, "task_id": task_id, "gcal_ok": None, "gcal_event_id": None}

    cal_id = cfg.get("gcal_calendar_id") or "primary"
    tz = cfg.get("gcal_timezone") or "Asia/Seoul"

    try:
        import gcal_sync
        start_dt, end_dt = gcal_sync.parse_due_datetime(str(task_due), tz=tz)
        # 설명에 상담 타입/날짜를 붙여두면 검색/관리 편함
        desc = f"[KFIT 상담일지] {consult_type} / {consult_date}\n\n{content[:800]}"
        ev_id, html_link = gcal_sync.create_event(
            calendar_id=str(cal_id),
            summary=str(task_title),
            start_dt=start_dt,
            end_dt=end_dt,
            description=desc,
            timezone=str(tz),
            interactive=False,
        )
        _set_task_gcal_info(task_id, calendar_id=str(cal_id), event_id=str(ev_id), html_link=str(html_link), sync_status="CREATED")
        return {"ok": True, "task_id": task_id, "gcal_ok": True, "gcal_event_id": str(ev_id)}
    except Exception:
        # OAuth 미완료/라이브러리 미설치 등
        _set_task_gcal_sync(task_id, sync_status="CREATE_FAIL")
        return {"ok": True, "task_id": task_id, "gcal_ok": False, "gcal_event_id": None}



def get_open_tasks(customer_id: int):
    """고객의 미완료 다음 일정 목록"""
    conn = get_connection()
    try:
        return pd.read_sql(
            "SELECT id, type, status, due_date, gcal_event_id, gcal_html_link, gcal_calendar_id, gcal_sync_status FROM tasks WHERE customer_id=? AND status='미완료' ORDER BY due_date ASC",
            conn,
            params=(int(customer_id),),
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

# [queries.py] 추가 및 수정 부분
def get_customer_detail(cid: int):
    """[추가] 특정 고객의 상세 정보 조회 (수정 팝업용)"""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row # 딕셔너리 형태로 접근 가능하게 설정
        cur = conn.cursor()
        cur.execute("SELECT * FROM customers WHERE id = ?", (cid,))
        row = cur.fetchone()
        return dict(row) if row else None
    except:
        return None
    finally:
        conn.close()

# [queries.py] 내부 update_customer_direct 함수 수정

def update_customer_direct(cid: int, name, phone, birth_date, gender, region, email, memo):
    """[추가] 고객 정보 직접 수정 (ID 기준) - 저장 시 공백 제거"""
    conn = get_connection()
    try:
        # 1. [핵심] 저장 전 이름 공백 제거
        clean_name = str(name).replace(" ", "").strip()
        
        phone_norm = normalize_phone(phone)
        # match_key 갱신
        match_key = make_match_key(clean_name, phone_last4(phone_norm))
        
        cur = conn.cursor()
        cur.execute("""
            UPDATE customers 
            SET name=?, phone=?, phone_norm=?, birth_date=?, gender=?, 
                region=?, email=?, memo=?, match_key=?
            WHERE id=?
        """, (clean_name, phone, phone_norm, birth_date, gender, region, email, memo, match_key, cid))
        conn.commit()
        return True, "수정되었습니다."
    except Exception as e:
        conn.rollback()
        return False, f"수정 실패: {str(e)}"
    finally:
        conn.close()
        

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
# ---- Connection Rebind Layer ----------------------------------------
# KFIT_QUERIES_CONN_REBIND
# 문제 배경:
# - 본 모듈은 `from database import get_connection` 형태로 연결 팩토리를 가져옵니다.
# - Python의 from-import는 "참조 복사"이므로, database.get_connection이 이후에 강화되어도
#   본 모듈은 기존 참조를 유지할 수 있습니다.
#
# 해결:
# - 본 구간에서 get_connection 이름을 다시 바인딩하여, 항상 database.get_connection(강화본)을
#   사용하도록 합니다. (기존 함수 정의부는 수정하지 않음 → UI/로직 영향 최소)
#
# 특허 포인트(명세서 기재용):
# - 데이터 파이프라인에서 "연결 생성 규칙"을 런타임 재바인딩으로 통일하여,
#   전 모듈의 트랜잭션 경합/잠금 오류를 체계적으로 감소시키는 '중앙 통제형 연결 정책'을 제공.
# ----------------------------------------------------------------------
import database as _kfit_db  # 로컬 모듈 (순환 참조 회피: 함수 재바인딩 용도)

def get_connection():  # noqa: F811  (의도적 재정의)
    """Always delegate to the hardened connection factory."""
    return _kfit_db.get_connection()
# [체크리스트]
# - UI 유지/존치: ✅ 유지됨 (Queries/API 확장)
# - 신규: hold_store/decision/approval/audit CRUD + 후보 추천 + create_customer_direct(중복 허용)
# - 업로드보류(관리) UI 지원: ✅ list_upload_holds / list_upload_hold_batches / list_upload_hold_reason_codes
# - 수정 범위: ✅ [데이터 정합성 보호 + 업로드보류 워크플로]
# - '..., 중략, 일부 생략' 금지: ✅ 준수(전체 파일 유지)
# - 수정 전 라인수: 2376
# - 수정 후 라인수: 2427 (+51)
# ---------------------------------------------------------
