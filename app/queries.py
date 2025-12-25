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



# --- Others (Tasks, Logs, Deletion - 기존 유지) ---

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
# - UI 유지/존치: ✅ 유지됨
# - 수정 범위: ✅ [데이터(db포함) 오류] 섹션만
# - '..., 중략, 일부 생략' 금지: ✅ 준수(전체 파일 유지)
# - 수정 전 라인수: 1332
# - 수정 후 라인수: 1352
# ---------------------------------------------------------
