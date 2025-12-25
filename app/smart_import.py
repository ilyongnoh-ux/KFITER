# smart_import.py
# 안전형 스마트 업로드(분석→확인→반영) 엔진
# - Streamlit UI는 main.py(데이터 업로드 페이지)에서 처리
# - 여기서는 "읽기/정규화/매칭 프리뷰/반영" 로직만 제공

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import io
import re
import hashlib
import pandas as pd

import database
import queries


# ---------------------------------------------------------
# [데이터(db포함) 오류] 계약자(개인/법인) 분기 헬퍼
# - Smart ETL이 계약자 우선으로 'row['name']'을 채우는 구조이므로,
#   법인 계약자는 CRM 관리 주체가 아니며, 피보험자를 고객으로 귀속시키는 정책을 적용한다.
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



def read_upload_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """업로드 파일을 DataFrame으로 로드 (csv/xlsx)."""
    bio = io.BytesIO(file_bytes)
    lower = (filename or "").lower()
    if lower.endswith(".csv"):
        # utf-8-sig 우선, 실패 시 기본
        try:
            return pd.read_csv(bio, encoding="utf-8-sig")
        except Exception:
            bio.seek(0)
            return pd.read_csv(bio)
    return pd.read_excel(bio)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_name(name: Any) -> str:
    """이름 공백 제거(기존 로직과 일치)"""
    if name is None:
        return ""
    return re.sub(r"\s+", "", str(name)).strip()


def _as_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _phone_last4_from_raw(phone_raw: Any) -> str:
    s = re.sub(r"\D+", "", _as_str(phone_raw))
    return s[-4:] if len(s) >= 4 else ""


def _fetch_customers_by_phone_norm(cur, phone_norm: str, limit: int = 10) -> List[Dict[str, Any]]:
    cur.execute(
        "SELECT id, name, phone, birth_date, phone_norm, match_key FROM customers "
        "WHERE phone_norm = ? ORDER BY id ASC LIMIT ?",
        (phone_norm, limit),
    )
    rows = cur.fetchall() or []
    return [
        {
            "id": int(r[0]),
            "name": r[1] or "",
            "phone": r[2] or "",
            "birth_date": r[3] or "",
            "phone_norm": r[4] or "",
            "match_key": r[5] or "",
        }
        for r in rows
    ]


def _fetch_customers_by_match_key(cur, match_key: str, limit: int = 20) -> List[Dict[str, Any]]:
    cur.execute(
        "SELECT id, name, phone, birth_date, phone_norm, match_key FROM customers "
        "WHERE match_key = ? ORDER BY id ASC LIMIT ?",
        (match_key, limit),
    )
    rows = cur.fetchall() or []
    return [
        {
            "id": int(r[0]),
            "name": r[1] or "",
            "phone": r[2] or "",
            "birth_date": r[3] or "",
            "phone_norm": r[4] or "",
            "match_key": r[5] or "",
        }
        for r in rows
    ]


def _fetch_customers_by_name_birth(cur, name_norm: str, birth_date: str, limit: int = 20) -> List[Dict[str, Any]]:
    # name은 공백이 있을 수도 있으니 REPLACE로 비교
    if birth_date:
        cur.execute(
            "SELECT id, name, phone, birth_date, phone_norm, match_key FROM customers "
            "WHERE REPLACE(name,' ','') = ? AND birth_date = ? "
            "ORDER BY id ASC LIMIT ?",
            (name_norm, birth_date, limit),
        )
    else:
        cur.execute(
            "SELECT id, name, phone, birth_date, phone_norm, match_key FROM customers "
            "WHERE REPLACE(name,' ','') = ? "
            "ORDER BY id ASC LIMIT ?",
            (name_norm, limit),
        )
    rows = cur.fetchall() or []
    return [
        {
            "id": int(r[0]),
            "name": r[1] or "",
            "phone": r[2] or "",
            "birth_date": r[3] or "",
            "phone_norm": r[4] or "",
            "match_key": r[5] or "",
        }
        for r in rows
    ]


def _fetch_customers_by_name_last4(cur, name_norm: str, last4: str, limit: int = 20) -> List[Dict[str, Any]]:
    cur.execute(
        "SELECT id, name, phone, birth_date, phone_norm, match_key FROM customers "
        "WHERE REPLACE(name,' ','') = ? AND substr(COALESCE(phone_norm,''), -4) = ? "
        "ORDER BY id ASC LIMIT ?",
        (name_norm, last4, limit),
    )
    rows = cur.fetchall() or []
    return [
        {
            "id": int(r[0]),
            "name": r[1] or "",
            "phone": r[2] or "",
            "birth_date": r[3] or "",
            "phone_norm": r[4] or "",
            "match_key": r[5] or "",
        }
        for r in rows
    ]


def _preview_contract_action(cur, *, customer_id: int, fin: Dict[str, Any]) -> Dict[str, Any]:
    """
    contracts 테이블에 대한 프리뷰 매칭.
    add_contract()의 우선순위와 동일하게:
    1) key_hash
    2) policy_no_norm(고객 내)
    3) stable_hash(고객 내)
    + (추가 안전장치) policy_no_norm이 다른 고객에 존재하면 보류
    """
    company = _as_str(fin.get("company"))
    product_name = _as_str(fin.get("product_name"))
    policy_no = _as_str(fin.get("policy_no"))
    premium = fin.get("premium")
    status = _as_str(fin.get("status"))
    start_date = _as_str(fin.get("start_date"))
    end_date = _as_str(fin.get("end_date"))
    insured_name = _as_str(fin.get("insured_name"))
    insured_phone = _as_str(fin.get("insured_phone"))
    insured_birth = _as_str(fin.get("insured_birth"))
    insured_gender = _as_str(fin.get("insured_gender"))
    coverage_summary = _as_str(fin.get("coverage_summary", ""))

    policy_no_norm = queries._norm_policy_no(policy_no)  # type: ignore[attr-defined]
    premium_norm = queries._norm_premium(premium)        # type: ignore[attr-defined]
    start_norm = queries._norm_date(start_date)          # type: ignore[attr-defined]
    end_norm = queries._norm_date(end_date)              # type: ignore[attr-defined]

    # (추가 안전) 증권번호가 다른 고객에 이미 있으면 보류 처리
    if policy_no_norm:
        cur.execute(
            "SELECT id, customer_id FROM contracts WHERE policy_no_norm = ? ORDER BY id ASC LIMIT 10",
            (policy_no_norm,),
        )
        rows_global = cur.fetchall() or []
        other = [r for r in rows_global if int(r[1]) != int(customer_id)]
        if other:
            return {
                "status": "보류",
                "reason": f"다른 고객에 동일 증권번호 존재({len(other)}건) - 중복 가능",
                "match_id": None,
            }

    key_hash = queries._contract_key_hash(  # type: ignore[attr-defined]
        customer_id=customer_id,
        company=company,
        policy_no_norm=policy_no_norm,
        product_name=product_name,
    )
    stable_hash = queries._contract_stable_hash(  # type: ignore[attr-defined]
        customer_id=customer_id,
        company=company,
        product_name=product_name,
        insured_name=insured_name,
        insured_birth=insured_birth,
        insured_phone=insured_phone,
    )
    content_hash = queries._contract_content_hash(  # type: ignore[attr-defined]
        customer_id=customer_id,
        company=company,
        product_name=product_name,
        policy_no_norm=policy_no_norm,
        premium_norm=premium_norm,
        status=status,
        start_date=start_norm,
        end_date=end_norm,
        insured_name=insured_name,
        insured_birth=insured_birth,
        insured_phone=insured_phone,
        insured_gender=insured_gender,
        coverage_summary=coverage_summary,
    )

    # 1) key_hash
    cur.execute("SELECT id, content_hash FROM contracts WHERE key_hash = ? LIMIT 1", (key_hash,))
    row = cur.fetchone()
    if row:
        return {
            "status": "유지" if (row[1] == content_hash) else "변경",
            "reason": "key_hash 매칭",
            "match_id": int(row[0]),
        }

    # 2) policy_no_norm within customer
    if policy_no_norm:
        cur.execute(
            "SELECT id, content_hash FROM contracts WHERE customer_id = ? AND policy_no_norm = ? "
            "ORDER BY id ASC LIMIT 5",
            (customer_id, policy_no_norm),
        )
        rows = cur.fetchall() or []
        if rows:
            if any(r[1] == content_hash for r in rows):
                return {"status": "유지", "reason": "증권번호 매칭(동일)", "match_id": int(rows[0][0])}
            if len(rows) == 1:
                return {"status": "변경", "reason": "증권번호 매칭(업데이트)", "match_id": int(rows[0][0])}
            return {"status": "보류", "reason": "증권번호 매칭 다수(모호)", "match_id": None}

    # 3) stable_hash within customer
    cur.execute(
        "SELECT id, content_hash FROM contracts WHERE customer_id = ? AND stable_hash = ? "
        "ORDER BY id ASC LIMIT 10",
        (customer_id, stable_hash),
    )
    rows = cur.fetchall() or []
    if rows:
        if any(r[1] == content_hash for r in rows):
            return {"status": "유지", "reason": "stable_hash 매칭(동일)", "match_id": int(rows[0][0])}
        if len(rows) == 1:
            return {"status": "변경", "reason": "stable_hash 매칭(업데이트)", "match_id": int(rows[0][0])}
        return {"status": "보류", "reason": "stable_hash 매칭 다수(모호)", "match_id": None}

    return {"status": "신규", "reason": "매칭 없음(신규)", "match_id": None}


def analyze_processed_df(df_processed: pd.DataFrame) -> Dict[str, Any]:
    """
    utils.KFITSmartETL().process() 결과(df_processed)를 분석하여
    '신규/변경/유지/보류/실패' 프리뷰를 만든다.
    """
    summary = {
        "고객_신규": 0, "고객_변경": 0, "고객_보류": 0, "고객_실패": 0,
        "계약_신규": 0, "계약_변경": 0, "계약_유지": 0, "계약_보류": 0, "계약_실패": 0,
        "총행": 0
    }
    rows_out: List[Dict[str, Any]] = []

    conn = database.get_connection()
    cur = conn.cursor()

    for seq, (_, row) in enumerate(df_processed.iterrows(), start=1):
        name_raw = row.get("name")
        phone_raw = row.get("phone")
        birth_date = _as_str(row.get("birth_date"))
        gender = _as_str(row.get("gender"))
        region = _as_str(row.get("region"))
        address = _as_str(row.get("address"))
        email = _as_str(row.get("email"))
        memo = _as_str(row.get("memo"))
        custom_data = row.get("custom_data") or ""
        match_key = _as_str(row.get("match_key"))

        name = normalize_name(name_raw)
        phone = _as_str(phone_raw)
        phone_norm = queries.normalize_phone(phone) if phone else ""
        last4 = _phone_last4_from_raw(phone)

        fin = row.get("financial")
        fin = fin if isinstance(fin, dict) else None

        # ---------------------------------------------------------
        # [데이터(db포함) 오류] 계약자/피보험자 분기(현실 케이스 1~3 대응)
        #
        # 케이스 정의:
        # 1) 계약자 = 피보험자  → 문제 없음(고객/상담주체=계약자=피보험자)
        # 2) 계약자 ≠ 피보험자 & 계약자=개인 → 상담일지/고객 연결은 계약자(개인) 기준
        # 3) 계약자 ≠ 피보험자 & 계약자=법인 → 상담일지/고객 연결은 피보험자 기준
        #
        # 구현:
        # - Smart ETL은 계약자 우선 정책으로 row['name']=계약자명, row['phone']=계약자 연락처를 채움
        # - 본 단계에서 policyholder(계약자) 정보를 별도 보관하고,
        #   'primary_role'에 따라 고객 매칭/생성에 사용할 name/phone/birth/gender를 재지정한다.
        # ---------------------------------------------------------
        policyholder_name_raw = _as_str(name_raw)
        policyholder_phone_raw = _as_str(phone_raw)
        policyholder_type = "CORP" if _is_corporate_name(policyholder_name_raw) else "PERSON"
        policyholder_norm = _norm_org_name(policyholder_name_raw)

        insured_name_raw = ""
        insured_phone_raw = ""
        insured_birth_raw = ""
        insured_gender_raw = ""
        if fin:
            insured_name_raw = _as_str(fin.get("insured_name"))
            insured_phone_raw = _as_str(fin.get("insured_phone"))
            insured_birth_raw = _as_str(fin.get("insured_birth"))
            insured_gender_raw = _as_str(fin.get("insured_gender"))

        # 계약자/피보험자 동일성 판단(공백 제거 기준)
        _ph_cmp = (policyholder_name_raw or "").replace(" ", "")
        _in_cmp = (insured_name_raw or "").replace(" ", "")
        if _ph_cmp and _in_cmp and _ph_cmp == _in_cmp:
            primary_role = "POLICYHOLDER"
        else:
            primary_role = "INSURED" if policyholder_type == "CORP" else "POLICYHOLDER"

        # 고객 매칭에 사용할 최종 name/phone/birth/gender 결정
        if primary_role == "INSURED" and insured_name_raw:
            # 법인 계약자 → 피보험자 기준 관리
            name_raw = insured_name_raw
            # insured_phone이 없으면 계약자 연락처(실무상 담당자/피보험자 휴대폰이 여기에 들어오는 경우가 많음)를 보조로 사용
            phone_raw = insured_phone_raw or policyholder_phone_raw
            # 고객 인적사항도 피보험자 것을 우선 사용
            birth_date = insured_birth_raw or birth_date
            gender = insured_gender_raw or gender
        else:
            # 개인 계약자 or 동일인 → 계약자 기준 관리(기존 로직 유지)
            pass

        # name/phone_norm/last4를 위에서 재지정된 name_raw/phone_raw 기준으로 다시 계산
        name = normalize_name(name_raw)
        phone = _as_str(phone_raw)
        phone_norm = queries.normalize_phone(phone) if phone else ""
        last4 = _phone_last4_from_raw(phone)


        # ----------------------
        # 고객 매칭 프리뷰
        # ----------------------
        cust_status = "실패"
        cust_reason = ""
        cust_id: Optional[int] = None
        candidates: List[Dict[str, Any]] = []

        if name and phone_norm:
            by_phone = _fetch_customers_by_phone_norm(cur, phone_norm)
            if len(by_phone) == 1:
                # [데이터 정합성 보호] 동일 연락처 1건 매칭이어도, 이름이 다르면 자동 흡수(업데이트)하지 않고 보류 처리
                # - 실무에서 가족/지인/법인담당자 등으로 전화번호가 재사용되는 케이스가 많고
                # - 이 상황을 자동 업데이트로 흡수하면 고객 DB가 영구적으로 꼬입니다.
                existing = by_phone[0]
                existing_name_norm = normalize_name(existing.get("name") or "")
                if existing_name_norm and name and existing_name_norm != name:
                    cust_status, cust_id = "보류", None
                    candidates = by_phone
                    cust_reason = "동일 연락처지만 이름 불일치 → 대표님 선택 필요(자동 흡수 금지)"
                else:
                    cust_status, cust_id = "변경", existing["id"]
                    cust_reason = "연락처 매칭"
            elif len(by_phone) > 1:
                cust_status, cust_id = "보류", None
                candidates = by_phone
                cust_reason = "동일 연락처 고객이 2명 이상(데이터 정리 필요)"
            else:
                # 전화번호는 새로 들어오지만, 이름/생일로 중복 의심되면 보류
                by_name_birth = _fetch_customers_by_name_birth(cur, name, birth_date) if name else []
                if by_name_birth:
                    cust_status, cust_id = "보류", None
                    candidates = by_name_birth
                    cust_reason = "동일 이름(±생일) 고객 존재 → 신규 생성 시 중복 위험"
                else:
                    cust_status, cust_id = "신규", None
                    cust_reason = "신규 고객"
        elif name:
            # 전화번호가 비어있거나 정규화 불가한 경우:
            # match_key / last4 / name 기반 후보가 있으면 보류, 없으면 실패
            if not match_key and last4:
                match_key = queries.make_match_key(name, last4)
            if match_key:
                by_key = _fetch_customers_by_match_key(cur, match_key)
                if by_key:
                    cust_status, cust_id = "보류", None
                    candidates = by_key
                    cust_reason = "match_key 후보 존재(수동 선택 필요)"
            if cust_status == "실패" and last4:
                by_last4 = _fetch_customers_by_name_last4(cur, name, last4)
                if by_last4:
                    cust_status, cust_id = "보류", None
                    candidates = by_last4
                    cust_reason = "이름+끝4자리 후보 존재(수동 선택 필요)"
            if cust_status == "실패":
                by_name = _fetch_customers_by_name_birth(cur, name, "")
                if by_name:
                    cust_status, cust_id = "보류", None
                    candidates = by_name
                    cust_reason = "동명이 고객 존재(수동 확인 필요)"
                else:
                    cust_status, cust_id = "실패", None
                    cust_reason = "연락처 누락/정규화 불가(신규 생성 불가)"
        else:
            cust_status, cust_id = "실패", None
            cust_reason = "이름 누락"

        summary["총행"] += 1
        summary[f"고객_{cust_status}"] = summary.get(f"고객_{cust_status}", 0) + 1

        # ----------------------
        # 계약 매칭 프리뷰
        # ----------------------
        cont_status = ""
        cont_reason = ""
        cont_id: Optional[int] = None

        if fin:
            if cust_status in ("변경", "신규") and (cust_id is not None or cust_status == "신규"):
                # 신규 고객은 customer_id가 아직 없지만, "다른 고객과 증권번호 중복" 같은 안전 체크를 위해
                # preview에서 customer_id가 없으면 임시 -1로 넣되, 글로벌 중복체크에서 보류될 수 있게 처리
                preview_cid = cust_id if cust_id is not None else -1
                if preview_cid == -1:
                    # 신규인 경우에도 글로벌 증권번호 중복 체크는 실행
                    policy_no_norm = queries._norm_policy_no(_as_str(fin.get("policy_no")))  # type: ignore[attr-defined]
                    if policy_no_norm:
                        cur.execute(
                            "SELECT id, customer_id FROM contracts WHERE policy_no_norm = ? ORDER BY id ASC LIMIT 10",
                            (policy_no_norm,),
                        )
                        rows_global = cur.fetchall() or []
                        if rows_global:
                            cont_status = "보류"
                            cont_reason = f"다른 고객에 동일 증권번호 존재({len(rows_global)}건) - 고객 확정 후 재검토"
                        else:
                            cont_status, cont_reason = "신규", "고객 신규 → 계약도 신규로 저장 예정"
                    else:
                        cont_status, cont_reason = "신규", "고객 신규(증권번호 없음) → 계약 신규로 저장 예정"
                else:
                    cont_preview = _preview_contract_action(cur, customer_id=preview_cid, fin=fin)
                    cont_status = cont_preview["status"]
                    cont_reason = cont_preview.get("reason", "")
                    cont_id = cont_preview.get("match_id")
            else:
                cont_status, cont_reason = "보류", "고객 보류/실패 → 계약 매칭 불가(수동 확인 필요)"

            summary[f"계약_{cont_status}"] = summary.get(f"계약_{cont_status}", 0) + 1


        # row_status는 계약상태 우선이 아니라 '정합성/안전' 우선으로 결정
        # - 고객이 보류인데 계약은 신규/유지로 찍히는 경우(증권번호 없음 등), UI에서 보류가 누락되면 DB가 꼬입니다.
        # - 따라서 실패 > 보류 > (계약상태 or 고객상태) 순으로 최종 행상태를 결정합니다.
        if cust_status == "실패" or cont_status == "실패":
            row_status = "실패"
        elif cust_status == "보류" or cont_status == "보류":
            row_status = "보류"
        else:
            row_status = cont_status if cont_status else cust_status

        rows_out.append({

            "seq": seq,
            "name": name,
            "phone": phone,
            "birth_date": birth_date,
            "gender": gender,
            "region": region,
            "customer_status": cust_status,
            "customer_id": cust_id,
            "customer_reason": cust_reason,
            "customer_candidates": candidates,
            "contract_status": cont_status,
            "contract_id": cont_id,
            "contract_reason": cont_reason,
            "row_status": row_status,
            "financial": fin,
            # apply에 필요한 원본 필드(ETL 결과)를 그대로 보존
            "address": address,
            "email": email,
            "memo": memo,
            "custom_data": custom_data,
            "match_key": match_key,
            # [데이터(db포함) 오류] 계약자/피보험자 분기 결과(계약 반영 및 검색용)
            "policyholder_name": policyholder_name_raw,
            "policyholder_phone": policyholder_phone_raw,
            "policyholder_type": policyholder_type,
            "policyholder_norm": policyholder_norm,
            "primary_role": primary_role,
        })


    # ---------------------------------------------------------
    # [데이터 정합성 보호] 업로드 파일 내부 중복 전화번호(이름 상이) 감지 → 자동 흡수 금지
    # ---------------------------------------------------------
    # 케이스: 같은 업로드 파일에 동일 연락처(phone_norm)에 서로 다른 이름이 등장하면
    #  - 아직 customers DB에 없더라도, 신규 고객을 2개 생성하여 동일 전화번호가 중복될 위험이 큽니다.
    #  - 따라서 해당 행은 모두 보류로 전환하고 대표님이 명시적으로 선택(매핑/신규/스킵)하도록 합니다.
    phone_to_names = {}
    for r in rows_out:
        pn = queries.normalize_phone(r.get("phone") or "")
        nm = normalize_name(r.get("name") or "")
        if pn and nm:
            phone_to_names.setdefault(pn, set()).add(nm)

    conflict_phones = {pn for pn, names in phone_to_names.items() if len(names) > 1}
    if conflict_phones:
        for r in rows_out:
            pn = queries.normalize_phone(r.get("phone") or "")
            if not pn or pn not in conflict_phones:
                continue
            names = sorted(phone_to_names.get(pn) or [])
            # 이미 실패인 경우는 유지, 그 외는 보류로 승격
            if r.get("customer_status") != "실패":
                r["customer_status"] = "보류"
                r["customer_reason"] = f"업로드 파일 내부 동일 연락처에 서로 다른 이름 존재 → 수동 결정 필요({', '.join(names)})"
                # 후보 정보는 UI에서 재검색/선택 가능하므로 그대로 둔다.
            # 최종 행상태도 보류로 강제
            if r.get("row_status") != "실패":
                r["row_status"] = "보류"
            # 계약은 고객 확정 전에는 안전하게 보류로 유지
            if r.get("contract_status") not in ("실패", "보류"):
                r["contract_status"] = "보류"
                r["contract_reason"] = "고객 보류(파일 내부 중복 연락처) → 계약 매칭/반영 보류"

    # ---------------------------------------------------------
    # summary 재계산(위에서 보류 전환 등 후처리로 summary가 달라질 수 있음)
    # ---------------------------------------------------------
    summary = {"총행": len(rows_out)}
    for r in rows_out:
        cs = r.get("customer_status") or ""
        if cs:
            summary[f"고객_{cs}"] = summary.get(f"고객_{cs}", 0) + 1
        ts = r.get("contract_status") or ""
        if ts:
            summary[f"계약_{ts}"] = summary.get(f"계약_{ts}", 0) + 1

    conn.close()
    return {"summary": summary, "rows": rows_out, "df_processed": df_processed}


def build_display_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """UI 표시용으로 가볍게 평탄화한 DataFrame"""
    out = []
    for r in rows:
        fin = r.get("financial") or {}
        out.append({
            "seq": r.get("seq"),
            "고객": r.get("name"),
            "연락처": r.get("phone"),
            "생일": r.get("birth_date"),
            "고객상태": r.get("customer_status"),
            "고객사유": r.get("customer_reason"),
            "계약상태": r.get("contract_status"),
            "계약사유": r.get("contract_reason"),
            "보험사": _as_str(fin.get("company")),
            "상품명": _as_str(fin.get("product_name")),
            "증권번호": _as_str(fin.get("policy_no")),
            "피보험자": _as_str(fin.get("insured_name")),
            "피보험자연락처": _as_str(fin.get("insured_phone")),
            "행상태": r.get("row_status"),
        })
    return pd.DataFrame(out)


def apply_import(
    rows: List[Dict[str, Any]],
    *,
    source: str = "full_upload_v2",
    file_hash: str | None = None,
    filename: str | None = None,
    apply_updates: bool = True,
    apply_same: bool = False,
    allow_hold: bool = False,
    decisions: Optional[Dict[int, Dict[str, Any]]] = None,
    progress_cb: Optional[callable] = None,
) -> Dict[str, Any]:
    """
    분석 결과(rows)를 실제 DB에 반영.
    - apply_updates: '변경' 반영 여부
    - apply_same: '유지'도 add_contract() 호출(검증)할지 여부
    - allow_hold: '보류'도 강제 반영(비추천) 여부
    - decisions: 보류행 수동 선택 정보 {seq: {"mode": "use_existing"/"create_new"/"skip", "customer_id": int}}
    - progress_cb: 진행 상황 콜백 (done:int, total:int, message:str)
    """
    stats = {
        "new_cust": 0, "update_cust": 0,
        "new_cont": 0, "update_cont": 0, "same_cont": 0, "ambig_cont": 0,
        "skipped": 0, "fail": 0
    }
    decisions = decisions or {}
    total = len(rows)

    def _cb(done: int, msg: str = ""):
        if progress_cb:
            try:
                progress_cb(done, total, msg)
            except Exception:
                pass

    _cb(0, "시작")

    for idx, r in enumerate(rows, start=1):
        seq = int(r.get("seq") or 0)
        cust_status = r.get("customer_status")
        cont_status = r.get("contract_status")
        name = r.get("name") or ""
        phone = r.get("phone") or ""

        try:
            # 1) 고객 결정
            dec = decisions.get(seq, {})
            mode = dec.get("mode")  # use_existing/create_new/skip
            chosen_cid = dec.get("customer_id")

            cid: Optional[int] = None

            if cust_status == "보류":
                if mode == "skip" or (mode is None and not allow_hold):
                    stats["skipped"] += 1
                    if file_hash and seq:
                        queries.set_upload_hold_status_by_file_row(file_hash, seq, "SKIPPED")
                        hold = queries.get_upload_hold_by_file_row(file_hash, seq)
                        if hold:
                            queries.insert_hold_decision(hold["id"], "SKIP", None, {"src": "apply_import"}, decided_by="upload_apply")
                            queries.insert_approval_proof(hold["id"], "APPROVED", {"decision": "SKIP"}, approved_by="upload_apply")
                            queries.audit_log("HOLD_DECISION_SKIP", "upload_holds", hold["id"], {})
                    continue
                if mode == "use_existing" and chosen_cid:
                    cid = int(chosen_cid)
                    if file_hash and seq:
                        queries.set_upload_hold_status_by_file_row(file_hash, seq, "OPEN")
                        hold = queries.get_upload_hold_by_file_row(file_hash, seq)
                        if hold:
                            queries.insert_hold_decision(hold["id"], "MAP_EXISTING", cid, {"src": "apply_import"}, decided_by="upload_apply")
                            queries.insert_approval_proof(hold["id"], "APPROVED", {"decision": "MAP_EXISTING", "customer_id": cid}, approved_by="upload_apply")
                            queries.audit_log("HOLD_DECISION_MAP_EXISTING", "upload_holds", hold["id"], {"customer_id": cid})
                elif mode == "create_new":
                    # [데이터 정합성 보호] 보류 상태에서의 '신규 생성'은 upsert(전화번호 기반 흡수) 금지
                    # - 동일 전화번호에 이름이 다른 케이스는 대표님이 명시적으로 신규 생성하더라도
                    #   기존 고객을 업데이트하면 안 된다.
                    ok, msg, cid = queries.create_customer_direct(
                        name=name,
                        phone=phone,
                        birth_date=r.get("birth_date") or "",
                        gender=r.get("gender") or "",
                        region=r.get("region") or "",
                        address=r.get("address") or "",
                        email=r.get("email") or "",
                        source=source,
                        memo=r.get("memo") or "",
                        custom_data=r.get("custom_data") if isinstance(r.get("custom_data"), dict) else None,
                        match_key=r.get("match_key") or "",
                    )
                    if not ok or not cid:
                        stats["fail"] += 1
                        # hold에는 남겨둔다
                        continue
                    stats["new_cust"] += 1
                    if file_hash and seq:
                        # 보류 결정 기록(사후 감사/추적용)
                        queries.set_upload_hold_status_by_file_row(file_hash, seq, "OPEN")
                        hold = queries.get_upload_hold_by_file_row(file_hash, seq)
                        if hold:
                            queries.insert_hold_decision(hold["id"], "CREATE_NEW", cid, {"src": "apply_import"}, decided_by="upload_apply")
                            queries.insert_approval_proof(hold["id"], "APPROVED", {"decision": "CREATE_NEW", "customer_id": cid}, approved_by="upload_apply")
                            queries.audit_log("HOLD_DECISION_CREATE_NEW", "upload_holds", hold["id"], {"customer_id": cid})
                else:
                    stats["skipped"] += 1
                    continue

            elif cust_status == "실패":
                stats["fail"] += 1
                continue

            else:
                # 신규/변경
                if cust_status == "변경" and not apply_updates:
                    stats["skipped"] += 1
                    continue

                ok, msg, cid = queries.upsert_customer_identity(
                    name=name,
                    phone=phone,
                    birth_date=r.get("birth_date") or "",
                    gender=r.get("gender") or "",
                    region=r.get("region") or "",
                    address=r.get("address") or "",
                    email=r.get("email") or "",
                    source=source,
                    memo=r.get("memo") or "",
                    custom_data=r.get("custom_data") or "",
                    match_key=r.get("match_key") or "",
                )
                if not ok or not cid:
                    stats["fail"] += 1
                    continue
                if cust_status == "신규":
                    stats["new_cust"] += 1
                else:
                    stats["update_cust"] += 1

            # 2) 계약 반영
            fin = r.get("financial")
            if not isinstance(fin, dict):
                continue

            if cont_status == "유지" and not apply_same:
                continue
            if cont_status == "변경" and not apply_updates:
                continue
            if cont_status == "보류" and not allow_hold:
                continue

            if cid is None:
                stats["fail"] += 1
                continue

            res = queries.add_contract(
                customer_id=cid,
                company=fin.get("company"),
                product_name=fin.get("product_name"),
                policy_no=fin.get("policy_no"),
                premium=fin.get("premium"),
                status=fin.get("status"),
                start_date=fin.get("start_date"),
                end_date=fin.get("end_date"),
                insured_name=fin.get("insured_name"),
                insured_phone=fin.get("insured_phone"),
                insured_birth=fin.get("insured_birth"),
                insured_gender=fin.get("insured_gender"),
                coverage_summary=fin.get("coverage_summary", ""),
                policyholder_name=r.get("policyholder_name"),
                policyholder_phone=r.get("policyholder_phone"),
                policyholder_type=r.get("policyholder_type"),
                policyholder_norm=r.get("policyholder_norm"),
                primary_role=r.get("primary_role"),
            )

            if res == "insert":
                stats["new_cont"] += 1
            elif res == "update":
                stats["update_cont"] += 1
            elif res == "same":
                stats["same_cont"] += 1
            elif res == "ambig":
                stats["ambig_cont"] += 1
            else:
                stats["fail"] += 1

            # [보류 연동] 계약까지 정상 반영된 보류건은 hold_store에서 자동 제거(RESOLVED)
            if file_hash and seq and cust_status == "보류" and res in ("insert", "update", "same"):
                queries.resolve_upload_hold_by_file_row(file_hash, seq)
                hold = queries.get_upload_hold_by_file_row(file_hash, seq)
                if hold:
                    queries.audit_log("HOLD_AUTO_RESOLVED_AFTER_APPLY", "upload_holds", hold["id"], {"contract_res": res, "customer_id": cid})

        finally:
            label = f"{name} / {phone}".strip(" /")
            _cb(idx, label)

    return stats


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
# [체크리스트]
# - UI 유지/존치: ✅ 유지됨 (분석/반영 로직 개선)
# - 고객 매핑 보류 조건: ✅ 동일 연락처 + 이름 불일치(자동 흡수 금지)
# - 파일 내부 중복 연락처(이름 불일치) 자동 보류: ✅ 반영됨
# - row_status 결정 규칙(실패>보류>나머지): ✅ 반영됨
# - 보류 처리시 create_new는 중복 허용 insert(create_customer_direct): ✅ 반영됨
# - hold_store 상태 업데이트(OPEN/SKIPPED/RESOLVED) + 결정 기록: ✅ 반영됨
# - 수정 범위: ✅ [데이터 정합성 보호 + 업로드보류 워크플로]
# - '..., 중략, 일부 생략' 금지: ✅ 준수(전체 파일 유지)
# - 수정 전 라인수: 723
# - 수정 후 라인수: 789 (+66)
# ---------------------------------------------------------
