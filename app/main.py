import streamlit as st
import pandas as pd
import os
import time
import json
import hashlib
import io
import urllib.parse
from datetime import datetime, timedelta
import html
import streamlit.components.v1 as components  # 팝업 제어용

# [모듈 임포트] 프로젝트 내 파일들
import utils    # CSS, 로고, 카드 디자인, 생일 체크 함수
import database # DB 초기화
import queries  # DB CRUD
import smart_import  # 안전형 스마트 업로드 엔진
import re
import threading

# ---------------------------------------------------------
# Query Params Helpers (Streamlit 버전 호환)
# ---------------------------------------------------------
def _kfit_get_qp():
    """쿼리 파라미터 읽기 (st.query_params / experimental 호환)"""
    try:
        return dict(st.query_params)
    except Exception:
        return st.experimental_get_query_params()

def _kfit_clear_qp():
    """쿼리 파라미터 초기화"""
    try:
        st.query_params.clear()
    except Exception:
        # 구버전 호환
        st.experimental_set_query_params()

# ---------------------------------------------------------
# [Helper] 커스텀 헤더 스타일 함수 (회색톤 + 폰트 키움)
# ---------------------------------------------------------
def ui_header(text):
    """
    Style: color #666 (Dim Gray), font-size 18px, bold
    """
    st.markdown(f"<div style='color:#666; font-size:18px; font-weight:700; margin-bottom:8px; margin-top:5px;'>{text}</div>", unsafe_allow_html=True)

# ---------------------------------------------------------
# 1. 앱 기본 설정
# ---------------------------------------------------------
st.set_page_config(
    page_title="KFIT Manager Pro",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------------
# 2. 메인 함수
# ---------------------------------------------------------
def main():
    utils.apply_custom_css()
    utils.sidebar_logo()
    database.init_db()

    # ---------------------------------------------------------
    # ✅ 딥링크(클릭 이동) 처리
    # ---------------------------------------------------------
    qp = _kfit_get_qp()
    go = qp.get("go")
    cid = qp.get("cid")
    if isinstance(go, list): go = go[0] if go else None
    if isinstance(cid, list): cid = cid[0] if cid else None

    if go == "consult" and cid:
        try:
            st.session_state["menu"] = "상담 일지"
            st.session_state["target_customer_id"] = int(cid)
            _kfit_clear_qp()
            st.rerun()
        except Exception:
            pass

    with st.sidebar:
        if "menu" not in st.session_state:
            st.session_state["menu"] = "대시보드"

        st.markdown("### 📋 메뉴 선택")
        menu = st.radio(
            "네비게이션",
            ["대시보드", "상담 일지", "고객 데이터 관리", "데이터 업로드", "설정"],
            key="menu",
            label_visibility="collapsed",
        )
        st.markdown("---")
        # 상단 import에 추가

        if st.button("🚪 프로그램 종료"):
            st.warning("종료합니다...")
            
            # ✅ 1) 브라우저 탭 닫기 시도 + 실패 시 빈 화면으로 전환
            components.html(
                """
                <script>
                (function () {
                    try {
                    // 탭 닫기 시도(브라우저 정책상 막힐 수 있음)
                    window.open('', '_self');
                    window.close();

                    // 닫기 실패 대비: 200ms 후에도 안 닫히면 빈 화면으로 이동
                    setTimeout(function(){
                        try { window.location.replace('about:blank'); } catch(e) {}
                        try { document.body.innerHTML = "<div style='font-family:sans-serif;padding:24px;color:#333;'>프로그램이 종료되었습니다.</div>"; } catch(e) {}
                    }, 200);
                    } catch (e) {
                    // 예외 시에도 빈 화면 처리
                    try { window.location.replace('about:blank'); } catch(e2) {}
                    }
                })();
                </script>
                """,
                height=0
            )

            # ✅ 2) JS가 클라이언트로 전달된 뒤 서버 프로세스 종료(1초 후)
            def _shutdown():
                time.sleep(2)
                os._exit(0)

            threading.Thread(target=_shutdown, daemon=True).start()

            # ✅ 이 실행 흐름은 여기서 멈춰서(렌더링 확정) 브라우저에 전달되게 함
            st.stop()

        st.markdown("<div style='font-size:11px; color:#888; margin-top:20px;'>🔒 KFIT Pro v2.1<br>- Borderless & Compact</div>", unsafe_allow_html=True)

    df_all = queries.get_all_customers()

    # ---------------------------------------------------------
    # [PAGE 1] 대시보드
    # ---------------------------------------------------------
    if menu == "대시보드":
        st.markdown("### 📊 Business Dashboard")

        # 데이터 준비
        days_lookahead = 7
        b_df = utils.check_upcoming_birthdays(df_all, days_lookahead)
        p_list = queries.get_upcoming_policy_anniversaries(days_lookahead)
        todos_df = queries.get_dashboard_todos(days_lookahead=7, include_overdue=True)

        # KPI 카드
        c1, c2, c3, c4 = st.columns(4, gap="large")
        utils.metric_card("👥", "총 관리 고객", f"{len(df_all)}명", c1)
        utils.metric_card("📅", "이번 달 상담", f"{queries.get_monthly_consultation_count()}건", c2)
        utils.metric_card("🎂", f"생일 {len(b_df)}명 · 청약 {len(p_list)}명", f"{(len(b_df) + len(p_list))}명", c3)
        utils.metric_card("🧠", "스마트 엔진", "Active", c4)

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

        # 3패널 스타일 및 렌더링 함수
        st.markdown("""
        <style>
          .kfit-panels { margin-top: 2px; }
          .kfit-panel { border-radius: 14px; border: 1px solid rgba(17,24,39,0.08); box-shadow: 0 6px 16px rgba(0,0,0,0.04); overflow: hidden; }
          .kfit-panel .hd { display:flex; align-items:center; justify-content:space-between; padding: 10px 12px; font-weight: 800; font-size: 14px; letter-spacing: -0.2px; }
          .kfit-pill{ font-size: 12px; padding: 2px 8px; border-radius: 999px; background: rgba(17,24,39,0.08); color: rgba(17,24,39,0.75); font-weight: 700; }
          .kfit-scroll { height: calc(100vh - 360px); min-height: 320px; max-height: 560px; overflow-y: auto; padding: 6px 8px 8px 8px; }
          .kfit-rowlink{ text-decoration:none !important; color: inherit; display:block; }
          .kfit-row{ padding: 6px 8px; border-radius: 12px; background: rgba(255,255,255,0.85); border: 1px solid rgba(17,24,39,0.06); margin-bottom: 4px; cursor: pointer; }
          .kfit-row:hover{ transform: translateY(-1px); box-shadow: 0 10px 18px rgba(0,0,0,0.06); }
          .kfit-line{ display:flex; align-items:baseline; gap: 6px; white-space: nowrap; overflow:hidden; }
          .kfit-name{ font-weight: 800; font-size: 13px; flex: 0 0 auto; }
          .kfit-rest{ font-size: 12px; color: rgba(17,24,39,0.70); font-weight: 700; overflow: hidden; text-overflow: ellipsis; }
          .theme-pink { background: linear-gradient(135deg, rgba(255,236,244,1) 0%, rgba(255,250,252,1) 70%); }
          .theme-blue { background: linear-gradient(135deg, rgba(226,244,255,1) 0%, rgba(248,252,255,1) 70%); }
          .theme-gray { background: linear-gradient(135deg, rgba(243,244,246,1) 0%, rgba(255,255,255,1) 70%); }
        </style>
        """, unsafe_allow_html=True)

        def _row_html(*, name: str, rest: str, href: str) -> str:
            name_e = html.escape(name or "")
            rest_e = html.escape(rest or "")
            href_e = html.escape(href or "")
            href_js = (href or "").replace("\\", "\\\\").replace("'", "\\'")
            return f"""<a class='kfit-rowlink' href='{href_e}' target='_self' onclick=\"window.location.assign('{href_js}'); return false;\"><div class='kfit-row'><div class='kfit-line'><span class='kfit-name'>{name_e}</span><span class='kfit-rest'>{rest_e}</span></div></div></a>"""

        def _render_panel(title: str, count: int, theme_cls: str, rows_html: str):
            st.markdown(f"""<div class='kfit-panel {theme_cls}'><div class='hd'><div>{title}</div><div class='kfit-pill'>{count}명</div></div><div class='kfit-scroll'>{rows_html}</div></div>""", unsafe_allow_html=True)

        # 패널 데이터 구성
        b_rows = []
        if not b_df.empty:
            b_ids = [int(x) for x in b_df["id"].tolist()]
            b_contract_map = queries.get_contract_brief_map(b_ids) if b_ids else {}
            for _, r in b_df.sort_values("d_day").iterrows():
                cid = int(r.get("id") or 0)
                comp, pol = b_contract_map.get(cid, ("", ""))
                rest = f"D-{int(r.get('d_day') or 0)} · {utils.fmt_mmdd_paren(r.get('next_bday'), utils.calc_age_on(r.get('birth_date'), r.get('next_bday')))} · {comp or '-'} · {pol or '-'}"
                b_rows.append(_row_html(name=str(r.get("name")), rest=rest, href=f"?go=consult&cid={cid}"))
        else: b_rows.append("<div style='padding:10px; color:rgba(17,24,39,0.65); font-weight:700;'>예정된 생일이 없습니다.</div>")

        p_rows = []
        if p_list:
            for it in sorted(p_list, key=lambda x: (x.get("d_day", 999), x.get("name", ""))):
                cid = int(it.get("customer_id") or 0)
                rest = f"D-{int(it.get('d_day') or 0)} · {utils.fmt_mmdd_paren(it.get('next_anniv'), it.get('years'))} · {it.get('company') or '보험사미상'} · {it.get('policy_no') or '-'}"
                p_rows.append(_row_html(name=str(it.get("name")), rest=rest, href=f"?go=consult&cid={cid}"))
        else: p_rows.append("<div style='padding:10px; color:rgba(17,24,39,0.65); font-weight:700;'>예정된 청약기념일이 없습니다.</div>")

        t_rows = []
        if not todos_df.empty:
            todos_df2 = todos_df.copy()
            todos_df2["__sort"] = pd.to_datetime(todos_df2["date"], errors="coerce")
            for _, r in todos_df2.sort_values("__sort").iterrows():
                cid = int(r.get("customer_id") or 0)
                src = str(r.get("source") or "")
                badge = "갱신" if src == "renewal" else "할일"
                msg = str(r.get("msg") or "")
                if len(msg) > 28: msg = msg[:27] + "…"
                rest = f"{badge} · {utils.fmt_dday(r.get('date'))} · {utils.fmt_mmdd_hhmm(r.get('date'))} · {msg}"
                t_rows.append(_row_html(name=str(r.get("name")), rest=rest, href=f"?go=consult&cid={cid}"))
        else: t_rows.append("<div style='padding:10px; color:rgba(17,24,39,0.65); font-weight:700;'>오늘 처리할 항목이 없습니다.</div>")

        left, right = st.columns([2, 1], gap="large")
        with left:
            cL, cM = st.columns(2, gap="medium")
            with cL: _render_panel("🎂 생일 알림(7일)", int(len(b_df)), "theme-pink", "".join(b_rows))
            with cM: _render_panel("💙 청약기념일(7일)", int(len(p_list)), "theme-blue", "".join(p_rows))
        with right:
            _render_panel("⏱️ 오늘의 할일 & 갱신알림", int(len(todos_df)) if not todos_df.empty else 0, "theme-gray", "".join(t_rows))

    # ---------------------------------------------------------
    # [PAGE 2] 상담 일지 (Dynamic Layout + Compact Input + Dim Header)
    # ---------------------------------------------------------
    elif menu == "상담 일지":
        
        # [핵심] 높이 자동 조절 CSS
        st.markdown("""
            <style>
            div[data-testid="stVerticalBlockBorderWrapper"] > div[style*="height: 777px"] {
                height: calc(100vh - 190px) !important;
                max-height: calc(100vh - 190px) !important;
                min-height: 400px !important;
            }
            /* 상단 헤더 여백 최소화 */
            .block-container { padding-top: 0.1rem !important; }        
            </style>
        """, unsafe_allow_html=True)

        st.markdown("### 📝 상담 일지")
        
        if df_all.empty:
            st.warning("데이터가 없습니다.")
            st.stop()

        df_view = df_all.copy()
        df_view["id"] = df_view["id"].astype(int)
        df_view["phone"] = df_view["phone"].fillna("")
        id_list = df_view["id"].tolist()
        disp_map = {int(r["id"]): f'{r["name"]} ({r["phone"]}) [{r["birth_date"]}]' for _, r in df_view.iterrows()}

        default_id = st.session_state.get("target_customer_id")
        try: default_id = int(default_id) if default_id is not None else id_list[0]
        except: default_id = id_list[0]
        if default_id not in id_list: default_id = id_list[0]

        # 검색창 (라벨 보임)
        sel_id = st.selectbox(
            "상담 대상", 
            id_list, 
            index=id_list.index(default_id), 
            format_func=lambda x: disp_map.get(int(x), str(x)), 
            key="consult_customer_id",
            label_visibility="collapsed" #제거하여 라벨이 보이도록 함
        )
        cid = int(sel_id)
        st.session_state["target_customer_id"] = cid
        target = df_view[df_view["id"] == cid].iloc[0]
        
        def create_google_cal_link(title, date_obj, time_obj, details):
            try:
                start_dt = datetime.combine(date_obj, time_obj)
                end_dt = start_dt + timedelta(hours=1)
                fmt = "%Y%m%dT%H%M%S"
                dates = f"{start_dt.strftime(fmt)}/{end_dt.strftime(fmt)}"
                base_url = "https://calendar.google.com/calendar/render"
                params = {"action": "TEMPLATE", "text": title, "dates": dates, "details": details, "ctz": "Asia/Seoul"}
                return f"{base_url}?{urllib.parse.urlencode(params)}"
            except: return None

        #st.divider()
        # [수정] st.divider() 제거 후 여백 없는 HTML 구분선 적용
        st.markdown("<hr style='margin: 0px 0px 20px 0px; border: 0; border-top: 1px solid #e6e6e6;'>", unsafe_allow_html=True)
        
        # [3단 레이아웃]
        c_input, c_info, c_cal = st.columns([1, 1.2, 1], gap="medium")
        MAGIC_HEIGHT = 777

        # ------------------------------------------------
        # [1열] 상담 입력 (Compact Mode + '메모' 추가 + Dim Header)
        # ------------------------------------------------
        with c_input:
            with st.container(height=MAGIC_HEIGHT, border=True):
                # 1. 헤더 (커스텀 스타일)
                ui_header("🖊️ 상담 내용 입력")

                # 2. 날짜와 방법(메모 추가됨)을 한 줄에 배치 (라벨 숨김)
                r1_c1, r1_c2 = st.columns([1, 1.8])
                with r1_c1:
                    ld = st.date_input("날짜", datetime.now().date(), label_visibility="collapsed")
                with r1_c2:
                    # '📝메모' 추가됨
                    lt = st.radio("방법", ["📞전화", "💬카톡", "🚶방문", "📝메모"], horizontal=True, label_visibility="collapsed")

                # 3. 내용 입력 (라벨 숨김)
                lc = st.text_area("내용", height=200, placeholder="[상담 내용] 핵심/니즈/결론/후속 액션을 입력하세요.", label_visibility="collapsed")

                # 4. 다음 일정 (커스텀 헤더)
                st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)
                ui_header("📅 다음 일정 예약")
                add_next = st.checkbox("일정 추가", value=False)
                
                next_due_str, next_title, cal_link = None, None, None

                if add_next:
                    n1, n2 = st.columns(2)
                    default_next_dt = datetime.now() + timedelta(days=1)
                    # 라벨 숨김
                    nd = n1.date_input("예약 날짜", default_next_dt.date(), key="next_d", label_visibility="collapsed")
                    nt = n2.time_input("예약 시간", default_next_dt.replace(minute=0, second=0).time(), key="next_t", label_visibility="collapsed")
                    next_title = st.text_input("일정 제목", value=f"{target['name']}님 상담", key="next_title", placeholder="일정 제목", label_visibility="collapsed")
                    
                    if next_title:
                        cal_link = create_google_cal_link(next_title, nd, nt, f"고객: {target['name']}\n연락처: {target['phone']}\n\n[메모]\n{lc}")
                    try: next_due_str = datetime.combine(nd, nt).strftime("%Y-%m-%d %H:%M")
                    except: next_due_str = None

                st.markdown("<div style='margin-top: 15px;'></div>", unsafe_allow_html=True)
                
                btn_label = "💾 저장 + 📅 일정등록" if add_next else "💾 저장하기"
                
                if st.button(btn_label, type="primary", use_container_width=True):
                    if not lc.strip(): st.error("내용을 입력해주세요.")
                    elif add_next and (not next_title or not next_due_str): st.error("일정 정보를 모두 입력해주세요.")
                    else:
                        ok = queries.add_consultation_with_optional_task(customer_id=cid, consult_type=lt, content=lc, consult_date=str(ld), task_title=next_title if add_next else None, task_due=next_due_str if add_next else None)
                        if ok:
                            st.toast("저장되었습니다!")
                            if add_next and cal_link:
                                # ✅ [강력 수정] 팝업창 재사용 강제 로직
                                # 1. 창 이름을 변수가 아닌 '문자열'로 직접 지정 ('KFIT_CRM_CALENDAR')
                                # 2. 삼항 연산자나 복잡한 로직 제거 -> 브라우저 혼동 방지
                                js_code = f"""
                                <script>
                                    // 1. 열고 싶은 주소
                                    var url = '{cal_link}';
                                    
                                    // 2. 창 크기 및 위치 계산
                                    var w = 1100; 
                                    var h = 850;
                                    var left = (window.screen.width - w); // 우측 끝
                                    var top = (window.screen.height - h); // 하단 끝
                                    
                                    // 3. 옵션 설정
                                    var features = 'width=' + w + ',height=' + h + ',left=' + left + ',top=' + top + ',scrollbars=yes,resizable=yes';
                                    
                                    // 4. [핵심] 이름을 'KFIT_CRM_CALENDAR'로 고정 (절대 변경 안됨)
                                    // 이 이름이 같으면 브라우저는 무조건 기존 창을 찾아냅니다.
                                    var pop = window.open(url, 'KFIT_CRM_CALENDAR', features);
                                    
                                    // 5. 포커스 (기존 창이 있으면 앞으로 당겨오기)
                                    if (pop) {{ 
                                        pop.focus(); 
                                    }}
                                </script>
                                """
                                components.html(js_code, height=0)
                                st.info("📅 캘린더 창을 띄웁니다.")
                                time.sleep(2)
                            else:
                                time.sleep(0.5)
                            st.rerun()
        # ------------------------------------------------
        # [2열] 고객 정보 & 히스토리 (Borderless & Compact)
        # ------------------------------------------------
        with c_info:

            with st.container(height=MAGIC_HEIGHT, border=True):
                ui_header(f"💡 {target['name']}님 정보")
            #    if target.get("memo"): st.info(f"{target['memo']}")
            #    else: st.caption("메모 없음")

            #    if target.get("custom_data"):
            #        try: st.json(json.loads(target["custom_data"]), expanded=False)
            #        except: st.caption("custom_data 원문"); st.text(str(target["custom_data"]))

                #st.markdown("---")
                ui_header("🎗️ 계약 현황")
                # [데이터(db포함) 오류] 법인 계약(계약자=법인) 검색 UI 위치 변경
                # - 대표님 지시(2025-12-25): 상담일지 화면은 고객 상담 흐름에 집중하기 위해 검색 입력창을 제거한다.
                # - 대신 '고객 데이터 관리 > 법인(관리)' 탭에서 법인 계약자명 기준 전체 계약 조회를 제공한다.
                # - 상담일지의 계약 현황은 선택된 고객(상담 주체) 기준 계약 목록만 표준 노출한다.
                con_df = queries.get_customer_contracts(cid)
                if not con_df.empty:
                    with st.container(height=215):
                        for _, r in con_df.iterrows():
                            end_val = r.get('end_date')
                            # [데이터(db포함) 오류] 만기 표시 규칙: 값이 있는 경우에만 '만:' 블록을 렌더링(빈값/NaN이면 숨김)
                            if pd.isna(end_val) if hasattr(pd, 'isna') else (end_val is None):
                                end_val = ''
                            end_val = str(end_val).strip() if end_val is not None else ''
                            end_html = ("<span style='color:#888; font-size:12px;'>만:" + end_val + "</span>"
                                       + "<span style='color:#ddd; margin:0 3px;'>|</span>") if (end_val and end_val != '-') else ""
                            # [데이터(db포함) 오류] Streamlit Markdown 파서가 '줄바꿈 + 들여쓰기'를 코드블록으로 오인하여
                            # HTML이 그대로 노출되는 현상 방지(계약 현황/법인 계약 조회 공통 이슈).
                            # - UI는 유지하되, HTML을 여러 줄 f"""..."""로 작성하지 않고
                            #   '줄바꿈 없는 1줄 문자열 조립(item_html)'로 렌더링 안정성을 확보한다.
                            item_html = (
                                "<div style='font-size:13px; border-bottom:1px solid #f0f0f0; padding:6px 0; line-height:1.4;'>"
                                f"<span style='font-weight:bold; color:#333;'>{r.get('company','')}</span>"
                                f"<span>{r.get('product_name','')}</span>"
                                f"<span style='color:#0056b3; font-size:12px;'>({r.get('status','')})</span>"
                                "<span style='color:#ddd; margin:0 3px;'>|</span>"
                                f"<span style='color:#666; font-size:12px;'>{r.get('policy_no','')}</span>"
                                "<span style='color:#ddd; margin:0 3px;'>|</span>"
                                f"<span style='color:#555; font-size:12px;'>청:{r.get('start_date') or '-'}</span>"
                                "<span style='color:#ddd; margin:0 3px;'>|</span>"
                                f"{end_html}"
                                f"<span style='color:#555; font-size:12px;'>{r.get('display_party_label','피')}:{r.get('insured_name') or '-'}</span>"
                                "</div>"
                            )
                            st.markdown(item_html, unsafe_allow_html=True)
                else: st.caption("계약 정보가 없습니다.")

                #st.markdown("---")
                ui_header("🗂️ 히스토리")

                # ✅ [안전화] 히스토리 삭제(실수 방지: '정말 삭제' 체크)
                log_df = queries.get_customer_logs(cid)

                if not log_df.empty:
                    if "id" in log_df.columns:
                        edit_df = log_df.copy().set_index("id")
                    else:
                        edit_df = log_df.copy()

                    # 체크박스 컬럼(맨 앞)
                    edit_df.insert(0, "선택", False)

                    edited = st.data_editor(
                        edit_df,
                        column_config={
                            "선택": st.column_config.CheckboxColumn("선택", width="small", default=False),
                            "날짜": st.column_config.TextColumn("날짜", width="small"),
                            "방법": st.column_config.TextColumn("방법", width="small"),
                            "내용": st.column_config.TextColumn("내용", width="large"),
                        },
                        disabled=[c for c in edit_df.columns if c != "선택"],
                        hide_index=True,  # id(index) 숨김
                        use_container_width=True,
                        height=285,
                        key=f"hist_editor_{cid}",
                    )

                    # 선택된 id 추출 (index가 id)
                    del_ids = []
                    try:
                        del_ids = edited[edited["선택"] == True].index.tolist()
                    except Exception:
                        del_ids = []

                    # -------------------------------------------------------------
                    # [변경] 삭제 로직: 체크박스 대신 '팝업(Dialog)' 방식 적용
                    # -------------------------------------------------------------
                    
                    # 1. 팝업창(Dialog) 함수 정의
                    # 함수 안에 UI와 삭제 로직을 모두 넣습니다.
                    @st.dialog("⚠️ 삭제 확인")
                    def show_delete_confirm(target_ids):
                        st.markdown(f"선택하신 **{len(target_ids)}건**의 기록을 정말 삭제하시겠습니까?")
                        st.caption("삭제된 데이터는 복구할 수 없습니다.")
                        
                        col_cancel, col_del = st.columns(2)
                        
                        # 취소 버튼
                        if col_cancel.button("취소", use_container_width=True):
                            st.rerun()
                            
                        # 삭제 실행 버튼
                        if col_del.button("확인(삭제)", type="primary", use_container_width=True):
                            success = queries.delete_consultations(target_ids)
                            if success:
                                st.toast(f"✅ {len(target_ids)}건 삭제 완료!")
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                st.error("삭제 실패. 로그를 확인하세요.")

                    # 2. 메인 UI: '삭제 버튼'만 배치
                    if st.button(
                        "🗑️ 선택 항목 삭제",
                        key=f"btn_del_hist_{cid}", # 키 충돌 방지
                        type="secondary",
                        use_container_width=True
                    ):
                        # 버튼 클릭 시 선택된 ID 확인
                        del_ids = []
                        try:
                            # 체크된 항목의 인덱스(ID) 가져오기
                            del_ids = edited[edited["선택"] == True].index.tolist()
                        except Exception:
                            del_ids = []

                        if not del_ids:
                            st.warning("삭제할 항목을 먼저 선택(체크)해주세요.")
                        else:
                            # 선택된 항목이 있을 때만 팝업 호출
                            show_delete_confirm(del_ids)                
                else:
                    st.caption("기록 없음")

        # ------------------------------------------------
        # [3열] 다음 일정 & 이웃고객 & 구글 캘린더
        # ------------------------------------------------
        with c_cal:
            # ✅ 높이 200px 고정 (내부 스크롤)
            with st.container(height=MAGIC_HEIGHT, border=True):
                ui_header("📅 다음 일정")

                # '다음 일정' 리스트 영역 (높이 200px 고정)
                with st.container(height=255):
                    open_tasks = queries.get_open_tasks(cid)
                    if not open_tasks.empty:
                        for _, tr in open_tasks.iterrows():
                            tid = int(tr.get("id") or 0)
                            title = str(tr.get('type'))
                            due_str = f"{utils.fmt_mmdd_hhmm(tr.get('due_date'))} ({utils.fmt_dday(tr.get('due_date'))})"

                            # ✅ [수정] 컬럼 분할 없이 '체크박스' 하나로 통합 (가장 깔끔한 UI)
                            # 라벨: "**제목(진하게)** :grey[날짜(회색)]" 
                            # (Streamlit 최신 버전은 :grey[] 문법으로 색상 지정 가능)
                            label = f"**{title}** :grey[{due_str}]"
                            
                            # 체크박스를 클릭(True)하면 -> 완료 처리 -> 재실행
                            if st.checkbox(label, key=f"chk_{tid}"):
                                queries.complete_task(tid)
                                time.sleep(0.2) # 사용자가 클릭한 것을 인지할 찰나의 시간 제공
                                st.rerun()

                            # 구분선 (여백 최소화)
                            #st.markdown("<hr style='margin: 4px 0; border-top: 1px dashed #eee;'>", unsafe_allow_html=True)
                    else:
                        st.caption("잡혀있는 다음 일정이 없습니다.")

                # ------------------------------------------------
                # (이하 이웃고객 추천 및 구글 캘린더 코드는 그대로 유지)
                # ------------------------------------------------
                ui_header("📍 이웃고객 추천")
                target_region_raw = str(target.get('region', '')).strip()
                
                if target_region_raw:
                    tokens = target_region_raw.split()
                    if len(tokens) >= 2:
                        key1, key2 = tokens[0], tokens[1]
                        neighbors = df_view[
                            (df_view['id'] != cid) & 
                            (df_view['region'].astype(str).str.contains(key1, na=False, regex=False)) &
                            (df_view['region'].astype(str).str.contains(key2, na=False, regex=False))
                        ]
                        search_info = f"'{key1} {key2}'"
                    elif len(tokens) == 1:
                        key1 = tokens[0]
                        neighbors = df_view[
                            (df_view['id'] != cid) & 
                            (df_view['region'].astype(str).str.contains(key1, na=False, regex=False))
                        ]
                        search_info = f"'{key1}'"
                    else:
                        neighbors = pd.DataFrame()
                        search_info = ""

                    if not neighbors.empty:
                        st.markdown(f"""
                        <div style="background-color:#e8f4f8; padding:8px 12px; border-radius:8px; font-size:14px; color:#004085; margin-bottom:10px;">
                            📍 <b>{search_info}</b> 지역 이웃: <b>{len(neighbors)}명</b>
                        </div>
                        """, unsafe_allow_html=True)
                        st.dataframe(neighbors[['name', 'birth_date', 'region']], use_container_width=True, hide_index=True, height=200)
                    else:
                        st.caption(f"{search_info} 근처(이웃)로 식별되는 다른 고객이 없습니다.")
                else:
                    st.caption("고객 정보에 '지역(region)' 데이터가 없습니다.")

                ui_header("🗓️ 나의 구글 캘린더")
                
                # ✅ [수정] components.html을 사용하여 자바스크립트 동작 보장
                # st.markdown 대신 독립적인 HTML 블록을 생성합니다.
                html_code = """
                <!DOCTYPE html>
                <html>
                <head>
                <style>
                    /* 기존 디자인과 동일한 CSS 적용 */
                    .cal-btn {
                        display: inline-block;
                        width: 100%;
                        background-color: #ffffff;
                        color: #444;
                        border: 1px solid #ccc;
                        text-align: center;
                        padding: 10px;
                        border-radius: 5px;
                        text-decoration: none;
                        font-weight: bold;
                        font-family: "Source Sans Pro", sans-serif; /* Streamlit 기본 폰트 */
                        font-size: 14px;
                        cursor: pointer;
                        transition: all 0.2s;
                        box-sizing: border-box;
                    }
                    .cal-btn:hover {
                        background-color: #f0f2f6;
                        border-color: #bbb;
                    }
                    body { margin: 0; padding: 0; }
                </style>
                </head>
                <body>
                    <div class="cal-btn" onclick="openPopup()">
                        🚀 내 구글 캘린더 열기 (팝업)
                    </div>

                    <script>
                        function openPopup() {
                            var w = 900;
                            var h = 700;
                            // 화면 해상도 기준 우측 하단 좌표 계산
                            var left = (window.screen.availWidth || window.screen.width) - w;
                            var top = (window.screen.availHeight || window.screen.height) - h;
                            
                            var features = 'width=' + w + ',height=' + h + ',left=' + left + ',top=' + top + ',scrollbars=yes,resizable=yes';
                            
                            // 팝업 열기 (기존 창 있으면 포커스)
                            var pop = window.open('https://calendar.google.com/calendar/r', 'kfit_cal_popup', features);
                            if (pop) { pop.focus(); }
                        }
                    </script>
                </body>
                </html>
                """
                # 높이를 버튼 크기에 맞춰 50px 정도로 설정
                components.html(html_code, height=50) 


    # [main.py]의 'elif menu == "고객 데이터 관리":' 부분을 아래 코드로 교체

    # ---------------------------------------------------------
    # [PAGE 3] 고객 데이터 관리 (기능 강화판)
    # ---------------------------------------------------------
    elif menu == "고객 데이터 관리":
        st.markdown("### 👥 고객 데이터 관리")
        st.markdown("""
        <style>
        /* ✅ 고객 데이터 관리 목록만(이 화면에서만 삽입되게) - 수정/삭제 버튼 초소형 */
        div[data-testid="stButton"] button[kind="secondary"]{
        padding: 2px 8px !important;
        height: 26px !important;
        font-size: 12px !important;
        line-height: 1 !important;
        border-radius: 8px !important;
        min-width: 0 !important;
        }

        /* ✅ 행(컨테이너) 위아래 여백 최소화 */
        .kfit-row-tight{
        margin: 0 !important;
        padding: 2px 0 !important;
        }
        .kfit-row-tight [data-testid="stMarkdownContainer"]{
        margin: 0 !important;
        padding: 0 !important;
        }

        /* ✅ 행 사이 구분선(위아래 마진 0) */
        .kfit-row-line{
        border: none !important;
        border-bottom: 1px solid #e5e7eb !important;
        margin: 0 !important;
        padding: 0 !important;
        }

        /* ✅ Streamlit 기본 블록 간격 살짝 축소(이 페이지에서만 주입) */
        section.main div.block-container{
        padding-top: 2rem; /* 필요하면 유지 */
        }
        </style>
        """, unsafe_allow_html=True)

        # -------------------------------------------------------
        # [Helper] 고객 수정/계약 등록 다이얼로그 (팝업)
        # -------------------------------------------------------
        @st.dialog("고객 정보 및 계약 관리", width="large")
        def show_edit_dialog(customer_id):
            # 최신 데이터 로드
            cust = queries.get_customer_detail(customer_id)
            if not cust:
                st.error("고객 정보를 찾을 수 없습니다.")
                return

            # 탭 분리: 정보 수정 / 계약 등록
            dt1, dt2 = st.tabs(["📝 정보 수정", "➕ 계약 수기 등록"])

            # [Tab 1] 정보 수정
            with dt1:
                with st.form(key=f"edit_form_{customer_id}"):
                    c1, c2 = st.columns(2)
                    new_name = c1.text_input("이름", value=cust['name'])
                    new_phone = c2.text_input("연락처", value=cust['phone'])
                    
                    c3, c4 = st.columns(2)
                    new_birth = c3.text_input("생년월일", value=cust['birth_date'] or "")
                    new_gender = c4.selectbox("성별", ["", "남", "여"], index=(["", "남", "여"].index(cust['gender']) if cust['gender'] in ["남", "여"] else 0))
                    
                    c5, c6 = st.columns(2)
                    new_region = c5.text_input("지역", value=cust['region'] or "")
                    new_email = c6.text_input("이메일", value=cust['email'] or "")
                    
                    new_memo = st.text_area("메모", value=cust['memo'] or "", height=80)
                    
                    if st.form_submit_button("💾 변경사항 저장", type="primary", use_container_width=True):
                        ok, msg = queries.update_customer_direct(
                            customer_id, new_name, new_phone, new_birth, new_gender, new_region, new_email, new_memo
                        )
                        if ok:
                            st.toast(msg)
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)

            # [Tab 2] 계약 등록
            with dt2:
                st.info(f"💡 {cust['name']}님의 계약을 수기로 등록합니다.")
                with st.form(key=f"add_cont_form_{customer_id}"):
                    ac1, ac2 = st.columns(2)
                    a_comp = ac1.text_input("보험사 (필수)", placeholder="예: 삼성생명")
                    a_prod = ac2.text_input("상품명 (필수)", placeholder="예: 통합보험")
                    
                    ac3, ac4 = st.columns(2)
                    a_pol = ac3.text_input("증권번호", placeholder="미입력시 자동생성")
                    a_prem = ac4.number_input("보험료(원)", min_value=0, step=1000)
                    
                    ac5, ac6 = st.columns(2)
                    a_start = ac5.date_input("계약일(시작)", value=datetime.now())
                    a_end = ac6.date_input("만기일(종료)", value=None)
                    
                    a_stat = st.selectbox("상태", ["정상", "실효", "해지", "만기"], index=0)
                    
                    if st.form_submit_button("➕ 계약 등록", use_container_width=True):
                        if not a_comp or not a_prod:
                            st.error("보험사와 상품명은 필수입니다.")
                        else:
                            res = queries.add_contract(
                                customer_id=customer_id,
                                company=a_comp,
                                product_name=a_prod,
                                policy_no=a_pol,
                                premium=a_prem,
                                status=a_stat,
                                start_date=str(a_start),
                                end_date=str(a_end) if a_end else ""
                            )
                            if res in ["insert", "update", "same"]:
                                st.success("계약이 등록되었습니다!")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("등록 실패 (DB 오류)")

        # -------------------------------------------------------
        # 메인 UI 구성
        # -------------------------------------------------------
        # [데이터(db포함) 오류] 고객 데이터 관리 탭 확장: "법인(관리)" 추가(법인 계약 검색 UI 이전)
        t1, t2, t3, t4 = st.tabs(["전체명단(관리)", "신규등록", "법인(관리)", "업로드보류(관리)"])
        
        # [main.py] 내부 '고객 데이터 관리' 탭 > '[Tab 1] 전체 명단' 부분 교체 코드

        # [Tab 1] 전체 명단
        with t1:
            if not df_all.empty:
                # 1. 검색창
                with st.container():
                    c_search, c_stat = st.columns([3, 1])
                    s = c_search.text_input("🔍 검색", placeholder="이름 또는 연락처...", label_visibility="collapsed")
                    s2 = str(s).strip()

                    if s2:
                        digits = re.sub(r"\D", "", s2)
                        if digits:  # 숫자 포함이면 전화검색
                            mask = df_all["phone_norm"].fillna("").astype(str).str.contains(digits, na=False, regex=False)
                        else:       # 그 외는 이름만
                            mask = df_all["name"].fillna("").astype(str).str.contains(s2, na=False, regex=False)

                        df_show = df_all.loc[mask]
                    else:
                        df_show = df_all

                    
                    c_stat.markdown(f"<div style='text-align:right; padding-top:10px; font-weight:bold; color:#666;'>총 {len(df_show)}명</div>", unsafe_allow_html=True)

                # 2. 헤더
                st.markdown("""
                    <div style="display: flex; font-weight: bold; background-color: #f0f2f6; padding: 8px; border-radius: 5px; font-size: 14px; color: #444; margin-bottom: 5px;">
                        <div style="flex: 1.5;">이름 <span style='font-size:11px; font-weight:normal; color:#888;'>(최근상담)</span></div>
                        <div style="flex: 2.0;">연락처</div>
                        <div style="flex: 1.5;">생일</div>
                        <div style="flex: 2.5;">주소</div>
                        <div style="flex: 1.5;">이메일</div>
                        <div style="flex: 1.6; text-align: center;">관리</div>
                    </div>
                """, unsafe_allow_html=True)

                # 3. 데이터 리스트
                with st.container(height=550, border=True):

                    # ✅ 이 컨테이너(목록 영역) 안에서만 버튼/마진을 최대한 타이트하게
                    st.markdown("""
                        <style>
                        /* 목록 컨테이너(보더 wrapper) 안의 요소만 타이트하게 */
                        div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stMarkdownContainer"]{
                        margin: 0 !important;
                        padding: 0 !important;
                        }
                        div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stButton"]{
                        margin: 0 !important;
                        padding: 0 !important;
                        }
                        div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stButton"] button{
                        padding: 2px 6px !important;
                        height: 24px !important;
                        font-size: 12px !important;
                        line-height: 1 !important;
                        border-radius: 8px !important;
                        min-width: 0 !important;
                        }
                        </style>
                        """, unsafe_allow_html=True)

                    def _txt(val, bold=False):
                        w = "700" if bold else "400"
                        color = "#333" if bold else "#555"
                        return f"""
                        <div style="
                            height:36px;
                            display:flex;
                            align-items:center;
                            font-size:13px;
                            font-weight:{w};
                            color:{color};
                            white-space:nowrap;
                            overflow:hidden;
                            text-overflow:ellipsis;
                            padding:0 4px;
                            margin:0;
                        ">{val}</div>
                        """


                    if len(df_show) == 0:
                        st.info("검색 결과가 없습니다.")
                    else:
                        for i, r in df_show.iterrows():
                            uid = r['id']
                            name = r['name']
                            phone = r['phone'] or "-"
                            birth = r.get('birth_date', '-') or "-"
                            addr = r.get('region', '-') or "-"
                            email = r.get('email', '') or "-"
                            last_contact = r.get('last_contact')

                            # 컬럼 비율(대표님 기존 유지)
                            c1, c2, c3, c4, c5, c6 = st.columns([1.5, 2.0, 1.5, 2.5, 1.5, 1.6], gap="small")

                            # ✅ 행 사이 구분선(위아래 마진 0)
                            st.markdown("<div style='border-bottom:1px solid #e5e7eb; margin:0;'></div>", unsafe_allow_html=True)

                            # 기존 edit/del 처리 로직은 아래에 그대로 두시면 됩니다.

                            
                            

                            # -----------------------------------------------------------
                            # [수정됨] 이름 셀 렌더링 함수 (날짜 표시 기능 추가)
                            # -----------------------------------------------------------
                            def _name_cell(nm, customer_id, last_date=None, highlight=False):
                                safe_nm = html.escape(str(nm) if nm is not None else "")
                                bg = "#E0F2FF" if highlight else "transparent"
                                cid = int(customer_id)
                                
                                # 날짜 포맷팅: 2024-12-21 -> 24.12.21 (공간 절약)
                                date_html = ""
                                if last_date and str(last_date).lower() not in ['none', 'nan', '']:
                                    s = str(last_date).strip()
                                    # YYYY-MM-DD 형태라면 2자리씩 끊어서 표시
                                    if len(s) >= 10:
                                        short_date = s[2:10].replace("-", ".") # 25.12.21
                                        date_html = f"<span style='font-size:11px; color:#999; font-weight:400; margin-left:4px;'>({short_date})</span>"
                                    else:
                                        date_html = f"<span style='font-size:11px; color:#999; font-weight:400; margin-left:4px;'>({s})</span>"

                                return f"""
                                <a href="?go=consult&cid={cid}" target="_self"
                                style="text-decoration:none; color:inherit;">
                                <div style="font-size:14px; font-weight:700; color:#333; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
                                            padding:6px 8px; border-radius:8px; background:{bg}; cursor:pointer;">
                                    {safe_nm} {date_html}
                                </div>
                                </a>
                                """

                            # 상담 건수 0이면 하이라이트
                            highlight = False
                            if "consult_count" in df_show.columns:
                                highlight = int(r.get("consult_count", 0) or 0) == 0

                            # 함수 호출 시 last_contact 전달
                            c1.markdown(_name_cell(name, uid, last_date=last_contact, highlight=highlight), unsafe_allow_html=True)
                            c2.markdown(_txt(phone), unsafe_allow_html=True)
                            c3.markdown(_txt(birth), unsafe_allow_html=True)
                            c4.markdown(_txt(addr), unsafe_allow_html=True)
                            c5.markdown(_txt(email), unsafe_allow_html=True)
                            
                            # [관리] 버튼 그룹
                            with c6:
                                b1, b2 = st.columns(2, gap="small")
                                if b1.button("수정", key=f"btn_edit_{uid}", use_container_width=True):
                                    show_edit_dialog(uid)
                                    
                                if b2.button("삭제", key=f"btn_del_{uid}", type="secondary", use_container_width=True):
                                    queries.delete_customer(uid)
                                    st.toast(f"{name}님 삭제됨")
                                    time.sleep(0.5)
                                    st.rerun()

                            #st.markdown("<hr style='margin: 0px 0px 4px 0px; border-top: 1px solid #f0f0f0;'>", unsafe_allow_html=True)
            else:
                st.info("등록된 고객 데이터가 없습니다.")

        # [Tab 2] 신규 등록 (기존 코드 유지)
        with t2:
            st.markdown("##### ➕ 신규 고객 수기 등록")
            with st.form("new_c_form"):
                f1, f2 = st.columns(2)
                n = f1.text_input("이름 (필수)", placeholder="홍길동")
                p = f2.text_input("연락처 (필수)", placeholder="010-0000-0000")
                
                f3, f4 = st.columns(2)
                b = f3.text_input("생년월일", placeholder="YYYY-MM-DD")
                g = f4.selectbox("성별", ["", "남", "여"])
                
                f5, f6 = st.columns(2)
                r_reg = f5.text_input("지역(시/도)", placeholder="서울, 경기 등")
                e_mail = f6.text_input("이메일")

                if st.form_submit_button("💾 저장하기", type="primary", use_container_width=True):
                    if not n or not p:
                        st.error("이름과 연락처는 필수입니다.")
                    else:
                        ok, msg, _ = queries.upsert_customer_identity(
                            name=n, phone=p, birth_date=b, gender=g, 
                            region=r_reg, email=e_mail, source="manual"
                        )
                        if ok:
                            st.success(f"{n}님 등록 완료!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)

        # [데이터(db포함) 오류] 법인(관리) 탭: 법인 계약자 기준 전체 계약 조회
        # - 대표님 지시(2025-12-25): 상담일지 화면의 법인 계약자 검색 입력창을 제거하고,
        #   고객 데이터 관리 화면에 "법인(관리)" 탭을 신설하여 법인 단위 계약 전체 조회를 제공한다.
        # - 특허 포인트(명세서 기재용):
        #   (1) 고객(상담 주체) 엔티티는 개인 중심으로 유지하면서도,
        #   (2) 계약(contract) 엔티티에 계약자(policyholder) 정보를 별도 저장하고,
        #   (3) 계약자명 정규화(policyholder_norm)를 이용해 법인 단위 검색/그룹핑을 제공한다.
        # -------------------------------------------------------
        with t3:
            st.markdown("##### 🏢 법인 계약 조회 (계약자 기준)")
            corp_q = st.text_input("법인명(계약자) 검색", value="", key="corp_contract_search_manage")
            if corp_q.strip():
                corp_df = queries.search_corporate_contracts(corp_q)
                if not corp_df.empty:
                    with st.container(height=320):
                        for _, r in corp_df.iterrows():
                            end_val = r.get('end_date')
                            # [데이터(db포함) 오류] 만기 표시 규칙: 값이 있는 경우에만 '만:' 블록을 렌더링(빈값/NaN이면 숨김)
                            if pd.isna(end_val) if hasattr(pd, 'isna') else (end_val is None):
                                end_val = ''
                            end_val = str(end_val).strip() if end_val is not None else ''
                            end_html = ("<span style='color:#888; font-size:12px;'>만:" + end_val + "</span>"
                                       + "<span style='color:#ddd; margin:0 3px;'>|</span>") if (end_val and end_val != '-') else ""
                            # [데이터(db포함) 오류] 법인 계약 조회 리스트: 계약자(계:) 외에 피보험자 성명(피:)도 함께 표시(공백 없이)
                            insured_raw = r.get('insured_name_raw')
                            if pd.isna(insured_raw) if hasattr(pd, 'isna') else (insured_raw is None):
                                insured_raw = ''
                            insured_raw = str(insured_raw).strip() if insured_raw is not None else ''
                            insured_html = ""
                            # 계약자와 피보험자가 다른 법인 계약의 경우에만 '피:'를 추가(중복 표기 방지)
                            if insured_raw and insured_raw != '-' and str(r.get('display_party_label','계')).strip() != '피':
                                insured_html = "<span style='color:#ddd; margin:0 3px;'>|</span>" + "<span style='color:#555; font-size:12px;'>피:" + insured_raw + "</span>"
                            item_html = (
                                "<div style='font-size:13px; border-bottom:1px solid #f0f0f0; padding:6px 0; line-height:1.4;'>"
                                f"<span style='font-weight:bold; color:#333;'>{r.get('company','')}</span>"
                                f"<span>{r.get('product_name','')}</span>"
                                f"<span style='color:#0056b3; font-size:12px;'>({r.get('status','')})</span>"
                                "<span style='color:#ddd; margin:0 3px;'>|</span>"
                                f"<span style='color:#666; font-size:12px;'>{r.get('policy_no','')}</span>"
                                "<span style='color:#ddd; margin:0 3px;'>|</span>"
                                f"<span style='color:#555; font-size:12px;'>청:{r.get('start_date') or '-'}</span>"
                                "<span style='color:#ddd; margin:0 3px;'>|</span>"
                                f"{end_html}"
                                f"<span style='color:#555; font-size:12px;'>{r.get('display_party_label','계')}:{r.get('insured_name') or '-'}</span>"
                                f"{insured_html}"
                                "</div>"
                            )
                            # [데이터(db포함) 오류] Streamlit의 Markdown 파서는 줄바꿈 + (4칸 이상) 들여쓰기를 코드블록으로 인식할 수 있어,
                            # HTML을 여러 줄로 작성하면 태그가 그대로 화면에 노출되는 현상이 발생할 수 있다.
                            # 따라서 UI는 유지하되, HTML은 '한 줄 문자열 조립' 형태(줄바꿈 없이)로 렌더링 안정성을 확보한다.
                            st.markdown(item_html, unsafe_allow_html=True)
                else:
                    st.caption("검색 결과가 없습니다.")
            else:
                st.caption("법인명(계약자)을 입력하면 해당 법인의 전체 계약을 보여줍니다.")


        # -------------------------------------------------------
        # [데이터(db포함) 오류] 업로드보류(관리) 탭: 업로드 과정에서 발생한 보류 항목을 사후에 수정/결정/감사추적
        # - 대표님 지시(2025-12-25): 업로드 중 즉시 처리도 가능하지만, 업로드 완료 후에도 보류 항목을 찾아
        #   '기존고객 매핑 / 신규 생성 / 스킵'을 명시적으로 결정하고, 해결된 것은 목록에서 제거되도록 한다.
        # - 특허 포인트(명세서 용어 1:1 매핑):
        #   hold_store(upload_holds) / decision(hold_decisions) / approval(approval_proofs) / audit(audit_logs)
        # -------------------------------------------------------
        with t4:
            st.markdown("##### 🟡 업로드 보류(관리)")
            st.caption("보류 1건마다 대표님이 명시적으로 결정합니다: 기존고객 매핑 / 신규 생성 / 스킵. 해결된 건은 자동으로 목록에서 제거됩니다.")

            f1, f2, f3, f4 = st.columns([1.2, 2.2, 2.0, 3.0])
            status_opt = f1.selectbox("상태", ["OPEN", "SKIPPED", "RESOLVED", "ALL"], index=0, key="hold_mgr_status")

            # 사유코드는 운영 중 추가될 수 있어 멀티 선택을 제공
            reason_code_pool = [
                "PHONE_NAME_MISMATCH_DB",
                "PHONE_DUP_DB",
                "PHONE_NAME_CONFLICT_FILE",
                "AMBIGUOUS",
                "REQUIRED_MISSING",
                "CONTRACT_WAIT_CUSTOMER",
                "OTHER",
            ]
            reason_codes = f2.multiselect("사유코드 필터(선택)", reason_code_pool, default=[], key="hold_mgr_reason")

            batch_list = queries.list_upload_hold_batches(limit=50)
            batch_opts = [("ALL", "(전체 업로드)")]
            for b in batch_list:
                label = f"{(b.get('filename') or '')[:28]} ({b.get('created_at','')[:16]}) · OPEN {b.get('open_count',0)}"
                batch_opts.append((b.get('upload_id'), label))
            batch_sel = f3.selectbox("업로드 배치", options=batch_opts, format_func=lambda x: x[1], index=0, key="hold_mgr_batch")
            upload_id_filter = None if batch_sel[0] == "ALL" else batch_sel[0]

            keyword = f4.text_input("키워드(이름/연락처/증권번호/상품명)", value="", key="hold_mgr_keyword")

            if status_opt == "ALL":
                statuses = None
            else:
                statuses = [status_opt]

            holds = queries.list_upload_holds(
                statuses=statuses,
                keyword=(keyword.strip() or None),
                upload_id=upload_id_filter,
                reason_codes=(reason_codes or None),
                limit=200,
            )

            st.markdown(f"**검색 결과:** {len(holds)}건")
            st.caption("후보 추천 기준: (1) 연락처 정확일치 → (2) 이름+생년월일 → (3) match_key. 최종 매핑은 대표님이 직접 선택합니다.")

            if not holds:
                st.info("조건에 맞는 보류 항목이 없습니다.")
            else:
                with st.container(height=420):
                    for h in holds:
                        hid = h.get('id')
                        title = f"[{h.get('status')}] #{h.get('row_no')} · {h.get('display_name','-')} ({h.get('display_phone','-')}) · {h.get('reason_code','-')}"
                        with st.expander(title, expanded=False):
                            st.write(h.get('reason_msg') or "-")

                            # 계약 요약
                            hint = h.get('contract_hint') or {}
                            ccols = st.columns([1.4, 2.2, 2.6])
                            ccols[0].metric("증권번호", hint.get('policy_no') or "-")
                            ccols[1].metric("보험사", hint.get('company') or "-")
                            ccols[2].write(f"상품명: {hint.get('product_name') or '-'}")

                            # 정정 입력(이름/연락처/생년월일)
                            corrected = h.get('corrected') or {}
                            orig = h.get('normalized') or {}
                            ncols = st.columns(3)
                            new_name = ncols[0].text_input("이름(정정)", value=(corrected.get('name') or orig.get('name') or ''), key=f"hold_name_{hid}")
                            new_phone = ncols[1].text_input("연락처(정정)", value=(corrected.get('phone') or orig.get('phone') or ''), key=f"hold_phone_{hid}")
                            new_birth = ncols[2].text_input("생년월일(정정)", value=(corrected.get('birth_date') or orig.get('birth_date') or ''), key=f"hold_birth_{hid}")

                            bcols = st.columns([1.2, 1.2, 1.2, 1.4])
                            if bcols[0].button("후보 다시찾기", key=f"hold_refresh_{hid}"):
                                ok, msg, _ = queries.update_upload_hold_corrected(hid, name=new_name, phone=new_phone, birth_date=new_birth)
                                if ok:
                                    st.success("정정 저장 완료")
                                else:
                                    st.error(msg)
                                st.rerun()

                            # 후보(기존고객) 선택
                            cands = h.get('candidates') or []
                            cand_labels = []
                            cand_map = {}
                            for c in cands:
                                cid = c.get('id')
                                label = f"[{cid}] {c.get('name','-')} · {c.get('phone','-')} · {c.get('birth_date','-')} ({','.join(c.get('reasons') or [])})"
                                cand_labels.append(label)
                                cand_map[label] = cid

                            decision = bcols[1].selectbox(
                                "처리결정",
                                ["기존 고객에 매핑", "신규 고객 생성", "이번 건 스킵(보류 유지)"],
                                index=0,
                                key=f"hold_dec_{hid}",
                            )

                            selected_cid = None
                            if decision == "기존 고객에 매핑":
                                if cand_labels:
                                    sel = st.selectbox("매핑할 기존 고객", cand_labels, key=f"hold_cand_{hid}")
                                    selected_cid = cand_map.get(sel)
                                else:
                                    st.warning("추천 후보가 없습니다. 정정 후 '후보 다시찾기'를 눌러보거나, '신규 고객 생성'을 선택하세요.")

                            if bcols[2].button("적용", type="primary", key=f"hold_apply_{hid}"):
                                if decision == "기존 고객에 매핑" and not selected_cid:
                                    st.error("기존 고객을 선택해야 합니다.")
                                else:
                                    # 결정 적용
                                    if decision == "기존 고객에 매핑":
                                        dcode = "MAP_EXISTING"
                                    elif decision == "신규 고객 생성":
                                        dcode = "CREATE_NEW"
                                    else:
                                        dcode = "SKIP"

                                    ok, msg, _ = queries.apply_upload_hold_decision(
                                        hold_id=hid,
                                        decision=dcode,
                                        target_customer_id=selected_cid,
                                        corrected={"name": new_name, "phone": new_phone, "birth_date": new_birth},
                                        decided_by="대표님",
                                    )
                                    if ok:
                                        st.success(msg)
                                        st.rerun()
                                    else:
                                        st.error(msg)

                            if bcols[3].button("보류해제(해결됨 표시)", key=f"hold_mark_resolved_{hid}"):
                                # 계약 반영이 불필요하거나 외부에서 이미 정리된 경우 수동 해결 처리
                                ok, msg, _ = queries.set_upload_hold_status(hid, "RESOLVED")
                                if ok:
                                    st.success("해결됨 처리 완료")
                                    st.rerun()
                                else:
                                    st.error(msg)

        # ---------------------------------------------------------
    # [PAGE 4] 데이터 업로드
    # ---------------------------------------------------------
    elif menu == "데이터 업로드":

        st.markdown("### 📂 스마트 일괄 등록")

        st.info(
            "📌 **업로드 전 안내**\n"
            "1. **비밀번호 해제:** 엑셀 파일에 암호가 있다면 해제해주세요.\n"
            "2. **기준 헤더 유지:** 헤더명은 기존 템플릿 형식을 유지해야 합니다.\n"
            "3. **누락값 점검:** 필수 항목(이름/연락처)은 누락되면 보류/실패로 분류됩니다.\n"
            "4. **안전형 동작:** 기본은 '분석 → 확인 → 반영' 순서로만 저장됩니다."
        )


        # -------------------------------------------------------
        up = st.file_uploader("📎 엑셀/CSV 업로드", type=["xlsx", "csv"])

        if up is not None:
            file_bytes = up.getvalue()
            file_hash = hashlib.sha256(file_bytes).hexdigest()

            # 파일이 바뀌면 분석/결정/실패수정 상태 초기화
            if st.session_state.get("smart_upload_file_hash") != file_hash:
                st.session_state["smart_upload_file_hash"] = file_hash
                st.session_state.pop("smart_upload_analysis", None)
                st.session_state.pop("smart_upload_decisions", None)
                st.session_state.pop("smart_upload_fail_edits", None)

            # 미리보기
            try:
                df_preview = smart_import.read_upload_file(file_bytes, up.name)
                with st.expander("📄 업로드 데이터 미리보기 (상위 10행)", expanded=False):
                    st.dataframe(df_preview.head(10), use_container_width=True)
            except Exception as e:
                st.error(f"파일을 읽는 중 오류가 발생했습니다: {e}")
                df_preview = None

            st.markdown("##### 🔒 실수 방지(동일 파일 재업로드 차단)")
            force = st.checkbox("⚠️ 동일 파일이라도 강제 처리(반영 단계에서만 적용)", value=False)

            c1, c2 = st.columns(2)

            # -------------------------
            # [1] 안전형 전체 통합: 분석 (✅ 프로그레스 추가)
            # -------------------------
            if c1.button("1. 전체 통합(안전형) - 먼저 분석", type="primary", use_container_width=True):
                if df_preview is None:
                    st.warning("먼저 파일이 정상적으로 로드되어야 합니다.")
                else:
                    statusA = st.empty()
                    progA = st.progress(0)
                    try:
                        statusA.info("📥 데이터 준비 중...")
                        progA.progress(10)

                        statusA.info("🧹 ETL 전처리 중...")
                        progA.progress(35)

                        etl = utils.KFITSmartETL()
                        df_processed = etl.process(df_preview)

                        statusA.info("🔎 분석 중(저장 전)...")
                        progA.progress(70)
                        analysis = smart_import.analyze_processed_df(df_processed)

                        # [데이터(db포함) 오류] 보류(hold) 항목을 DB에 영속 저장
                        # - 업로드 중 즉시 해결 못한 건을 업로드 이후에도 "고객데이터관리 > 업로드보류(관리)"에서 처리할 수 있도록 함
                        try:
                            queries.sync_upload_holds(file_hash=file_hash, filename=up.name, analyzed_rows=analysis.get("rows", []))
                        except Exception as e:
                            # 업로드 분석은 계속 진행; hold_store 저장만 실패한 것으로 간주
                            st.warning(f"보류항목 DB저장 실패(분석은 계속): {e}")

                        st.session_state["smart_upload_analysis"] = analysis
                        st.session_state.setdefault("smart_upload_decisions", {})

                        progA.progress(100)
                        statusA.success("✅ 분석 완료! 아래에서 결과를 확인하고 반영 버튼을 눌러주세요.")
                        st.rerun()
                    except Exception as e:
                        statusA.error("❌ 분석 실패")
                        st.error(f"분석 중 오류가 발생했습니다: {e}")
                        try:
                            progA.empty()
                        except Exception:
                            pass

            # -------------------------
            # [2] 계약만 추가(기존 기능) (✅ 프로그레스 + 실패 UI + 히스토리 유지)
            # -------------------------
            if c2.button("2. 계약만 추가(마스킹 매칭)", type="secondary", use_container_width=True):
                if df_preview is None:
                    st.warning("먼저 파일이 정상적으로 로드되어야 합니다.")
                else:
                    action = "masked_contracts"
                    prev = queries.get_upload_history(file_hash, action)
                    if prev and not force:
                        st.warning("⚠️ 동일 파일(계약만 추가)이 이미 처리되었습니다. 강제 처리 체크 후 다시 시도하세요.")
                    else:
                        statusC = st.empty()
                        progC = st.progress(0)
                        try:
                            statusC.info("📥 계약 데이터 준비 중...")
                            progC.progress(15)

                            statusC.info("🔗 마스킹 매칭 및 반영 중...")
                            progC.progress(60)

                            res = queries.bulk_import_masked_contracts(df_preview)

                            # res 형태가 dict/tuple/기타일 수 있으니 안전 처리
                            ok, msg, stats = True, "", {}
                            if isinstance(res, tuple):
                                if len(res) == 3:
                                    ok, msg, stats = res
                                elif len(res) == 2:
                                    ok, msg = res
                                    stats = {}
                                else:
                                    stats = {"result": str(res)}
                            elif isinstance(res, dict):
                                stats = res
                            else:
                                stats = {"result": str(res)}

                            progC.progress(90)
                            try:
                                queries.upsert_upload_history(file_hash, action, up.name, up.size, stats)
                            except Exception:
                                pass

                            progC.progress(100)
                            if ok:
                                statusC.success("✅ 계약만 추가 완료")
                                if msg:
                                    st.success(msg)
                                else:
                                    st.success(f"완료: {stats}")
                            else:
                                statusC.error("❌ 계약만 추가 실패")
                                if msg:
                                    st.error(msg)
                                else:
                                    st.error(f"실패: {stats}")

                        except Exception as e:
                            statusC.error("❌ 오류 발생")
                            st.error(f"계약만 추가 중 오류가 발생했습니다: {e}")
                            try:
                                queries.upsert_upload_history(file_hash, action, up.name, up.size, {"ok": False, "msg": str(e)})
                            except Exception:
                                pass

            # -------------------------
            # 분석 결과 UI
            # -------------------------
            analysis = st.session_state.get("smart_upload_analysis")
            if analysis and st.session_state.get("smart_upload_file_hash") == file_hash:
                st.markdown("---")
                st.markdown("### 🔎 분석 결과(저장 전)")

                summary = analysis.get("summary", {})
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("총 행", summary.get("총행", 0))
                m2.metric("고객 신규", summary.get("고객_신규", 0))
                m3.metric("고객 변경", summary.get("고객_변경", 0))
                m4.metric("고객 보류", summary.get("고객_보류", 0))
                m5.metric("고객 실패", summary.get("고객_실패", 0))

                n1, n2, n3, n4, n5 = st.columns(5)
                n1.metric("계약 신규", summary.get("계약_신규", 0))
                n2.metric("계약 변경", summary.get("계약_변경", 0))
                n3.metric("계약 유지", summary.get("계약_유지", 0))
                n4.metric("계약 보류", summary.get("계약_보류", 0))
                n5.metric("계약 실패", summary.get("계약_실패", 0))

                display_df = smart_import.build_display_df(analysis.get("rows", []))
                with st.expander("📋 상세 리스트(필터)", expanded=True):
                    status_filter = st.multiselect(
                        "행상태 필터",
                        options=sorted([x for x in display_df["행상태"].dropna().unique().tolist() if x]),
                        default=sorted([x for x in display_df["행상태"].dropna().unique().tolist() if x]),
                    )
                    if status_filter:
                        st.dataframe(display_df[display_df["행상태"].isin(status_filter)], use_container_width=True)
                    else:
                        st.dataframe(display_df, use_container_width=True)

                rows_all = analysis.get("rows", [])

                # -------------------------
                # ✅ 보류(수동 선택) UI (컨테이너로 묶어서 스크롤 처리)
                # -------------------------
                hold_rows = [r for r in rows_all if r.get("row_status") == "보류"]
                if hold_rows:
                    with st.expander(f"🟡 보류 {len(hold_rows)}건 - 수동 처리 선택(필수)", expanded=False):
                        st.caption("보류는 자동 반영되지 않습니다. 기존 고객 선택 / 신규 생성 / 건너뛰기 중 선택하세요.")
                        dec = st.session_state.setdefault("smart_upload_decisions", {})

                        max_show = 100
                        with st.container(height=260, border=True):
                            for r in hold_rows[:max_show]:
                                seq = int(r.get("seq", 0))
                                fin = r.get("financial") or {}

                                st.markdown(
                                    f"**[{seq}행] {r.get('name','')} / {r.get('phone','')} / 생일:{r.get('birth_date','')}**\n\n"
                                    f"- 고객 보류 사유: {r.get('customer_reason','')}\n"
                                    f"- 계약 보류 사유: {r.get('contract_reason','')}\n"
                                    f"- 계약: {fin.get('company','')} | {fin.get('product_name','')} | 증권:{fin.get('policy_no','')}\n"
                                )

                                cand = r.get("customer_candidates") or []
                                options = [("skip", None, "이번 행 건너뛰기")]
                                if str(r.get("phone", "")).strip():
                                    options.append(("create_new", None, "신규 고객 생성(중복 가능)"))

                                for c in cand:
                                    label = f"기존 고객 사용: #{c.get('id')} | {c.get('name')} | {c.get('phone')} | 생일:{c.get('birth_date','')}"
                                    options.append(("use_existing", int(c.get("id")), label))

                                labels = [o[2] for o in options]
                                default_idx = 0
                                prev_choice = dec.get(seq, {})
                                if prev_choice.get("mode") == "create_new" and "신규 고객 생성(중복 가능)" in labels:
                                    default_idx = labels.index("신규 고객 생성(중복 가능)")
                                elif prev_choice.get("mode") == "use_existing":
                                    for j, o in enumerate(options):
                                        if o[0] == "use_existing" and o[1] == prev_choice.get("customer_id"):
                                            default_idx = j
                                            break

                                sel = st.selectbox("처리 선택", labels, index=default_idx, key=f"hold_dec_{file_hash}_{seq}")

                                for o in options:
                                    if o[2] == sel:
                                        mode, cid2, _ = o
                                        if mode == "use_existing":
                                            dec[seq] = {"mode": "use_existing", "customer_id": cid2}
                                        elif mode == "create_new":
                                            dec[seq] = {"mode": "create_new"}
                                        else:
                                            dec[seq] = {"mode": "skip"}
                                        break

                                st.markdown("<hr style='margin:6px 0;border:0;border-top:1px solid #eee;'>", unsafe_allow_html=True)

                        if len(hold_rows) > max_show:
                            st.info(f"보류가 많아 {max_show}건까지만 표시했습니다. (총 {len(hold_rows)}건)")

                # -------------------------
                # ✅ 실패 목록 UI (컨테이너 + 수정 가능 Data Editor)
                # -------------------------
                fail_rows = [r for r in rows_all if r.get("row_status") == "실패"]
                if fail_rows:
                    with st.expander(f"🔴 실패 {len(fail_rows)}건 - 수정/재시도", expanded=False):
                        st.caption("실패건은 자동 반영되지 않습니다. 아래에서 값을 수정한 뒤, 옵션으로 '실패 수정분을 보류로 간주' 후 반영을 시도할 수 있습니다.")
                        fail_df = []
                        for r in fail_rows:
                            fin = r.get("financial") or {}
                            fail_df.append({
                                "seq": int(r.get("seq", 0)),
                                "name": str(r.get("name", "") or ""),
                                "phone": str(r.get("phone", "") or ""),
                                "birth_date": str(r.get("birth_date", "") or ""),
                                "company": str(fin.get("company", "") or ""),
                                "product_name": str(fin.get("product_name", "") or ""),
                                "policy_no": str(fin.get("policy_no", "") or ""),
                                "customer_reason": str(r.get("customer_reason", "") or ""),
                                "contract_reason": str(r.get("contract_reason", "") or ""),
                            })
                        fail_df = pd.DataFrame(fail_df)

                        # 다운로드(수정용) 제공
                        csv_bytes = fail_df.to_csv(index=False).encode("utf-8-sig")
                        st.download_button(
                            "⬇️ 실패 목록 CSV 다운로드",
                            data=csv_bytes,
                            file_name=f"upload_fail_{file_hash[:10]}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )

                        with st.container(height=240, border=True):
                            edited_fail = st.data_editor(
                                fail_df,
                                use_container_width=True,
                                hide_index=True,
                                key=f"fail_editor_{file_hash}",
                                column_config={
                                    "seq": st.column_config.NumberColumn("행", disabled=True, width="small"),
                                    "customer_reason": st.column_config.TextColumn("고객 사유", disabled=True, width="large"),
                                    "contract_reason": st.column_config.TextColumn("계약 사유", disabled=True, width="large"),
                                },
                                disabled=["seq", "customer_reason", "contract_reason"],
                            )

                        # 세션에 저장(반영 버튼에서 사용)
                        try:
                            st.session_state["smart_upload_fail_edits"] = edited_fail.to_dict("records")
                        except Exception:
                            st.session_state["smart_upload_fail_edits"] = []

                # -------------------------
                # 반영 옵션 + 반영 실행 (✅ 프로그레스 추가)
                # -------------------------
                with st.expander("⚙️ 반영 옵션", expanded=False):
                    apply_updates = st.checkbox("변경(업데이트)도 반영", value=True, key=f"apply_updates_{file_hash}")
                    apply_same = st.checkbox("유지(동일)도 검증(느려짐)", value=False, key=f"apply_same_{file_hash}")
                    allow_hold = st.checkbox("보류도 강제 반영(비추천)", value=False, key=f"allow_hold_{file_hash}")
                    treat_fixed_fail_as_hold = st.checkbox("✅ 실패 수정분을 '보류'로 간주하고 반영 시도", value=False, key=f"fail_as_hold_{file_hash}")

                a1, a2 = st.columns(2)

                # ---------------------------------------------------------
                # [데이터(db포함) 오류] 업로드 최종 반영 UX/안정성 패치(CTO 권고 반영)
                #  - 목적1: 반영(저장) 클릭 시 진행 상황을 팝업(모달)로 표시하여 사용자 불안/오해(멈춤 착각) 감소
                #  - 목적2: '닫기' 클릭 시 프로그레스가 100%→중간값으로 순간 이동하는 플리커(flicker) 제거
                #  - 설계 원칙: UI(버튼/레이아웃/기능 흐름) 유지. '표시 방식'만 모달로 분리.
                #
                # [특허 출원 대비 메모]
                #  (A) 데이터 반영 파이프라인에서 '저장 전 분석→수동결정→최종반영' 단계의 상태를
                #      UI 세션 상태(State)로 일관되게 관리하여 오류/중단/재시도를 안전하게 만드는 방법.
                #  (B) 장시간 작업(ETL/DB upsert) 동안 '현재 단계/진행률/완료 상태'를 모달로 제공하고,
                #      완료 후에는 진행률을 불변(100%)으로 고정한 뒤 사용자 의도(닫기)로만 종료하는 UX.
                #  (C) 닫기 버튼 동작 시, '모달 오픈 플래그를 먼저 내리고 rerun'하여 렌더 플리커를 원천 차단.
                # ---------------------------------------------------------
                # 세션 키 충돌 방지를 위해 접두사 '_kfit_apply_' 사용
                st.session_state.setdefault('_kfit_apply_modal_open', False)
                st.session_state.setdefault('_kfit_apply_modal_done', False)
                st.session_state.setdefault('_kfit_apply_modal_progress', 0)
                st.session_state.setdefault('_kfit_apply_modal_msg', '')
                st.session_state.setdefault('_kfit_apply_modal_stats', None)
                st.session_state.setdefault('_kfit_apply_modal_payload', None)

                if a1.button("✅ 선택한 내용 반영(저장)", type="primary", use_container_width=True):
                    action = "full_upload_v2"
                    prev = queries.get_upload_history(file_hash, action)
                    if prev and not force:
                        st.warning("⚠️ 동일 파일(전체 통합)이 이미 반영되었습니다. 강제 처리 체크 후 다시 시도하세요.")
                    else:
                        # 모달 오픈 + 실행에 필요한 최소 스냅샷 저장(동일 런에서 즉시 모달 실행)
                        st.session_state['_kfit_apply_modal_open'] = True
                        st.session_state['_kfit_apply_modal_done'] = False
                        st.session_state['_kfit_apply_modal_progress'] = 0
                        st.session_state['_kfit_apply_modal_msg'] = "반영 준비 중..."
                        st.session_state['_kfit_apply_modal_stats'] = None
                        st.session_state['_kfit_apply_modal_payload'] = {
                            'file_hash': file_hash,
                            'action': action,
                            'filename': up.name,
                            'filesize': up.size,
                            'rows_all': rows_all,
                            'apply_updates': apply_updates,
                            'apply_same': apply_same,
                            'allow_hold': allow_hold,
                            'treat_fixed_fail_as_hold': treat_fixed_fail_as_hold,
                            'decisions': st.session_state.get('smart_upload_decisions', {}) or {},
                            'fail_edits': st.session_state.get('smart_upload_fail_edits') or [],
                        }

                # 모달 실행 조건: 현재 파일 해시와 payload가 일치할 때만(다른 파일로 바뀐 경우 오작동 방지)
                payload = st.session_state.get('_kfit_apply_modal_payload')
                if (st.session_state.get('_kfit_apply_modal_open')
                        and isinstance(payload, dict)
                        and payload.get('file_hash') == file_hash
                        and hasattr(st, 'dialog')):

                    @st.dialog("💾 업로드 반영 진행중", width="large")
                    def _kfit_apply_modal_run():
                        # --- UI placeholders (모달 내부) ---
                        status_box = st.empty()
                        bar = st.progress(int(st.session_state.get('_kfit_apply_modal_progress', 0)))

                        def _set_progress(pct: int, msg: str, *, final: bool = False):
                            """진행률/메시지 업데이트(완료 후에는 절대 감소하지 않도록 고정)"""
                            pct = max(0, min(100, int(pct)))
                            prev_pct = int(st.session_state.get('_kfit_apply_modal_progress', 0) or 0)
                            # 완료 상태(final=True)에서는 100% 고정, 그 외에는 단조 증가만 허용
                            if final:
                                pct = 100
                            else:
                                pct = max(prev_pct, pct)
                            st.session_state['_kfit_apply_modal_progress'] = pct
                            st.session_state['_kfit_apply_modal_msg'] = msg
                            # 즉시 렌더
                            if final:
                                status_box.success(msg)
                            else:
                                status_box.info(msg)
                            bar.progress(pct)

                        # 이미 완료된 상태면(사용자가 닫기 전 rerun 등) 완료 화면만 재표시
                        if st.session_state.get('_kfit_apply_modal_done', False):
                            msg = st.session_state.get('_kfit_apply_modal_msg', '✅ 반영 완료')
                            status_box.success(msg)
                            bar.progress(100)
                            stats = st.session_state.get('_kfit_apply_modal_stats')
                            if stats is not None:
                                st.success(f"✅ 반영 완료: {stats}")
                        else:
                            try:
                                # 1) 준비 단계
                                _set_progress(15, "✅ 반영 준비(행 구성/검증) 중...")

                                # rows 복사(원본 session 분석값 오염 방지)
                                rows_to_apply = []
                                for r in payload.get('rows_all') or []:
                                    rr = dict(r)
                                    if isinstance(rr.get('financial'), dict):
                                        rr['financial'] = dict(rr['financial'])
                                    rows_to_apply.append(rr)

                                # 실패 수정본을 rows에 반영
                                fail_edits = payload.get('fail_edits') or []
                                fail_edit_map = {}
                                for e in fail_edits:
                                    try:
                                        seq = int(e.get('seq') or 0)
                                        if seq > 0:
                                            fail_edit_map[seq] = e
                                    except Exception:
                                        continue

                                for rr in rows_to_apply:
                                    try:
                                        seq = int(rr.get('seq') or 0)
                                    except Exception:
                                        continue
                                    if seq in fail_edit_map:
                                        e = fail_edit_map[seq]
                                        rr['name'] = e.get('name', rr.get('name'))
                                        rr['phone'] = e.get('phone', rr.get('phone'))
                                        rr['birth_date'] = e.get('birth_date', rr.get('birth_date'))

                                        fin = rr.get('financial') or {}
                                        fin['company'] = e.get('company', fin.get('company'))
                                        fin['product_name'] = e.get('product_name', fin.get('product_name'))
                                        fin['policy_no'] = e.get('policy_no', fin.get('policy_no'))
                                        rr['financial'] = fin

                                        # 옵션 체크 시, 실패를 보류로 간주해 반영 시도(저장 전 검증 단계에서 실패를 구조화)
                                        if payload.get('treat_fixed_fail_as_hold') and rr.get('row_status') == '실패':
                                            rr['row_status'] = '보류'

                                # 2) DB 반영 실행
                                _set_progress(55, "💾 DB 반영 실행 중...")

                                stats = smart_import.apply_import(
                                    rows_to_apply,
                                    source=payload.get('action'),
                                    file_hash=payload.get('file_hash'),
                                    filename=payload.get('filename'),
                                    apply_updates=payload.get('apply_updates', True),
                                    apply_same=payload.get('apply_same', False),
                                    allow_hold=payload.get('allow_hold', False),
                                    decisions=payload.get('decisions') or {},
                                )

                                # 3) 업로드 이력 기록
                                _set_progress(85, "🧾 업로드 이력 기록 중...")
                                try:
                                    queries.upsert_upload_history(
                                        payload.get('file_hash'),
                                        payload.get('action'),
                                        payload.get('filename'),
                                        payload.get('filesize'),
                                        stats,
                                    )
                                except Exception:
                                    pass

                                # 4) 완료(100% 고정)
                                st.session_state['_kfit_apply_modal_stats'] = stats
                                st.session_state['_kfit_apply_modal_done'] = True
                                _set_progress(100, "✅ 반영 완료", final=True)
                                st.success(f"✅ 반영 완료: {stats}")

                                # 분석/결정/실패수정 상태 초기화(다음 업로드 작업을 위해 정리)
                                st.session_state.pop('smart_upload_analysis', None)
                                st.session_state.pop('smart_upload_decisions', None)
                                st.session_state.pop('smart_upload_fail_edits', None)

                            except Exception as e:
                                # 실패도 '완료 상태'로 고정하여 UX 일관성 유지(사용자는 닫기로 종료)
                                st.session_state['_kfit_apply_modal_done'] = True
                                st.session_state['_kfit_apply_modal_stats'] = None
                                _set_progress(100, "❌ 반영 중 오류 발생", final=True)
                                st.error(f"반영 중 오류가 발생했습니다: {e}")
                                try:
                                    queries.upsert_upload_history(
                                        payload.get('file_hash'),
                                        payload.get('action'),
                                        payload.get('filename'),
                                        payload.get('filesize'),
                                        {'ok': False, 'msg': str(e)},
                                    )
                                except Exception:
                                    pass

                        st.markdown('---')
                        # [핵심] 닫기 클릭 시: 오픈 플래그를 먼저 내리고 rerun → 플리커(중간 진행률 재표시) 차단
                        if st.button('닫기', type='primary', use_container_width=True, key=f"apply_close_{file_hash}"):
                            st.session_state['_kfit_apply_modal_open'] = False
                            st.session_state['_kfit_apply_modal_payload'] = None
                            st.rerun()

                    _kfit_apply_modal_run()

                if a2.button("🧹 분석 초기화", use_container_width=True):
                    st.session_state.pop("smart_upload_analysis", None)
                    st.session_state.pop("smart_upload_decisions", None)
                    st.session_state.pop("smart_upload_fail_edits", None)
                    st.success("초기화 완료")
                    st.rerun()

    # ---------------------------------------------------------
    # [PAGE 5] 설정
    # ---------------------------------------------------------
    elif menu == "설정":
        st.markdown("### ⚙️ 설정")
        if st.button("⚠️ 데이터 전체 초기화"):
            if os.path.exists(database.DB_PATH): os.remove(database.DB_PATH)
            database.init_db()
            st.toast("초기화됨"); time.sleep(1); st.rerun()

if __name__ == "__main__":
    main()
# ---------------------------------------------------------
# [기술이사 메모] UI 변경/영향도
#  - 기존: 페이지 본문에 progress/status를 그려서, rerun 타이밍에 사용자에게 '중간으로 되감긴 듯' 보이는 플리커가 발생 가능
#  - 개선: 모달 내에서 진행률을 단조 증가/완료(100%)로 고정하고, '닫기'는 오픈 플래그를 내린 뒤 rerun하여 플리커를 원천 차단
#  - 사용자 체감: 완료가 '완료로 남아있다가' 닫힘(불안 요소 제거). 본문 UI(버튼/옵션/레이아웃)는 유지.# ---- (코드블록 끝 표기 요구 대응) ------------------------------------------
# 수정 전/후 줄수 및 체크리스트는 파일 말미에 자동 기입됩니다.
# ---------------------------------------------------------

# ---------------------------------------------------------
# [체크리스트]
# - UI 유지/존치: ✅ 유지됨 (요청된 "업로드보류(관리)" 탭 추가만 반영)
# - 법인(관리) 화면: ✅ 유지됨
# - 계약사항/계약현황 HTML 노출 수정: ✅ 유지됨
# - '만:' 표시 조건(내용 있을 때만): ✅ 유지됨
# - 법인계약 리스트 '피:성명' 표기: ✅ 유지됨
# - 업로드보류(관리) 기능: ✅ 추가됨(hold_store/decision/approval/audit 연동)
# - 업로드 분석 시 hold_store 저장(sync_upload_holds): ✅ 반영됨
# - 업로드 반영 시 hold 자동 RESOLVED 처리: ✅ 반영됨(file_hash/filename 전달)
# - 수정 범위: ✅ [데이터 정합성 보호 + 업로드보류(관리)] 중심
# - '..., 중략, 일부 생략' 금지: ✅ 준수(전체 파일 유지)
# - 수정 전 라인수: 1568
# - 수정 후 라인수: 1717 (+149)
# ---------------------------------------------------------
