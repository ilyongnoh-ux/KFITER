import streamlit as st
import pandas as pd
import re
from io import BytesIO

# 페이지 설정
st.set_page_config(page_title="구글 주소록 클리너", layout="wide")

def clean_phone_number(phone):
    """전화번호에서 숫자만 남기고 포맷팅 (선택 사항)"""
    if pd.isna(phone):
        return phone
    # 숫자만 추출
    clean_num = re.sub(r'[^0-9]', '', str(phone))
    return clean_num

def main():
    st.title("📞 구글 주소록 데이터 클리너 & 뷰어")
    st.markdown("구글 주소록 CSV를 업로드하여 정리하고, 눈으로 확인한 뒤 다운로드하세요.")

    # 1. 파일 업로드
    uploaded_file = st.file_uploader("구글 주소록 CSV 파일 업로드", type=['csv'])

    if uploaded_file is not None:
        try:
            # 구글 CSV는 보통 utf-8이지만, 엑셀 저장 시 cp949가 될 수 있어 처리
            try:
                df = pd.read_csv(uploaded_file, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(uploaded_file, encoding='cp949')

            st.success(f"파일 로드 성공! 총 {len(df)}개의 연락처가 있습니다.")
            
            # --- 사이드바: 필터 및 정리 옵션 ---
            st.sidebar.header("정리 옵션")
            
            # 필드 선택 (기본적으로 중요한 필드 미리 선택)
            all_columns = df.columns.tolist()
            default_cols = ['Name', 'Given Name', 'Phone 1 - Value', 'E-mail 1 - Value', 'Group Membership']
            # 실제 파일에 있는 컬럼만 default로 설정
            valid_default = [c for c in default_cols if c in all_columns]
            
            selected_cols = st.sidebar.multiselect(
                "남길 필드 선택 (나머지는 삭제됨)",
                all_columns,
                default=valid_default
            )

            remove_no_phone = st.sidebar.checkbox("전화번호 없는 연락처 삭제", value=True)
            clean_phone_format = st.sidebar.checkbox("전화번호 특수문자(-) 제거", value=True)

            # --- 데이터 가공 로직 ---
            if selected_cols:
                df_view = df[selected_cols].copy()
            else:
                df_view = df.copy()

            # 1. 전화번호 없는 행 제거
            if remove_no_phone and 'Phone 1 - Value' in df_view.columns:
                before_count = len(df_view)
                df_view = df_view.dropna(subset=['Phone 1 - Value'])
                st.sidebar.info(f"전화번호 없는 {before_count - len(df_view)}개 삭제됨")

            # 2. 전화번호 정제
            if clean_phone_format and 'Phone 1 - Value' in df_view.columns:
                df_view['Phone 1 - Value'] = df_view['Phone 1 - Value'].apply(clean_phone_number)

            # --- 메인 뷰어 (Data Editor) ---
            st.subheader("📝 데이터 미리보기 및 수정")
            st.caption("아래 표에서 데이터를 직접 더블클릭하여 수정할 수 있습니다.")
            
            # st.data_editor를 쓰면 화면에서 엑셀처럼 수정 가능
            edited_df = st.data_editor(
                df_view,
                num_rows="dynamic", # 행 추가/삭제 가능
                use_container_width=True,
                height=600
            )

            # --- 다운로드 버튼 ---
            st.divider()
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.info(f"최종 정리된 연락처: {len(edited_df)}명")
                
            with col2:
                # CSV 변환 (한글 깨짐 방지 utf-8-sig)
                csv_buffer = BytesIO()
                edited_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
                
                st.download_button(
                    label="📥 정리된 CSV 다운로드",
                    data=csv_buffer.getvalue(),
                    file_name="cleaned_contacts.csv",
                    mime="text/csv"
                )

        except Exception as e:
            st.error(f"파일을 읽는 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()