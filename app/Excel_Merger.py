import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import shutil  # 파일 백업을 위한 라이브러리

class ExcelMergerAppV3:
    def __init__(self, root):
        self.root = root
        self.root.title("KFIT 데이터 병합 툴 v3.0 (Auto Save)")
        self.root.geometry("550x650")

        self.df_a = None
        self.df_b = None
        self.file_path_a = ""  # 파일 경로 저장용 변수

        # --- UI 구성 ---
        
        # 1. 파일 A (Target)
        tk.Label(root, text="[1단계] 기준 파일 A (여기에 데이터가 저장됩니다)", font=("bold", 10), bg="#e6f0ff", width=60).pack(pady=(15, 5))
        self.btn_load_a = tk.Button(root, text="파일 A 열기", command=self.load_file_a)
        self.btn_load_a.pack()
        self.lbl_file_a = tk.Label(root, text="파일 없음", fg="gray")
        self.lbl_file_a.pack()

        # 2. 파일 B (Source)
        tk.Label(root, text="[2단계] 참조 파일 B (데이터를 가져올 곳)", font=("bold", 10), bg="#fff0e6", width=60).pack(pady=(15, 5))
        self.btn_load_b = tk.Button(root, text="파일 B 열기", command=self.load_file_b)
        self.btn_load_b.pack()
        self.lbl_file_b = tk.Label(root, text="파일 없음", fg="gray")
        self.lbl_file_b.pack()

        ttk.Separator(root, orient='horizontal').pack(fill='x', pady=15)

        # 3. 매핑 조건
        tk.Label(root, text="[3단계] 일치 조건 (2개의 키 값)", font=("bold", 10)).pack()
        
        frame_match = tk.Frame(root)
        frame_match.pack(pady=5)
        
        tk.Label(frame_match, text="구분", font=("bold", 9)).grid(row=0, column=0)
        tk.Label(frame_match, text="Key 1 (예: 이름)", font=("bold", 9), fg="blue").grid(row=0, column=1, padx=5)
        tk.Label(frame_match, text="Key 2 (예: 생년월일)", font=("bold", 9), fg="green").grid(row=0, column=2, padx=5)

        tk.Label(frame_match, text="파일 A:").grid(row=1, column=0, pady=5)
        self.combo_key1_a = ttk.Combobox(frame_match, state="readonly", width=18)
        self.combo_key1_a.grid(row=1, column=1, padx=5)
        self.combo_key2_a = ttk.Combobox(frame_match, state="readonly", width=18)
        self.combo_key2_a.grid(row=1, column=2, padx=5)

        tk.Label(frame_match, text="파일 B:").grid(row=2, column=0, pady=5)
        self.combo_key1_b = ttk.Combobox(frame_match, state="readonly", width=18)
        self.combo_key1_b.grid(row=2, column=1, padx=5)
        self.combo_key2_b = ttk.Combobox(frame_match, state="readonly", width=18)
        self.combo_key2_b.grid(row=2, column=2, padx=5)

        # 4. 복사 설정
        tk.Label(root, text="[4단계] 복사할 데이터 설정", font=("bold", 10)).pack(pady=(15, 5))
        frame_copy = tk.Frame(root)
        frame_copy.pack(pady=5)

        tk.Label(frame_copy, text="가져올 값 (파일 B):").grid(row=0, column=0, padx=5)
        self.combo_val_b = ttk.Combobox(frame_copy, state="readonly", width=25)
        self.combo_val_b.grid(row=0, column=1, padx=5)

        tk.Label(frame_copy, text="넣을 곳 (파일 A):").grid(row=1, column=0, padx=5)
        self.combo_target_a = ttk.Combobox(frame_copy, width=25) 
        self.combo_target_a.grid(row=1, column=1, padx=5)

        # 5. 실행 버튼
        self.btn_run = tk.Button(root, text="A 파일에 바로 업데이트 (덮어쓰기)", command=self.process_data, bg="#ffcccc", font=("bold", 11), height=2)
        self.btn_run.pack(pady=30, fill='x', padx=50)

    def load_file_a(self):
        filename = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if filename:
            self.file_path_a = filename  # 경로 저장
            self.df_a = pd.read_excel(filename)
            self.lbl_file_a.config(text=os.path.basename(filename), fg="blue")
            cols = list(self.df_a.columns)
            self.combo_key1_a['values'] = cols
            self.combo_key2_a['values'] = cols
            self.combo_target_a['values'] = cols

    def load_file_b(self):
        filename = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if filename:
            self.df_b = pd.read_excel(filename)
            self.lbl_file_b.config(text=os.path.basename(filename), fg="blue")
            cols = list(self.df_b.columns)
            self.combo_key1_b['values'] = cols
            self.combo_key2_b['values'] = cols
            self.combo_val_b['values'] = cols

    def process_data(self):
        if self.df_a is None or self.df_b is None:
            messagebox.showwarning("경고", "파일 A와 B를 모두 선택해주세요.")
            return

        k1_a, k2_a = self.combo_key1_a.get(), self.combo_key2_a.get()
        k1_b, k2_b = self.combo_key1_b.get(), self.combo_key2_b.get()
        val_b = self.combo_val_b.get()
        target_a = self.combo_target_a.get()

        if not (k1_a and k2_a and k1_b and k2_b and val_b and target_a):
            messagebox.showwarning("경고", "모든 조건을 선택해야 합니다.")
            return

        # 확인 메시지
        if not messagebox.askyesno("확인", "정말로 A 파일에 덮어쓰시겠습니까?\n(안전을 위해 원본은 _backup 파일로 자동 저장됩니다)"):
            return

        try:
            # 데이터 타입 보정 (문자열 변환 및 공백 제거)
            self.df_a[k1_a] = self.df_a[k1_a].astype(str).str.strip()
            self.df_a[k2_a] = self.df_a[k2_a].astype(str).str.strip()
            self.df_b[k1_b] = self.df_b[k1_b].astype(str).str.strip()
            self.df_b[k2_b] = self.df_b[k2_b].astype(str).str.strip()

            # 매핑 로직 (딕셔너리 활용)
            lookup_dict = dict(zip(zip(self.df_b[k1_b], self.df_b[k2_b]), self.df_b[val_b]))
            search_keys = zip(self.df_a[k1_a], self.df_a[k2_a])
            mapped_values = [lookup_dict.get(k) for k in search_keys]

            # 결과 반영
            if target_a not in self.df_a.columns:
                self.df_a[target_a] = None
            
            update_series = pd.Series(mapped_values, index=self.df_a.index)
            mask = update_series.notna()
            self.df_a.loc[mask, target_a] = update_series[mask]

            # --- 저장 및 백업 로직 ---
            
            # 1. 백업 파일 생성
            backup_path = self.file_path_a.replace(".xlsx", "_backup.xlsx")
            shutil.copy(self.file_path_a, backup_path)

            # 2. 원본 덮어쓰기
            self.df_a.to_excel(self.file_path_a, index=False)
            
            messagebox.showinfo("성공", f"완료되었습니다!\n\n1. 업데이트: {os.path.basename(self.file_path_a)}\n2. 백업파일: {os.path.basename(backup_path)}\n\n(총 {mask.sum()}건 반영됨)")

        except Exception as e:
            messagebox.showerror("에러", f"오류 발생: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = ExcelMergerAppV3(root)
    root.mainloop()