import sys
import os
import csv
import pandas as pd
import numpy as np
from datetime import datetime
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QLabel, QLineEdit, QPushButton, QFileDialog, QProgressDialog, QMessageBox,
    QTextEdit, QListWidget, QListWidgetItem, QAbstractItemView, QTableView, QComboBox, QCheckBox, QSplitter, QDialog, QTabWidget
)
from PyQt6.QtCore import Qt, QAbstractTableModel, QThread, pyqtSignal, QSortFilterProxyModel, QTimer
from PyQt6.QtGui import QColor, QBrush, QIcon
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name
from pandas.api.types import is_string_dtype
from PyQt6.QtWidgets import QHeaderView

# 버전 정보
VERSION = "v1.0"

def normalize_headers(headers):
    return [h.strip().lower() for h in headers]

def log_with_timestamp(msg):
    return f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {msg}"

class HistoryListWidget(QListWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.main_app = parent
    
    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_Delete:
            if self.main_app:
                self.main_app.delete_selected_history_files()
        else:
            super().keyPressEvent(event)

class FullViewDialog(QDialog):
    def __init__(self, source_model, current_filter, parent=None):
        super().__init__(parent)
        self.setWindowTitle("전체 결과 보기")
        self.resize(800, 600)
        self.source_model = source_model

        layout = QVBoxLayout(self)
        
        filter_layout = QHBoxLayout()
        filter_label = QLabel("결과 필터:")
        self.filter_combo = QComboBox()
        filter_layout.addWidget(filter_label)
        filter_layout.addWidget(self.filter_combo)
        layout.addLayout(filter_layout)
        
        self.proxy_model = StatusFilterProxyModel()
        self.proxy_model.setSourceModel(source_model)
        if "Status" in source_model._df.columns:
            idx = list(source_model._df.columns).index("Status")
            self.proxy_model.setFilterKeyColumn(idx)
        
        self.table_view = QTableView(self)
        self.table_view.setModel(self.proxy_model)
        self.table_view.setStyleSheet("QTableView { color: darkblue; font-size: 10px; }")
        self.table_view.resizeColumnsToContents()
        layout.addWidget(self.table_view)
        
        close_button = QPushButton("닫기", self)
        close_button.clicked.connect(self.accept)
        layout.addWidget(close_button)
        
        self.update_filter_counts()
        self.filter_combo.setCurrentText(current_filter)
        self.filter_combo.currentTextChanged.connect(self.filter_changed)
    
    def update_filter_counts(self):
        if hasattr(self.source_model, "_df"):
            df = self.source_model._df
            total_count = len(df)
            ok_count = (df["Status"] == "Both (OK)").sum() if "Status" in df.columns else 0
            both_fail = (df["Status"] == "Both (FAIL)").sum() if "Status" in df.columns else 0
            csv1_only = (df["Status"] == "CSV1 only").sum() if "Status" in df.columns else 0
            csv2_only = (df["Status"] == "CSV2 only").sum() if "Status" in df.columns else 0
            self.filter_combo.clear()
            self.filter_combo.addItem(f"전체 ({total_count}건)")
            self.filter_combo.addItem(f"Both OK ({ok_count}건)")
            self.filter_combo.addItem(f"Both FAIL ({both_fail}건)")
            self.filter_combo.addItem(f"CSV1 only ({csv1_only}건)")
            self.filter_combo.addItem(f"CSV2 only ({csv2_only}건)")
    
    def filter_changed(self, text):
        if text.startswith("전체"):
            self.proxy_model.setFilterStatus("")
        elif text.startswith("Both OK"):
            self.proxy_model.setFilterStatus("Both (OK)")
        elif text.startswith("Both FAIL"):
            self.proxy_model.setFilterStatus("Both (FAIL)")
        elif text.startswith("CSV1 only"):
            self.proxy_model.setFilterStatus("CSV1 only")
        elif text.startswith("CSV2 only"):
            self.proxy_model.setFilterStatus("CSV2 only")
    
    def sync_filter(self, text):
        self.filter_combo.setCurrentText(text)

class FileLoadWorker(QThread):
    finished_signal = pyqtSignal(object, object, object)
    log_signal = pyqtSignal(str)
    error_signal = pyqtSignal(str)
    
    def __init__(self, file_path, na_values, delimiter):
        super().__init__()
        self.file_path = file_path
        self.na_values = na_values
        self.delimiter = delimiter
    
    def run(self):
        try:
            self.log_signal.emit(f"파일 로드 시작: {self.file_path}")
            delimiter = self.delimiter
            with open(self.file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=delimiter)
                headers = next(reader)
            chunks = pd.read_csv(
                self.file_path,
                encoding='utf-8',
                sep=delimiter,
                dtype=str,
                na_values=self.na_values,
                keep_default_na=False,
                chunksize=10000,
                engine='python'
            )
            df = pd.concat(chunks, ignore_index=True)
            df.fillna('NULL', inplace=True)
            self.log_signal.emit(f"파일 로드 완료: {self.file_path}, 행 수: {len(df)}, 컬럼 수: {len(df.columns)}")
            self.finished_signal.emit(df, delimiter, headers)
        except Exception as e:
            self.error_signal.emit(str(e))

class DataFrameModel(QAbstractTableModel):
    def __init__(self, df=pd.DataFrame(), parent=None):
        super().__init__(parent)
        self._df = df

    def rowCount(self, parent=None):
        return len(self._df)

    def columnCount(self, parent=None):
        return len(self._df.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        row = index.row()
        col = index.column()
        value = self._df.iat[row, col]
        if role == Qt.ItemDataRole.DisplayRole:
            return str(value)
        if role == Qt.ItemDataRole.BackgroundRole and "Status" in self._df.columns:
            status_val = self._df.iloc[row]["Status"]
            if status_val == "Both (OK)":
                return QBrush(QColor(144,238,144))
            elif status_val == "Both (FAIL)":
                return QBrush(QColor(255,200,200))
            elif status_val == "CSV1 only":
                return QBrush(QColor(211,211,211))
            elif status_val == "CSV2 only":
                return QBrush(QColor(169,169,169))
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            return self._df.columns[section]
        else:
            return str(section+1)

class StatusFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.filter_status = ""
    
    def setFilterStatus(self, status):
        self.filter_status = status
        self.invalidateFilter()
    
    def filterAcceptsRow(self, source_row, source_parent):
        if self.filter_status == "":
            return True
        idx = self.sourceModel().index(source_row, self.sourceModel()._df.columns.get_loc("Status"), source_parent)
        return self.sourceModel().data(idx, Qt.ItemDataRole.DisplayRole) == self.filter_status

class SortTableModel(QAbstractTableModel):
    def __init__(self, data=pd.DataFrame(columns=["컬럼", "조건"]), parent=None):
        super().__init__(parent)
        self._data = data

    def rowCount(self, parent=None):
        return len(self._data)

    def columnCount(self, parent=None):
        return len(self._data.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole:
            return str(self._data.iat[index.row(), index.column()])
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole and orientation == Qt.Orientation.Horizontal:
            return self._data.columns[section]
        return None

    def setData(self, index, value, role=Qt.ItemDataRole.EditRole):
        if role == Qt.ItemDataRole.EditRole:
            self._data.iat[index.row(), index.column()] = value
            self.dataChanged.emit(index, index)
            return True
        return False

class CompareWorker(QThread):
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(pd.DataFrame)
    error_signal = pyqtSignal(str)
    
    def __init__(self, file1_path, file2_path, file1_delimiter, file2_delimiter,
                 key_columns, exclude_columns, na_values,
                 ignore_case=False, numeric_compare=False,
                 file1_df=None, file2_df=None):
        super().__init__()
        self.file1_path = file1_path
        self.file2_path = file2_path
        self.file1_delimiter = file1_delimiter
        self.file2_delimiter = file2_delimiter
        self.key_columns = key_columns
        self.exclude_columns = exclude_columns
        self.na_values = na_values
        self.ignore_case = ignore_case
        self.numeric_compare = numeric_compare
        # 재정렬된 데이터가 있으면 받음
        self.file1_df = file1_df
        self.file2_df = file2_df

    def run(self):
        try:
            # 파일1 데이터 로드 또는 재정렬 데이터 사용
            if self.file1_df is None:
                self.log_signal.emit("CSV 파일 1 데이터 로드 시작...")
                df1 = self.load_csv_data(self.file1_path, self.file1_delimiter)
            else:
                df1 = self.file1_df.copy()
                self.log_signal.emit("CSV 파일 1 재정렬된 데이터 사용")
            df1.columns = normalize_headers(df1.columns.tolist())
            # __cmp_order__가 문자열이더라도 merge에 문제 없도록
            norm_keys = [k.strip().lower() for k in self.key_columns]
            self.progress_signal.emit(20)
            
            # 파일2 데이터 로드 또는 재정렬 데이터 사용
            if self.file2_df is None:
                self.log_signal.emit("CSV 파일 2 데이터 로드 시작...")
                df2 = self.load_csv_data(self.file2_path, self.file2_delimiter)
            else:
                df2 = self.file2_df.copy()
                self.log_signal.emit("CSV 파일 2 재정렬된 데이터 사용")
            df2.columns = normalize_headers(df2.columns.tolist())
            
            csv1_col_order = list(df1.columns)
            ordered_df2_cols = [col for col in csv1_col_order if col in df2.columns]
            df2 = df2[ordered_df2_cols]
            self.progress_signal.emit(40)
            
            if self.ignore_case:
                for key in norm_keys:
                    if key in df1.columns and is_string_dtype(df1[key]):
                        df1[key] = df1[key].str.lower()
                    if key in df2.columns and is_string_dtype(df2[key]):
                        df2[key] = df2[key].str.lower()
            
            self.log_signal.emit("Outer Join 수행 중...")

            # df1의 원래 인덱스를 유지하기 위해 인덱스 추가
            if self.file1_df is not None:
                df1 = self.file1_df.copy()
                df1.index.name = "original_index_csv1"
            if self.file2_df is not None:
                df2 = self.file2_df.copy()
            else:
                df2 = self.load_csv_data(self.file2_path, self.file2_delimiter)
            df1.columns = normalize_headers(df1.columns.tolist())
            df2.columns = normalize_headers(df2.columns.tolist())
            norm_keys = [k.strip().lower() for k in self.key_columns]

            merge_result = pd.merge(df1, df2, how='outer', on=norm_keys,
                                    suffixes=('_x', '_y'), indicator=True, sort=False)
            self.progress_signal.emit(60)
            self.log_signal.emit(f"Merge 완료: 총 {len(merge_result)} 행")

            # __cmp_order__로 정렬 (CSV1 기준 유지)
            # if "__cmp_order__" in merge_result.columns:
            #     merge_result = merge_result.sort_values(by="__cmp_order__", na_position='last')
            #     merge_result.reset_index(drop=True, inplace=True)

            # CSV1의 원래 순서로 재정렬 (original_index_csv1 사용)
            if "original_index_csv1" in merge_result.columns:
                merge_result = merge_result.sort_values(by="original_index_csv1", na_position='last')
                merge_result.drop(columns=["original_index_csv1"], inplace=True, errors='ignore')
            else:
                # CSV1 순서 복원을 위해 df1과 조인하여 순서 맞춤
                df1_order = df1[norm_keys].reset_index().rename(columns={"index": "original_index_csv1"})
                merge_result = pd.merge(df1_order, merge_result, how='right', on=norm_keys)
                merge_result = merge_result.sort_values(by="original_index_csv1", na_position='last')
                merge_result.drop(columns=["original_index_csv1"], inplace=True)

            merge_result.reset_index(drop=True, inplace=True)
            
            self.log_signal.emit("공통 컬럼 비교 시작...")
            common_columns = [col for col in df1.columns 
                              if col not in norm_keys and col not in ["csv1_order", "order"]
                              and col in df2.columns and col not in self.exclude_columns]
            both = merge_result['_merge'] == 'both'
            left_only = merge_result['_merge'] == 'left_only'
            right_only = merge_result['_merge'] == 'right_only'

            def compare_cells(self, x, y):
                    """ 두 값 x, y를 비교하는 함수. 숫자인 경우 정밀도 비교, 문자열인 경우 대소문자 무시 비교 수행 """
                    try:
                        # 숫자인지 먼저 확인 (문자열로 된 숫자 포함)
                        is_x_numeric = isinstance(x, (int, float)) or (isinstance(x, str) and x.replace(',', '').replace('.', '', 1).lstrip('-').isdigit())
                        is_y_numeric = isinstance(y, (int, float)) or (isinstance(y, str) and y.replace(',', '').replace('.', '', 1).lstrip('-').isdigit())

                        if is_x_numeric and is_y_numeric and self.numeric_compare:
                            # 숫자 실수 비교 옵션이 체크된 경우
                            try:
                                # 쉼표 제거 후 float로 변환
                                nx = float(str(x).replace(',', ''))
                                ny = float(str(y).replace(',', ''))
                                # 실수 비교: 값이 같으면 동일 처리 (0.00 == 0, 1,000 == 1000 등)
                                if nx == ny:
                                    return x
                                else:
                                    return f"[{x}] != [{y}]"
                            except ValueError:
                                # 변환 실패 시 문자열로 처리
                                pass
                        elif is_x_numeric and is_y_numeric:
                            # 숫자 실수 비교 옵션이 없는 경우, 문자열로 비교
                            pass
                        else:
                            # 둘 중 하나라도 숫자가 아닌 경우, 대소문자 무시 옵션 적용
                            if self.ignore_case:
                                if str(x).lower() == str(y).lower():
                                    return x
                                else:
                                    return f"[{x}] != [{y}]"

                        # 기본적으로 정확한 값 비교
                        if x == y:
                            return x
                        else:
                            return f"[{x}] != [{y}]"
                    except Exception as e:
                        self.log_signal.emit(f"compare_cells 오류: {e}")
                        return f"[{x}] != [{y}]"

            for col in common_columns:
                col_x, col_y = f"{col}_x", f"{col}_y"
                self.log_signal.emit(f"컬럼 비교: {col} 진행 중...")
                merge_result.loc[both, col] = merge_result.loc[both].apply(lambda row: compare_cells(self, row[col_x], row[col_y]), axis=1)
            
            status = pd.Series("", index=merge_result.index)
            status[both] = np.where(
                merge_result.loc[both, common_columns].apply(lambda r: " != " in r.astype(str).str.cat(sep=""), axis=1),
                "Both (FAIL)",
                "Both (OK)"
            )
            status[left_only] = "CSV1 only"
            status[right_only] = "CSV2 only"
            merge_result["Status"] = status
            
            remarks = []
            for idx, row in merge_result.iterrows():
                if row["Status"] == "Both (FAIL)":
                    diff_cols = [col for col in common_columns if isinstance(row[col], str) and " != " in row[col]]
                    remarks.append(",".join(f"[{col}]" for col in diff_cols))
                else:
                    remarks.append("")
            merge_result["비고"] = remarks
            
            for col in self.exclude_columns:
                for suffix in ["", "_x", "_y"]:
                    col_name = col + suffix
                    if col_name in merge_result.columns:
                        merge_result.drop(columns=[col_name], inplace=True)
            
            final_result = merge_result.copy()
            for col in common_columns:
                col_x = f"{col}_x"
                col_y = f"{col}_y"
                final_result.loc[left_only, col] = merge_result.loc[left_only, col_x]
                final_result.loc[right_only, col] = merge_result.loc[right_only, col_y]
                final_result.drop(columns=[col_x, col_y], inplace=True, errors='ignore')

            final_result.drop(columns=["_merge"], inplace=True)
            
            csv1_col_order = list(df1.columns)
            ordered_columns = [col for col in csv1_col_order if col in final_result.columns]
            remaining_columns = [col for col in final_result.columns if col not in ordered_columns]
            final_result = final_result[ordered_columns + remaining_columns]

            # 컬럼 순서 재정렬 (정렬된 행 순서를 유지)
            csv1_col_order = list(df1.columns)
            ordered_columns = [col for col in csv1_col_order if col in final_result.columns]
            remaining_columns = [col for col in final_result.columns if col not in ordered_columns]
            # reindex 대신 column 재선택으로 행 순서 유지
            final_result = final_result[ordered_columns + remaining_columns]
            
            new_columns = []
            for col in final_result.columns:
                if col in norm_keys or col in common_columns or col in ["Status", "비고", "ORDER"]:
                    new_columns.append(col)
                else:
                    new_columns.append("(X) " + col)
            final_result.columns = new_columns

            # 정렬된 결과 로그로 출력 (상위 5행)
            # self.log_signal.emit("정렬된 최종 결과 (상위 5행):")
            # self.log_signal.emit(final_result.head().to_string())
            
            self.progress_signal.emit(90)
            self.log_signal.emit("컬럼 비교 및 상태 산출 완료.")
            self.progress_signal.emit(100)
            self.finished_signal.emit(final_result)
        
        except Exception as e:
            self.error_signal.emit(str(e))
    
    def load_csv_data(self, file_path, delimiter):
        self.log_signal.emit(f"파일 로드 시작: {file_path}")
        chunks = pd.read_csv(
            file_path,
            encoding='utf-8',
            sep=delimiter,
            dtype=str,
            na_values=self.na_values,
            keep_default_na=False,
            chunksize=10000,
            engine='python'
        )
        df = pd.concat(chunks, ignore_index=True)
        df.fillna('NULL', inplace=True)
        self.log_signal.emit(f"파일 로드 완료: {file_path}, 행 수: {len(df)}, 컬럼 수: {len(df.columns)}")
        return df

class SaveXlsxWorker(QThread):
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()
    error_signal = pyqtSignal(str)
    
    def __init__(self, compare_result, file_path):
        super().__init__()
        self.compare_result = compare_result
        self.file_path = file_path
    
    def run(self):
        try:
            self.log_signal.emit("XLSX 저장 시작...")
            writer = pd.ExcelWriter(self.file_path, engine='xlsxwriter')
            df = self.compare_result.copy()
            df.index = range(1, len(df)+1)
            total_count = len(df)
            ok_count = (df["Status"] == "Both (OK)").sum()
            both_fail = (df["Status"] == "Both (FAIL)").sum()
            csv1_only = (df["Status"] == "CSV1 only").sum()
            csv2_only = (df["Status"] == "CSV2 only").sum()
            df.to_excel(writer, sheet_name='RESULT', index=True, startrow=1)
            self.progress_signal.emit(30)
            workbook = writer.book
            worksheet = writer.sheets['RESULT']
            index_vals = [str(x) for x in df.index]
            index_width = max(len("index"), max(len(x) for x in index_vals)) + 2
            worksheet.set_column(0, 0, index_width)
            for i, col in enumerate(df.columns):
                max_data_len = df[col].astype(str).map(len).max() if not df[col].empty else 0
                header_len = len(col)
                width = max(max_data_len, header_len) + 2
                worksheet.set_column(i+1, i+1, width)
            
            summary_format = workbook.add_format({'bold': True, 'font_color': 'blue', 'align': 'center'})
            ok_format = workbook.add_format({'bg_color': '#90EE90'})
            fail_format = workbook.add_format({'bg_color': '#FFC8C8'})
            csv1_format = workbook.add_format({'bg_color': '#D3D3D3'})
            csv2_format = workbook.add_format({'bg_color': '#A9A9A9'})
            total_columns = df.shape[1] + 1
            summary_text = f"전체 건수: {total_count}, Both OK: {ok_count}, Both FAIL: {both_fail}, CSV1 only: {csv1_only}, CSV2 only: {csv2_only}"
            worksheet.merge_range(0, 0, 0, total_columns - 1, summary_text, summary_format)
            self.progress_signal.emit(50)
            worksheet.freeze_panes(2, 1)
            status_col_index = df.columns.get_loc("Status") + 1
            status_excel_col = xl_col_to_name(status_col_index)
            data_start_row = 2
            data_end_row = data_start_row + len(df) - 1
            data_end_col = total_columns - 1
            worksheet.conditional_format(data_start_row, 0, data_end_row, data_end_col, {
                'type': 'formula',
                'criteria': f'=${status_excel_col}3="Both (FAIL)"',
                'format': fail_format
            })
            worksheet.conditional_format(data_start_row, 0, data_end_row, data_end_col, {
                'type': 'formula',
                'criteria': f'=${status_excel_col}3="Both (OK)"',
                'format': ok_format
            })
            worksheet.conditional_format(data_start_row, 0, data_end_row, data_end_col, {
                'type': 'formula',
                'criteria': f'=${status_excel_col}3="CSV1 only"',
                'format': csv1_format
            })
            worksheet.conditional_format(data_start_row, 0, data_end_row, data_end_col, {
                'type': 'formula',
                'criteria': f'=${status_excel_col}3="CSV2 only"',
                'format': csv2_format
            })
            self.progress_signal.emit(90)
            writer.close()
            self.progress_signal.emit(100)
            self.log_signal.emit("XLSX 저장 완료.")
            self.finished_signal.emit()
        except Exception as e:
            self.error_signal.emit(str(e))

class CSVComparerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"CSV Comparer {VERSION}")
        self.setGeometry(100, 100, 800, 700)
        self.setWindowIcon(QIcon("icon.png"))
        self.file1_path = ""
        self.file2_path = ""
        self.file1_headers = []
        self.file2_headers = []
        self.file1_delimiter = "\t"
        self.file2_delimiter = "\t"
        self.compare_result = None
        self.default_na_values = ["", "nan", "NULL"]
        self.additional_na_values = []
        self.df1 = None
        self.df2 = None
        self.default_csv_delimiters = ["\t", ","]
        self.additional_csv_delimiters = []
        self.sort_model = SortTableModel()  # 정렬 선택 그리드 모델
        self.exclude_columns = []  # exclude_columns 초기화 추가
        self.setup_ui()
        self.update_csv_delim_list_widget()
        self.update_csv_delimiter_combos()
    
    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # 메뉴바 추가
        menubar = self.menuBar()
        help_menu = menubar.addMenu("도움말")
        help_action = help_menu.addAction("도움말 보기")
        help_action.triggered.connect(self.show_help_dialog)
        about_action = help_menu.addAction("정보")
        about_action.triggered.connect(self.show_about_dialog)
        
        file_group = QGroupBox("파일 선택")
        file_layout = QVBoxLayout()
        file1_layout = QHBoxLayout()
        file1_label = QLabel("CSV 파일 1:")
        self.file1_entry = QLineEdit()
        self.file1_entry.setFixedWidth(300)
        self.file1_entry.setReadOnly(True)
        self.file1_delimiter_combo = QComboBox()
        self.file1_delimiter_combo.setFixedWidth(100)
        self.file1_load_button = QPushButton("찾기")
        self.file1_load_button.clicked.connect(self.load_file1)
        self.file1_reset_button = QPushButton("초기화")
        self.file1_reset_button.clicked.connect(self.reset_file1)
        file1_layout.addWidget(file1_label)
        file1_layout.addWidget(self.file1_entry)
        file1_layout.addWidget(self.file1_delimiter_combo)
        file1_layout.addWidget(self.file1_load_button)
        file1_layout.addWidget(self.file1_reset_button)
        
        file2_layout = QHBoxLayout()
        file2_label = QLabel("CSV 파일 2:")
        self.file2_entry = QLineEdit()
        self.file2_entry.setFixedWidth(300)
        self.file2_entry.setReadOnly(True)
        self.file2_delimiter_combo = QComboBox()
        self.file2_delimiter_combo.setFixedWidth(100)
        self.file2_load_button = QPushButton("찾기")
        self.file2_load_button.clicked.connect(self.load_file2)
        self.file2_reset_button = QPushButton("초기화")
        self.file2_reset_button.clicked.connect(self.reset_file2)
        file2_layout.addWidget(file2_label)
        file2_layout.addWidget(self.file2_entry)
        file2_layout.addWidget(self.file2_delimiter_combo)
        file2_layout.addWidget(self.file2_load_button)
        file2_layout.addWidget(self.file2_reset_button)
        
        file_layout.addLayout(file1_layout)
        file_layout.addLayout(file2_layout)
        file_group.setLayout(file_layout)
        
        key_group = QGroupBox("비교 설정")
        key_layout = QVBoxLayout()
        tab_widget = QTabWidget()
        
        # 재정렬 탭 추가
        sort_tab = QWidget()
        sort_tab_layout = QHBoxLayout()
        
        # ---------- setup_ui() 내 재정렬 탭 수정 부분 ----------
        # 정렬 컬럼 리스트 (available list)
        sort_available_layout = QVBoxLayout()
        sort_available_label = QLabel("정렬 컬럼")
        self.sort_available_list = QListWidget()
        # 단일 선택으로 변경하고, 클릭 시 토글 처리하도록 연결
        self.sort_available_list.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.sort_available_list.itemClicked.connect(self.toggle_sort_column)
        sort_available_layout.addWidget(sort_available_label)
        sort_available_layout.addWidget(self.sort_available_list)

        # 정렬 선택 그리드 (selected grid) - QTableView 사용 (컬럼명, 조건)
        sort_selected_layout = QVBoxLayout()
        self.sort_selected_label = QLabel("선택 정렬 컬럼 (0개)")
        self.sort_selected_table = QTableView()
        #self.sort_selected_table.setDisabled(True)
        self.sort_selected_table.setModel(self.sort_model)
        # 헤더 숨기기
        self.sort_selected_table.horizontalHeader().setVisible(False)
        # 헤더를 표시하고 마지막 컬럼이 늘어나도록 설정
        self.sort_selected_table.horizontalHeader().setStretchLastSection(True)
        # verticalHeader의 ResizeMode를 Fixed로 설정
        self.sort_selected_table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        # 더블 클릭 시 조건 전환 연결
        self.sort_selected_table.doubleClicked.connect(self.toggle_sort_order)
        # 목록의 아이템 높이와 동일하게 행 높이 설정 (목록에 아이템이 있다면)
        if self.sort_available_list.count() > 0:
            default_height = self.sort_available_list.sizeHintForRow(0)
            self.sort_selected_table.verticalHeader().setDefaultSectionSize(default_height)
        else:
            self.sort_selected_table.verticalHeader().setDefaultSectionSize(20)  # 기본값

        sort_selected_layout.addWidget(self.sort_selected_label)
        sort_selected_layout.addWidget(self.sort_selected_table)

        # 버튼 레이아웃 (선택 초기화 및 재정렬)
        sort_buttons_layout = QHBoxLayout()
        clear_sort_btn = QPushButton("선택 초기화")
        clear_sort_btn.clicked.connect(self.clear_sort_selection)
        reorder_btn = QPushButton("로드 데이터 재정렬")
        reorder_btn.clicked.connect(self.reorder_loaded_data)
        sort_buttons_layout.addWidget(clear_sort_btn)
        sort_buttons_layout.addWidget(reorder_btn)
        sort_selected_layout.addLayout(sort_buttons_layout)

        # 두 레이아웃을 탭에 추가
        sort_tab_layout.addLayout(sort_available_layout)
        sort_tab_layout.addLayout(sort_selected_layout)
        sort_tab.setLayout(sort_tab_layout)
        
        key_tab = QWidget()
        key_tab_layout = QHBoxLayout()
        available_key_layout = QVBoxLayout()
        available_key_label = QLabel("(필수) Outer Join Key")
        self.key_list = QListWidget()
        self.key_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self.key_list.itemSelectionChanged.connect(self.update_exclude_list)
        available_key_layout.addWidget(available_key_label)
        available_key_layout.addWidget(self.key_list)
        
        selected_key_layout = QVBoxLayout()
        self.selected_key_label = QLabel("선택 Outer Join Key (0개)")
        self.selected_key_list = QListWidget()
        self.selected_key_list.setDisabled(True)
        clear_key_btn = QPushButton("선택 초기화")
        clear_key_btn.clicked.connect(self.clear_selected_key)
        selected_key_layout.addWidget(self.selected_key_label)
        selected_key_layout.addWidget(self.selected_key_list)
        selected_key_layout.addWidget(clear_key_btn)
        
        key_tab_layout.addLayout(available_key_layout)
        key_tab_layout.addLayout(selected_key_layout)
        key_tab.setLayout(key_tab_layout)
        
        exclude_tab = QWidget()
        exclude_tab_layout = QHBoxLayout()
        available_exclude_layout = QVBoxLayout()
        available_exclude_label = QLabel("비교 제외 컬럼")
        self.exclude_list = QListWidget()
        self.exclude_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        available_exclude_layout.addWidget(available_exclude_label)
        available_exclude_layout.addWidget(self.exclude_list)
        
        selected_exclude_layout = QVBoxLayout()
        self.selected_exclude_label = QLabel("선택 비교 제외 컬럼 (0개)")
        self.selected_exclude_list = QListWidget()
        self.selected_exclude_list.setDisabled(True)
        clear_exclude_btn = QPushButton("선택 초기화")
        clear_exclude_btn.clicked.connect(self.clear_selected_exclude)
        selected_exclude_layout.addWidget(self.selected_exclude_label)
        selected_exclude_layout.addWidget(self.selected_exclude_list)
        selected_exclude_layout.addWidget(clear_exclude_btn)
        
        exclude_tab_layout.addLayout(available_exclude_layout)
        exclude_tab_layout.addLayout(selected_exclude_layout)
        exclude_tab.setLayout(exclude_tab_layout)
        
        null_tab = QWidget()
        null_layout = QVBoxLayout()
        null_label = QLabel("NULL 치환 문자열")
        add_layout = QHBoxLayout()
        self.null_line_edit = QLineEdit()
        self.null_line_edit.setPlaceholderText("추가할 문자열 입력")
        self.add_null_button = QPushButton("추가")
        self.add_null_button.clicked.connect(self.add_null_value)
        self.delete_null_button = QPushButton("삭제")
        self.delete_null_button.clicked.connect(self.delete_null_value)
        add_layout.addWidget(self.null_line_edit)
        add_layout.addWidget(self.add_null_button)
        add_layout.addWidget(self.delete_null_button)
        self.null_list_widget = QListWidget()
        for val in self.default_na_values:
            display_val = "<빈 문자열>" if val == "" else val
            self.null_list_widget.addItem(QListWidgetItem(display_val))
        null_layout.addWidget(null_label)
        null_layout.addLayout(add_layout)
        null_layout.addWidget(self.null_list_widget)
        null_tab.setLayout(null_layout)
        
        csv_delim_tab = QWidget()
        csv_delim_layout = QVBoxLayout()
        csv_delim_label = QLabel("CSV 구분자")
        add_csv_delim_layout = QHBoxLayout()
        self.csv_delim_line_edit = QLineEdit()
        self.csv_delim_line_edit.setPlaceholderText("추가할 CSV 구분자 입력 (예: \\t, , 등)")
        self.add_csv_delim_button = QPushButton("추가")
        self.add_csv_delim_button.clicked.connect(self.add_csv_delimiter)
        self.delete_csv_delim_button = QPushButton("삭제")
        self.delete_csv_delim_button.clicked.connect(self.delete_csv_delimiter)
        add_csv_delim_layout.addWidget(self.csv_delim_line_edit)
        add_csv_delim_layout.addWidget(self.add_csv_delim_button)
        add_csv_delim_layout.addWidget(self.delete_csv_delim_button)
        self.csv_delim_list_widget = QListWidget()
        csv_delim_layout.addWidget(csv_delim_label)
        csv_delim_layout.addLayout(add_csv_delim_layout)
        csv_delim_layout.addWidget(self.csv_delim_list_widget)
        csv_delim_tab.setLayout(csv_delim_layout)
        
        tab_widget.addTab(sort_tab, "재정렬")
        tab_widget.addTab(key_tab, "Outer Join Key")
        tab_widget.addTab(exclude_tab, "비교 제외 컬럼")
        tab_widget.addTab(null_tab, "NULL 치환 문자열")
        tab_widget.addTab(csv_delim_tab, "CSV 구분자")
        
        key_layout.addWidget(tab_widget)
        key_group.setLayout(key_layout)
        
        work_group = QGroupBox("작업")
        work_layout = QHBoxLayout()
        self.compare_button = QPushButton("비교")
        self.compare_button.clicked.connect(self.compare_files)
        self.compare_button.setEnabled(False)
        overall_reset_btn = QPushButton("전체 초기화")
        overall_reset_btn.clicked.connect(self.reset)
        self.save_button = QPushButton("XLSX 저장")
        self.save_button.clicked.connect(self.save_to_xlsx)
        self.save_button.setEnabled(False)
        self.ignore_case_checkbox = QCheckBox("대소문자 무시")
        self.numeric_compare_checkbox = QCheckBox("숫자 실수 비교")
        work_layout.addWidget(self.ignore_case_checkbox)
        work_layout.addWidget(self.numeric_compare_checkbox)
        work_layout.addWidget(self.compare_button)
        work_layout.addWidget(overall_reset_btn)
        work_layout.addWidget(self.save_button)
        work_group.setLayout(work_layout)
        # 작업 그룹의 높이를 고정 (필요에 따라 조정 가능)
        work_group.setFixedHeight(work_group.sizeHint().height())
        
        self.result_group = QGroupBox("결과")
        result_layout = QVBoxLayout()
        filter_layout = QHBoxLayout()
        filter_label = QLabel("결과 필터:")
        self.filter_combo = QComboBox()
        self.filter_combo.addItems(["전체", "Both OK", "Both FAIL", "CSV1 only", "CSV2 only"])
        self.filter_combo.currentTextChanged.connect(self.apply_filter)
        filter_layout.addWidget(filter_label)
        filter_layout.addWidget(self.filter_combo)
        self.result_view = QTableView()
        self.result_view.setStyleSheet("QTableView { color: darkblue; font-size: 10px; }")
        self.proxy_model = StatusFilterProxyModel()
        result_layout.addLayout(filter_layout)
        result_layout.addWidget(self.result_view)
        self.result_group.setLayout(result_layout)
        
        self.xlsx_history_group = QGroupBox("XLSX 저장 이력")
        history_layout = QVBoxLayout()
        self.xlsx_history_list = HistoryListWidget(self)
        self.xlsx_history_list.itemDoubleClicked.connect(self.open_history_file)
        self.clear_history_button = QPushButton("이력 초기화")
        self.clear_history_button.clicked.connect(self.clear_history)
        history_layout.addWidget(self.xlsx_history_list)
        history_layout.addWidget(self.clear_history_button)
        self.xlsx_history_group.setLayout(history_layout)
        self.xlsx_history_group.setVisible(False)
        
        self.result_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.result_splitter.addWidget(self.result_group)
        self.result_splitter.addWidget(self.xlsx_history_group)
        self.result_splitter.setSizes([int(self.width() * 0.7), int(self.width() * 0.3)])
        
        log_group = QGroupBox("로그")
        log_layout = QVBoxLayout()
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        log_group.setLayout(log_layout)

        # 수직 스플리터 - 작업 그룹 크기 조절 비활성화
        middle_splitter = QSplitter(Qt.Orientation.Vertical)
        middle_splitter.addWidget(key_group)
        middle_splitter.addWidget(work_group)
        middle_splitter.addWidget(self.result_splitter)
        middle_splitter.addWidget(log_group)
        # 작업 그룹의 인덱스(1)에 대해 크기 조절 비활성화
        middle_splitter.setStretchFactor(0, 1)  # 비교 설정은 확장 가능
        middle_splitter.setStretchFactor(1, 0)  # 작업은 고정
        middle_splitter.setStretchFactor(2, 1)  # 결과는 확장 가능
        middle_splitter.setStretchFactor(3, 1)  # 로그는 확장 가능
        # 초기 크기 설정 (작업 그룹 고정 크기 유지)
        middle_splitter.setSizes([200, work_group.height(), 300, 200])

        main_layout.addWidget(file_group)
        main_layout.addWidget(middle_splitter)
        
        self.key_list.itemSelectionChanged.connect(self.update_key_selection_info)
        self.exclude_list.itemSelectionChanged.connect(self.update_exclude_selection_info)
    
    def open_history_file(self, item):
        file_path = item.data(Qt.ItemDataRole.UserRole)
        try:
            os.startfile(file_path)
        except Exception as ex:
            QMessageBox.warning(self, "경고", f"파일 열기 실패: {ex}")
    
    def delete_selected_history_files(self):
        selected_items = self.xlsx_history_list.selectedItems()
        for item in selected_items:
            file_path = item.data(Qt.ItemDataRole.UserRole)
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as ex:
                QMessageBox.warning(self, "경고", f"파일 삭제 실패: {ex}")
            row = self.xlsx_history_list.row(item)
            self.xlsx_history_list.takeItem(row)
        if self.xlsx_history_list.count() == 0:
            self.xlsx_history_group.setVisible(False)
            self.result_splitter.setSizes([self.result_group.sizeHint().width(), 0])
    
    def clear_history(self):
        reply = QMessageBox.question(self, "확인", "모든 이력을 삭제하시겠습니까?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            count = self.xlsx_history_list.count()
            for i in range(count-1, -1, -1):
                item = self.xlsx_history_list.item(i)
                file_path = item.data(Qt.ItemDataRole.UserRole)
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as ex:
                    QMessageBox.warning(self, "경고", f"파일 삭제 실패: {ex}")
                self.xlsx_history_list.takeItem(i)
            self.xlsx_history_group.setVisible(False)
            self.result_splitter.setSizes([self.result_group.sizeHint().width(), 0])
    
    def log_message(self, msg):
        self.log_text.append(log_with_timestamp(msg))
        # 스크롤바를 최하단으로 이동
        scrollbar = self.log_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def load_file1(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV File 1", "", "CSV Files (*.csv)")
        if file_path:
            self.file1_entry.setText(file_path)
            delim = self.file1_delimiter_combo.currentData() or "\t"
            self.file1_progress = QProgressDialog("파일 로딩 중...", "취소", 0, 0, self)
            self.file1_progress.setWindowModality(Qt.WindowModality.WindowModal)
            self.file1_progress.show()
            self.file1_load_button.setEnabled(False)
            self.file1_load_button.setText("로딩중...")
            na_values = self.default_na_values + self.additional_na_values
            self.file1_worker = FileLoadWorker(file_path, na_values, delim)
            self.file1_worker.log_signal.connect(lambda msg: self.log_message("[파일 1] " + msg))
            self.file1_worker.finished_signal.connect(self.on_file1_loaded)
            self.file1_worker.error_signal.connect(lambda e: (self.file1_progress.close(), QMessageBox.critical(self, "Error", f"파일 1 로드 오류: {e}")))
            self.file1_worker.start()
    
    def on_file1_loaded(self, df, delimiter, headers):
        self.df1 = df
        self.file1_headers = headers
        self.file1_delimiter = delimiter
        self.log_message(f"[파일 1] 로드 완료: 행 수 = {len(df)}, 컬럼 수 = {len(df.columns)}")
        self.file1_progress.close()
        self.file1_load_button.setText(f"로딩완료: 행 수 = {len(df)}")
        self.update_list_widgets()
        self.update_compare_button_state()
    
    def reset_file1(self):
        self.file1_path = ""
        self.file1_headers = []
        self.file1_delimiter = "\t"
        self.df1 = None
        self.file1_entry.clear()
        self.file1_load_button.setText("찾기")
        self.file1_load_button.setEnabled(True)
        self.log_message("[파일 1] 초기화 완료")
        self.update_list_widgets()
        self.update_compare_button_state()
    
    def load_file2(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV File 2", "", "CSV Files (*.csv)")
        if file_path:
            self.file2_entry.setText(file_path)
            delim = self.file2_delimiter_combo.currentData() or "\t"
            self.file2_progress = QProgressDialog("파일 로딩 중...", "취소", 0, 0, self)
            self.file2_progress.setWindowModality(Qt.WindowModality.WindowModal)
            self.file2_progress.show()
            self.file2_load_button.setEnabled(False)
            self.file2_load_button.setText("로딩중...")
            na_values = self.default_na_values + self.additional_na_values
            self.file2_worker = FileLoadWorker(file_path, na_values, delim)
            self.file2_worker.log_signal.connect(lambda msg: self.log_message("[파일 2] " + msg))
            self.file2_worker.finished_signal.connect(self.on_file2_loaded)
            self.file2_worker.error_signal.connect(lambda e: (self.file2_progress.close(), QMessageBox.critical(self, "Error", f"파일 2 로드 오류: {e}")))
            self.file2_worker.start()
    
    def on_file2_loaded(self, df, delimiter, headers):
        self.df2 = df
        self.file2_headers = headers
        self.file2_delimiter = delimiter
        self.log_message(f"[파일 2] 로드 완료: 행 수 = {len(df)}, 컬럼 수 = {len(df.columns)}")
        self.file2_progress.close()
        self.file2_load_button.setText(f"로딩완료: 행 수 = {len(df)}")
        self.update_list_widgets()
        self.update_compare_button_state()
    
    def reset_file2(self):
        self.file2_path = ""
        self.file2_headers = []
        self.file2_delimiter = "\t"
        self.df2 = None
        self.file2_entry.clear()
        self.file2_load_button.setText("찾기")
        self.file2_load_button.setEnabled(True)
        self.log_message("[파일 2] 초기화 완료")
        self.update_list_widgets()
        self.update_compare_button_state()
    
    # ---------- update_list_widgets() 수정 부분 ----------
    def update_list_widgets(self):
        self.key_list.clear()
        self.exclude_list.clear()
        self.sort_available_list.clear()   # 정렬 컬럼 리스트 초기화 추가
        headers = self.file1_headers if self.file1_headers else self.file2_headers
        if headers:
            for header in headers:
                self.key_list.addItem(QListWidgetItem(header))
                self.exclude_list.addItem(QListWidgetItem(header))
                self.sort_available_list.addItem(QListWidgetItem(header))  # 정렬 컬럼 리스트에도 추가
        self.update_selected_key_list()
        self.update_selected_exclude_list()
    
    def update_selected_key_list(self):
        self.selected_key_list.clear()
        for item in self.key_list.selectedItems():
            self.selected_key_list.addItem(QListWidgetItem(item.text()))
        count = len(self.key_list.selectedItems())
        self.selected_key_label.setText(f"선택 Outer Join Key ({count}개)")
    
    def update_selected_exclude_list(self):
        self.selected_exclude_list.clear()
        for item in self.exclude_list.selectedItems():
            self.selected_exclude_list.addItem(QListWidgetItem(item.text()))
        count = len(self.exclude_list.selectedItems())
        self.selected_exclude_label.setText(f"선택 비교 제외 컬럼 ({count}개)")
    
    def update_key_selection_info(self):
        self.update_selected_key_list()
    
    def update_exclude_selection_info(self):
        self.update_selected_exclude_list()
    
    def update_compare_button_state(self):
        if (self.file1_entry.text() and self.file2_entry.text() and 
            self.file1_load_button.text().startswith("로딩완료") and self.file2_load_button.text().startswith("로딩완료") and
            self.file1_headers and self.file2_headers):
            self.compare_button.setEnabled(True)
            self.save_button.setEnabled(False)
        else:
            self.compare_button.setEnabled(False)
            self.save_button.setEnabled(False)
    
    def reorder_loaded_data(self):
        if self.df1 is None and self.df2 is None:
            QMessageBox.warning(self, "경고", "로드된 데이터가 없습니다.")
            return
        if self.sort_model._data.empty:
            QMessageBox.warning(self, "경고", "정렬 조건을 선택하세요.")
            return

        sort_columns = self.sort_model._data["컬럼"].tolist()
        sort_orders = [True if cond == "ASC" else False for cond in self.sort_model._data["조건"]]

        # 선택된 정렬 조건을 "컬럼명 조건" 형식의 문자열로 조합 (쿼리처럼)
        sort_conditions_str = ", ".join([f"{col} {('ASC' if order else 'DESC')}" 
                                        for col, order in zip(sort_columns, sort_orders)])

        # 재정렬 전에 두 DataFrame의 전체 행 수 중 최대값을 구함 (글로벌 최대값)
        global_max = 0

        if self.df1 is not None:
            valid_columns = [col for col in sort_columns if col in self.df1.columns]
            valid_orders = [sort_orders[i] for i, col in enumerate(sort_columns) if col in self.df1.columns]
            if valid_columns:
                # 대소문자 무시를 위해 임시 컬럼 생성
                temp_columns = []
                for col in valid_columns:
                    temp_col = f"{col}_lower"
                    self.df1[temp_col] = self.df1[col].str.lower()
                    temp_columns.append(temp_col)
                self.df1 = self.df1.sort_values(by=temp_columns, ascending=valid_orders)
                self.df1.drop(columns=temp_columns, inplace=True)
                self.df1.reset_index(drop=True, inplace=True)  # 인덱스 재설정
                self.log_message(f"[파일 1] 재정렬 완료 (대소문자 무시): {sort_conditions_str}")

        if self.df2 is not None:
            valid_columns = [col for col in sort_columns if col in self.df2.columns]
            valid_orders = [sort_orders[i] for i, col in enumerate(sort_columns) if col in self.df2.columns]
            if valid_columns:
                temp_columns = []
                for col in valid_columns:
                    temp_col = f"{col}_lower"
                    self.df2[temp_col] = self.df2[col].str.lower()
                    temp_columns.append(temp_col)
                self.df2 = self.df2.sort_values(by=temp_columns, ascending=valid_orders)
                self.df2.drop(columns=temp_columns, inplace=True)
                self.df2.reset_index(drop=True, inplace=True)
                self.log_message(f"[파일 2] 재정렬 완료 (대소문자 무시): {sort_conditions_str}")

        # LPAD 처럼 왼쪽 0 채우기를 위해 최대 길이 계산 (글로벌 최대 행 수의 자릿수)
        width = len(str(global_max))

        if self.df1 is not None:
            self.df1["__cmp_order__"] = self.df1.index.astype(str).str.zfill(width)
        if self.df2 is not None:
            self.df2["__cmp_order__"] = self.df2.index.astype(str).str.zfill(width)

        # 재정렬 순번 컬럼이 Outer Join Key로 사용되도록 헤더 업데이트
        if self.df1 is not None:
            self.file1_headers = list(self.df1.columns)
        if self.df2 is not None:
            self.file2_headers = list(self.df2.columns)
        self.update_list_widgets()

        # self.log_message(f"CSV1 재정렬 후 (상위 5행): {self.df1.head().to_string()}")

        QMessageBox.information(self, "재정렬 완료", "로드된 데이터가 선택된 조건으로 재정렬되었습니다.")

    
    def compare_files(self):
        self.result_view.setModel(None)
        self.compare_result = None
        key_columns = [item.text() for item in self.key_list.selectedItems()]
        exclude_columns = [item.text() for item in self.exclude_list.selectedItems()]

        # 재정렬 후 __cmp_order__를 Outer Join Key에 추가
        if self.df1 is not None and "__cmp_order__" in self.df1.columns:
            if "__cmp_order__" not in key_columns:
                key_columns.append("__cmp_order__")
            if "__cmp_order__" not in exclude_columns:
                exclude_columns.append("__cmp_order__")

        if not key_columns:
            QMessageBox.critical(self, "Error", "Outer Join Key를 선택하세요.")
            return

        outer_keys = ", ".join(key_columns)
        exclude_cols = ", ".join(exclude_columns) if exclude_columns else "없음"
        null_values = [self.null_list_widget.item(i).text() for i in range(self.null_list_widget.count())]
        null_values_str = ", ".join(null_values) if null_values else "없음"
        ignore_case_str = "적용" if self.ignore_case_checkbox.isChecked() else "미적용"
        numeric_compare_str = "적용" if self.numeric_compare_checkbox.isChecked() else "미적용"
        confirm_msg = "다음 설정으로 비교를 진행합니다.\n\n"
        confirm_msg += f"Outer Join Key: {outer_keys}\n"
        confirm_msg += f"비교 제외 컬럼: {exclude_cols}\n"
        confirm_msg += f"NULL 치환 문자열: {null_values_str}\n"
        confirm_msg += f"대소문자 무시: {ignore_case_str}\n"
        confirm_msg += f"숫자 실수 비교: {numeric_compare_str}\n\n"
        confirm_msg += "비교를 진행하시겠습니까?"
        ret = QMessageBox.question(self, "최종 설정 확인", confirm_msg, QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ret != QMessageBox.StandardButton.Yes:
            return
        self.log_message("비교 시작...")
        progress = QProgressDialog("파일 비교 진행 중...", "취소", 0, 100, self)
        progress.setWindowTitle("진행 상황")
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.setAutoClose(False)
        progress.setAutoReset(False)
        progress.canceled.connect(lambda: self.compare_button.setEnabled(True))
        progress.show()
        self.compare_button.setEnabled(False)

        file1_df = self.df1
        file2_df = self.df2

        combined_na_values = self.default_na_values + self.additional_na_values
        self.worker = CompareWorker(
            self.file1_entry.text(), self.file2_entry.text(),
            self.file1_delimiter, self.file2_delimiter,
            key_columns, exclude_columns, combined_na_values,
            self.ignore_case_checkbox.isChecked(),
            self.numeric_compare_checkbox.isChecked(),
            file1_df, file2_df
        )
        self.worker.progress_signal.connect(progress.setValue)
        self.worker.log_signal.connect(self.log_message)
        self.worker.finished_signal.connect(lambda merge_result: self.on_compare_finished(merge_result, progress))
        self.worker.error_signal.connect(self.on_compare_error)
        self.worker.start()
    
    def on_compare_finished(self, merge_result, progress):
        total_count = len(merge_result)
        ok_count = (merge_result["Status"] == "Both (OK)").sum()
        both_fail = (merge_result["Status"] == "Both (FAIL)").sum()
        csv1_only = (merge_result["Status"] == "CSV1 only").sum()
        csv2_only = (merge_result["Status"] == "CSV2 only").sum()
        self.log_message(f"비교 완료: 전체 건수 = {total_count}, Both OK = {ok_count}, Both FAIL = {both_fail}, CSV1 only = {csv1_only}, CSV2 only = {csv2_only}")

        # CompareWorker에서 이미 ORDER로 정렬되었으므로 추가 정렬 불필요
        self.populate_result_view(merge_result)
        self.compare_result = merge_result
        self.compare_button.setEnabled(False)
        self.save_button.setEnabled(True)
        progress.close()
        self.filter_combo.clear()
        self.filter_combo.addItem(f"전체 ({total_count}건)")
        self.filter_combo.addItem(f"Both OK ({ok_count}건)")
        self.filter_combo.addItem(f"Both FAIL ({both_fail}건)")
        self.filter_combo.addItem(f"CSV1 only ({csv1_only}건)")
        self.filter_combo.addItem(f"CSV2 only ({csv2_only}건)")
        QMessageBox.information(self, "비교 완료", "파일 비교가 완료되었습니다.")
    
    def on_compare_error(self, e):
        QMessageBox.critical(self, "Error", f"파일 비교 오류: {e}")
        self.log_message(f"파일 비교 오류: {e}")
        self.compare_button.setEnabled(True)
    
    def populate_result_view(self, df):
        model = DataFrameModel(df)
        self.proxy_model.setSourceModel(model)
        if "Status" in df.columns:
            idx = list(df.columns).index("Status")
            self.proxy_model.setFilterKeyColumn(idx)
        self.result_view.setModel(self.proxy_model)
        self.result_view.resizeColumnsToContents()
    
    def apply_filter(self, text):
        QTimer.singleShot(100, lambda: self._update_filter(text))
    
    def _update_filter(self, text):
        if text.startswith("전체"):
            self.proxy_model.setFilterStatus("")
        elif text.startswith("Both OK"):
            self.proxy_model.setFilterStatus("Both (OK)")
        elif text.startswith("Both FAIL"):
            self.proxy_model.setFilterStatus("Both (FAIL)")
        elif text.startswith("CSV1 only"):
            self.proxy_model.setFilterStatus("CSV1 only")
        elif text.startswith("CSV2 only"):
            self.proxy_model.setFilterStatus("CSV2 only")
    
    def save_to_xlsx(self):
        if self.compare_result is None or self.compare_result.empty:
            QMessageBox.warning(self, "경고", "저장할 비교 결과가 없습니다.")
            return
        csv1_path = self.file1_entry.text()
        if not csv1_path:
            QMessageBox.warning(self, "경고", "CSV 파일 1 정보가 없습니다.")
            return
        base, _ = os.path.splitext(os.path.basename(csv1_path))
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        default_name = f"{base}_RESULT_{timestamp}.xlsx"
        default_path = os.path.join(os.path.dirname(csv1_path), default_name)
        file_path = default_path
        self.log_message(f"XLSX 저장 시작: {file_path}")
        self.xlsx_progress = QProgressDialog("XLSX 저장 진행 중...", "취소", 0, 100, self)
        self.xlsx_progress.setWindowTitle("XLSX 저장")
        self.xlsx_progress.setWindowModality(Qt.WindowModality.WindowModal)
        self.xlsx_progress.setAutoClose(False)
        self.xlsx_progress.setAutoReset(False)
        self.xlsx_progress.show()
        self.xlsx_worker = SaveXlsxWorker(self.compare_result, file_path)
        self.xlsx_worker.progress_signal.connect(self.xlsx_progress.setValue)
        self.xlsx_worker.log_signal.connect(self.log_message)
        self.xlsx_worker.finished_signal.connect(lambda: self.on_xlsx_saved(file_path))
        self.xlsx_worker.error_signal.connect(lambda e: QMessageBox.critical(self, "Error", f"XLSX 저장 중 오류: {e}"))
        self.xlsx_worker.start()
    
    def on_xlsx_saved(self, file_path):
        self.xlsx_progress.close()
        msg = f"비교 결과가 XLSX 파일로 저장되었습니다: {file_path}"
        ret = QMessageBox.information(self, "저장 완료", f"{msg}\n파일을 열려면, 'Yes'를 선택하세요.", 
                                      QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ret == QMessageBox.StandardButton.Yes:
            try:
                os.startfile(file_path)
            except Exception as ex:
                QMessageBox.warning(self, "경고", f"파일 열기 실패: {ex}")
        self.log_message(msg)
        item = QListWidgetItem(os.path.basename(file_path))
        item.setData(Qt.ItemDataRole.UserRole, file_path)
        self.xlsx_history_list.insertItem(0, item)
        self.xlsx_history_group.setVisible(True)
        self.result_splitter.setSizes([int(self.width() * 0.7), int(self.width() * 0.3)])
    
    def reset(self):
        self.file1_path = ""
        self.file2_path = ""
        self.file1_headers = []
        self.file2_headers = []
        self.df1 = None
        self.df2 = None
        self.file1_entry.clear()
        self.file2_entry.clear()
        self.key_list.clear()
        self.exclude_list.clear()
        self.selected_key_list.clear()
        self.selected_exclude_list.clear()
        self.sort_available_list.clear()
        self.sort_model._data = pd.DataFrame(columns=["컬럼", "조건"])
        self.sort_model.layoutChanged.emit()
        self.update_sort_selection()
        self.additional_na_values = []
        self.exclude_columns = []  # reset에서 exclude_columns 초기화 추가
        self.null_list_widget.clear()
        for val in self.default_na_values:
            display_val = "<빈 문자열>" if val == "" else val
            self.null_list_widget.addItem(QListWidgetItem(display_val))
        self.result_view.setModel(None)
        self.log_text.clear()
        self.compare_result = None
        self.compare_button.setEnabled(False)
        self.save_button.setEnabled(False)
        self.file1_load_button.setText("찾기")
        self.file1_load_button.setEnabled(True)
        self.file2_load_button.setText("찾기")
        self.file2_load_button.setEnabled(True)
        self.filter_combo.clear()
        self.filter_combo.addItems(["전체", "Both OK", "Both FAIL", "CSV1 only", "CSV2 only"])
        self.proxy_model = StatusFilterProxyModel()
        self.result_view.setModel(self.proxy_model)
        self.log_message("전체 초기화 완료")
        self.xlsx_history_list.clear()
        self.xlsx_history_group.setVisible(False)
        self.result_splitter.setSizes([self.width(), 0])
    
    def update_exclude_list(self):
        selected_keys = {item.text() for item in self.key_list.selectedItems()}
        for i in range(self.exclude_list.count()):
            item = self.exclude_list.item(i)
            text = item.text().replace(" (선택 불가)", "")
            if text in selected_keys:
                item.setSelected(False)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsSelectable)
                item.setText(f"{text} (선택 불가)")
            else:
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsSelectable)
                item.setText(text)
    
    def add_null_value(self):
        text = self.null_line_edit.text().strip()
        if text and text not in self.additional_na_values:
            self.additional_na_values.append(text)
            self.null_list_widget.addItem(QListWidgetItem(text))
            self.null_line_edit.clear()
    
    def delete_null_value(self):
        selected = self.null_list_widget.selectedItems()
        if not selected:
            return
        for item in selected:
            val = item.text()
            if val in self.additional_na_values:
                self.additional_na_values.remove(val)
            self.null_list_widget.takeItem(self.null_list_widget.row(item))
    
    def update_csv_delimiter_combos(self):
        self.file1_delimiter_combo.clear()
        self.file2_delimiter_combo.clear()
        all_delims = self.default_csv_delimiters + self.additional_csv_delimiters
        for delim in all_delims:
            display_text = "<탭>" if delim == "\t" else "<콤마>" if delim == "," else delim
            self.file1_delimiter_combo.addItem(display_text, delim)
            self.file2_delimiter_combo.addItem(display_text, delim)
        index = self.file1_delimiter_combo.findData("\t")
        if index != -1:
            self.file1_delimiter_combo.setCurrentIndex(index)
        index = self.file2_delimiter_combo.findData("\t")
        if index != -1:
            self.file2_delimiter_combo.setCurrentIndex(index)

    def update_csv_delim_list_widget(self):
        self.csv_delim_list_widget.clear()
        all_delims = self.default_csv_delimiters + self.additional_csv_delimiters
        for delim in all_delims:
            display_text = "<탭>" if delim == "\t" else "<콤마>" if delim == "," else delim
            self.csv_delim_list_widget.addItem(display_text)

    def add_csv_delimiter(self):
        text = self.csv_delim_line_edit.text().strip()
        if text and text not in self.default_csv_delimiters and text not in self.additional_csv_delimiters:
            self.additional_csv_delimiters.append(text)
            self.csv_delim_line_edit.clear()
            self.update_csv_delim_list_widget()
            self.update_csv_delimiter_combos()

    def delete_csv_delimiter(self):
        selected_items = self.csv_delim_list_widget.selectedItems()
        for item in selected_items:
            actual = "\t" if item.text() == "<탭>" else "," if item.text() == "<콤마>" else item.text()
            if actual in self.additional_csv_delimiters:
                self.additional_csv_delimiters.remove(actual)
        self.update_csv_delim_list_widget()
        self.update_csv_delimiter_combos()

    def clear_selected_key(self):
        self.key_list.clearSelection()
        self.update_selected_key_list()

    def clear_selected_exclude(self):
        self.exclude_list.clearSelection()
        self.update_selected_exclude_list()

    # ---------- 새로운 함수 추가 ----------
    def toggle_sort_column(self, item):
        column = item.text()
        # 이미 선택되어 있는지 확인
        if column in self.sort_model._data["컬럼"].values:
            # 이미 선택되어 있으면 제거 (토글 off)
            self.sort_model._data = self.sort_model._data[self.sort_model._data["컬럼"] != column].reset_index(drop=True)
        else:
            # 선택되지 않은 경우 "ASC" 조건으로 추가 (토글 on)
            new_row = pd.DataFrame([[column, "ASC"]], columns=["컬럼", "조건"])
            self.sort_model._data = pd.concat([self.sort_model._data, new_row], ignore_index=True)
        self.sort_model.layoutChanged.emit()
        self.update_sort_selection()

    def clear_sort_selection(self):
        self.sort_model._data = pd.DataFrame(columns=["컬럼", "조건"])
        self.sort_model.layoutChanged.emit()
        self.update_sort_selection()

    def update_sort_selection(self):
        count = len(self.sort_model._data)
        self.sort_selected_label.setText(f"선택 정렬 컬럼 ({count}개)")

    # --- 조건 컬럼 토글 함수 추가 ---
    def toggle_sort_order(self, index):
        # 조건 컬럼은 두번째 열(index 1)임
        if index.column() == 1:
            row = index.row()
            current_value = self.sort_model._data.iat[row, 1]
            new_value = "DESC" if current_value == "ASC" else "ASC"
            self.sort_model._data.iat[row, 1] = new_value
            self.sort_model.dataChanged.emit(index, index)

    # 도움말 다이얼로그 표시 함수 추가
    def show_help_dialog(self):
        help_dialog = QDialog(self)
        help_dialog.setWindowTitle("도움말")
        help_dialog.setGeometry(200, 200, 450, 300)
        
        layout = QVBoxLayout()
        help_text = QTextEdit()
        help_text.setReadOnly(True)
        help_text.setText(
            "CSV Comparer 사용 방법\n\n"
            "1. 파일 선택\n"
            "   - 'CSV 파일 1'과 'CSV 파일 2'를 찾아서 로드하세요.\n"
            "   - 구분자는 콤보 박스에서 선택하거나 사용자 정의로 추가 가능합니다.\n\n"
            "2. 비교 설정\n"
            "   - 'Outer Join Key': 비교 시 기준이 되는 컬럼을 선택하세요.\n"
            "   - '비교 제외 컬럼': 비교에서 제외할 컬럼을 선택하세요.\n"
            "   - '재정렬': 데이터를 정렬할 컬럼과 순서를 설정하세요.\n"
            "   - 'NULL 치환 문자열': NULL로 간주할 값을 추가/삭제하세요.\n\n"
            "3. 작업\n"
            "   - '대소문자 무시': 문자열 비교 시 대소문자를 무시합니다.\n"
            "   - '숫자 실수 비교': 숫자를 실수로 비교합니다 (예: 0.00 = 0).\n"
            "   - '비교': 두 파일을 비교합니다.\n"
            "   - 'XLSX 저장': 결과를 엑셀 파일로 저장합니다.\n\n"
            "4. 결과\n"
            "   - 필터를 사용해 결과를 확인하세요.\n\n"
            "문의: dh05.seo@emro.co.kr"
        )
        layout.addWidget(help_text)
        
        close_button = QPushButton("닫기")
        close_button.clicked.connect(help_dialog.accept)
        layout.addWidget(close_button)
        
        help_dialog.setLayout(layout)
        help_dialog.exec()

    # 정보 다이얼로그 표시 함수 추가
    def show_about_dialog(self):
        QMessageBox.about(self, "정보", 
                          f"CSV Comparer {VERSION}\n"
                          "CSV 파일 비교 도구\n"
                          "개발자: D.SEO\n"
                          "최종 업데이트: 2025-03-05")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CSVComparerApp()
    window.show()
    sys.exit(app.exec())