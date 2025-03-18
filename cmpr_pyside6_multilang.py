import sys
import os
import csv
import pandas as pd
import numpy as np
from datetime import datetime
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QLabel, QLineEdit, QPushButton, QFileDialog, QProgressDialog, QMessageBox,
    QTextEdit, QListWidget, QListWidgetItem, QAbstractItemView, QTableView, QComboBox, QCheckBox, QSplitter, QDialog, QTabWidget, QMenu
)
from PySide6.QtCore import Qt, QAbstractTableModel, QThread, Signal, QSortFilterProxyModel, QTimer
from PySide6.QtGui import QColor, QBrush, QIcon
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name
from pandas.api.types import is_string_dtype
from PySide6.QtWidgets import QHeaderView
import resources
import json

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

class FileLoadWorker(QThread):
    finished_signal = Signal(object, object, object)
    log_signal = Signal(str)
    error_signal = Signal(str)

    def __init__(self, file_path, na_values, delimiter):
        super().__init__()
        self.file_path = file_path
        self.na_values = na_values
        self.delimiter = delimiter

    def run(self):
        try:
            self.log_signal.emit(f"Start loading file: {self.file_path}")
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
            self.log_signal.emit(f"File loaded complete: {self.file_path}, number of rows: {len(df)}, number of columns: {len(df.columns)}")
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
    def __init__(self, data=pd.DataFrame(columns=["column", "order"]), parent=None):
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
    progress_signal = Signal(int)
    log_signal = Signal(str)
    finished_signal = Signal(pd.DataFrame)
    error_signal = Signal(str)

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
        self.file1_df = file1_df
        self.file2_df = file2_df

    def run(self):
        try:
            if self.file1_df is None:
                self.log_signal.emit("Start loading CSV file 1 data...")
                df1 = self.load_csv_data(self.file1_path, self.file1_delimiter)
            else:
                df1 = self.file1_df.copy()
                self.log_signal.emit("Using the rearranged data in CSV file 1")
            df1.columns = normalize_headers(df1.columns.tolist())
            norm_keys = [k.strip().lower() for k in self.key_columns]
            self.progress_signal.emit(20)

            if self.file2_df is None:
                self.log_signal.emit("Start loading CSV file 2 data...")
                df2 = self.load_csv_data(self.file2_path, self.file2_delimiter)
            else:
                df2 = self.file2_df.copy()
                self.log_signal.emit("Using the rearranged data in CSV file 2")
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

            self.log_signal.emit("Performing Outer Join...")
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
            self.log_signal.emit(f"Merge complete: Total {len(merge_result)} rows")
            if "original_index_csv1" in merge_result.columns:
                merge_result = merge_result.sort_values(by="original_index_csv1", na_position='last')
                merge_result.drop(columns=["original_index_csv1"], inplace=True, errors='ignore')
            else:
                df1_order = df1[norm_keys].reset_index().rename(columns={"index": "original_index_csv1"})
                merge_result = pd.merge(df1_order, merge_result, how='right', on=norm_keys)
                merge_result = merge_result.sort_values(by="original_index_csv1", na_position='last')
                merge_result.drop(columns=["original_index_csv1"], inplace=True)

            merge_result.reset_index(drop=True, inplace=True)

            self.log_signal.emit("Start comparing common columns...")
            common_columns = [col for col in df1.columns
                              if col not in norm_keys and col not in ["csv1_order", "order"]
                              and col in df2.columns and col not in self.exclude_columns]
            both = merge_result['_merge'] == 'both'
            left_only = merge_result['_merge'] == 'left_only'
            right_only = merge_result['_merge'] == 'right_only'

            def compare_cells(self, x, y):
                    try:
                        is_x_numeric = isinstance(x, (int, float)) or (isinstance(x, str) and x.replace(',', '').replace('.', '', 1).lstrip('-').isdigit())
                        is_y_numeric = isinstance(y, (int, float)) or (isinstance(y, str) and y.replace(',', '').replace('.', '', 1).lstrip('-').isdigit())

                        if is_x_numeric and is_y_numeric and self.numeric_compare:
                            try:
                                nx = float(str(x).replace(',', ''))
                                ny = float(str(y).replace(',', ''))
                                if nx == ny:
                                    return x
                                else:
                                    return f"[{x}] != [{y}]"
                            except ValueError:
                                pass
                        elif is_x_numeric and is_y_numeric:
                            pass
                        else:
                            if self.ignore_case:
                                if str(x).lower() == str(y).lower():
                                    return x
                                else:
                                    return f"[{x}] != [{y}]"

                        if x == y:
                            return x
                        else:
                            return f"[{x}] != [{y}]"
                    except Exception as e:
                        self.log_signal.emit(f"compare_cells error: {e}")
                        return f"[{x}] != [{y}]"

            for col in common_columns:
                col_x, col_y = f"{col}_x", f"{col}_y"
                self.log_signal.emit(f"Column comparison: {col} in progress...")
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
            merge_result["Remark"] = remarks

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

            new_columns = []
            for col in final_result.columns:
                if col in norm_keys or col in common_columns or col in ["Status", "Remark", "ORDER"]:
                    new_columns.append(col)
                else:
                    new_columns.append("(X) " + col)
            final_result.columns = new_columns

            self.progress_signal.emit(90)
            self.log_signal.emit("Column comparison and status calculation completed.")
            self.progress_signal.emit(100)
            self.finished_signal.emit(final_result)

        except Exception as e:
            self.error_signal.emit(str(e))

    def load_csv_data(self, file_path, delimiter):
        self.log_signal.emit(f"Start loading file: {file_path}")
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
        self.log_signal.emit(f"File loaded complete: {file_path}, number of rows: {len(df)}, number of columns: {len(df.columns)}")
        return df

class SaveXlsxWorker(QThread):
    progress_signal = Signal(int)
    log_signal = Signal(str)
    finished_signal = Signal()
    error_signal = Signal(str)

    def __init__(self, compare_result, file_path):
        super().__init__()
        self.compare_result = compare_result
        self.file_path = file_path

    def run(self):
        try:
            self.log_signal.emit("Start saving xlsx...")
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
            summary_text = f"All: {total_count}, Both OK: {ok_count}, Both FAIL: {both_fail}, CSV1 only: {csv1_only}, CSV2 only: {csv2_only}"
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
            self.log_signal.emit("xlsx save complete.")
            self.finished_signal.emit()
        except Exception as e:
            self.error_signal.emit(str(e))

class cmprApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.current_language = "ko"  # 기본 언어 설정 (한국어)
        self.load_language_data()  # 언어 데이터 로드
        self.setWindowTitle(self.tr("window_title").format(version=VERSION))
        self.setGeometry(100, 100, 800, 700)
        self.setWindowIcon(QIcon(":/asset/cmpr-icon.png"))
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
        self.sort_model = SortTableModel()
        self.exclude_columns = []
        self.setup_ui()
        self.update_csv_delim_list_widget()
        self.update_csv_delimiter_combos()

    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        menubar = self.menuBar()
        help_menu = menubar.addMenu(self.tr("help_menu"))
        help_action = help_menu.addAction(self.tr("help_action"))
        help_action.triggered.connect(self.show_help_dialog)
        about_action = help_menu.addAction(self.tr("about_action"))
        about_action.triggered.connect(self.show_about_dialog)

        language_menu = menubar.addMenu(self.tr("language_menu"))
        en_action = language_menu.addAction(self.tr("lang_en"))
        ko_action = language_menu.addAction(self.tr("lang_ko"))
        en_action.triggered.connect(lambda: self.change_language("en"))
        ko_action.triggered.connect(lambda: self.change_language("ko"))

        self.file_group = QGroupBox(self.tr("file_group"))
        file_layout = QVBoxLayout()
        file1_layout = QHBoxLayout()
        self.file1_label = QLabel(self.tr("file1_label"))
        self.file1_entry = QLineEdit()
        self.file1_entry.setFixedWidth(300)
        self.file1_entry.setReadOnly(True)
        self.file1_delimiter_combo = QComboBox()
        self.file1_delimiter_combo.setFixedWidth(100)
        self.file1_load_button = QPushButton(self.tr("browse_button"))
        self.file1_load_button.clicked.connect(self.load_file1)
        self.file1_reset_button = QPushButton(self.tr("reset_button"))
        self.file1_reset_button.clicked.connect(self.reset_file1)
        file1_layout.addWidget(self.file1_label)
        file1_layout.addWidget(self.file1_entry)
        file1_layout.addWidget(self.file1_delimiter_combo)
        file1_layout.addWidget(self.file1_load_button)
        file1_layout.addWidget(self.file1_reset_button)

        file2_layout = QHBoxLayout()
        self.file2_label = QLabel(self.tr("file2_label"))
        self.file2_entry = QLineEdit()
        self.file2_entry.setFixedWidth(300)
        self.file2_entry.setReadOnly(True)
        self.file2_delimiter_combo = QComboBox()
        self.file2_delimiter_combo.setFixedWidth(100)
        self.file2_load_button = QPushButton(self.tr("browse_button"))
        self.file2_load_button.clicked.connect(self.load_file2)
        self.file2_reset_button = QPushButton(self.tr("reset_button"))
        self.file2_reset_button.clicked.connect(self.reset_file2)
        file2_layout.addWidget(self.file2_label)
        file2_layout.addWidget(self.file2_entry)
        file2_layout.addWidget(self.file2_delimiter_combo)
        file2_layout.addWidget(self.file2_load_button)
        file2_layout.addWidget(self.file2_reset_button)

        file_layout.addLayout(file1_layout)
        file_layout.addLayout(file2_layout)
        self.file_group.setLayout(file_layout)

        self.key_group = QGroupBox(self.tr("key_group"))
        key_layout = QVBoxLayout()
        self.tab_widget = QTabWidget()

        sort_tab = QWidget()
        sort_tab_layout = QHBoxLayout()
        sort_available_layout = QVBoxLayout()
        self.sort_available_label = QLabel(self.tr("sort_available_label"))
        self.sort_available_list = QListWidget()
        self.sort_available_list.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.sort_available_list.itemClicked.connect(self.toggle_sort_column)
        sort_available_layout.addWidget(self.sort_available_label)
        sort_available_layout.addWidget(self.sort_available_list)

        sort_selected_layout = QVBoxLayout()
        self.sort_selected_label = QLabel(self.tr("sort_selected_label").format(count=0))
        self.sort_selected_table = QTableView()
        self.sort_selected_table.setModel(self.sort_model)
        self.sort_selected_table.horizontalHeader().setVisible(False)
        self.sort_selected_table.horizontalHeader().setStretchLastSection(True)
        self.sort_selected_table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.sort_selected_table.doubleClicked.connect(self.toggle_sort_order)
        if self.sort_available_list.count() > 0:
            default_height = self.sort_available_list.sizeHintForRow(0)
            self.sort_selected_table.verticalHeader().setDefaultSectionSize(default_height)
        else:
            self.sort_selected_table.verticalHeader().setDefaultSectionSize(20)

        sort_buttons_layout = QHBoxLayout()
        self.clear_sort_btn = QPushButton(self.tr("clear_sort_button"))
        self.clear_sort_btn.clicked.connect(self.clear_sort_selection)
        self.reorder_btn = QPushButton(self.tr("reorder_button"))
        self.reorder_btn.clicked.connect(self.reorder_loaded_data)
        sort_buttons_layout.addWidget(self.clear_sort_btn)
        sort_buttons_layout.addWidget(self.reorder_btn)
        sort_selected_layout.addWidget(self.sort_selected_label)
        sort_selected_layout.addWidget(self.sort_selected_table)
        sort_selected_layout.addLayout(sort_buttons_layout)

        sort_tab_layout.addLayout(sort_available_layout)
        sort_tab_layout.addLayout(sort_selected_layout)
        sort_tab.setLayout(sort_tab_layout)

        key_tab = QWidget()
        key_tab_layout = QHBoxLayout()
        available_key_layout = QVBoxLayout()
        self.available_key_label = QLabel(self.tr("available_key_label"))
        self.key_list = QListWidget()
        self.key_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self.key_list.itemSelectionChanged.connect(self.update_exclude_list)
        available_key_layout.addWidget(self.available_key_label)
        available_key_layout.addWidget(self.key_list)

        selected_key_layout = QVBoxLayout()
        self.selected_key_label = QLabel(self.tr("selected_key_label").format(count=0))
        self.selected_key_list = QListWidget()
        self.selected_key_list.setDisabled(True)
        self.clear_key_btn = QPushButton(self.tr("clear_key_button"))
        self.clear_key_btn.clicked.connect(self.clear_selected_key)
        selected_key_layout.addWidget(self.selected_key_label)
        selected_key_layout.addWidget(self.selected_key_list)
        selected_key_layout.addWidget(self.clear_key_btn)

        key_tab_layout.addLayout(available_key_layout)
        key_tab_layout.addLayout(selected_key_layout)
        key_tab.setLayout(key_tab_layout)

        exclude_tab = QWidget()
        exclude_tab_layout = QHBoxLayout()
        available_exclude_layout = QVBoxLayout()
        self.available_exclude_label = QLabel(self.tr("available_exclude_label"))
        self.exclude_list = QListWidget()
        self.exclude_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        available_exclude_layout.addWidget(self.available_exclude_label)
        available_exclude_layout.addWidget(self.exclude_list)

        selected_exclude_layout = QVBoxLayout()
        self.selected_exclude_label = QLabel(self.tr("selected_exclude_label").format(count=0))
        self.selected_exclude_list = QListWidget()
        self.selected_exclude_list.setDisabled(True)
        self.clear_exclude_btn = QPushButton(self.tr("clear_exclude_button"))
        self.clear_exclude_btn.clicked.connect(self.clear_selected_exclude)
        selected_exclude_layout.addWidget(self.selected_exclude_label)
        selected_exclude_layout.addWidget(self.selected_exclude_list)
        selected_exclude_layout.addWidget(self.clear_exclude_btn)

        exclude_tab_layout.addLayout(available_exclude_layout)
        exclude_tab_layout.addLayout(selected_exclude_layout)
        exclude_tab.setLayout(exclude_tab_layout)

        null_tab = QWidget()
        null_layout = QVBoxLayout()
        self.null_label = QLabel(self.tr("null_label"))
        add_layout = QHBoxLayout()
        self.null_line_edit = QLineEdit()
        self.null_line_edit.setPlaceholderText(self.tr("null_placeholder"))
        self.add_null_button = QPushButton(self.tr("add_button"))
        self.add_null_button.clicked.connect(self.add_null_value)
        self.delete_null_button = QPushButton(self.tr("delete_button"))
        self.delete_null_button.clicked.connect(self.delete_null_value)
        add_layout.addWidget(self.null_line_edit)
        add_layout.addWidget(self.add_null_button)
        add_layout.addWidget(self.delete_null_button)
        self.null_list_widget = QListWidget()
        for val in self.default_na_values:
            display_val = "<blank>" if val == "" else val
            self.null_list_widget.addItem(QListWidgetItem(display_val))
        null_layout.addWidget(self.null_label)
        null_layout.addLayout(add_layout)
        null_layout.addWidget(self.null_list_widget)
        null_tab.setLayout(null_layout)

        csv_delim_tab = QWidget()
        csv_delim_layout = QVBoxLayout()
        self.csv_delim_label = QLabel(self.tr("csv_delim_label"))
        add_csv_delim_layout = QHBoxLayout()
        self.csv_delim_line_edit = QLineEdit()
        self.csv_delim_line_edit.setPlaceholderText(self.tr("csv_delim_placeholder"))
        self.add_csv_delim_button = QPushButton(self.tr("add_button"))
        self.add_csv_delim_button.clicked.connect(self.add_csv_delimiter)
        self.delete_csv_delim_button = QPushButton(self.tr("delete_button"))
        self.delete_csv_delim_button.clicked.connect(self.delete_csv_delimiter)
        add_csv_delim_layout.addWidget(self.csv_delim_line_edit)
        add_csv_delim_layout.addWidget(self.add_csv_delim_button)
        add_csv_delim_layout.addWidget(self.delete_csv_delim_button)
        self.csv_delim_list_widget = QListWidget()
        csv_delim_layout.addWidget(self.csv_delim_label)
        csv_delim_layout.addLayout(add_csv_delim_layout)
        csv_delim_layout.addWidget(self.csv_delim_list_widget)
        csv_delim_tab.setLayout(csv_delim_layout)

        self.tab_widget.addTab(sort_tab, self.tr("sort_tab"))
        self.tab_widget.addTab(key_tab, self.tr("key_tab"))
        self.tab_widget.addTab(exclude_tab, self.tr("exclude_tab"))
        self.tab_widget.addTab(null_tab, self.tr("null_tab"))
        self.tab_widget.addTab(csv_delim_tab, self.tr("csv_delim_tab"))

        key_layout.addWidget(self.tab_widget)
        self.key_group.setLayout(key_layout)

        self.work_group = QGroupBox(self.tr("work_group"))
        work_layout = QHBoxLayout()
        self.compare_button = QPushButton(self.tr("compare_button"))
        self.compare_button.clicked.connect(self.compare_files)
        self.compare_button.setEnabled(False)
        self.overall_reset_btn = QPushButton(self.tr("overall_reset_button"))
        self.overall_reset_btn.clicked.connect(self.reset)
        self.save_button = QPushButton(self.tr("save_button"))
        self.save_button.clicked.connect(self.save_to_xlsx)
        self.save_button.setEnabled(False)
        self.ignore_case_checkbox = QCheckBox(self.tr("ignore_case_checkbox"))
        self.numeric_compare_checkbox = QCheckBox(self.tr("numeric_compare_checkbox"))
        work_layout.addWidget(self.ignore_case_checkbox)
        work_layout.addWidget(self.numeric_compare_checkbox)
        work_layout.addWidget(self.compare_button)
        work_layout.addWidget(self.overall_reset_btn)
        work_layout.addWidget(self.save_button)
        self.work_group.setLayout(work_layout)
        self.work_group.setFixedHeight(self.work_group.sizeHint().height())

        self.result_group = QGroupBox(self.tr("result_group"))
        result_layout = QVBoxLayout()
        filter_layout = QHBoxLayout()
        self.filter_label = QLabel(self.tr("filter_label"))
        self.filter_combo = QComboBox()
        self.filter_combo.addItems([self.tr("filter_all"), "Both OK", "Both FAIL", "CSV1 only", "CSV2 only"])
        self.filter_combo.currentTextChanged.connect(self.apply_filter)
        filter_layout.addWidget(self.filter_label)
        filter_layout.addWidget(self.filter_combo)
        self.result_view = QTableView()
        self.result_view.setStyleSheet("QTableView { color: darkblue; font-size: 10px; }")
        self.proxy_model = StatusFilterProxyModel()
        result_layout.addLayout(filter_layout)
        result_layout.addWidget(self.result_view)
        self.result_group.setLayout(result_layout)

        self.xlsx_history_group = QGroupBox(self.tr("xlsx_history_group"))
        history_layout = QVBoxLayout()
        self.xlsx_history_list = HistoryListWidget(self)
        self.xlsx_history_list.itemDoubleClicked.connect(self.open_history_file)
        self.clear_history_button = QPushButton(self.tr("clear_history_button"))
        self.clear_history_button.clicked.connect(self.clear_history)
        history_layout.addWidget(self.xlsx_history_list)
        history_layout.addWidget(self.clear_history_button)
        self.xlsx_history_group.setLayout(history_layout)
        self.xlsx_history_group.setVisible(False)

        self.result_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.result_splitter.addWidget(self.result_group)
        self.result_splitter.addWidget(self.xlsx_history_group)
        self.result_splitter.setSizes([int(self.width() * 0.7), int(self.width() * 0.3)])

        self.log_group = QGroupBox(self.tr("log_group"))
        log_layout = QVBoxLayout()
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        self.log_group.setLayout(log_layout)

        middle_splitter = QSplitter(Qt.Orientation.Vertical)
        middle_splitter.addWidget(self.key_group)
        middle_splitter.addWidget(self.work_group)
        middle_splitter.addWidget(self.result_splitter)
        middle_splitter.addWidget(self.log_group)
        middle_splitter.setStretchFactor(0, 1)
        middle_splitter.setStretchFactor(1, 0)
        middle_splitter.setStretchFactor(2, 1)
        middle_splitter.setStretchFactor(3, 1)
        middle_splitter.setSizes([200, self.work_group.height(), 300, 200])

        main_layout.addWidget(self.file_group)
        main_layout.addWidget(middle_splitter)

        self.key_list.itemSelectionChanged.connect(self.update_key_selection_info)
        self.exclude_list.itemSelectionChanged.connect(self.update_exclude_selection_info)

    def open_history_file(self, item):
        file_path = item.data(Qt.ItemDataRole.UserRole)
        try:
            os.startfile(file_path)
        except Exception as ex:
            QMessageBox.warning(self, self.tr("warning_title"), f"Failed to open file: {ex}")

    def delete_selected_history_files(self):
        selected_items = self.xlsx_history_list.selectedItems()
        for item in selected_items:
            file_path = item.data(Qt.ItemDataRole.UserRole)
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as ex:
                QMessageBox.warning(self, self.tr("warning_title"), f"File deletion failed: {ex}")
            row = self.xlsx_history_list.row(item)
            self.xlsx_history_list.takeItem(row)
        if self.xlsx_history_list.count() == 0:
            self.xlsx_history_group.setVisible(False)
            self.result_splitter.setSizes([self.result_group.sizeHint().width(), 0])

    def clear_history(self):
        reply = QMessageBox.question(self, self.tr("confirm"), self.tr("clear_history_confirm"),    # 히스토리 삭제 확인
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
                    QMessageBox.warning(self, self.tr("warning_title"), f"File deletion failed: {ex}")  # 파일 삭제 실패
                self.xlsx_history_list.takeItem(i)
            self.xlsx_history_group.setVisible(False)
            self.result_splitter.setSizes([self.result_group.sizeHint().width(), 0])

    def log_message(self, msg):
        self.log_text.append(log_with_timestamp(msg))
        scrollbar = self.log_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def load_file1(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV File 1", "", "CSV Files (*.csv)")
        if file_path:
            self.file1_entry.setText(file_path)
            delim = self.file1_delimiter_combo.currentData() or "\t"
            self.file1_progress = QProgressDialog(self.tr("progress_loading"), self.tr("cancel"), 0, 0, self)   # 로딩 중 프로그레스바
            self.file1_progress.setWindowModality(Qt.WindowModality.WindowModal)
            self.file1_progress.show()
            self.file1_load_button.setEnabled(False)
            self.file1_load_button.setText(self.tr("loading"))                                                  # 로딩 중 버튼 텍스트 변경
            na_values = self.default_na_values + self.additional_na_values
            self.file1_worker = FileLoadWorker(file_path, na_values, delim)
            self.file1_worker.log_signal.connect(lambda msg: self.log_message("[File 1] " + msg))
            self.file1_worker.finished_signal.connect(self.on_file1_loaded)
            self.file1_worker.error_signal.connect(lambda e: (self.file1_progress.close(), QMessageBox.critical(self, "Error", f"File 1 Load Error: {e}")))
            self.file1_worker.start()

    def on_file1_loaded(self, df, delimiter, headers):
        self.df1 = df
        self.file1_headers = headers
        self.file1_delimiter = delimiter
        self.log_message(f"[File 1] Load completed: row count = {len(df)}, column count = {len(df.columns)}")
        self.file1_progress.close()
        self.file1_load_button.setText(self.tr("loaded").format(count=len(df))) # 로드 완료 버튼 텍스트 변경
        self.update_list_widgets()
        self.update_compare_button_state()

    def reset_file1(self):
        self.file1_path = ""
        self.file1_headers = []
        self.file1_delimiter = "\t"
        self.df1 = None
        self.file1_entry.clear()
        self.file1_load_button.setText(self.tr("browse_button"))    # 찾기기
        self.file1_load_button.setEnabled(True)
        self.log_message("[File 1] Initialization complete")
        self.update_list_widgets()
        self.update_compare_button_state()

    def load_file2(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV File 2", "", "CSV Files (*.csv)")
        if file_path:
            self.file2_entry.setText(file_path)
            delim = self.file2_delimiter_combo.currentData() or "\t"
            self.file2_progress = QProgressDialog(self.tr("progress_loading"), self.tr("cancel"), 0, 0, self)   # 로딩 중 프로그레스바
            self.file2_progress.setWindowModality(Qt.WindowModality.WindowModal)
            self.file2_progress.show()
            self.file2_load_button.setEnabled(False)
            self.file2_load_button.setText(self.tr("loading"))                                                  # 로딩 중 버튼 텍스트 변경
            na_values = self.default_na_values + self.additional_na_values
            self.file2_worker = FileLoadWorker(file_path, na_values, delim)
            self.file2_worker.log_signal.connect(lambda msg: self.log_message("[File 2] " + msg))
            self.file2_worker.finished_signal.connect(self.on_file2_loaded)
            self.file2_worker.error_signal.connect(lambda e: (self.file2_progress.close(), QMessageBox.critical(self, self.tr("error"), f"Error loading file 2: {e}")))
            self.file2_worker.start()

    def on_file2_loaded(self, df, delimiter, headers):
        self.df2 = df
        self.file2_headers = headers
        self.file2_delimiter = delimiter
        self.log_message(f"[File 2] Load completed: row count = {len(df)}, column count = {len(df.columns)}")
        self.file2_progress.close()
        self.file2_load_button.setText(self.tr("loaded").format(count=len(df)))
        self.update_list_widgets()
        self.update_compare_button_state()

    def reset_file2(self):
        self.file2_path = ""
        self.file2_headers = []
        self.file2_delimiter = "\t"
        self.df2 = None
        self.file2_entry.clear()
        self.file2_load_button.setText(self.tr("browse_button"))
        self.file2_load_button.setEnabled(True)
        self.log_message("[File 2] Initialization complete")
        self.update_list_widgets()
        self.update_compare_button_state()

    def update_list_widgets(self):
        self.key_list.clear()
        self.exclude_list.clear()
        self.sort_available_list.clear()
        headers = self.file1_headers if self.file1_headers else self.file2_headers
        if headers:
            for header in headers:
                self.key_list.addItem(QListWidgetItem(header))
                self.exclude_list.addItem(QListWidgetItem(header))
                self.sort_available_list.addItem(QListWidgetItem(header))
        self.update_selected_key_list()
        self.update_selected_exclude_list()

    def update_selected_key_list(self):
        self.selected_key_list.clear()
        for item in self.key_list.selectedItems():
            self.selected_key_list.addItem(QListWidgetItem(item.text()))
        count = len(self.key_list.selectedItems())
        self.selected_key_label.setText(self.tr("selected_key_label").format(count=count))  # 선택 비교 키 컬럼 (n개)

    def update_selected_exclude_list(self):
        self.selected_exclude_list.clear()
        for item in self.exclude_list.selectedItems():
            self.selected_exclude_list.addItem(QListWidgetItem(item.text()))
        count = len(self.exclude_list.selectedItems())
        self.selected_exclude_label.setText(self.tr("selected_exclude_label").format(count=count))  # 선택 제외 컬럼 (n개)

    def update_key_selection_info(self):
        self.update_selected_key_list()

    def update_exclude_selection_info(self):
        self.update_selected_exclude_list()

    def update_compare_button_state(self):
        if (self.file1_entry.text() and self.file2_entry.text() and
            self.file1_load_button.text().startswith(self.tr("file_load_complete")) and self.file2_load_button.text().startswith(self.tr("file_load_complete")) and
            self.file1_headers and self.file2_headers):
            self.compare_button.setEnabled(True)
            self.save_button.setEnabled(False)
        else:
            self.compare_button.setEnabled(False)
            self.save_button.setEnabled(False)

    def reorder_loaded_data(self):
        if self.df1 is None and self.df2 is None:
            QMessageBox.warning(self, self.tr("warning_title"), self.tr("no_data_warning")) # 데이터가 없습니다.
            return
        if self.sort_model._data.empty:
            QMessageBox.warning(self, self.tr("warning_title"), self.tr("no_sort_warning")) # 정렬 조건이 없습니다.
            return

        sort_columns = self.sort_model._data["column"].tolist()
        sort_orders = [True if cond == "ASC" else False for cond in self.sort_model._data["order"]]

        sort_conditions_str = ", ".join([f"{col} {('ASC' if order else 'DESC')}"
                                        for col, order in zip(sort_columns, sort_orders)])

        global_max = 0

        if self.df1 is not None:
            valid_columns = [col for col in sort_columns if col in self.df1.columns]
            valid_orders = [sort_orders[i] for i, col in enumerate(sort_columns) if col in self.df1.columns]
            if valid_columns:
                temp_columns = []
                for col in valid_columns:
                    temp_col = f"{col}_lower"
                    self.df1[temp_col] = self.df1[col].str.lower()
                    temp_columns.append(temp_col)
                self.df1 = self.df1.sort_values(by=temp_columns, ascending=valid_orders)
                self.df1.drop(columns=temp_columns, inplace=True)
                self.df1.reset_index(drop=True, inplace=True)
                self.log_message(f"[File 1] Reorder completed (Ignore Case): {sort_conditions_str}")

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
                self.log_message(f"[File 2] Reorder completed (Ignore Case): {sort_conditions_str}")

        width = len(str(global_max))

        if self.df1 is not None:
            self.df1["__cmp_order__"] = self.df1.index.astype(str).str.zfill(width)
        if self.df2 is not None:
            self.df2["__cmp_order__"] = self.df2.index.astype(str).str.zfill(width)

        if self.df1 is not None:
            self.file1_headers = list(self.df1.columns)
        if self.df2 is not None:
            self.file2_headers = list(self.df2.columns)
        self.update_list_widgets()

        QMessageBox.information(self, self.tr("info_title"), self.tr("reorder_complete"))   # 재정렬 완료

    def compare_files(self):
        self.result_view.setModel(None)
        self.compare_result = None
        key_columns = [item.text() for item in self.key_list.selectedItems()]
        exclude_columns = [item.text() for item in self.exclude_list.selectedItems()]
        if self.df1 is not None and "__cmp_order__" in self.df1.columns:
            if "__cmp_order__" not in key_columns:
                key_columns.append("__cmp_order__")
            if "__cmp_order__" not in exclude_columns:
                exclude_columns.append("__cmp_order__")

        if not key_columns:
            QMessageBox.critical(self, self.tr("error"), self.tr("no_key_error"))   # 비교 키 컬럼을 선택하세요.
            return

        outer_keys = ", ".join(key_columns)
        exclude_cols = ", ".join(exclude_columns) if exclude_columns else "None"
        null_values = [self.null_list_widget.item(i).text() for i in range(self.null_list_widget.count())]
        null_values_str = ", ".join(null_values) if null_values else "None"
        ignore_case_str = "apply" if self.ignore_case_checkbox.isChecked() else "Not applied"
        numeric_compare_str = "apply" if self.numeric_compare_checkbox.isChecked() else "Not applied"
        confirm_msg = self.tr("confirm_msg").format(keys=outer_keys, exclude=exclude_cols, nulls=null_values_str, ignore_case=ignore_case_str, numeric_compare=numeric_compare_str) # 비교 설정 확인
        ret = QMessageBox.question(self, self.tr("compare_confirm_title"), confirm_msg, QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) # 비교 설정 확인
        if ret != QMessageBox.StandardButton.Yes:
            return
        self.log_message("Start comparing...")
        progress = QProgressDialog("File comparison in progress...", self.tr("cancel"), 0, 100, self)
        progress.setWindowTitle("progress")
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
        self.log_message(f"Compare complete: All = {total_count}, Both OK = {ok_count}, Both FAIL = {both_fail}, CSV1 only = {csv1_only}, CSV2 only = {csv2_only}")
        self.populate_result_view(merge_result)
        self.compare_result = merge_result
        self.compare_button.setEnabled(False)
        self.save_button.setEnabled(True)
        progress.close()
        self.filter_combo.clear()
        self.filter_combo.addItem(f"All ({total_count})")
        self.filter_combo.addItem(f"Both OK ({ok_count})")
        self.filter_combo.addItem(f"Both FAIL ({both_fail})")
        self.filter_combo.addItem(f"CSV1 only ({csv1_only})")
        self.filter_combo.addItem(f"CSV2 only ({csv2_only})")
        QMessageBox.information(self, self.tr("compare_complete"), self.tr("compare_finished_message")) # 비교 완료

    def on_compare_error(self, e):
        QMessageBox.critical(self, self.tr("error"), self.tr("compare_error").format(error=e))
        self.log_message(self.tr("compare_error").format(error=e))
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
        if text.startswith("All"):
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
            QMessageBox.warning(self, self.tr("warning_title"), self.tr("no_result_warning"))   # 결과가 없습니다.
            return
        csv1_path = self.file1_entry.text()
        if not csv1_path:
            QMessageBox.warning(self, self.tr("warning_title"), self.tr("no_file1_warning"))    # 파일 1이 없습니다.
            return
        base, _ = os.path.splitext(os.path.basename(csv1_path))
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        default_name = f"{base}_RESULT_{timestamp}.xlsx"
        default_path = os.path.join(os.path.dirname(csv1_path), default_name)
        file_path = default_path
        self.log_message(f"Start saving xlsx: {file_path}")
        self.xlsx_progress = QProgressDialog(self.tr("progress_saving"), self.tr("cancel"), 0, 100, self)               # 저장 중 프로그레스바
        self.xlsx_progress.setWindowTitle(self.tr("save_button"))                                                       # 저장 중 프로그레스바 타이틀
        self.xlsx_progress.setWindowModality(Qt.WindowModality.WindowModal)
        self.xlsx_progress.setAutoClose(False)
        self.xlsx_progress.setAutoReset(False)
        self.xlsx_progress.show()
        self.xlsx_worker = SaveXlsxWorker(self.compare_result, file_path)
        self.xlsx_worker.progress_signal.connect(self.xlsx_progress.setValue)
        self.xlsx_worker.log_signal.connect(self.log_message)
        self.xlsx_worker.finished_signal.connect(lambda: self.on_xlsx_saved(file_path))
        self.xlsx_worker.error_signal.connect(lambda e: QMessageBox.critical(self, self.tr("Error"), self.tr("xlsx_save_error").format(error=e)))    # 저장 중 오류 메시지
        self.xlsx_worker.start()

    def on_xlsx_saved(self, file_path):
        self.xlsx_progress.close()
        msg = self.tr("xlsx_save_complete").format(file_path=file_path)
        ret = QMessageBox.information(self, self.tr("save_finished"), self.tr("save_finished_message").format(msg=msg),   # 저장 완료 메시지
                                      QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ret == QMessageBox.StandardButton.Yes:
            try:
                os.startfile(file_path)
            except Exception as ex:
                QMessageBox.warning(self, self.tr("warning_title"), self.tr("file_open_error").format(error=ex))    # 파일 열기 오류
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
        self.sort_model._data = pd.DataFrame(columns=["column", "order"])
        self.sort_model.layoutChanged.emit()
        self.update_sort_selection()
        self.additional_na_values = []
        self.exclude_columns = []
        self.null_list_widget.clear()
        for val in self.default_na_values:
            display_val = "<blank>" if val == "" else val
            self.null_list_widget.addItem(QListWidgetItem(display_val))
        self.result_view.setModel(None)
        self.log_text.clear()
        self.compare_result = None
        self.compare_button.setEnabled(False)
        self.save_button.setEnabled(False)
        self.file1_load_button.setText(self.tr("browse_button"))
        self.file1_load_button.setEnabled(True)
        self.file2_load_button.setText(self.tr("browse_button"))
        self.file2_load_button.setEnabled(True)
        self.filter_combo.clear()
        self.filter_combo.addItems(["All", "Both OK", "Both FAIL", "CSV1 only", "CSV2 only"])
        self.proxy_model = StatusFilterProxyModel()
        self.result_view.setModel(self.proxy_model)
        self.log_message(self.tr("reset_complete")) # 전체 초기화 완료
        self.xlsx_history_list.clear()
        self.xlsx_history_group.setVisible(False)
        self.result_splitter.setSizes([self.width(), 0])

    def update_exclude_list(self):
        selected_keys = {item.text() for item in self.key_list.selectedItems()}
        for i in range(self.exclude_list.count()):
            item = self.exclude_list.item(i)
            text = item.text().replace(" (Not selectable)", "")
            if text in selected_keys:
                item.setSelected(False)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsSelectable)
                item.setText(f"{text} (Not selectable)")
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
            display_text = "<tab>" if delim == "\t" else "<comma>" if delim == "," else delim
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
            display_text = "<tab>" if delim == "\t" else "<comma>" if delim == "," else delim
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
            actual = "\t" if item.text() == "<tab>" else "," if item.text() == "<comma>" else item.text()
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

    def toggle_sort_column(self, item):
        column = item.text()
        if column in self.sort_model._data["column"].values:
            self.sort_model._data = self.sort_model._data[self.sort_model._data["column"] != column].reset_index(drop=True)
        else:
            new_row = pd.DataFrame([[column, "ASC"]], columns=["column", "order"])
            self.sort_model._data = pd.concat([self.sort_model._data, new_row], ignore_index=True)
        self.sort_model.layoutChanged.emit()
        self.update_sort_selection()

    def clear_sort_selection(self):
        self.sort_model._data = pd.DataFrame(columns=["column", "order"])
        self.sort_model.layoutChanged.emit()
        self.update_sort_selection()

    def update_sort_selection(self):
        count = len(self.sort_model._data)
        self.sort_selected_label.setText(self.tr("sort_selected_label").format(count=count))    # 선택된 정렬 컬럼 (n개)

    def toggle_sort_order(self, index):
        if index.column() == 1:
            row = index.row()
            current_value = self.sort_model._data.iat[row, 1]
            new_value = "DESC" if current_value == "ASC" else "ASC"
            self.sort_model._data.iat[row, 1] = new_value
            self.sort_model.dataChanged.emit(index, index)

    def show_help_dialog(self):
        help_dialog = QDialog(self)
        help_dialog.setWindowTitle(self.tr("help_title"))                   # 도움말 창 제목
        help_dialog.setGeometry(200, 200, 450, 300)

        layout = QVBoxLayout()
        help_text = QTextEdit()
        help_text.setReadOnly(True)
        help_text.setText(self.tr("help_content"))                          # 도움말 내용
        layout.addWidget(help_text)

        close_button = QPushButton( self.tr("close"))                       # 닫기 버튼
        close_button.clicked.connect(help_dialog.accept)
        layout.addWidget(close_button)

        help_dialog.setLayout(layout)
        help_dialog.exec()

    def show_about_dialog(self):
        QMessageBox.about(self, self.tr("about_title"),                     # 정보 창
                          self.tr("about_content").format(version=VERSION)) # 정보 내용

    # 다국어 지원을 위한 메서드 추가
    def load_language_data(self):
        try:
            with open("languages.json", "r", encoding="utf-8") as f:
                self.language_data = json.load(f)
        except FileNotFoundError:
            self.language_data = {}
            self.log_message("Could not find language file (languages.json).")
        except json.JSONDecodeError:
            self.language_data = {}
            self.log_message("The language file format is incorrect.")

    def tr(self, key):
        if (self.current_language in self.language_data and
                key in self.language_data[self.current_language]):
            return self.language_data[self.current_language][key]
        return key  # 기본값으로 키 반환

    def change_language(self, language_code):
        self.current_language = language_code
        self.update_ui_texts()

    def update_ui_texts(self):
        self.setWindowTitle(self.tr("window_title").format(version=VERSION))            # 창 제목

        menus = self.menuBar().findChildren(QMenu)
        if len(menus) >= 2:
            help_menu = menus[0]
            language_menu = menus[1]
            help_menu.setTitle(self.tr("help_menu"))                                    # 도움말 메뉴
            actions = help_menu.actions()
            if len(actions) >= 2:
                actions[0].setText(self.tr("help_action"))                              # 도움말 액션
                actions[1].setText(self.tr("about_action"))                             # 정보 액션
            language_menu.setTitle(self.tr("language_menu"))                            # 언어 메뉴
            lang_actions = language_menu.actions()
            if len(lang_actions) >= 2:
                lang_actions[0].setText(self.tr("lang_en"))                             # 영어 액션
                lang_actions[1].setText(self.tr("lang_ko"))                             # 한국어 액션

        self.file_group.setTitle(self.tr("file_group"))                                 # 파일 그룹
        self.file1_label.setText(self.tr("file1_label"))                                # 파일 1 레이블
        self.file2_label.setText(self.tr("file2_label"))                                # 파일 2 레이블
        self.file1_load_button.setText(self.tr("browse_button"))                        # 파일 1 로드 버튼
        self.file1_reset_button.setText(self.tr("reset_button"))                        # 파일 1 초기화 버튼
        self.file2_load_button.setText(self.tr("browse_button"))                        # 파일 2 로드 버튼
        self.file2_reset_button.setText(self.tr("reset_button"))                        # 파일 2 초기화 버튼

        self.key_group.setTitle(self.tr("key_group"))                                   # 키 그룹   
        self.tab_widget.setTabText(0, self.tr("sort_tab"))                              # 정렬 탭
        self.sort_available_label.setText(self.tr("sort_available_label"))              # 사용 가능한 정렬 컬럼 레이블
        self.sort_selected_label.setText(self.tr("sort_selected_label").format(count=len(self.sort_model._data)))                   # 선택된 정렬 컬럼 레이블
        self.clear_sort_btn.setText(self.tr("clear_sort_button"))                       # 정렬 초기화 버튼
        self.reorder_btn.setText(self.tr("reorder_button"))                             # 재정렬 버튼
        self.tab_widget.setTabText(1, self.tr("key_tab"))                               # 키 탭
        self.available_key_label.setText(self.tr("available_key_label"))                # 사용 가능한 키 레이블
        self.selected_key_label.setText(self.tr("selected_key_label").format(count=len(self.key_list.selectedItems())))             # 선택된 키 레이블
        self.clear_key_btn.setText(self.tr("clear_key_button"))                         # 키 초기화 버튼
        self.tab_widget.setTabText(2, self.tr("exclude_tab"))                           # 비교 제외 탭
        self.available_exclude_label.setText(self.tr("available_exclude_label"))        # 비교 제외 컬럼 선택 레이블
        self.selected_exclude_label.setText(self.tr("selected_exclude_label").format(count=len(self.exclude_list.selectedItems()))) # 비교 제외 컬럼 선택 레이블
        self.clear_exclude_btn.setText(self.tr("clear_exclude_button"))                 # 비교 제외 컬럼 초기화 버튼
        self.tab_widget.setTabText(3, self.tr("null_tab"))                              # NULL 탭   
        self.null_label.setText(self.tr("null_label"))                                  # NULL 레이블
        self.null_line_edit.setPlaceholderText(self.tr("null_placeholder"))             # NULL 추가 입력창             
        self.add_null_button.setText(self.tr("add_button"))                             # NULL 추가 버튼
        self.delete_null_button.setText(self.tr("delete_button"))                       # NULL 삭제 버튼
        self.tab_widget.setTabText(4, self.tr("csv_delim_tab"))                         # CSV 구분자 탭
        self.csv_delim_label.setText(self.tr("csv_delim_label"))                        # CSV 구분자 레이블
        self.csv_delim_line_edit.setPlaceholderText(self.tr("csv_delim_placeholder"))   # CSV 구분자 추가 입력창
        self.add_csv_delim_button.setText(self.tr("add_button"))                        # CSV 구분자 추가 버튼
        self.delete_csv_delim_button.setText(self.tr("delete_button"))                  # CSV 구분자 삭제 버튼    

        self.work_group.setTitle(self.tr("work_group"))                                 # 작업 그룹        
        self.compare_button.setText(self.tr("compare_button"))                          # 비교 버튼
        self.overall_reset_btn.setText(self.tr("overall_reset_button"))                 # 전체 초기화 버튼
        self.save_button.setText(self.tr("save_button"))                                # 저장 버튼
        self.ignore_case_checkbox.setText(self.tr("ignore_case_checkbox"))              # 대소문자 무시
        self.numeric_compare_checkbox.setText(self.tr("numeric_compare_checkbox"))      # 숫자 실수 비교

        self.result_group.setTitle(self.tr("result_group"))                             # 결과 그룹은 언어 변경 없음
        self.filter_label.setText(self.tr("filter_label"))                              # 필터 레이블은 언어 변경 없음
        self.xlsx_history_group.setTitle(self.tr("xlsx_history_group"))                 # XLSX 이력 그룹은 언어 변경 없음
        self.clear_history_button.setText(self.tr("clear_history_button"))              # 이력 삭제 버튼은 언어 변경 없음
        self.log_group.setTitle(self.tr("log_group"))                                   # 로그 그룹은 언어 변경 없음

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = cmprApp()
    window.show()
    sys.exit(app.exec())