import sys
import os
import io
import csv
import threading
import time
from datetime import datetime, timedelta
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QTextEdit, QCheckBox, QMessageBox,
    QTabWidget, QFileDialog, QListWidget, QListWidgetItem, QDialog,
    QDialogButtonBox, QGridLayout, QTimeEdit, QGroupBox, QFormLayout,
    QDateTimeEdit, QDateEdit
)
from PyQt5.QtCore import Qt, QTime, QDate
from googleapiclient.http import MediaIoBaseUpload
from Google import Create_Service
import mysql.connector


def timestamped_log(msg):
    return f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"


class AddScheduleTimeDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Schedule Time (HH:mm:ss)")
        self.setModal(True)
        self.resize(220, 100)
        layout = QVBoxLayout()
        self.time_edit = QTimeEdit(QTime.currentTime())
        self.time_edit.setDisplayFormat("HH:mm:ss")
        layout.addWidget(self.time_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.setLayout(layout)

    def selected_time_str(self):
        return self.time_edit.time().toString("HH:mm:ss")


class DriveMySQLUploader(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MySQL to Google Drive Uploader")
        self.setGeometry(150, 100, 1000, 700)

        self.tables_and_queries = {
            "result": "SELECT * FROM `result`;",
            "st01 loading": "SELECT * FROM `st01 loading`;",
            "st03 pre0 data": "SELECT * FROM `st03 pre0 data`;",
            "st03 pre data": "SELECT * FROM `st03 pre data`;",
            "st04 fs0 data":"SELECT * FROM `st04 fs0 data`;",
            "st04 fs1 data": "SELECT * FROM `st04 fs1 data`;",
            "st04 fs data": "SELECT * FROM `st04 fs data`;",
            "st05 fs0 data": "SELECT * FROM `st05 fs0 data`;",
            "st05 fs1 data": "SELECT * FROM `st05 fs1 data`;",
            "st05 fs data": "SELECT * FROM `st05 fs data`;",
            "st06 fs0 data": "SELECT * FROM `st06 fs0 data`;",
            "st06 fs1 data": "SELECT * FROM `st06 fs1 data`;",
            "st06 fs data": "SELECT * FROM `st06 fs data`;",
            "st07 gap": "SELECT * FROM `st07 gap`;",
            "st07 illumination": "SELECT * FROM `st07 illumination`;",
            "st08 pin data": "SELECT * FROM `st08 pin data`;",
            "st09 laser": "SELECT * FROM `st09 laser`;",
            "st10 scaner": "SELECT * FROM `st10 scaner`;",
            "sws data": "SELECT * FROM `sws data`;",
        }

        self.service = None
        try:
            self.service = self.authenticate_drive()
        except SystemExit:
            raise

        self.auto_sync_thread = None
        self.auto_sync_stop_flag = threading.Event()
        self.schedule_threads = []
        self.schedule_stop_flags = []

        self.init_ui()

    def authenticate_drive(self):
        CLIENT_SECRET_FILE = "credentials.json"
        API_NAME = "drive"
        API_VERSION = "v3"
        SCOPES = ["https://www.googleapis.com/auth/drive"]
        try:
            service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)
            about = service.about().get(fields="user(emailAddress)").execute()
            self.statusBar().showMessage(f"Authenticated as: {about['user']['emailAddress']}")
            return service
        except Exception as e:
            QMessageBox.critical(self, "Google Drive Authentication Error", str(e))
            sys.exit(1)

    def init_ui(self):
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        self.tabs.addTab(self.manual_upload_tab(), "Manual Upload")
        self.tabs.addTab(self.auto_upload_tab(), "Auto Upload")

    # ---------------- Manual Upload Tab ----------------
    def manual_upload_tab(self):
        widget = QWidget()
        layout = QVBoxLayout()

        form = QGroupBox("Database Connection")
        form_layout = QFormLayout()
        self.manual_db_host = QLineEdit()
        self.manual_db_user = QLineEdit()
        self.manual_db_pass = QLineEdit()
        self.manual_db_pass.setEchoMode(QLineEdit.Password)
        self.manual_db_name = QLineEdit()
        form_layout.addRow("Host:", self.manual_db_host)
        form_layout.addRow("User:", self.manual_db_user)
        form_layout.addRow("Password:", self.manual_db_pass)
        form_layout.addRow("Database:", self.manual_db_name)
        form.setLayout(form_layout)
        layout.addWidget(form)

        # Table selection
        layout.addWidget(QLabel("Select Tables to Upload:"))
        self.manual_tables_list_widget = QListWidget()
        self.manual_tables_list_widget.setSelectionMode(QListWidget.NoSelection)
        for table_name in self.tables_and_queries.keys():
            item = QListWidgetItem(table_name)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked)
            self.manual_tables_list_widget.addItem(item)
        layout.addWidget(self.manual_tables_list_widget)

        # Date-time range
        layout.addWidget(QLabel("Select Date-Time Range:"))
        range_layout = QHBoxLayout()
        self.manual_start_date = QDateEdit(QDate.currentDate())
        self.manual_start_date.setCalendarPopup(True)
        self.manual_start_time = QTimeEdit(QTime.currentTime())
        self.manual_start_time.setDisplayFormat("HH:mm:ss")

        self.manual_end_date = QDateEdit(QDate.currentDate())
        self.manual_end_date.setCalendarPopup(True)
        self.manual_end_time = QTimeEdit(QTime.currentTime())
        self.manual_end_time.setDisplayFormat("HH:mm:ss")

        range_layout.addWidget(QLabel("From Date:"))
        range_layout.addWidget(self.manual_start_date)
        range_layout.addWidget(QLabel("Time:"))
        range_layout.addWidget(self.manual_start_time)

        range_layout.addWidget(QLabel("To Date:"))
        range_layout.addWidget(self.manual_end_date)
        range_layout.addWidget(QLabel("Time:"))
        range_layout.addWidget(self.manual_end_time)

        layout.addLayout(range_layout)

        self.btn_enable_sql = QCheckBox("Enable Custom SQL")
        self.btn_enable_sql.toggled.connect(self.toggle_custom_sql)
        layout.addWidget(self.btn_enable_sql)

        self.manual_query = QTextEdit()
        self.manual_query.setEnabled(False)
        layout.addWidget(self.manual_query)

        fn_layout = QHBoxLayout()
        fn_layout.addWidget(QLabel("Custom File Name (with .csv):"))
        self.manual_file_name = QLineEdit()
        fn_layout.addWidget(self.manual_file_name)
        layout.addLayout(fn_layout)

        sub_layout = QHBoxLayout()
        self.manual_optional_subfolder_checkbox = QCheckBox("Create subfolder under Manual")
        self.manual_optional_subfolder_name = QLineEdit()
        sub_layout.addWidget(self.manual_optional_subfolder_checkbox)
        sub_layout.addWidget(self.manual_optional_subfolder_name)
        layout.addLayout(sub_layout)

        layout.addWidget(QLabel("Destination Drive Folder ID (optional):"))
        self.manual_drive_folder_id = QLineEdit()
        layout.addWidget(self.manual_drive_folder_id)

        self.manual_delete_checkbox = QCheckBox("Delete rows after upload")
        layout.addWidget(self.manual_delete_checkbox)

        btn_upload = QPushButton("Upload Now (Manual)")
        btn_upload.clicked.connect(self.manual_upload_clicked)
        layout.addWidget(btn_upload)

        layout.addWidget(QLabel("Logs:"))
        self.manual_log = QTextEdit()
        self.manual_log.setReadOnly(True)
        layout.addWidget(self.manual_log)

        self.manual_tables_list_widget.itemChanged.connect(self.update_custom_file_name_state)
        self.btn_enable_sql.toggled.connect(self.update_custom_file_name_state)
        self.update_custom_file_name_state()

        widget.setLayout(layout)
        return widget

    def toggle_custom_sql(self, checked):
        self.manual_query.setEnabled(checked)
        self.manual_tables_list_widget.setEnabled(not checked)
        self.update_custom_file_name_state()

    def update_custom_file_name_state(self):
        if self.btn_enable_sql.isChecked():
            self.manual_file_name.setEnabled(True)
        else:
            selected_count = sum(
                1 for i in range(self.manual_tables_list_widget.count())
                if self.manual_tables_list_widget.item(i).checkState() == Qt.Checked
            )
            self.manual_file_name.setEnabled(selected_count == 1)

    def manual_upload_clicked(self):
        threading.Thread(target=self._manual_upload_worker, daemon=True).start()

    def _manual_upload_worker(self):
        try:
            if self.btn_enable_sql.isChecked():
                query = self.manual_query.toPlainText().strip()
                if not query:
                    self.log_append(self.manual_log, timestamped_log("⚠️ Custom SQL is empty."))
                    return
                self.process_manual_upload(query, "custom_query")
            else:
                selected_tables = [self.manual_tables_list_widget.item(i).text()
                                   for i in range(self.manual_tables_list_widget.count())
                                   if self.manual_tables_list_widget.item(i).checkState() == Qt.Checked]
                if not selected_tables:
                    self.log_append(self.manual_log, timestamped_log("⚠️ No tables selected for upload."))
                    return

                start_dt = datetime.combine(self.manual_start_date.date().toPyDate(),
                                            self.manual_start_time.time().toPyTime())
                end_dt = datetime.combine(self.manual_end_date.date().toPyDate(),
                                          self.manual_end_time.time().toPyTime())
                start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

                for table in selected_tables:
                    query = f"SELECT * FROM `{table}` WHERE Date_Time BETWEEN '{start_str}' AND '{end_str}';"
                    self.process_manual_upload(query, table)
        except Exception as e:
            self.log_append(self.manual_log, timestamped_log(f"❌ Manual upload error: {e}"))

    def process_manual_upload(self, query, table_name):
        self.log_append(self.manual_log, timestamped_log(f"🔍 Fetching data for {table_name}..."))
        data = self.fetch_data_from_db(
            self.manual_db_host.text().strip(),
            self.manual_db_user.text().strip(),
            self.manual_db_pass.text(),
            self.manual_db_name.text().strip(),
            query
        )
        if not data or len(data) <= 1:
            self.log_append(self.manual_log, timestamped_log(f"⚠️ No data found for {table_name}."))
            return
        self.log_append(self.manual_log, timestamped_log(f"📄 Preparing CSV for {table_name}..."))
        csv_buffer = self.convert_data_to_csv(data)
        if self.manual_drive_folder_id.text().strip():
            target_folder_id = self.manual_drive_folder_id.text().strip()
        else:
            manual_root_id = self.get_or_create_folder("Manual", "root")
            if self.manual_optional_subfolder_checkbox.isChecked():
                sub = self.manual_optional_subfolder_name.text().strip()
                if sub:
                    manual_root_id = self.get_or_create_folder(sub, manual_root_id)
            target_folder_id = manual_root_id

        if self.manual_file_name.isEnabled():
            filename = self.manual_file_name.text().strip()
            if not filename:
                filename = f"{table_name.replace(' ', '_').lower()}.csv"
        else:
            filename = f"{table_name.replace(' ', '_').lower()}.csv"

        self.log_append(self.manual_log, timestamped_log(f"⬆️ Uploading file '{filename}' to Google Drive..."))
        self.upload_file_to_drive(filename, csv_buffer, target_folder_id)
        self.log_append(self.manual_log, timestamped_log(f"✅ Uploaded {filename} to Manual folder."))

        if self.manual_delete_checkbox.isChecked():
            self.delete_uploaded_rows(
                self.manual_db_host.text().strip(),
                self.manual_db_user.text().strip(),
                self.manual_db_pass.text(),
                self.manual_db_name.text().strip(),
                table_name
            )
            self.log_append(self.manual_log, timestamped_log(f"🗑️ Deleted rows from table {table_name} after upload."))

    # ---------------- Auto Upload Tab ----------------
    def auto_upload_tab(self):
        widget = QWidget()
        layout = QVBoxLayout()

        form = QGroupBox("Database Connection")
        form_layout = QFormLayout()
        self.auto_db_host = QLineEdit()
        self.auto_db_user = QLineEdit()
        self.auto_db_pass = QLineEdit()
        self.auto_db_pass.setEchoMode(QLineEdit.Password)
        self.auto_db_name = QLineEdit()
        form_layout.addRow("Host:", self.auto_db_host)
        form_layout.addRow("User:", self.auto_db_user)
        form_layout.addRow("Password:", self.auto_db_pass)
        form_layout.addRow("Database:", self.auto_db_name)

        # Start Date Picker
        self.auto_start_date = QDateEdit()
        self.auto_start_date.setCalendarPopup(True)
        self.auto_start_date.setDate(QDate.currentDate().addDays(-7))
        form_layout.addRow("Start Date:", self.auto_start_date)

        form.setLayout(form_layout)
        layout.addWidget(form)

        layout.addWidget(QLabel("Select Tables to Upload:"))
        self.tables_list_widget = QListWidget()
        for table_name in self.tables_and_queries.keys():
            item = QListWidgetItem(table_name)
            item.setCheckState(Qt.Unchecked)
            self.tables_list_widget.addItem(item)
        layout.addWidget(self.tables_list_widget)

        from PyQt5.QtWidgets import QRadioButton, QButtonGroup
        self.mode_group = QButtonGroup(widget)
        self.interval_radio = QRadioButton("Interval Sync")
        self.schedule_radio = QRadioButton("Schedule Sync")
        self.interval_radio.setChecked(True)
        self.mode_group.addButton(self.interval_radio)
        self.mode_group.addButton(self.schedule_radio)

        mode_layout = QHBoxLayout()
        mode_layout.addWidget(self.interval_radio)
        mode_layout.addWidget(self.schedule_radio)
        layout.addLayout(mode_layout)

        self.interval_widget = QWidget()
        interval_layout = QHBoxLayout()
        interval_layout.addWidget(QLabel("Sync Interval (HH:MM:SS):"))
        self.auto_interval = QTimeEdit(QTime(0, 5, 0))
        self.auto_interval.setDisplayFormat("HH:mm:ss")
        interval_layout.addWidget(self.auto_interval)
        self.interval_widget.setLayout(interval_layout)
        layout.addWidget(self.interval_widget)

        self.schedule_widget = QWidget()
        schedule_layout = QVBoxLayout()
        btn_add_schedule = QPushButton("Add Schedule Time")
        btn_add_schedule.clicked.connect(self.open_schedule_time_dialog)
        schedule_layout.addWidget(btn_add_schedule)

        self.schedule_times_list = QListWidget()
        schedule_layout.addWidget(self.schedule_times_list)

        btn_remove_schedule = QPushButton("Remove Selected Times")
        btn_remove_schedule.clicked.connect(self.remove_selected_schedule_times)
        schedule_layout.addWidget(btn_remove_schedule)

        self.schedule_widget.setLayout(schedule_layout)
        layout.addWidget(self.schedule_widget)

        self.auto_delete_checkbox = QCheckBox("Delete rows after upload")
        layout.addWidget(self.auto_delete_checkbox)

        btn_start = QPushButton("Start Auto Sync")
        btn_start.clicked.connect(self.start_auto_sync)
        layout.addWidget(btn_start)

        btn_stop = QPushButton("Stop Auto Sync")
        btn_stop.clicked.connect(self.stop_auto_sync)
        layout.addWidget(btn_stop)

        layout.addWidget(QLabel("Logs:"))
        self.auto_log = QTextEdit()
        self.auto_log.setReadOnly(True)
        layout.addWidget(self.auto_log)

        self.interval_radio.toggled.connect(self.toggle_mode_widgets)
        self.toggle_mode_widgets()

        widget.setLayout(layout)
        return widget

    def toggle_mode_widgets(self):
        if self.interval_radio.isChecked():
            self.interval_widget.show()
            self.schedule_widget.hide()
        else:
            self.interval_widget.hide()
            self.schedule_widget.show()

    def open_schedule_time_dialog(self):
        dlg = AddScheduleTimeDialog(self)
        if dlg.exec_():
            t = dlg.selected_time_str()
            existing = [self.schedule_times_list.item(i).text() for i in range(self.schedule_times_list.count())]
            if t not in existing:
                item = QListWidgetItem(t)
                item.setCheckState(Qt.Checked)
                self.schedule_times_list.addItem(item)
                self.log_append(self.auto_log, timestamped_log(f"➕ Added schedule time {t}"))
            else:
                self.log_append(self.auto_log, timestamped_log(f"⚠️ Schedule time {t} already exists"))

    def remove_selected_schedule_times(self):
        for item in self.schedule_times_list.selectedItems():
            self.schedule_times_list.takeItem(self.schedule_times_list.row(item))

    def start_auto_sync(self):
        selected_tables = [self.tables_list_widget.item(i).text()
                           for i in range(self.tables_list_widget.count())
                           if self.tables_list_widget.item(i).checkState() == Qt.Checked]
        if not selected_tables:
            QMessageBox.warning(self, "Input Error", "Please select at least one table to upload.")
            return

        if self.interval_radio.isChecked():
            qtime = self.auto_interval.time()
            interval_td = timedelta(hours=qtime.hour(), minutes=qtime.minute(), seconds=qtime.second())
            if interval_td.total_seconds() <= 0:
                QMessageBox.warning(self, "Input Error", "Please enter a valid interval greater than 0 seconds")
                return
            if self.auto_sync_thread and self.auto_sync_thread.is_alive():
                QMessageBox.information(self, "Info", "Auto Sync is already running.")
                return
            self.auto_sync_stop_flag.clear()
            self.log_append(self.auto_log, timestamped_log(f"⏳ Starting interval auto sync every {str(interval_td)}..."))
            self.auto_sync_thread = threading.Thread(
                target=self.interval_sync_worker,
                args=(selected_tables, interval_td),
                daemon=True
            )
            self.auto_sync_thread.start()
        else:
            schedule_times = [self.schedule_times_list.item(i).text()
                              for i in range(self.schedule_times_list.count())
                              if self.schedule_times_list.item(i).checkState() == Qt.Checked]
            if not schedule_times:
                QMessageBox.warning(self, "Input Error", "Please select at least one schedule time.")
                return
            self.stop_schedule_syncs()
            self.schedule_stop_flags = []
            self.schedule_threads = []
            for st in schedule_times:
                stop_flag = threading.Event()
                self.schedule_stop_flags.append(stop_flag)
                thread = threading.Thread(target=self.schedule_worker,
                                          args=(selected_tables, st, stop_flag),
                                          daemon=True)
                self.schedule_threads.append(thread)
                thread.start()
            self.log_append(self.auto_log, timestamped_log(f"✅ Scheduled sync will run at: {', '.join(schedule_times)} for tables: {', '.join(selected_tables)}"))

    def stop_auto_sync(self):
        self.log_append(self.auto_log, timestamped_log("🛑 Stopping auto sync..."))
        self.auto_sync_stop_flag.set()
        self.stop_schedule_syncs()

    def stop_schedule_syncs(self):
        for flag in self.schedule_stop_flags:
            flag.set()
        for thread in self.schedule_threads:
            thread.join(timeout=1)
        self.schedule_stop_flags = []
        self.schedule_threads = []

    def interval_sync_worker(self, selected_tables, interval_td):
        self.log_append(self.auto_log, timestamped_log("▶️ Interval sync worker started."))
        while not self.auto_sync_stop_flag.is_set():
            try:
                self.log_append(self.auto_log, timestamped_log("▶️ Running interval sync..."))
                self.run_all_queries(selected_tables, auto_mode=True)
                self.log_append(self.auto_log, timestamped_log("✅ Interval sync iteration completed."))
            except Exception as e:
                self.log_append(self.auto_log, timestamped_log(f"❌ Interval sync error: {e}"))
            wait_seconds = int(interval_td.total_seconds())
            while wait_seconds > 0 and not self.auto_sync_stop_flag.is_set():
                time.sleep(min(wait_seconds, 1))
                wait_seconds -= 1
        self.log_append(self.auto_log, timestamped_log("🛑 Interval sync worker stopped."))

    def schedule_worker(self, selected_tables, schedule_time_str, stop_flag):
        self.log_append(self.auto_log, timestamped_log(f"⏳ Schedule worker started for {schedule_time_str}"))
        while not stop_flag.is_set():
            now = datetime.now()
            target_today = datetime.strptime(schedule_time_str, "%H:%M:%S").replace(
                year=now.year, month=now.month, day=now.day
            )
            target_time = target_today
            if now >= target_time:
                target_time = target_today + timedelta(days=1)
            wait_seconds = int((target_time - now).total_seconds())
            wait_time_str = str(timedelta(seconds=wait_seconds))
            self.log_append(self.auto_log, timestamped_log(f"⏳ Waiting {wait_time_str} seconds until scheduled sync at {schedule_time_str}"))
            while wait_seconds > 0 and not stop_flag.is_set():
                time.sleep(min(wait_seconds, 1))
                wait_seconds -= 1
            if stop_flag.is_set():
                self.log_append(self.auto_log, timestamped_log("🛑 Scheduled sync stopped before running."))
                return
            while not stop_flag.is_set():
                try:
                    self.log_append(self.auto_log, timestamped_log(f"▶️ Running scheduled sync for tables: {', '.join(selected_tables)}"))
                    self.run_all_queries(selected_tables, auto_mode=True)
                    self.log_append(self.auto_log, timestamped_log("✅ Scheduled sync completed successfully."))
                    break
                except Exception as e:
                    self.log_append(self.auto_log, timestamped_log(f"❌ Scheduled sync error: {e}. Retrying in 60 seconds..."))
                retry_wait = 60
                while retry_wait > 0 and not stop_flag.is_set():
                    time.sleep(min(retry_wait, 1))
                    retry_wait -= 1

    def run_all_queries(self, selected_tables, auto_mode=False):
        ts_folder_name = datetime.now().strftime("%Y%m%d%H%M%S")
        root_folder_id = "root"
        auto_root_id = self.get_or_create_folder("Auto", root_folder_id)
        year_str = datetime.now().strftime("%Y")
        year_folder_id = self.get_or_create_folder(year_str, auto_root_id)
        date_str = datetime.now().strftime("%Y%m%d")
        date_folder_id = self.get_or_create_folder(date_str, year_folder_id)
        timestamp_folder_id = self.get_or_create_folder(ts_folder_name, date_folder_id)
        self.log_append(self.auto_log, timestamped_log(f"Using Drive folder structure: Auto/{year_str}/{date_str}/{ts_folder_name}"))

        for table in selected_tables:
            query = self.tables_and_queries.get(table)
            if not query:
                self.log_append(self.auto_log, timestamped_log(f"⚠️ No query found for table {table}, skipping..."))
                continue
            if auto_mode:
                start_date_str = self.auto_start_date.date().toString("yyyy-MM-dd")
                query = f"SELECT * FROM `{table}` WHERE Date_Time >= '{start_date_str}';"
            csv_file_name = f"{table.replace(' ', '_').lower()}.csv"
            try:
                self.log_append(self.auto_log, timestamped_log(f"⏳ Processing table '{table}'..."))
                data = self.fetch_data_from_db(
                    self.auto_db_host.text().strip(),
                    self.auto_db_user.text().strip(),
                    self.auto_db_pass.text(),
                    self.auto_db_name.text().strip(),
                    query
                )
                if not data or len(data) <= 1:
                    self.log_append(self.auto_log, timestamped_log(f"⚠️ No data found for table {table}, skipping upload."))
                    continue
                csv_buffer = self.convert_data_to_csv(data)
                self.upload_file_to_drive(csv_file_name, csv_buffer, timestamp_folder_id)
                self.log_append(self.auto_log, timestamped_log(f"✅ Uploaded {csv_file_name} to Drive."))

                if self.auto_delete_checkbox.isChecked():
                    self.delete_uploaded_rows(
                        self.auto_db_host.text().strip(),
                        self.auto_db_user.text().strip(),
                        self.auto_db_pass.text(),
                        self.auto_db_name.text().strip(),
                        table
                    )
                    self.log_append(self.auto_log, timestamped_log(f"🗑️ Deleted rows from table {table} after upload."))
            except Exception as e:
                self.log_append(self.auto_log, timestamped_log(f"❌ Error processing table {table}: {e}"))
                raise

    # ---------------- DB and Drive helpers ----------------
    def fetch_data_from_db(self, host, user, password, dbname, query):
        conn = None
        try:
            conn = mysql.connector.connect(host=host, user=user, password=password, database=dbname)
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            result = [columns] + list(rows)
            return result
        finally:
            if conn:
                conn.close()

    def convert_data_to_csv(self, data):
        output = io.StringIO()
        writer = csv.writer(output)
        for row in data:
            writer.writerow(row)
        text = output.getvalue()
        byte_buf = io.BytesIO(text.encode("utf-8"))
        byte_buf.seek(0)
        return byte_buf

    def upload_file_to_drive(self, file_name, file_buffer, folder_id):
        file_metadata = {"name": file_name, "parents": [folder_id]}
        media = MediaIoBaseUpload(file_buffer, mimetype="text/csv")
        res = self.service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        return res

    def delete_uploaded_rows(self, host, user, password, dbname, table_name):
        conn = mysql.connector.connect(host=host, user=user, password=password, database=dbname)
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM `{table_name}`;")
        conn.commit()
        conn.close()

    def get_or_create_folder(self, folder_name, parent_id):
        safe_name = folder_name.replace("'", "\\'")
        query = f"mimeType='application/vnd.google-apps.folder' and name='{safe_name}' and '{parent_id}' in parents and trashed=false"
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])
        if files:
            return files[0]["id"]
        file_metadata = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
        folder = self.service.files().create(body=file_metadata, fields="id").execute()
        return folder["id"]

    def log_append(self, widget, msg):
        widget.append(msg)
        widget.verticalScrollBar().setValue(widget.verticalScrollBar().maximum())


def main():
    app = QApplication(sys.argv)
    win = DriveMySQLUploader()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
