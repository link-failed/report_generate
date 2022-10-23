from src.adapter.parser import DbtJsonLogAdapter

log_path = "create_report/temp_files/json_logs"
dja = DbtJsonLogAdapter(log_path=log_path)
print(dja.get_df())
