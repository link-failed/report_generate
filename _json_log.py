from src.adapter.parser import DbtJsonLogAdapter

log_path = "/home/ceci/Desktop/report_generate/create_report/temp_files/json_logs/dir1"
dja = DbtJsonLogAdapter(log_path=log_path)
# print(dja.get_df().contents)
# print(len(dja.metadatas.get_histories()))
# print(len(dja.metadatas.get_contents()))
print(dja.metadatas.contents)