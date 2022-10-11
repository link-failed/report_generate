import csv
import re

with open("../create_report/temp_files/logs_example/pg_log.txt", "r") as f:
    # get the start index of each LOG
    log_index = []
    pid_rule = r'\[(.*?)\]'
    db_name_rule = r'@(.*?)'
    user_name_rule = r' (.*?)@'
    csv_header = ["query_name", "execution_time", "query_started_at", "query_finished_at", "query_status", "pid",
                  "thread_name"]
    for line in f:
        if " LOG " in line:
            pid_candidate = re.search(pid_rule, line)
            log_content = line[line.find(" LOG ") : line.find(" LOG ") + len(" LOG ")]
            if pid_candidate is not None:
                pid_candidate = pid_candidate.group(0)[1:-1]
                if pid_candidate.isdigit():
                    pid = int(pid_candidate)


    with open("/home/ceci/Desktop/report_generate/create_report/temp_files/res/txt_parse_res.csv", 'w') as output:
        writer = csv.writer(output)
        writer.writerow(csv_header)

"""
    calculation of query's started time:
    the log_time of first the query's BEGIN
    
    calculation of query's finished time:
    the log_time of the COMMIT / ROLLBACK + duration in this line
"""