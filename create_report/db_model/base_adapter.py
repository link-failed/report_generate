import json
import csv
import re
import os
import parse_function


class BaseAdapter():
    def __init__(self, log_path):
        self.log_path = log_path

    # def read_file():
    #     return metadata


# class TxtAdapter(BaseAdapter):
#     def __init__(self, log_path):
#         self.log_path = log_path

class JsonAdapter(BaseAdapter):
    def __init__(self, log_path):
        self.log_path = log_path

    def current_queries_info(self, log_path):
        self.log_path = log_path
        res_path = '../res/'
        res_name = 'all_dbt_log.csv'

        csv_header = ["node_name", "execution_time", "node_started_at", "node_finished_at", "node_status", "dbt_pid",
                      "thread_name"]

        log_files = os.listdir(log_path)

        with open(res_path + res_name, 'w') as output:
            writer = csv.writer(output)
            writer.writerow(csv_header)

            for log_file in log_files:
                if log_file[:3] == 'dbt':
                    print("parse " + log_file + "...")
                    # To append, not to overwrite
                    with open(log_path + log_file, 'r') as f:
                        for jsonStr in f.readlines():
                            json_data = json.loads(jsonStr)
                            execution_time = parse_function.find_execution_time(jsonStr)
                            if execution_time == "" or execution_time == "0":
                                continue

                            for k, v in json_data.items():
                                if k == 'level':
                                    level = v
                                    if level == "debug":
                                        continue
                                elif k == 'pid':
                                    dbt_pid = v
                                elif k == 'thread_name':
                                    thread_name = v

                            rule = r'\"node_info\": {(.*?)},'
                            if re.search(rule, jsonStr) is not None:
                                record = re.search(rule, jsonStr).group(1)

                                node_status = parse_function.find_node_status(record)
                                if node_status == 'compiling' or node_status == "error":
                                    continue

                                node_finished_at = parse_function.find_node_finished(record)
                                node_name = parse_function.find_node_name(record)
                                node_started_at = parse_function.find_node_started(record)

                                this_row = [node_name, execution_time, node_started_at, node_finished_at, node_status,
                                            dbt_pid, thread_name]

                            else:
                                continue

                            writer.writerow(this_row)

    def history_queries_info(self, log_path):
        self.log_path = log_path
        # log_path = '../logs_example/'
        res_path = '../res/'
        res_name = 'all_dbt_log.csv'

        csv_header = ["node_name", "execution_time", "node_started_at", "node_finished_at", "node_status", "dbt_pid",
                      "thread_name"]

        log_files = os.listdir(log_path)

        with open(res_path + res_name, 'w') as output:
            writer = csv.writer(output)
            writer.writerow(csv_header)

            for log_file in log_files:
                if log_file[:3] == 'dbt':
                    print("parse " + log_file + "...")
                    # To append, not to overwrite
                    with open(log_path + log_file, 'r') as f:
                        for jsonStr in f.readlines():
                            json_data = json.loads(jsonStr)
                            execution_time = parse_function.find_execution_time(jsonStr)
                            if execution_time == "" or execution_time == "0":
                                continue

                            for k, v in json_data.items():
                                if k == 'level':
                                    level = v
                                    if level == "debug":
                                        continue
                                elif k == 'pid':
                                    dbt_pid = v
                                elif k == 'thread_name':
                                    thread_name = v

                            rule = r'\"node_info\": {(.*?)},'
                            if re.search(rule, jsonStr) is not None:
                                record = re.search(rule, jsonStr).group(1)

                                node_status = parse_function.find_node_status(record)
                                if node_status == 'compiling' or node_status == "error":
                                    continue

                                node_finished_at = parse_function.find_node_finished(record)
                                node_name = parse_function.find_node_name(record)
                                node_started_at = parse_function.find_node_started(record)

                                this_row = [node_name, execution_time, node_started_at, node_finished_at, node_status,
                                            dbt_pid, thread_name]

                            else:
                                continue

                            writer.writerow(this_row)


class TxtAdapter(BaseAdapter):
    def __init(self, log_path):
        self.log_path = log_path
