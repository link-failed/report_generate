import json
import csv
import re
import os
import parse_function

"""
    metadata format:
    current_info
    history_info
"""


class BaseAdapter(object):
    def __init__(self, log_path):
        self.log_path = log_path

    # def read_file():
    #     return metadata


class DbtJsonAdapter(BaseAdapter):
    def __init__(self, log_path):
        self.log_path = log_path

    def current_queries_info(self, log_path):
        self.log_path = log_path
        # find the last
        begin_flag = '"msg": "Running with dbt='
        csv_header = ["query_name", "execution_time", "query_started_at", "query_finished_at", "query_status", "dbt_pid",
                      "thread_name"]
        # log_path = '/home/ceci/Desktop/mimic-dbt/logs/'
        log_name = 'dbt.log'
        res_path = '../res/'
        res_name = 'this_dbt_log.csv'
        with open(log_path + log_name, 'r') as f, open(res_path + res_name, 'w') as output:
            writer = csv.writer(output)

            # add header for the csv output
            writer.writerow(csv_header)

            all_log = f.readlines()
            start_index = [x for x in range(len(all_log)) if begin_flag in all_log[x]]
            if not start_index:
                this_lines = all_log
            else:
                this_lines = all_log[start_index[-1]:]

            for jsonStr in this_lines:
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
                        # thread_name = int(v[7:])

                rule = r'\"node_info\": {(.*?)},'
                if re.search(rule, jsonStr) is not None:
                    record = re.search(rule, jsonStr).group(1)

                    query_status = parse_function.find_node_status(record)
                    if query_status == 'compiling':
                        continue

                    query_finished_at = parse_function.find_node_finished(record)
                    query_name = parse_function.find_node_name(record)
                    query_started_at = parse_function.find_node_started(record)

                    this_row = [query_name, execution_time, query_started_at, query_finished_at, query_status, dbt_pid,
                                thread_name]

                else:
                    continue

                writer.writerow(this_row)

    def history_queries_info(self, log_path):
        # TODO
        # self.log_path = log_path
        self.log_path = '../temp_files/logs_example/'
        res_path = '../res/'
        res_name = 'all_dbt_log.csv'

        csv_header = ["query_name", "duration", "query_started_at", "query_finished_at", "query_status", "dbt_pid",
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
                            duration = parse_function.find_execution_time(jsonStr)
                            if duration == "" or duration == "0":
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

                                query_status = parse_function.find_node_status(record)
                                if query_status == 'compiling' or query_status == "error":
                                    continue

                                query_finished_at = parse_function.find_node_finished(record)
                                query_name = parse_function.find_node_name(record)
                                query_started_at = parse_function.find_node_started(record)

                                this_row = [query_name, duration, query_started_at, query_finished_at, query_status,
                                            dbt_pid, thread_name]

                            else:
                                continue

                            writer.writerow(this_row)


class DbtTxtAdapter(BaseAdapter):
    def __init__(self, log_path):
        self.log_path = log_path


"""
    https://www.postgresql.org/docs/current/runtime-config-logging.html
    write the csv and return nothing
"""


class PgCsvAdapter(BaseAdapter):
    def __init__(self, log_path):
        self.log_path = log_path

    def current_queries_info(self, log_path):
        self.log_path = log_path
        # TODO:
        log_path = '../temp_files/logs_example/'
        with open(log_path, "r") as f:
            """
                duration_info format:
                index: {duration: message, tag: command_tag}

                query_info format:
                index: query_name

                query_duration format:
                query_name: duration (ms)
            """
            res_path = '../res/'
            res_name = 'this_pg_log.csv'
            content = csv.reader(f)
            duration_info = {}
            query_info = {}
            query_duration = {}
            duration_rule = r'duration: (.*?) ms'
            query_name_rule = r'\"node_id\": \"(.*?)\"'
            csv_header = ["query_name", "execution_time", "query_started_at", "query_finished_at", "query_status",
                          "pid",
                          "thread_name"]

            with open(res_path + res_name, 'w') as output:
                # with open(res_path + res_name, 'w') as output:
                writer = csv.writer(output)
                writer.writerow(csv_header)

                # get query names and durations
                for index, line in enumerate(content):
                    # column 8: command_tag text
                    # column 14: message text
                    if line[13] != "":
                        # to find duration
                        if len(line[13]) > 9 and line[13][:9] == "duration:":
                            duration = re.search(duration_rule, line[13]).group(1)
                            this_duration_info = {"duration": float(duration), "tag": line[7], "log_time": line[0]}
                            duration_info[index] = this_duration_info
                            print(str(index) + str(this_duration_info))

                        # to find query name
                        elif len(line[13]) > 10 and line[13][:10] == "statement:":
                            query_name = re.search(query_name_rule, line[13])
                            if query_name is not None:
                                query_name = query_name.group(1)
                                last_dot_index = len(query_name) - query_name[::-1].index(".")
                                query_name = query_name[last_dot_index:]
                                this_query_info = {"query_name": query_name, "start_time": line[8], "pid": line[3]}
                                query_info[index] = this_query_info
                                print(str(index) + str(this_query_info))

                last_index = list(duration_info.keys())[-1]

                # calculate query duration
                for index in query_info:
                    query_name = query_info[index]["query_name"]
                    # find next query index
                    temp = list(query_info)
                    # in case it's the last line
                    next_index = min(index + 1, last_index)
                    for i in range(min(index + 1, last_index), last_index + 1):
                        if i in query_info.keys() and query_info[i]["query_name"] != query_name:
                            next_index = i
                            break
                    to_record_flag = False
                    # print(str(index) + "  " + str(next_index))

                    # first time the query is record
                    if query_name not in query_duration:
                        to_record_flag = True
                        query_started_at = query_info[index]["start_time"]
                        query_duration[query_name] = 0

                    query_status = "error"
                    query_finished_at = ""
                    for i in range(index + 1, next_index):
                        if i in duration_info.keys():
                            if duration_info[i]["tag"] == "COMMIT":
                                query_status = "success"
                                query_finished_at = duration_info[i]["log_time"]
                            elif duration_info[i]["tag"] == "ROLLBACK":
                                query_finished_at = duration_info[i]["log_time"]
                            if i in duration_info:
                                query_duration[query_name] += duration_info[i]["duration"]

                    pg_pid = query_info[index]["pid"]
                    thread_name = ""

                    if to_record_flag:
                        this_row = [query_name, duration, query_started_at, query_finished_at, query_status,
                                    pg_pid, thread_name]
                        writer.writerow(this_row)

        """
            calculation of query's started time:
            the session_start_time of first the query's BEGIN

            calculation of query's finished time:
            the session_start_time of the COMMIT / ROLLBACK + duration in this line
        """
