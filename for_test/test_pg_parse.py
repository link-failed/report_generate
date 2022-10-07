import csv
import re

with open("../create_report/temp_files/logs_example/pg-29.csv", "r") as f:
    """
        duration_info format:
        index: {duration: message, tag: command_tag}
        
        query_info format:
        index: query_name
        
        query_duration format:
        query_name: duration (ms)
    """
    log_path = '../temp_files/logs_example/'
    res_path = '../res/'
    res_name = 'this_pg_log.csv'
    content = csv.reader(f)
    duration_info = {}
    query_info = {}
    query_duration = {}
    duration_rule = r'duration: (.*?) ms'
    query_name_rule = r'\"node_id\": \"(.*?)\"'
    csv_header = ["query_name", "execution_time", "query_started_at", "query_finished_at", "query_status", "dbt_pid",
                  "thread_name"]

    with open("/home/ceci/Desktop/report_generate/create_report/temp_files/res/test.csv", 'w') as output:
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