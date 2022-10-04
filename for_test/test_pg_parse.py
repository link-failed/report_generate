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
    content = csv.reader(f)
    duration_info = {}
    query_info = {}
    query_duration = {}
    duration_rule = r'duration: (.*?) ms'
    query_name_rule = r'\"node_id\": \"(.*?)\"'

    # get query names and durations
    for index, line in enumerate(content):
        # column 8: command_tag text
        # column 14: message text
        if line[13] != "":
            # to find duration
            if len(line[13]) > 9 and line[13][:9] == "duration:":
                duration = re.search(duration_rule, line[13]).group(1)
                this_duration_info = {"duration": float(duration), "tag": line[7]}
                duration_info[index] = this_duration_info
                # print(str(index) + str(this_duration_info))

            # to find query name
            elif len(line[13]) > 10 and line[13][:10] == "statement:":
                query_name = re.search(query_name_rule, line[13])
                if query_name is not None:
                    query_name = query_name.group(1)
                    last_dot_index = len(query_name) - query_name[::-1].index(".")
                    query_name = query_name[last_dot_index:]
                    query_info[index] = query_name
                    # print(query_name)

    last_index = list(duration_info.keys())[-1]

    # calculate query duration
    for index in query_info:
        # find next query index
        temp = list(query_info)
        next_index = temp[min(temp.index(index) + 1, len(temp) - 1)]
        next_index = min(next_index, last_index)
        query_name = query_info[index]
        # print(str(index) + "  " + str(next_index))

        if query_name not in query_duration:
            query_duration[query_name] = 0

        for i in range(index + 1, next_index):
            if i in duration_info:
                query_duration[query_name] += duration_info[i]["duration"]

    print(query_duration)



