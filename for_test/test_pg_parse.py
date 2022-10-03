import csv
import re

with open("../create_report/temp_files/logs_example/pg-29.csv", "r") as f:
    """
        duration_info format:
        index: {message: message, tag: command_tag}
        
        query_info format:
        index: query_name
    """
    content = csv.reader(f)
    duration_info = {}
    query_info = {}
    duration_rule = r'duration: (.*?) ms'
    query_name_rule = r'\"node_id\": \"(.*?)\"'
    
    for index, line in enumerate(content):
        # column 8: command_tag text
        # column 14: message text
        if line[13] != "":
            # to find duration
            if len(line[13]) > 9 and line[13][:9] == "duration:":
                duration = re.search(duration_rule, line[13]).group(1)
                this_duration_info = {"duration": float(duration), "tag": line[7]}
                duration_info[index] = this_duration_info
            # to find query name
            elif len(line[13]) > 10 and line[13][:10] == "statement:":
                query_name = re.search(query_name_rule, line[13])
                if query_name is not None:
                    query_name = query_name.group(1)
                    print(query_name)
                    query_info[index] = query_name


