import json
import re

with open("test.log", "r") as f:
    lines = f.readlines()
    query_end_time_rule = re.compile(r'"node_finished_at": (.*?),')
    query_name_rule = re.compile(r'"node_name": (.*?),')
    query_start_time_rule = re.compile(r'"node_started_at": (.*?),')
    rule = query_end_time_rule + query_name_rule + query_start_time_rule
    # rows_affected_rule = re.compile(r'"rows_affected": (.*?),')
    # duration_rule = re.compile(r'"execution_time": (.*?),')
    # invocation_id_rule = re.compile(r'"invocation_id": (.*?),')
    #
    # query_index_rule = re.compile(r'"index": (.*?),')
    # thread_name_rule = re.compile(r'"thread_name": (.*?),')
    for line in lines:
        line = json.loads(line)


