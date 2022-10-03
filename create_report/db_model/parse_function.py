import re


# log format: {query_name : {start_time: duration}}
def get_all_query_names(history_duration: str) -> list[str]:
    query_names = []
    with open(history_duration, "r") as f:
        for i in f:
            query_names.append(i)
    return query_names


def find_node(line):
    rule = r'\"node_info\": (.*?):'
    return re.search(rule, line)


def find_node_name(line):
    rule = r'\"node_name\": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_materialized(line):
    rule = r'\"materialized\": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_node_finished(line):
    rule = r'\"node_finished_at\": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_node_path(line):
    rule = r'\"node_path\": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_node_started(line):
    rule = r'\"node_started_at\": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_node_status(line):
    rule = r'\"node_status\": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_resource_type(line):
    rule = r'"resource_type": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_rows_affected(line):
    rule = r'"rows_affected": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_execution_time(line):
    rule = r'"execution_time": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_failures(line):
    rule = r'"failures": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")


def find_message(line):
    rule = r'\"message\": (.*?),'
    res = "" if re.search(rule, line) is None else re.search(rule, line).group(1)
    return str(res).replace("\"", "")