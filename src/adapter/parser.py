from abc import ABC, abstractmethod
import glob, os
import logging
import re
from datetime import datetime
import json
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import pandas as pd
import csv
from palettable.cartocolors.sequential import agSunset_7, TealGrn_7
from palettable.lightbartlein.diverging import BlueGray_8, BrownBlue10_10
import mpld3

import pandas as pd
from pendulum import duration
from src.base.dataframe import LogDataframe


class BaseAdapter(ABC):
    """The adapter class """

    def __init__(self) -> None:
        super().__init__()
        self.df = LogDataframe()

    @abstractmethod
    def get_metadata(self):
        raise NotImplementedError('Please implement methods')

    def get_df(self):
        return self.df


class DbtLogAdapter(BaseAdapter):
    def __init__(self, log_path) -> None:
        super().__init__()
        self.re_exp_query_ok = re.compile(
            r'.*(\d+:\d+:\d+)\.\d+\s\[info\s\]\s\[Thread-(\d+).*\]\:\s(\d+)\sof\s(\d+)\sOK created table model .*\.(\w+) .*SELECT\s(\d+).*in\s(\d+\.\d+)s')
        self.re_exp_query_start = re.compile(
            r'(\d+:\d+:\d+)\.\d+\s\[info\s\]\s\[Thread-(\d+).*\]\:\s(\d+)\sof\s(\d+)\sSTART .* model .*\.(\w+) .+ \[RUN\]')
        self.re_exp_project_start_time = re.compile(
            r'=*\s(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*\s\|\s([0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]*)\s')
        self.periods = {}
        self.metadatas = {}
        self.running_id = None
        self.logs = self._get_log_files(log_path=log_path)

        self._parse_log_file()

    def _get_log_files(self, log_path):
        EXT = '.log'
        logs = []
        for fn in glob.glob(os.path.join(log_path, '*%s' % EXT)):
            if not os.path.isfile(fn):
                logging.debug('Skipping file %s' % fn)
                continue
            logs.append(fn)
        return logs

    def _parse_log_file(self):
        for logfile in self.logs:
            with open(logfile) as f:
                for line in iter(f.readline, ''):
                    self._parse_project_start_line(line=line)
                    self._parse_query_start_line(line)
                    self._parse_query_ok_line(line)

    def _parse_project_start_line(self, line):
        match = self.re_exp_project_start_time.search(line)
        if match:
            project_start_time = match.group(1)
            self.running_id = match.group(2)
            self.periods[project_start_time] = self.running_id
            self.metadatas[self.running_id] = {}

            self.df.insert_running_date(project_start_time, self.running_id)

    def _parse_query_ok_line(self, line):
        match = self.re_exp_query_ok.search(line)

        if match:
            metadata = self.metadatas.get(self.running_id)
            query_end_time = datetime.strptime(match.group(1), '%H:%M:%S')
            thread_id = match.group(2)
            query_index = match.group(3)
            total_query_count = match.group(4)
            query_name = match.group(5)
            rows_effect = match.group(6)
            query_duration = match.group(7)
            metadata.get(query_name)['duration'] = float(query_duration)
            metadata.get(query_name)['query_end_time'] = query_end_time
            metadata.get(query_name)['thread_name'] = thread_id
            metadata.get(query_name)['rows_effect'] = rows_effect

            data = dict(
                duration=float(query_duration),
                end_time=query_end_time,
                thread_name=int(thread_id),
                rows_effect=rows_effect
            )

            self.df.insert(self.running_id, query_name, **data)

            # print(rows_affect)

    def _parse_query_start_line(self, line):
        match = self.re_exp_query_start.search(line)
        if match:
            metadata = self.metadatas[self.running_id]
            query_start_time = datetime.strptime(match.group(1), '%H:%M:%S')
            thread_id = match.group(2)
            query_index = match.group(3)
            total_query = match.group(4)
            query_name = match.group(5)
            metadata[query_name] = {'query_start_time': query_start_time, 'total_query': total_query,
                                    'query_index': query_index, 'query_name': query_name}
            data = dict(
                start_time=query_start_time,
                total=total_query,
                qindex=int(query_index)
            )
            self.df.insert(self.running_id, query_name, **data)

    def get_period(self):
        return self.periods

    def get_metadata(self):
        return self.metadatas


class DbtJsonLogAdapter(BaseAdapter):
    def __init__(self, log_path) -> None:
        super().__init__()
        self.query_start_time_rule = re.compile(r'"node_started_at": (.*?),')
        self.query_end_time_rule = re.compile(r'"node_finished_at": (.*?),')
        self.query_name_rule = re.compile(r'"node_name": (.*?),')
        self.rows_affected_rule = re.compile(r'"rows_affected": (.*?),')
        self.query_index_rule = re.compile(r'"index": (.*?),')
        self.duration_rule = re.compile(r'"execution_time": (.*?),')
        self.running_id_rule = re.compile(r'"invocation_id": (.*?),')
        self.thread_name_rule = re.compile(r'"thread_name": (.*?),')
        self.total_query_rule = re.compile(r'(\d+)\sof\s(\d+)\sSTART .* model .*\.(\w+) .+ \[RUN\]')

        self.periods = {}
        self.metadatas = {}
        self.running_id = None
        self.logs = self._get_log_files(log_path=log_path)

        self._parse_log_file()

    def _get_log_files(self, log_path):
        EXT = '.log'
        logs = []
        for fn in glob.glob(os.path.join(log_path, '*%s' % EXT)):
            if not os.path.isfile(fn):
                logging.debug('Skipping file %s' % fn)
                continue
            logs.append(fn)
        return logs

    def _parse_log_file(self):
        for logfile in self.logs:
            with open(logfile) as f:
                for line in f:
                    json_data = json.loads(line)
                    if json_data["level"] == "info":
                        self._parse_info_line(line=line)
                    elif json_data["level"] == "debug":
                        self._parse_debug_line(line=line)

    def _parse_info_line(self, line):
        match = self.re_exp_project_start_time.search(line)
        if match:
            project_start_time = match.group(1)
            self.running_id = match.group(2)
            self.periods[project_start_time] = self.running_id
            self.metadatas[self.running_id] = {}

            self.df.insert_running_date(project_start_time, self.running_id)

    def _parse_debug_line(self, line):
        json_data = json.loads(line)
        msg_str = json_data["msg"]
        total_query = re.search(self.total_query_rule, msg_str)

        json_record = json.loads(line)
        execution_time = re.search(self.duration_rule, line)
        query_name = re.search(self.query_name_rule, line)
        query_index = re.search(self.query_index_rule, line)
        running_id = re.search(self.running_id_rule, line)
        thread_name = re.search(self.thread_name_rule, line)
        rows_effect = re.search(self.rows_affected_rule, line)
        if execution_time != "" and execution_time != "0":
            metadata = self.metadatas.get(self.running_id)
            if "data" in json_record:
                json_data = json_record["data"]

            rule = r'"node_info": {(.*?)},'
            if re.search(rule, line) is not None:
                record = re.search(rule, line).group(1)
                query_start_time = re.search(self.query_start_time_rule, record)
                query_end_time = re.search(self.query_end_time_rule, record)

                metadata.get(query_name)['duration'] = float(duration)
                metadata.get(query_name)['query_end_time'] = query_end_time
                metadata.get(query_name)['thread_name'] = int(thread_name[7:0])
                metadata.get(query_name)['rows_effect'] = rows_effect
                metadata[query_name] = {'query_start_time': query_start_time, 'total_query': total_query,
                                        'query_index': query_index, 'query_name': query_name}

                data = dict(
                    duration=float(duration),
                    end_time=query_end_time,
                    thread_name=int(thread_name[7:]),
                    rows_effect=rows_effect
                )

                self.df.insert(self.running_id, query_name, **data)

        match = self.re_exp_query_ok.search(line)

    def get_period(self):
        return self.periods

    def get_metadata(self):
        return self.metadatas
