from abc import ABC, abstractmethod
import glob, os
import logging
import re
from datetime import datetime

import pandas as pd


class BaseAdapter(ABC):
    """The adapter class """

    @abstractmethod
    def get_metadata():
        raise NotImplementedError('Please implement methods')
    


class DbtLogAdapter(BaseAdapter):
    def __init__(self, log_path) -> None:
        super().__init__()
        self.re_exp_query_ok = re.compile(r'.*(\d+:\d+:\d+)\.\d+\s\[info\s\]\s\[Thread-(\d+).*\]\:\s(\d+)\sof\s(\d+)\sOK created table model .*\.(\w+) .*SELECT\s(\d+).*in\s(\d+\.\d+)s')
        self.re_exp_query_start = re.compile(r'(\d+:\d+:\d+)\.\d+\s\[info\s\]\s\[Thread-(\d+).*\]\:\s(\d+)\sof\s(\d+)\sSTART .* model .*\.(\w+) .+ \[RUN\]')
        self.re_exp_project_start_time = re.compile(r'=*\s(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*\s\|\s([0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]*)\s')
        self.periods = {}
        self.metadatas = {}
        self.running_id = None
        self.logs = self._get_log_files(log_path = log_path)
        

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
                    self._parse_project_start_line(line= line)
                    self._parse_query_start_line(line)
                    self._parse_query_ok_line(line)

    def _parse_project_start_line(self, line):
        match = self.re_exp_project_start_time.search(line)
        if match:
            project_start_time = match.group(1)
            self.running_id = match.group(2)
            self.periods[project_start_time] = self.running_id 
            self.metadatas[self.running_id] = {}

    def _parse_query_ok_line(self, line):
        match = self.re_exp_query_ok.search(line)
        
        if match:
            metadata = self.metadatas.get(self.running_id)
            query_end_time = datetime.strptime(match.group(1), '%H:%M:%S')
            thread_id = match.group(2)
            query_index = match.group(3)
            total_query_count = match.group(4)
            query_name = match.group(5)
            rows_affect = match.group(6)
            query_duration = match.group(7)
            metadata.get(query_name)['duration'] = float(query_duration)
            metadata.get(query_name)['query_end_time'] = query_end_time
            metadata.get(query_name)['thread_id'] = thread_id
            metadata.get(query_name)['rows_effect'] = rows_affect

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
            metadata[query_name] = {'query_start_time': query_start_time, 'total_query': total_query, 'query_index': query_index, 'query_name': query_name}
        


    def get_period(self):
        return self.periods

    def get_metadata(self):
        return self.metadatas