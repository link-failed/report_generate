import glob
import json
import logging
import os
import math
import re
from abc import ABC, abstractmethod
from datetime import datetime

from src.base.dataframe import LogDataframe


# import matplotlib.pyplot as plt
# from matplotlib.patches import Patch
# import pandas as pd
# import csv
# from palettable.cartocolors.sequential import agSunset_7, TealGrn_7
# from palettable.lightbartlein.diverging import BlueGray_8, BrownBlue10_10


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


# class DbtLogAdapter(BaseAdapter):
#     def __init__(self, log_path) -> None:
#         super().__init__()
#         self.re_exp_query_ok = re.compile(
#             r'.*(\d+:\d+:\d+)\.\d+\s\[info\s\]\s\[Thread-(\d+).*\]\:\s(\d+)\sof\s(\d+)\sOK created table model .*\.(\w+) .*SELECT\s(\d+).*in\s(\d+\.\d+)s')
#         self.re_exp_query_start = re.compile(
#             r'(\d+:\d+:\d+)\.\d+\s\[info\s\]\s\[Thread-(\d+).*\]\:\s(\d+)\sof\s(\d+)\sSTART .* model .*\.(\w+) .+ \[RUN\]')
#         self.re_exp_project_start_time = re.compile(
#             r'=*\s(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*\s\|\s([0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]*)\s')
#         self.periods = {}
#         self.metadatas = {}
#         self.running_id = None
#         self.logs = self._get_log_files(log_path=log_path)
#
#         self._parse_log_file()
#
#     def _get_log_files(self, log_path):
#         EXT = '.log'
#         logs = []
#         for fn in glob.glob(os.path.join(log_path, '*%s' % EXT)):
#             if not os.path.isfile(fn):
#                 logging.debug('Skipping file %s' % fn)
#                 continue
#             logs.append(fn)
#         return logs
#
#     def _parse_log_file(self):
#         for logfile in self.logs:
#             with open(logfile) as f:
#                 for line in iter(f.readline, ''):
#                     self._parse_project_start_line(line=line)
#                     self._parse_query_start_line(line)
#                     self._parse_query_ok_line(line)
#
#     def _parse_project_start_line(self, line):
#         match = self.re_exp_project_start_time.search(line)
#         if match:
#             project_start_time = match.group(1).replace("\"", "")
#             self.running_id = match.group(2)
#             self.periods[project_start_time] = self.running_id
#             self.metadatas[self.running_id] = {}
#
#             self.df.insert_running_date(project_start_time, self.running_id)
#
#     def _parse_query_ok_line(self, line):
#         match = self.re_exp_query_ok.search(line)
#
#         if match:
#             metadata = self.metadatas.get(self.running_id)
#             query_end_time = datetime.strptime(match.group(1), '%H:%M:%S')
#             thread_id = match.group(2)
#             query_index = match.group(3)
#             total_query_count = match.group(4)
#             query_name = match.group(5)
#             rows_effect = match.group(6)
#             query_duration = match.group(7)
#             metadata.get(query_name)['duration'] = float(query_duration)
#             metadata.get(query_name)['query_end_time'] = query_end_time
#             metadata.get(query_name)['thread_name'] = thread_id
#             metadata.get(query_name)['rows_effect'] = rows_effect
#
#             data = dict(
#                 duration=float(query_duration),
#                 end_time=query_end_time,
#                 thread_name=int(thread_id),
#                 rows_effect=rows_effect
#             )
#
#             self.df.insert(self.running_id, query_name, **data)
#
#             # print(rows_affect)
#
#     def _parse_query_start_line(self, line):
#         match = self.re_exp_query_start.search(line)
#         if match:
#             metadata = self.metadatas[self.running_id]
#             query_start_time = datetime.strptime(match.group(1), '%H:%M:%S')
#             thread_id = match.group(2)
#             query_index = match.group(3)
#             total_query = match.group(4)
#             query_name = match.group(5)
#             metadata[query_name] = {'query_start_time': query_start_time, 'total_query': total_query,
#                                     'query_index': query_index, 'query_name': query_name}
#             data = dict(
#                 start_time=query_start_time,
#                 total=total_query,
#                 qindex=int(query_index)
#             )
#             self.df.insert(self.running_id, query_name, **data)
#
#     def get_period(self):
#         return self.periods
#
#     def get_metadata(self):
#         return self.metadatas


class DbtJsonLogAdapter(BaseAdapter):
    def __init__(self, log_path) -> None:
        super().__init__()
        self.query_start_time_rule = re.compile(r'"node_started_at": (.*?),')
        self.query_end_time_rule = re.compile(r'"node_finished_at": (.*?),')
        self.query_name_rule = re.compile(r'"node_name": (.*?),')
        self.rows_affected_rule = re.compile(r'"rows_affected": (.*?)}')
        self.query_index_rule = re.compile(r'"index": (.*?),')
        self.duration_rule = re.compile(r'"execution_time": (.*?),')
        self.running_id_rule = re.compile(r'"invocation_id": (.*?),')
        self.thread_name_rule = re.compile(r'"thread_name": (.*?),')
        self.total_query_rule = re.compile(r'"total": (.*?)}')

        self.periods = {}
        self.metadatas = LogDataframe()
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
                    if "Running with dbt=" in line:
                        self._parse_project_start_line(line=line)
                        # print("project start line")
                    elif "\"description\"" in line and "\"description\": \"\"," not in line:
                        self._parse_query_start_line(line=line)
                        # print("query start line")
                    elif "Finished running node" in line:
                        self._parse_query_end_line(line=line)
                        # print("debug line")

    """
        query start line:
            example: {"code": "Q033", "data": {"description": "table model public.abx_prescriptions_list", "index": 1, "node_info": {"materialized": "table", "node_finished_at": null, "node_name": "abx_prescriptions_list", "node_path": "treatment/abx_prescriptions_list.sql", "node_started_at": "2022-07-27T22:52:09.288228", "node_status": "started", "resource_type": "model", "unique_id": "model.mimic.abx_prescriptions_list"}, "total": 107}, "invocation_id": "56c57f22-0b72-49cd-a574-997b2fd901b7", "level": "info", "log_version": 2, "msg": "1 of 107 START table model public.abx_prescriptions_list ....................... [RUN]", "pid": 44508, "thread_name": "Thread-1", "ts": "2022-07-27T22:52:09.288709Z", "type": "log_line"}
            info: index, start_time, total
            flag: contains msg "START table model"
        query end line:
            example: {"code": "Q024", "data": {"node_info": {"materialized": "table", "node_finished_at": "2022-07-27T22:52:49.633350", "node_name": "abx_prescriptions_list", "node_path": "treatment/abx_prescriptions_list.sql", "node_started_at": "2022-07-27T22:52:09.288228", "node_status": "success", "resource_type": "model", "unique_id": "model.mimic.abx_prescriptions_list"}, "run_result": {"adapter_response": {"_message": "SELECT 156", "code": "SELECT", "rows_affected": 156}, "agate_table": null, "execution_time": 40.34322166442871, "failures": null, "message": "SELECT 156", "node": {"alias": "abx_prescriptions_list", "build_path": "target/run/mimic/models/treatment/abx_prescriptions_list.sql", "checksum": {"checksum": "57bfe86b6968a1143dd63473d753eb935ebdcf3241c576785df6ce0461b31e05", "name": "sha256"}, "columns": {}, "compiled": true, "compiled_path": "target/compiled/mimic/models/treatment/abx_prescriptions_list.sql", "compiled_sql": " \nwith t1 as\n(\n  select\n    drug, drug_name_generic\n    , route\n    , case\n      when lower(drug) like '%adoxa%' then 1\n      when lower(drug) like '%ala-tet%' then 1\n      when lower(drug) like '%alodox%' then 1\n      when lower(drug) like '%amikacin%' then 1\n      when lower(drug) like '%amikin%' then 1\n      when lower(drug) like '%amoxicillin%' then 1\n      when lower(drug) like '%amoxicillin%clavulanate%' then 1\n      when lower(drug) like '%clavulanate%' then 1\n      when lower(drug) like '%ampicillin%' then 1\n      when lower(drug) like '%augmentin%' then 1\n      when lower(drug) like '%avelox%' then 1\n      when lower(drug) like '%avidoxy%' then 1\n      when lower(drug) like '%azactam%' then 1\n      when lower(drug) like '%azithromycin%' then 1\n      when lower(drug) like '%aztreonam%' then 1\n      when lower(drug) like '%axetil%' then 1\n      when lower(drug) like '%bactocill%' then 1\n      when lower(drug) like '%bactrim%' then 1\n      when lower(drug) like '%bethkis%' then 1\n      when lower(drug) like '%biaxin%' then 1\n      when lower(drug) like '%bicillin l-a%' then 1\n      when lower(drug) like '%cayston%' then 1\n      when lower(drug) like '%cefazolin%' then 1\n      when lower(drug) like '%cedax%' then 1\n      when lower(drug) like '%cefoxitin%' then 1\n      when lower(drug) like '%ceftazidime%' then 1\n      when lower(drug) like '%cefaclor%' then 1\n      when lower(drug) like '%cefadroxil%' then 1\n      when lower(drug) like '%cefdinir%' then 1\n      when lower(drug) like '%cefditoren%' then 1\n      when lower(drug) like '%cefepime%' then 1\n      when lower(drug) like '%cefotetan%' then 1\n      when lower(drug) like '%cefotaxime%' then 1\n      when lower(drug) like '%cefpodoxime%' then 1\n      when lower(drug) like '%cefprozil%' then 1\n      when lower(drug) like '%ceftibuten%' then 1\n      when lower(drug) like '%ceftin%' then 1\n      when lower(drug) like '%cefuroxime %' then 1\n      when lower(drug) like '%cefuroxime%' then 1\n      when lower(drug) like '%cephalexin%' then 1\n      when lower(drug) like '%chloramphenicol%' then 1\n      when lower(drug) like '%cipro%' then 1\n      when lower(drug) like '%ciprofloxacin%' then 1\n      when lower(drug) like '%claforan%' then 1\n      when lower(drug) like '%clarithromycin%' then 1\n      when lower(drug) like '%cleocin%' then 1\n      when lower(drug) like '%clindamycin%' then 1\n      when lower(drug) like '%cubicin%' then 1\n      when lower(drug) like '%dicloxacillin%' then 1\n      when lower(drug) like '%doryx%' then 1\n      when lower(drug) like '%doxycycline%' then 1\n      when lower(drug) like '%duricef%' then 1\n      when lower(drug) like '%dynacin%' then 1\n      when lower(drug) like '%ery-tab%' then 1\n      when lower(drug) like '%eryped%' then 1\n      when lower(drug) like '%eryc%' then 1\n      when lower(drug) like '%erythrocin%' then 1\n      when lower(drug) like '%erythromycin%' then 1\n      when lower(drug) like '%factive%' then 1\n      when lower(drug) like '%flagyl%' then 1\n      when lower(drug) like '%fortaz%' then 1\n      when lower(drug) like '%furadantin%' then 1\n      when lower(drug) like '%garamycin%' then 1\n      when lower(drug) like '%gentamicin%' then 1\n      when lower(drug) like '%kanamycin%' then 1\n      when lower(drug) like '%keflex%' then 1\n      when lower(drug) like '%ketek%' then 1\n      when lower(drug) like '%levaquin%' then 1\n      when lower(drug) like '%levofloxacin%' then 1\n      when lower(drug) like '%lincocin%' then 1\n      when lower(drug) like '%macrobid%' then 1\n      when lower(drug) like '%macrodantin%' then 1\n      when lower(drug) like '%maxipime%' then 1\n      when lower(drug) like '%mefoxin%' then 1\n      when lower(drug) like '%metronidazole%' then 1\n      when lower(drug) like '%minocin%' then 1\n      when lower(drug) like '%minocycline%' then 1\n      when lower(drug) like '%monodox%' then 1\n      when lower(drug) like '%monurol%' then 1\n      when lower(drug) like '%morgidox%' then 1\n      when lower(drug) like '%moxatag%' then 1\n      when lower(drug) like '%moxifloxacin%' then 1\n      when lower(drug) like '%myrac%' then 1\n      when lower(drug) like '%nafcillin sodium%' then 1\n      when lower(drug) like '%nicazel doxy 30%' then 1\n      when lower(drug) like '%nitrofurantoin%' then 1\n      when lower(drug) like '%noroxin%' then 1\n      when lower(drug) like '%ocudox%' then 1\n      when lower(drug) like '%ofloxacin%' then 1\n      when lower(drug) like '%omnicef%' then 1\n      when lower(drug) like '%oracea%' then 1\n      when lower(drug) like '%oraxyl%' then 1\n      when lower(drug) like '%oxacillin%' then 1\n      when lower(drug) like '%pc pen vk%' then 1\n      when lower(drug) like '%pce dispertab%' then 1\n      when lower(drug) like '%panixine%' then 1\n      when lower(drug) like '%pediazole%' then 1\n      when lower(drug) like '%penicillin%' then 1\n      when lower(drug) like '%periostat%' then 1\n      when lower(drug) like '%pfizerpen%' then 1\n      when lower(drug) like '%piperacillin%' then 1\n      when lower(drug) like '%tazobactam%' then 1\n      when lower(drug) like '%primsol%' then 1\n      when lower(drug) like '%proquin%' then 1\n      when lower(drug) like '%raniclor%' then 1\n      when lower(drug) like '%rifadin%' then 1\n      when lower(drug) like '%rifampin%' then 1\n      when lower(drug) like '%rocephin%' then 1\n      when lower(drug) like '%smz-tmp%' then 1\n      when lower(drug) like '%septra%' then 1\n      when lower(drug) like '%septra ds%' then 1\n      when lower(drug) like '%septra%' then 1\n      when lower(drug) like '%solodyn%' then 1\n      when lower(drug) like '%spectracef%' then 1\n      when lower(drug) like '%streptomycin sulfate%' then 1\n      when lower(drug) like '%sulfadiazine%' then 1\n      when lower(drug) like '%sulfamethoxazole%' then 1\n      when lower(drug) like '%trimethoprim%' then 1\n      when lower(drug) like '%sulfatrim%' then 1\n      when lower(drug) like '%sulfisoxazole%' then 1\n      when lower(drug) like '%suprax%' then 1\n      when lower(drug) like '%synercid%' then 1\n      when lower(drug) like '%tazicef%' then 1\n      when lower(drug) like '%tetracycline%' then 1\n      when lower(drug) like '%timentin%' then 1\n      when lower(drug) like '%tobi%' then 1\n      when lower(drug) like '%tobramycin%' then 1\n      when lower(drug) like '%trimethoprim%' then 1\n      when lower(drug) like '%unasyn%' then 1\n      when lower(drug) like '%vancocin%' then 1\n      when lower(drug) like '%vancomycin%' then 1\n      when lower(drug) like '%vantin%' then 1\n      when lower(drug) like '%vibativ%' then 1\n      when lower(drug) like '%vibra-tabs%' then 1\n      when lower(drug) like '%vibramycin%' then 1\n      when lower(drug) like '%zinacef%' then 1\n      when lower(drug) like '%zithromax%' then 1\n      when lower(drug) like '%zmax%' then 1\n      when lower(drug) like '%zosyn%' then 1\n      when lower(drug) like '%zyvox%' then 1\n    else 0\n    end as antibiotic\n  from prescriptions\n  where drug_type in ('MAIN','ADDITIVE')\n  -- we exclude routes via the eye, ears, or topically\n  and route not in ('OU','OS','OD','AU','AS','AD', 'TP')\n  and lower(route) not like '%ear%'\n  and lower(route) not like '%eye%'\n  -- we exclude certain types of antibiotics: topical creams, gels, desens, etc\n  and lower(drug) not like '%cream%'\n  and lower(drug) not like '%desensitization%'\n  and lower(drug) not like '%ophth oint%'\n  and lower(drug) not like '%gel%'\n  -- other routes not sure about...\n  -- for sure keep: ('IV','PO','PO/NG','ORAL', 'IV DRIP', 'IV BOLUS')\n  -- ? VT, PB, PR, PL, NS, NG, NEB, NAS, LOCK, J TUBE, IVT\n  -- ? IT, IRR, IP, IO, INHALATION, IN, IM\n  -- ? IJ, IH, G TUBE, DIALYS\n  -- ?? enemas??\n)\nselect\n  drug --, drug_name_generic\n  , count(*) as numobs\nfrom t1\nwhere antibiotic = 1\ngroup by drug --, drug_name_generic\norder by numobs desc", "config": {"alias": null, "column_types": {}, "database": null, "enabled": true, "full_refresh": null, "materialized": "table", "meta": {}, "on_schema_change": "ignore", "persist_docs": {}, "post-hook": [], "pre-hook": [], "quoting": {}, "schema": null, "tags": [], "unique_key": null}, "created_at": 1658942123.3358812, "database": "mimic", "deferred": false, "depends_on": {"macros": ["macro.dbt.drop_relation_if_exists", "macro.dbt.run_hooks", "macro.dbt.statement", "macro.dbt.create_indexes", "macro.dbt.persist_docs"], "nodes": []}, "description": "", "docs": {"show": true}, "extra_ctes": [], "extra_ctes_injected": true, "fqn": ["mimic", "treatment", "abx_prescriptions_list"], "meta": {}, "name": "abx_prescriptions_list", "original_file_path": "models/treatment/abx_prescriptions_list.sql", "package_name": "mimic", "patch_path": null, "path": "treatment/abx_prescriptions_list.sql", "raw_sql": "{{ config(materialized = 'table') }} \nwith t1 as\n(\n  select\n    drug, drug_name_generic\n    , route\n    , case\n      when lower(drug) like '%adoxa%' then 1\n      when lower(drug) like '%ala-tet%' then 1\n      when lower(drug) like '%alodox%' then 1\n      when lower(drug) like '%amikacin%' then 1\n      when lower(drug) like '%amikin%' then 1\n      when lower(drug) like '%amoxicillin%' then 1\n      when lower(drug) like '%amoxicillin%clavulanate%' then 1\n      when lower(drug) like '%clavulanate%' then 1\n      when lower(drug) like '%ampicillin%' then 1\n      when lower(drug) like '%augmentin%' then 1\n      when lower(drug) like '%avelox%' then 1\n      when lower(drug) like '%avidoxy%' then 1\n      when lower(drug) like '%azactam%' then 1\n      when lower(drug) like '%azithromycin%' then 1\n      when lower(drug) like '%aztreonam%' then 1\n      when lower(drug) like '%axetil%' then 1\n      when lower(drug) like '%bactocill%' then 1\n      when lower(drug) like '%bactrim%' then 1\n      when lower(drug) like '%bethkis%' then 1\n      when lower(drug) like '%biaxin%' then 1\n      when lower(drug) like '%bicillin l-a%' then 1\n      when lower(drug) like '%cayston%' then 1\n      when lower(drug) like '%cefazolin%' then 1\n      when lower(drug) like '%cedax%' then 1\n      when lower(drug) like '%cefoxitin%' then 1\n      when lower(drug) like '%ceftazidime%' then 1\n      when lower(drug) like '%cefaclor%' then 1\n      when lower(drug) like '%cefadroxil%' then 1\n      when lower(drug) like '%cefdinir%' then 1\n      when lower(drug) like '%cefditoren%' then 1\n      when lower(drug) like '%cefepime%' then 1\n      when lower(drug) like '%cefotetan%' then 1\n      when lower(drug) like '%cefotaxime%' then 1\n      when lower(drug) like '%cefpodoxime%' then 1\n      when lower(drug) like '%cefprozil%' then 1\n      when lower(drug) like '%ceftibuten%' then 1\n      when lower(drug) like '%ceftin%' then 1\n      when lower(drug) like '%cefuroxime %' then 1\n      when lower(drug) like '%cefuroxime%' then 1\n      when lower(drug) like '%cephalexin%' then 1\n      when lower(drug) like '%chloramphenicol%' then 1\n      when lower(drug) like '%cipro%' then 1\n      when lower(drug) like '%ciprofloxacin%' then 1\n      when lower(drug) like '%claforan%' then 1\n      when lower(drug) like '%clarithromycin%' then 1\n      when lower(drug) like '%cleocin%' then 1\n      when lower(drug) like '%clindamycin%' then 1\n      when lower(drug) like '%cubicin%' then 1\n      when lower(drug) like '%dicloxacillin%' then 1\n      when lower(drug) like '%doryx%' then 1\n      when lower(drug) like '%doxycycline%' then 1\n      when lower(drug) like '%duricef%' then 1\n      when lower(drug) like '%dynacin%' then 1\n      when lower(drug) like '%ery-tab%' then 1\n      when lower(drug) like '%eryped%' then 1\n      when lower(drug) like '%eryc%' then 1\n      when lower(drug) like '%erythrocin%' then 1\n      when lower(drug) like '%erythromycin%' then 1\n      when lower(drug) like '%factive%' then 1\n      when lower(drug) like '%flagyl%' then 1\n      when lower(drug) like '%fortaz%' then 1\n      when lower(drug) like '%furadantin%' then 1\n      when lower(drug) like '%garamycin%' then 1\n      when lower(drug) like '%gentamicin%' then 1\n      when lower(drug) like '%kanamycin%' then 1\n      when lower(drug) like '%keflex%' then 1\n      when lower(drug) like '%ketek%' then 1\n      when lower(drug) like '%levaquin%' then 1\n      when lower(drug) like '%levofloxacin%' then 1\n      when lower(drug) like '%lincocin%' then 1\n      when lower(drug) like '%macrobid%' then 1\n      when lower(drug) like '%macrodantin%' then 1\n      when lower(drug) like '%maxipime%' then 1\n      when lower(drug) like '%mefoxin%' then 1\n      when lower(drug) like '%metronidazole%' then 1\n      when lower(drug) like '%minocin%' then 1\n      when lower(drug) like '%minocycline%' then 1\n      when lower(drug) like '%monodox%' then 1\n      when lower(drug) like '%monurol%' then 1\n      when lower(drug) like '%morgidox%' then 1\n      when lower(drug) like '%moxatag%' then 1\n      when lower(drug) like '%moxifloxacin%' then 1\n      when lower(drug) like '%myrac%' then 1\n      when lower(drug) like '%nafcillin sodium%' then 1\n      when lower(drug) like '%nicazel doxy 30%' then 1\n      when lower(drug) like '%nitrofurantoin%' then 1\n      when lower(drug) like '%noroxin%' then 1\n      when lower(drug) like '%ocudox%' then 1\n      when lower(drug) like '%ofloxacin%' then 1\n      when lower(drug) like '%omnicef%' then 1\n      when lower(drug) like '%oracea%' then 1\n      when lower(drug) like '%oraxyl%' then 1\n      when lower(drug) like '%oxacillin%' then 1\n      when lower(drug) like '%pc pen vk%' then 1\n      when lower(drug) like '%pce dispertab%' then 1\n      when lower(drug) like '%panixine%' then 1\n      when lower(drug) like '%pediazole%' then 1\n      when lower(drug) like '%penicillin%' then 1\n      when lower(drug) like '%periostat%' then 1\n      when lower(drug) like '%pfizerpen%' then 1\n      when lower(drug) like '%piperacillin%' then 1\n      when lower(drug) like '%tazobactam%' then 1\n      when lower(drug) like '%primsol%' then 1\n      when lower(drug) like '%proquin%' then 1\n      when lower(drug) like '%raniclor%' then 1\n      when lower(drug) like '%rifadin%' then 1\n      when lower(drug) like '%rifampin%' then 1\n      when lower(drug) like '%rocephin%' then 1\n      when lower(drug) like '%smz-tmp%' then 1\n      when lower(drug) like '%septra%' then 1\n      when lower(drug) like '%septra ds%' then 1\n      when lower(drug) like '%septra%' then 1\n      when lower(drug) like '%solodyn%' then 1\n      when lower(drug) like '%spectracef%' then 1\n      when lower(drug) like '%streptomycin sulfate%' then 1\n      when lower(drug) like '%sulfadiazine%' then 1\n      when lower(drug) like '%sulfamethoxazole%' then 1\n      when lower(drug) like '%trimethoprim%' then 1\n      when lower(drug) like '%sulfatrim%' then 1\n      when lower(drug) like '%sulfisoxazole%' then 1\n      when lower(drug) like '%suprax%' then 1\n      when lower(drug) like '%synercid%' then 1\n      when lower(drug) like '%tazicef%' then 1\n      when lower(drug) like '%tetracycline%' then 1\n      when lower(drug) like '%timentin%' then 1\n      when lower(drug) like '%tobi%' then 1\n      when lower(drug) like '%tobramycin%' then 1\n      when lower(drug) like '%trimethoprim%' then 1\n      when lower(drug) like '%unasyn%' then 1\n      when lower(drug) like '%vancocin%' then 1\n      when lower(drug) like '%vancomycin%' then 1\n      when lower(drug) like '%vantin%' then 1\n      when lower(drug) like '%vibativ%' then 1\n      when lower(drug) like '%vibra-tabs%' then 1\n      when lower(drug) like '%vibramycin%' then 1\n      when lower(drug) like '%zinacef%' then 1\n      when lower(drug) like '%zithromax%' then 1\n      when lower(drug) like '%zmax%' then 1\n      when lower(drug) like '%zosyn%' then 1\n      when lower(drug) like '%zyvox%' then 1\n    else 0\n    end as antibiotic\n  from prescriptions\n  where drug_type in ('MAIN','ADDITIVE')\n  -- we exclude routes via the eye, ears, or topically\n  and route not in ('OU','OS','OD','AU','AS','AD', 'TP')\n  and lower(route) not like '%ear%'\n  and lower(route) not like '%eye%'\n  -- we exclude certain types of antibiotics: topical creams, gels, desens, etc\n  and lower(drug) not like '%cream%'\n  and lower(drug) not like '%desensitization%'\n  and lower(drug) not like '%ophth oint%'\n  and lower(drug) not like '%gel%'\n  -- other routes not sure about...\n  -- for sure keep: ('IV','PO','PO/NG','ORAL', 'IV DRIP', 'IV BOLUS')\n  -- ? VT, PB, PR, PL, NS, NG, NEB, NAS, LOCK, J TUBE, IVT\n  -- ? IT, IRR, IP, IO, INHALATION, IN, IM\n  -- ? IJ, IH, G TUBE, DIALYS\n  -- ?? enemas??\n)\nselect\n  drug --, drug_name_generic\n  , count(*) as numobs\nfrom t1\nwhere antibiotic = 1\ngroup by drug --, drug_name_generic\norder by numobs desc", "refs": [], "relation_name": "\"mimic\".\"public\".\"abx_prescriptions_list\"", "resource_type": "model", "root_path": "/home/ceci/Desktop/mimic-dbt", "schema": "public", "sources": [], "tags": [], "unique_id": "model.mimic.abx_prescriptions_list", "unrendered_config": {"materialized": "table"}}, "status": "success", "thread_id": "Thread-1", "timing": [{"completed_at": "2022-07-27T22:52:09.294620Z", "name": "compile", "started_at": "2022-07-27T22:52:09.290109Z"}, {"completed_at": "2022-07-27T22:52:49.631073Z", "name": "execute", "started_at": "2022-07-27T22:52:09.295362Z"}]}, "unique_id": "model.mimic.abx_prescriptions_list"}, "invocation_id": "56c57f22-0b72-49cd-a574-997b2fd901b7", "level": "debug", "log_version": 2, "msg": "Finished running node model.mimic.abx_prescriptions_list", "pid": 44508, "thread_name": "Thread-1", "ts": "2022-07-27T22:52:49.633502Z", "type": "log_line"}
            info: end_time, duration, total_query, rows_affect, thread_name
            flag: contains msg "Finished running node"
    """

    def _parse_project_start_line(self, line):
        json_data = json.loads(line)
        if "invocation_id" in json_data:
            self.running_id = json_data["invocation_id"]
            project_start_time = json_data["ts"]
            # project_start_time = datetime.strptime(project_start_time, "%Y-%m-%dT%H:%M:%S.%fZ")

            self.periods[project_start_time] = self.running_id

            self.metadatas.contents[self.running_id] = {}
            self.df.insert_running_date(project_start_time, self.running_id)

            print("project id inserted; " + str(project_start_time) + "; " + self.running_id)

    def _parse_query_start_line(self, line):
        json_data = json.loads(line)
        if "invocation_id" in json_data:
            metadata = self.metadatas.get_contents_by_id(self.running_id)
            if "data" in json_data and "index" in json_data["data"]:
                qindex = int(json_data["data"]["index"])
                total_query = json_data["data"]["total"]
                query_name = re.search(self.query_name_rule, line).group(1).replace("\"", "")
                # total_query = re.search(self.total_query_rule, line)
                start_time_str = re.search(self.query_start_time_rule, line).group(1).replace("\"", "")
                query_start_time = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%f')
                # if total_query is not None:
                #     total_query = int(total_query.group(1))
                # else:
                #     total_query = -1
                metadata[query_name] = {'query_start_time': query_start_time, 'total_query': total_query,
                                        'query_index': qindex, 'query_name': query_name}
                data = dict(
                    start_time=query_start_time,
                    total=total_query,
                    qindex=qindex
                )
                try:
                    self.df.insert(self.running_id, query_name, **data)
                    # print("query start line df inserted")
                except Exception as e:
                    print(repr(e) + "\n")

    def _parse_query_end_line(self, line):
        query_duration = re.search(self.duration_rule, line)
        thread_name = re.search(self.thread_name_rule, line).group(1)
        rows_effect = re.search(self.rows_affected_rule, line)
        if rows_effect is not None:
            rows_effect = int(rows_effect.group(1))
        else:
            rows_effect = -1

        if query_duration is not None and query_duration != "" and query_duration != "0":
            metadata = self.metadatas.get_contents_by_id(self.running_id)

            rule = r'"node_info": {(.*?)},'
            if re.search(rule, line) is not None:
                end_time_str = re.search(self.query_end_time_rule, line).group(1).replace("\"", "")
                query_name = re.search(self.query_name_rule, line).group(1).replace("\"", "")
                query_end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S.%f')
                try:
                    metadata.get(query_name)['duration'] = float(query_duration.group(1))
                    metadata.get(query_name)['query_end_time'] = query_end_time
                    metadata.get(query_name)['thread_name'] = thread_name.replace("\"", "")
                    metadata.get(query_name)['rows_effect'] = rows_effect
                except Exception as e:
                    print("\n" + repr(e) + "  " + query_name)

                data = dict(
                    duration=float(query_duration.group(1)),
                    end_time=query_end_time,
                    thread_name=thread_name.replace("\"", ""),
                    rows_effect=rows_effect
                )
                # print(self.running_id)
                # print(query_name)
                # print(query_end_time)
                # print(thread_name.replace("\"", ""))
                # print(int(rows_effect.group(1)))

                try:
                    self.df.insert(self.running_id, query_name, **data)
                    # print("query end line df inserted: [" + query_name + "]")
                except Exception as e:
                    # print(self.df.get_contents(self.running_id))
                    print(repr(e) + "\n")

    def get_period(self):
        return self.periods

    def get_metadata(self):
        return self.metadatas
