from dataclasses import dataclass
from datetime import datetime

from pendulum import duration
# from . import BaseAdapter
from .parser import BaseAdapter
# from parser import BaseAdapter


import logging
import re
from typing import List, Optional
import glob, os
import json
from dacite import from_dict
from dacite.config import Config

import dateutil

# "run_result":{
#          "adapter_response":{
#             "_message":"SELECT 13583",
#             "code":"SELECT",
#             "rows_affected":13583
#          },
#          "agate_table":null,
#          "execution_time":0.9121809005737305,
#          "failures":null,
#          "message":"SELECT 13583",
#          "node":{
#             "alias":"ffp_transfusion",
#             "build_path":"target/run/mimic/models/fluid_balance/ffp_transfusion.sql",
#             "checksum":{
#                "checksum":"23fe8ba49b096b4f114c600f852de8c44624d50b4edae0104ea7473015a1b980",
#                "name":"sha256"
#             },
#             "columns":{
               
#             },
#             "compiled":true,
#             "compiled_path":"target/compiled/mimic/models/fluid_balance/ffp_transfusion.sql",
#             "compiled_sql":"-- Retrieves instances of fresh frozen plasma transfusions\nWITH raw_ffp AS (\n  SELECT\n      CASE\n        WHEN amount IS NOT NULL THEN amount\n        WHEN stopped IS NOT NULL THEN 0\n        -- impute 200 mL when unit is not documented\n        -- this is an approximation which holds ~90% of the time\n        ELSE 200\n      END AS amount\n    , amountuom\n    , icustay_id\n    , charttime\n  FROM inputevents_cv\n  WHERE itemid IN\n  (\n    30005,  -- Fresh Frozen Plasma\n    30180   -- Fresh Froz Plasma\n  )\n  AND amount > 0\n  AND icustay_id IS NOT NULL\n  UNION ALL\n  SELECT amount\n    , amountuom\n    , icustay_id\n    , endtime AS charttime\n  FROM inputevents_mv\n  WHERE itemid in\n  (\n    220970   -- Fresh Frozen Plasma\n  )\n  AND amount > 0\n  AND icustay_id IS NOT NULL\n),\npre_icu_ffp as (\n  SELECT\n    sum(amount) as amount, icustay_id\n  FROM inputevents_cv\n  WHERE itemid IN (\n    44172,  -- FFP GTT         \n    44236,  -- E.R. FFP        \n    46410,  -- angio FFP\n    46418,  -- ER ffp\n    46684,  -- ER FFP\n    44819,  -- FFP ON FARR 2\n    46530,  -- Floor FFP       \n    44044,  -- FFP Drip\n    46122,  -- ER in FFP\n    45669,  -- ED FFP\n    42323   -- er ffp\n  )\n  AND amount > 0\n  AND icustay_id IS NOT NULL\n  GROUP BY icustay_id\n  UNION ALL\n  SELECT\n    sum(amount) as amount, icustay_id\n  FROM inputevents_mv\n  WHERE itemid IN (\n    227072  -- PACU FFP Intake\n  )\n  AND amount > 0\n  AND icustay_id IS NOT NULL\n  GROUP BY icustay_id\n),\ncumulative AS (\n  SELECT\n    sum(amount) over (PARTITION BY icustay_id ORDER BY charttime DESC) AS amount\n    , amountuom\n    , icustay_id\n    , charttime\n    , DATETIME_DIFF(lag(charttime) over (PARTITION BY icustay_id ORDER BY charttime ASC), charttime, 'HOUR') AS delta\n  FROM raw_ffp\n)\n-- We consider any transfusions started within 1 hr of the last one\n-- to be part of the same event\nSELECT\n    cm.icustay_id\n  , cm.charttime\n  , ROUND(CAST(cm.amount AS numeric) - CASE\n      WHEN ROW_NUMBER() OVER w = 1 THEN CAST(0 AS numeric)\n      ELSE cast(lag(cm.amount) OVER w AS numeric)\n    END, 2) AS amount\n  , ROUND(CAST(cm.amount AS numeric) + CASE\n      WHEN pre.amount IS NULL THEN CAST(0 AS numeric)\n      ELSE CAST(pre.amount AS numeric)\n    END, 2) AS totalamount\n  , cm.amountuom\nFROM cumulative AS cm\nLEFT JOIN pre_icu_ffp AS pre\n  USING (icustay_id)\nWHERE delta IS NULL OR delta < -1\nWINDOW w AS (PARTITION BY cm.icustay_id ORDER BY cm.charttime DESC)\nORDER BY icustay_id, charttime",
#             "config":{
#                "alias":null,
#                "column_types":{
                  
#                },
#                "database":null,
#                "enabled":true,
#                "full_refresh":null,
#                "grants":{
                  
#                },
#                "materialized":"table",
#                "meta":{
                  
#                },
#                "on_schema_change":"ignore",
#                "persist_docs":{
                  
#                },
#                "post-hook":[
                  
#                ],
#                "pre-hook":[
                  
#                ],
#                "quoting":{
                  
#                },
#                "schema":null,
#                "tags":[
                  
#                ],
#                "unique_key":null
#             },
#             "config_call_dict":{
               
#             },
#             "created_at":1665776729.929077,
#             "database":"mimic",
#             "deferred":false,
#             "depends_on":{
#                "macros":[
#                   "macro.dbt.load_cached_relation",
#                   "macro.dbt.make_intermediate_relation",
#                   "macro.dbt.make_backup_relation",
#                   "macro.dbt.drop_relation_if_exists",
#                   "macro.dbt.run_hooks",
#                   "macro.dbt.statement",
#                   "macro.dbt.create_indexes",
#                   "macro.dbt.should_revoke",
#                   "macro.dbt.apply_grants",
#                   "macro.dbt.persist_docs"
#                ],
#                "nodes":[
                  
#                ]
#             },
#             "description":"",
#             "docs":{
#                "show":true
#             },
#             "extra_ctes":[
               
#             ],
#             "extra_ctes_injected":true,
#             "fqn":[
#                "mimic",
#                "fluid_balance",
#                "ffp_transfusion"
#             ],
#             "meta":{
               
#             },
#             "metrics":[
               
#             ],
#             "name":"ffp_transfusion",
#             "original_file_path":"models/fluid_balance/ffp_transfusion.sql",
#             "package_name":"mimic",
#             "patch_path":null,
#             "path":"fluid_balance/ffp_transfusion.sql",
#             "raw_sql":"-- Retrieves instances of fresh frozen plasma transfusions\nWITH raw_ffp AS (\n  SELECT\n      CASE\n        WHEN amount IS NOT NULL THEN amount\n        WHEN stopped IS NOT NULL THEN 0\n        -- impute 200 mL when unit is not documented\n        -- this is an approximation which holds ~90% of the time\n        ELSE 200\n      END AS amount\n    , amountuom\n    , icustay_id\n    , charttime\n  FROM inputevents_cv\n  WHERE itemid IN\n  (\n    30005,  -- Fresh Frozen Plasma\n    30180   -- Fresh Froz Plasma\n  )\n  AND amount > 0\n  AND icustay_id IS NOT NULL\n  UNION ALL\n  SELECT amount\n    , amountuom\n    , icustay_id\n    , endtime AS charttime\n  FROM inputevents_mv\n  WHERE itemid in\n  (\n    220970   -- Fresh Frozen Plasma\n  )\n  AND amount > 0\n  AND icustay_id IS NOT NULL\n),\npre_icu_ffp as (\n  SELECT\n    sum(amount) as amount, icustay_id\n  FROM inputevents_cv\n  WHERE itemid IN (\n    44172,  -- FFP GTT         \n    44236,  -- E.R. FFP        \n    46410,  -- angio FFP\n    46418,  -- ER ffp\n    46684,  -- ER FFP\n    44819,  -- FFP ON FARR 2\n    46530,  -- Floor FFP       \n    44044,  -- FFP Drip\n    46122,  -- ER in FFP\n    45669,  -- ED FFP\n    42323   -- er ffp\n  )\n  AND amount > 0\n  AND icustay_id IS NOT NULL\n  GROUP BY icustay_id\n  UNION ALL\n  SELECT\n    sum(amount) as amount, icustay_id\n  FROM inputevents_mv\n  WHERE itemid IN (\n    227072  -- PACU FFP Intake\n  )\n  AND amount > 0\n  AND icustay_id IS NOT NULL\n  GROUP BY icustay_id\n),\ncumulative AS (\n  SELECT\n    sum(amount) over (PARTITION BY icustay_id ORDER BY charttime DESC) AS amount\n    , amountuom\n    , icustay_id\n    , charttime\n    , DATETIME_DIFF(lag(charttime) over (PARTITION BY icustay_id ORDER BY charttime ASC), charttime, 'HOUR') AS delta\n  FROM raw_ffp\n)\n-- We consider any transfusions started within 1 hr of the last one\n-- to be part of the same event\nSELECT\n    cm.icustay_id\n  , cm.charttime\n  , ROUND(CAST(cm.amount AS numeric) - CASE\n      WHEN ROW_NUMBER() OVER w = 1 THEN CAST(0 AS numeric)\n      ELSE cast(lag(cm.amount) OVER w AS numeric)\n    END, 2) AS amount\n  , ROUND(CAST(cm.amount AS numeric) + CASE\n      WHEN pre.amount IS NULL THEN CAST(0 AS numeric)\n      ELSE CAST(pre.amount AS numeric)\n    END, 2) AS totalamount\n  , cm.amountuom\nFROM cumulative AS cm\nLEFT JOIN pre_icu_ffp AS pre\n  USING (icustay_id)\nWHERE delta IS NULL OR delta < -1\nWINDOW w AS (PARTITION BY cm.icustay_id ORDER BY cm.charttime DESC)\nORDER BY icustay_id, charttime",
#             "refs":[
               
#             ],
#             "relation_name":"\"mimic\".\"public\".\"ffp_transfusion\"",
#             "resource_type":"model",
#             "root_path":"/Users/chenchunyu/Documents/workspace/Experiment/mimic/mimic-dbt",
#             "schema":"public",
#             "sources":[
               
#             ],
#             "tags":[
               
#             ],
#             "unique_id":"model.mimic.ffp_transfusion",
#             "unrendered_config":{
#                "materialized":"table"
#             }
#          },
#          "status":"success",
#          "thread_id":"Thread-7",
#          "timing":[
#             {
#                "completed_at":"2022-10-14T19:45:42.698834Z",
#                "name":"compile",
#                "started_at":"2022-10-14T19:45:42.688293Z"
#             },
#             {
#                "completed_at":"2022-10-14T19:45:43.593382Z",
#                "name":"execute",
#                "started_at":"2022-10-14T19:45:42.701351Z"
#             }
#          ]
#       },

@dataclass
class RunResult:
    pass
@dataclass
class NodeInfo:
    materialized: str
    node_finished_at: Optional[str]
    node_name: str
    node_path: str
    node_started_at:  Optional[str]
    node_status: str
    resource_type: str
    resource_type: str
    unique_id: str

@dataclass
class Data:
    description: str    
    index: int
    total: int
    node_info: NodeInfo

    # "materialized": "view",
    # "node_finished_at": null,
    # "node_name": "male_list_view",
    # "node_path": "human/name_list.sql",
    # "node_started_at": "2021-12-02T21:47:03.477004",
    # "node_status": "started",
    # "resource_type": "model",
    # "type": "node_status",
    # "unique_id": "model.jaffle_shop.name_list"

@dataclass
class LogUnit:
    code: str
    type: str
    invocation_id: str
    level: str
    log_version: int
    msg: str
    pid: int
    thread_name: str
    ts: str
    data: Data
    

class DbtJsonLogAdapter(BaseAdapter):
    def __init__(self, log_path, ext = ['.log', '.log.*']) -> None:
        super().__init__()
        log_files = self._get_log_files(log_path= log_path, ext= ext)
        self._parse_log_file(log_files)


    def _get_log_files(self, log_path, ext = ['.log']):
        logs = []
        for EXT in ext:
            for fn in glob.glob(os.path.join(log_path, '*%s' % EXT)):
                if not os.path.isfile(fn):
                    logging.debug('Skipping file %s' % fn)
                    continue
                logs.append(fn)
        return logs
    
    def _parse_log_file(self, logfiles):
        for logfile in logfiles:
            with open(logfile) as f:
                for line in f:
                    json_data = json.loads(line)
                    
                    # self.df.get_contents(logunit.invocation_id)
                    self._parse_log_line(logunit= json_data)

                    # if json_data["level"] == "info":
                    #     self._parse_info_line(line=line)
                    # elif json_data["level"] == "debug":
                    #     self._parse_debug_line(line=line)

    def _parse_log_line(self, logunit:json):
        if logunit['code'] == 'A001':
            print( logunit['invocation_id'])
            self.df.insert_running_date(datetime.strptime(logunit['ts'], '%Y-%m-%dT%H:%M:%S.%f%z').strftime("%Y-%m-%d %H:%M:%S"), logunit['invocation_id'])
        invocation_id = logunit['invocation_id']
        logdata = logunit.get('data')
        if 'node_info' in logdata:
            idx = logdata.get('index')
            total = logdata.get('total')
            node_info = from_dict(data_class= NodeInfo, data= logdata.get('node_info'))
            if node_info.node_status == 'success':
                start_time = datetime.strptime(node_info.node_started_at, '%Y-%m-%dT%H:%M:%S.%f')
                end_time = datetime.strptime(node_info.node_finished_at, '%Y-%m-%dT%H:%M:%S.%f')
                duration = (end_time - start_time).seconds
                thread_name = logunit.get('thread_name')
                rows_affected = None
                if 'run_result' in logdata:
                    rows_affected = logdata.get('run_result').get('adapter_response').get('rows_affected')
                data = dict(
                    duration=float(duration),
                    status = node_info.node_status ,
                    start_time=start_time,
                    end_time=end_time,
                    thread_name= thread_name,
                    rows_effect=rows_affected
                )
                name = node_info.node_name
                self.df.insert(invocation_id, name= name, ** data)
            elif node_info.node_status == 'started':
                data = dict(
                    total = total,
                    status = node_info.node_status,
                    qindex = idx
                )
                name = node_info.node_name
                self.df.insert(invocation_id, name= name, ** data)

                # {"code": "Q033", "data": {"description": "table model public.ffp_transfusion", "index": 25, "node_info": {"materialized": "table", "node_finished_at": null, "node_name": "ffp_transfusion", "node_path": "fluid_balance/ffp_transfusion.sql", "node_started_at": "2022-10-14T19:45:42.682758", "node_status": "started", "resource_type": "model", "unique_id": "model.mimic.ffp_transfusion"}, "total": 104}, "invocation_id": "afb78d02-efa3-4186-bef2-5dfd8eaddb8e", "level": "info", "log_version": 2, "msg": "25 of 104 START 'node Exec' table model public.ffp_transfusion  ................ [RUN]", "pid": 74807, "thread_name": "Thread-7", "ts": "2022-10-14T19:45:42.684512Z", "type": "log_line"}


                

        

    def get_metadata(self):
        return self.df


if __name__ == '__main__':

    data = {
    "invocation_id": "30206572-f52f-4b91-af6d-d2b18fdbbbb8",
    "level": "info",
    "log_version": 1,
    "msg": "7 of 7 START view model dbt_testing.name_list.............................. [RUN]",
    "code": "Q033",
    "pid": 81915,
    "thread_name": "Thread-4",
    "ts": "2021-12-02T21:47:03.480384Z",
    "type": "log_line",
    "data":
    {
        "description": "view model dbt_testing.name_list",
        "index": 7,
        "total": 7
    },   
    "node_info":
    {
        "materialized": "view",
        "node_finished_at": 'null',
        "node_name": "male_list_view",
        "node_path": "human/name_list.sql",
        "node_started_at": "2021-12-02T21:47:03.477004",
        "node_status": "started",
        "resource_type": "model",
        "type": "node_status",
        "unique_id": "model.jaffle_shop.name_list"
    }    
    }
    pa = '/Users/chenchunyu/Documents/workspace/Experiment/mimic/mimic-dbt/logs'
    dja = DbtJsonLogAdapter(log_path= pa)
    
    # result = from_dict(data_class = LogUnit, data= data, config= Config(forward_references={'null': "null"}))
    
   