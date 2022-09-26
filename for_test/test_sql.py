from sqlalchemy import create_engine
import pandas as pd

# psycopg2 lib is required
engine = create_engine("postgresql://ceci:mimic@localhost:5432/mimic")

query_sql = """SELECT
                    idstat.relname AS TABLE_NAME,
                    pg_size_pretty(pg_relation_size(idstat.relid)) AS table_size,
                    indexrelname AS index_name,
                    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
                    idstat.idx_scan AS index_scans_count,
                    tabstat.idx_scan AS table_reads_index_count,
                    tabstat.seq_scan AS table_reads_seq_count,
                    tabstat.seq_scan + tabstat.idx_scan AS table_reads_count,
                    n_tup_upd + n_tup_ins + n_tup_del AS table_writes_count
                FROM
                    pg_stat_user_indexes AS idstat
                JOIN
                    pg_indexes
                    ON
                    indexrelname = indexname
                    AND
                    idstat.schemaname = pg_indexes.schemaname
                JOIN
                    pg_stat_user_tables AS tabstat
                    ON
                    idstat.relid = tabstat.relid
                WHERE
                    indexdef !~* 'unique'
                ORDER BY
                    idstat.idx_scan DESC,
                    pg_relation_size(indexrelid) DESC
            """

records = pd.read_sql(query_sql, engine)

print(type(records))
# index_info_list = []
#
# for row in records.values:
#     index_info_list.append({
#         "table": row[0],
#         "table_size": row[1],
#         "index": row[2],
#         "index_size": row[3],
#         "scans_count": row[4],
#         "reads_index_count": row[5],
#         "reads_seq_count": row[6],
#         "table_reads_count": row[7],
#         "table_writes_count": row[8],
#     })

# for i in index_info_list:
#     print(index_info_list)
