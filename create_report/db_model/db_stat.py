from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import pandas as pd
# psycopg2 lib is required


def get_postgres_index(postgres_engine: Engine):
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
    return records
