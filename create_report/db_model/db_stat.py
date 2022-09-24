import psycopg2


def get_postgres_index():
    # specify schema in 'options'
    connection = psycopg2.connect(user="ceci",
                                  password="mimic",
                                  host="localhost",
                                  port="5432",
                                  database="mimic",
                                  options="-c search_path=public")
    index_info_list = []
    try:
        cursor = connection.cursor()
        index_info_query = """SELECT
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
                                        pg_relation_size(indexrelid) DESC"""

        cursor.execute(index_info_query)
        print("Selecting rows from mobile table using cursor.fetchall")
        mobile_records = cursor.fetchall()

        print("Print each row and it's columns values")
        for row in mobile_records:
            index_info_list.append({
                "table": row[0],
                "table_size": row[1],
                "index": row[2],
                "index_size": row[3],
                "scans_count": row[4],
                "reads_index_count": row[5],
                "reads_seq_count": row[6],
                "table_reads_count": row[7],
                "table_writes_count": row[8],
            })
            # for i in range(len(row)):
            #     print(str(row[i]) + "  |   ", end="")
            # print("")

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
        return index_info_list
