"""
Redshift Multithreading connector for Fivetran Connector SDK.
Demonstrates threaded extraction from Amazon Redshift.
"""

import threading
import psycopg2
from fivetran_connector_sdk import connector, config, state, records, log, schema

CONFIG = config.Config(
    host=config.StringField(),
    port=config.IntegerField(default=5439),
    database=config.StringField(),
    user=config.StringField(),
    password=config.SecretField(),
    threads=config.IntegerField(default=4)
)

SCHEMA = schema.Schema(
    name="redshift_table",
    columns={
        "id": schema.StringColumn(),
        "data": schema.JSONColumn(),
    }
)

@connector(
    name="RedshiftMultithreadingConnector",
    version="0.1.0",
    config=CONFIG,
    schema=SCHEMA,
)
def run_connector(ctx: state.Context):
    def worker(offset):
        conn = psycopg2.connect(
            host=ctx.config.host,
            dbname=ctx.config.database,
            user=ctx.config.user,
            password=ctx.config.password,
            port=ctx.config.port,
        )
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM some_table LIMIT 100 OFFSET {offset}")
        for row in cur.fetchall():
            records.write("redshift_table", {"id": row[0], "data": row})
        conn.close()

    threads = []
    for i in range(ctx.config.threads):
        t = threading.Thread(target=worker, args=(i * 100,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return ctx.update_state({"last_sync": "now"})
