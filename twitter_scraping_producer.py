import boto3
import pendulum
from datetime import datetime
import psycopg2
from psycopg2 import extras
import json
import configparser
import os

if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    c = conn.cursor(cursor_factory=extras.RealDictCursor)

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    c.execute("""select * from aws_credentials;""")
    aws_credential = c.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['region_name']
    )
    sqs = aws_session.resource('sqs')

    c.execute("SELECT * FROM query WHERE status = 'PHASE1'")
    created_queries = c.fetchall()
    for created_query in created_queries:
        subqueries = list()
        since = pendulum.instance(created_query['since']).int_timestamp
        until = pendulum.instance(created_query['until']).int_timestamp
        # one subquery per day in the interval:
        number_of_subqueries = (pendulum.utcfromtimestamp(until) - pendulum.utcfromtimestamp(since)).days + 1
        interval = (until - since) // number_of_subqueries
        for x in range(number_of_subqueries):
            aux_until = since + interval
            if x == number_of_subqueries - 1:
                aux_until = until
            subqueries.append({'query_alias': created_query['query_alias'],
                               'since': datetime.utcfromtimestamp(since),
                               'until': datetime.utcfromtimestamp(aux_until),
                               'complete': False})
            since = aux_until
        dataText = ','.join(c.mogrify('(%s,%s,%s,%s)',
                                      (subquery['query_alias'],
                                       subquery['since'],
                                       subquery['until'],
                                       subquery['complete'])).decode('utf-8') for subquery in subqueries)
        c.execute("INSERT INTO subquery (query_alias, since, until, complete) VALUES " + dataText)
        c.execute("UPDATE query SET status = 'PHASE2' WHERE query_alias = %s", (created_query['query_alias'],))
        conn.commit()

    subqueries_queue = sqs.get_queue_by_name(QueueName='subqueries')
    subqueries_queue.purge()

    # if there are queries that were not totally retrieved...
    c.execute("SELECT * FROM query WHERE status = 'PHASE2'")
    incomplete_queries = c.fetchall()
    for query in incomplete_queries:
        # if there are queries that were not totally retrieved...
        c.execute("SELECT * FROM subquery WHERE NOT complete AND query_alias = %s", (query['query_alias'],))
        incomplete_subqueries = c.fetchall()

        for incomplete_subquery in incomplete_subqueries:
            subquery = {}
            subquery['query_alias'] = incomplete_subquery['query_alias']
            subquery['query'] = query['query']
            subquery['language'] = query['language']
            subquery['since'] = incomplete_subquery['since'].isoformat()
            subquery['until'] = incomplete_subquery['until'].isoformat()
            subquery['seconds_of_tolerance'] = query['seconds_of_tolerance']
            subqueries_queue.send_message(MessageBody=json.dumps(subquery))

    conn.close()