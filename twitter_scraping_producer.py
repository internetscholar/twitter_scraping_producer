import boto3
import psycopg2
from psycopg2 import extras
import json
import configparser
import os
import logging


def main():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    c = conn.cursor(cursor_factory=extras.RealDictCursor)
    logging.info('Connected to database %s', config['database']['db_name'])

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    c.execute("""select * from aws_credentials;""")
    aws_credential = c.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['default_region']
    )
    sqs = aws_session.resource('sqs')
    subqueries_queue = sqs.get_queue_by_name(QueueName='twitter_scraping')
    subqueries_queue.purge()
    logging.info('Cleaned queue twitter_scraping')

    c.execute("""
        insert into twitter_scraping_subquery (query_alias, since)
        select query_alias,
               generate_series(since,
                               until - interval '1 second',
                               subquery_interval_in_seconds)
        from twitter_scraping_query,
             project
        where twitter_scraping_query.project_name = project.project_name and
              project.active
        on conflict do nothing;
    """)
    conn.commit()
    logging.info('Create records on twitter_scraping_subquery')

    c.execute("""
        select twitter_scraping_subquery.query_alias,
               search_terms,
               language,
               extract(epoch from twitter_scraping_subquery.since)::integer as since,
               extract(epoch from least(twitter_scraping_subquery.since + subquery_interval_in_seconds,
                                        twitter_scraping_query.until,
                                        min(twitter_dry_tweet.published_at) + interval '1 second',
                                        min(twitter_scraping_attempt.until))
                   ::timestamp(0))::integer as until,
               extract(epoch from tolerance_in_seconds)::integer as tolerance_in_seconds
        from twitter_scraping_subquery left outer join twitter_dry_tweet using (query_alias, since) 
                                      left outer join twitter_scraping_attempt using (query_alias, since),
             twitter_scraping_query,
             project
        where twitter_scraping_query.project_name = project.project_name and
              project.active and
              twitter_scraping_query.query_alias = twitter_scraping_subquery.query_alias and
              not twitter_scraping_subquery.complete and
              (twitter_dry_tweet.published_at is null or
               twitter_dry_tweet.published_at >= twitter_scraping_subquery.since)
        group by twitter_scraping_subquery.query_alias,
                 twitter_scraping_subquery.since,
                 subquery_interval_in_seconds,
                 twitter_scraping_query.until,
                 tolerance_in_seconds,
                 search_terms,
                 language
        order by twitter_scraping_subquery.query_alias, since, until;
    """)
    logging.info('Queried all subqueries that have not been processed yet')

    incomplete_queries = c.fetchall()
    for query in incomplete_queries:
        subqueries_queue.send_message(MessageBody=json.dumps(query))
    logging.info('Enqueued all incomplete subqueries')

    conn.close()
    logging.info('Disconnected from database')


if __name__ == '__main__':
    main()
