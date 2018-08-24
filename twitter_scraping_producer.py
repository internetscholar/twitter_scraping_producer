import boto3
import psycopg2
from psycopg2 import extras
import json
import configparser
import os


def main():
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
    subqueries_queue = sqs.get_queue_by_name(QueueName='twitter_scraping')
    subqueries_queue.purge()

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

    c.execute("""
        select twitter_scraping_subquery.query_alias,
               search_terms,
               language,
               extract(epoch from twitter_scraping_subquery.since)::integer as since,
               extract(epoch from least(twitter_scraping_subquery.since + subquery_interval_in_seconds,
                                        twitter_scraping_query.until,
                                        min(twitter_dry_tweet.published_at) + interval '1 second')
                   ::timestamp(0))::integer as until,
               extract(epoch from tolerance_in_seconds)::integer as tolerance_in_seconds
        from twitter_scraping_subquery left outer join twitter_dry_tweet using (query_alias, since) ,
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
                 language;
    """)

    incomplete_queries = c.fetchall()
    for query in incomplete_queries:
        subqueries_queue.send_message(MessageBody=json.dumps(query))

    conn.close()


if __name__ == '__main__':
    main()
