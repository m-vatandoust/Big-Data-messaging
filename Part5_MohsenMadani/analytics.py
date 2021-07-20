from kafka import KafkaConsumer
from json import loads
from clickhouse_driver import Client
from datetime import datetime

# Connections:
clickhouse_client = Client('localhost')
clickhouse_client.execute('DROP TABLE IF EXISTS default.tweets')

# Create table:
clickhouse_client.execute('''
    CREATE TABLE default.tweets (
        tweet_id Nullable(String),
        sender_username Nullable(String),
        keywords Array(Nullable(String)),
        keywords_size UInt8 DEFAULT length(keywords),
        hashtags Array(Nullable(String)),
        hashtags_size UInt8 DEFAULT length(hashtags),
        type Nullable(String),
        receive_date_time DateTime('Asia/Tehran'),
        receive_date Date DEFAULT toDate(receive_date_time),
        receive_date_int UInt64 DEFAULT toYYYYMMDDhhmmss(receive_date_time),
        send_date_time DateTime('Asia/Tehran'),
        send_date Date DEFAULT toDate(send_date_time),
        send_date_int UInt64 DEFAULT toYYYYMMDDhhmmss(send_date_time),
        uuid UUID
    ) ENGINE = MergeTree 
      PARTITION BY send_date
      ORDER BY send_date;
''')

# Create consumer:
consumer = KafkaConsumer(
    'analytics',  # HERE IS THE NAME OF CHANNEL WHICH YOU GET DATA FROM.
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     auto_commit_interval_ms=100)

# Save in Click-House:
for message in consumer: 
    tweet = message.value
    # ******************* TWEET IS THE MESSAGE TOU GET *******************

    # Insert into db:
    clickhouse_client.execute(f'''
        INSERT INTO default.tweets (tweet_id, sender_username, keywords, hashtags, type, receive_date_time, send_date_time, uuid) VALUES (
            {tweet['id']},
            '{tweet['senderUsername']}',
            {tweet['Keywords']},
            {tweet['hashtags']},
            '{tweet['type']}',
            '{datetime.strptime(tweet['Receipt_time'], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')}',
            '{datetime.strptime(tweet['sendTime'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')}',
            '{tweet['UUID']}'
        )
    ''')
