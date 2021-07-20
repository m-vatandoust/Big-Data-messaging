from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
from cassandra.query import tuple_factory
import json
import uuid
from kafka import KafkaConsumer
from json import loads


def insert_data(data, session):
    time_index = data.get('sendTimePersian')
    message_id = data.get("id")
    hashtags = data.get("hashtags")
    sender_name = data.get("senderUsername")
    keywords = data.get("Keywords")
    content = data.get("content")
    query = f"insert into total_posts (time_index, message_id, content) values ('{time_index}', '{message_id}', '{content}')"
    session.execute(query)
    for hashtag in hashtags:
        query = f"insert into hashtags (time_index, message_id, hashtag, content) values ('{time_index}', '{message_id}', '{hashtag}', '{content}')"
        session.execute(query)
    query = f"insert into userNames (time_index, sender_name, content) values ('{time_index}', '{sender_name}', '{content}')"
    session.execute(query)
    for keyword in keywords:
        query = f"insert into keywords (time_index, message_id, keyword) values ('{time_index}', '{message_id}', '{keyword}')"
        session.execute(query)


try:
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=200,
        row_factory=tuple_factory
    )
    cluster = Cluster(execution_profiles={
                      EXEC_PROFILE_DEFAULT: profile}, port=9042)
    session = cluster.connect('mykeyspace')
    consumer = KafkaConsumer(
        'persistence',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    print(1)
    query = """
    drop table total_posts;
    """
    session.execute(query)
    query = """
    drop table userNames;
    """
    session.execute(query)
    query = """
    drop table keywords;
    """
    session.execute(query)
    query = """
    drop table hashtags;
    """
    session.execute(query)
    query = """
    create table total_posts(
           time_index varchar,
           message_id text,
           content text,
           primary key(time_index)
        );
    """
    session.execute(query)
    query = """
    create table userNames(
           time_index varchar,
           content text,
           sender_name varchar,
           primary key(time_index)
        );
    """
    session.execute(query)
    query = """
    create table keywords(
           time_index varchar,
           message_id text,
           keyword varchar,
           primary key(time_index)
        );
    """
    session.execute(query)
    query = """
    create table hashtags(
           time_index varchar,
           hashtag varchar,
           message_id text,
           content text,
           primary key(time_index)
        );
    """
    session.execute(query)
    for message in consumer:
        print("ok")
        message = message.value
        print(message)
        insert_data(message, session)
except KeyboardInterrupt:
    pass
