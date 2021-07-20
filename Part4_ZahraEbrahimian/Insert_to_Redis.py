from kafka import KafkaConsumer,KafkaProducer
from json import loads,dumps
from redis_Management import RedisManagement, add_Redis
import json

redis_client = RedisManagement(exp_duration=3600*24*7)
key_times = ['year', 'month', 'day', 'hour']

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'),
						 api_version=(0, 10, 1)
						 )

consumer = KafkaConsumer(
     'chanal_2', # HERE IS THE NAME OF CHANAL WHICH YOU GET DATA FROM.
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     auto_commit_interval_ms=100,
	 api_version=(0, 10, 1)
	 )

consumer.subscribe(['chanal_2'])
for message in consumer: 
    data = (list(message)[6])
    print(data)
    add_Redis(data, redis_client, 1,key_times=key_times)
	
consumer.close()

#Send data to next topic :
#producer.send('Analytics', value=data)
    
