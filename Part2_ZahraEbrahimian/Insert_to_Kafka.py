from kafka import KafkaConsumer,KafkaProducer
from json import loads,dumps
from elasticsearch import Elasticsearch

elasticSearch= Elasticsearch([{'host':'localhost','port':9200}])

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'),
						 api_version=(0, 10, 1))

consumer = KafkaConsumer(
     'persistence', # NAME OF CHANAL WHICH WE GET DATA FROM.
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     auto_commit_interval_ms=100,
	 api_version=(0, 10, 1))


for message in consumer: 
    tweet = message.value
    
    # send data to elasticSearch:
    elasticSearch.index(index="tweeter",doc_type="tweeter",body=tweet)
   
    # Send data to next topic if its nessecary:
    # roducer.send('persistence', value=tweet)
    
