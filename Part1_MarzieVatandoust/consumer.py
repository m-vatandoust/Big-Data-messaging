from kafka import KafkaConsumer,KafkaProducer
from json import loads,dumps
import re
import persian
from datetime import datetime
import uuid
import functions as F

seenIds = set()
hashtag = {}
syms = open('Main_Symbols.txt', encoding='utf8').read().splitlines()
stop_words = open('Persian_StopWords.txt', encoding='utf8').read().splitlines()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

consumer = KafkaConsumer(
    'PreProcess',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     auto_commit_interval_ms=100)

fetched = 0
cnt = 0
for message in consumer:
    cnt +=1 
    tweet = message.value
    
    if tweet['id'] not in seenIds:
        try:
            # Add UUID
            tweet['UUID'] = str(uuid.uuid1())

            # add Time Stamp of receiving:
            tweet['Receipt_time'] = str(datetime.now())

            # Add Key words : 1.remove stop words 2.max tf/idf
            tweet['Keywords'] = F.FindKeyWords(stop_words , syms ,tweet['content'])

            # Extract links and save in an array
            res = re.search("(?P<url>https?://[^\s]+)", tweet['content'])
            if res:
                tweet['links'] = res.group("url")

            # Add hashtags:
            tweet['hashtags']= re.findall(r"#(\w+)", tweet['content']) # Find Hashtags
            tweet['content'] =  persian.convert_ar_characters(tweet['content']) # Convert arabic to persian

            fetched +=1
            for h in tweet['hashtags']: hashtag.update({fetched: h}) # Update hashtag dictionary
            seenIds.add(tweet['id'])
            
            print(str(tweet['Keywords']) +''+ str(cnt))

            # Send data to next topic
            producer.send('persistence', value=tweet)
        
        except Exception as e:
            print(e)

        
