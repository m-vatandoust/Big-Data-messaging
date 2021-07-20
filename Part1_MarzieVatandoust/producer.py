from time import sleep
from json import dumps
from kafka import KafkaProducer

import json, requests
import pprint
import time
import re
import persian
import numpy as np

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

url = "https://www.sahamyab.com/guest/twiter/list?v=0.1"
headers = {'User-Agent': 'Mozilla/5.0  Chrome/50.0.2661.102'}
total = 5
fetched = 0
seenIds = set()
hashtag = {}

tic=time.perf_counter()

while True:
    response = requests.get(url= url, headers=headers)
    if response.status_code != 200:
        print('HTTP', response.status_code)
        continue
    
    data = response.json()['items']
    
    for tweet in data:
        if tweet['id'] not in seenIds:
            try:
                producer.send('PreProcess', value=tweet)
                
                # buff=WriteMongo('',tweet)
                # buff={**buff,'mainID':main.count()}
                # main.insert_one(buff)

                seenIds.add(tweet['id'])
                fetched +=1
                print('tweet '+ str(tweet['content']) + " fetched,     total: "+ str(fetched))
            except Exception as e:
                print(e)
    time.sleep(.5)
                

