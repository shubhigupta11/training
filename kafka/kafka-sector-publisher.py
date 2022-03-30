import os
import json
from confluent_kafka import Producer

SECTOR_TOPIC = "sectors"
producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None: 
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}] offset {}'.format(msg.topic(), msg.partition(), msg.offset()))

for fileName in os.listdir("../sectors"):
    print (fileName)

    with open("../sectors/" + fileName, "r") as file:
        for line in file:
            line = line.strip()
            if line == '':
                continue
            # print (line)
            arr = line.split(",")
            sector = {
                "Company":  arr[0],  
                "Industry": arr[1] , 
                "Symbol": arr[2],
                "Series" : arr[3],
                "ISIN": arr[4],
            }
 

            #print(sector)
            payload =json.dumps(sector)
            print (payload)

            key = sector["Symbol"].encode('utf-8')
            value = payload.encode('utf-8')
            producer.produce(SECTOR_TOPIC, key=key, value = value , callback=delivery_report)

    producer.flush()
# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.