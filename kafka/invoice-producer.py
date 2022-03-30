from confluent_kafka import Producer
import random
import uuid
import datetime
import json
import time

# kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic orders

# for testing to check whether we have invoice generated or not.
# kafka-console-consumer --bootstrap-server localhost:9092 --topic orders


TOPIC = "orders"
SAMPLES = 100
DELAY = 10

states = ['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA',
          'HI','IA','ID','IL','IN','KS','KY','LA','MA','MD','ME',
          'MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM',
          'NV','NY','OH','OK','OR','PA','RI','SC','SD','TN','TX',
          'UT','VA','VT','WA','WI','WV','WY']

# broker runs in linux machine
producer = Producer({'bootstrap.servers':'localhost:9092'})

for i in range(SAMPLES):
    order_id =  random.randint(1,100000)
    state =  random.choice(states)

    current_time = datetime.datetime.now()
    timestamp = int(current_time.timestamp()*1000)

    number_of_items = random.randint(1, 10)
    
    for j in range(number_of_items):
        # MM/dd/yyyy hh:mm
        item_id = random.randint(1,100)
        quantity = random.randint(1, 10)
        price = random.randint(1, 50)
        order = {   "order_id": order_id,
                    "order_date":timestamp,
                    "item_id": item_id,
                    "price": price,
                    "qty": quantity,
                    "state" : state  }
    
        key = str(timestamp).encode('utf-8')
        order_str = json.dumps(order).encode('utf-8')
        print ('order_str',order_str)

        producer.produce(topic=TOPIC, value=order_str, key=key)
        producer.flush()
        
    time.sleep(DELAY)