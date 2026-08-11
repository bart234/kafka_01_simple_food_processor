
from confluent_kafka import Consumer
import json

#group_id - unique string that identify that consumer group (if we will run that consumer on multi instances)
#auto.offset.reset - in simple way, it say what that consumer should do, if it connect/reconnect to kafka and see list of 
#               events  and dont know which one was readed and done - so in that case it read "earliest"

#consumer constantly asking kafka about messegages
#kafka do not send that itself to these consumers
#subscribe "to topic" - is not like should be usualy
#consume not waiting, consumer actyvly checking
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order_consumer_group_",
    "auto.offset.reset": "earliest"
}

consumer_ = Consumer(consumer_config)

#we select to which even we would like subscribe
consumer_.subscribe(["orders"])

print("Consumer is running, subscribed to orders topic")

try:
    while True:
        msg=consumer_.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error msg: {msg.error()}")
            continue
        mmessege= msg.value().decode("utf-8")
        order_data = json.loads(mmessege)
        print(f"Data from kafka: {order_data["quantity"]} x  {order_data["item"]} x  {order_data["user"]}")
except KeyboardInterrupt:
    print("Stopping consumer")

#we can handle Keyboard Interruption error
#we have to prepare  disconnection - if connection will break
#it have to be gracefully disconnected, without any data llos