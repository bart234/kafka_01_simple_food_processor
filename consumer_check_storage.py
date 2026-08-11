from confluent_kafka import Consumer,Producer
import json

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "check_storage_consumer_group_",
    "auto.offset.reset": "earliest"
}



consumer_ = Consumer(consumer_config)

consumer_.subscribe(["orders"])

# def send_info_to_data_consumer(order_data):
#     producer = Producer({'bootstrap.servers': 'localhost:9092'})
#     confirmation_in_kafka_format = json.dumps({"confirmed":"ok","qty":order_data["quantity"],"item":order_data['item']}).encode("utf-8")
#     producer.produce(
#             topic="confirmation", 
#             value=confirmation_in_kafka_format)
#     producer.flush()

print("Consumer 2 is running, subscribed to orders topic")

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
        print(f"Quantity confirmed : {order_data["quantity"]} ")
        # send_info_to_data_consumer(order_data)
except KeyboardInterrupt:
    print("Stopping consumer")
finally:
    #we always want ot close that connection
    consumer_.close()