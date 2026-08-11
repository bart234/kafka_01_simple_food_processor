from confluent_kafka import Producer
import uuid,json

#kafka producer with info where kafka is accessable -> 'localhost:9092' 
#address from kafka_ad_listeners - all nodes, clusters will be availible under that adress
producer = Producer({'bootstrap.servers': 'localhost:9092'})

#add function to autocallback
def delivery_report(err,msg):
    if err: 
        print(f"Delivery error {err}")
    else:
        print(f" Delivered {msg.value().decode("utf-8")}")
        #if will show all availible data in that variable
        #if error we can execute dir(msg) in terminal to check variable
        #print(dir(msg)) 
    

order = {"order_id": str(uuid.uuid4()),
         "user":"t2",
         "item":"mushroom pizza",
         "quantity":2}

#kafka data are bites 
order_in_kafka_format = json.dumps(order).encode("utf-8")

#topic is a kind of eent which we end with data
#callback is for  info if delivery was completed
producer.produce(
    topic="orders", 
    value=order_in_kafka_format,
    callback=delivery_report)

#it is a force send. Kafka group some events to send them in batches
#that buffor for group will be lost (with everything inside) if kafka crash
#so flush will force to send buffor before app will crash - what allow to restore that action
producer.flush()

#to check what kind of event kafka have saved - currently it should be orders
#docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092 

#to get some details about that events we can search exactly that
#docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders

#by connection with kafka by cel we can also track what was send to that topic-event list - we have to use kafka-console-consumer
#docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-begining
