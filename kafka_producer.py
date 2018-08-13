from kafka import KafkaConsumer, KafkaProducer


topic_name = "floatTopic"
key = "key1"
value = "value1"

_producer = KafkaProducer(bootstrap_servers=['IPADDRESS:9092'],
                          api_version=(0, 10))

def sendMessage(value):
    key = "key1"
    key_bytes = bytes(key, encoding='utf-8')
    value_bytes = bytes(value, encoding='utf-8')
    _producer.send(topic_name, key=key_bytes, value=value_bytes)
    _producer.flush()
    print(str(value))



import numpy as np

random_number = np.random.rand(10000,1)

for i in random_number:
    sendMessage(str(i[0]))
