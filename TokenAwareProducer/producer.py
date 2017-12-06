from kafka import KafkaProducer
from dse.cluster import Cluster
from cassandra.metadata import Metadata, protect_name, murmur3
from random import randint

def getPartition(cluster, key):
    ring = cluster.metadata.token_map.ring
    for i in range(0, len(ring) - 2):
        token = murmur3(key)
        # print str(ring[i].value) + "_" + str(ring[i + 1].value)
        if token > ring[i].value and token < ring[i + 1].value:
            return cluster.metadata.token_map.token_to_host_owner[ring[i + 1]]



cluster = Cluster(['172.31.39.214'])
session = cluster.connect()
producer = KafkaProducer(bootstrap_servers=['172.31.39.214:9092'])


for i in range(0, 1000000):
    key = str(randint(0, 1000000))
    partitionKey = str(getPartition(cluster, key)).replace('.', '')
    if partitionKey:
        future1 = producer.send(partitionKey, key=key, value=("test" + key))




    # # Block for 'synchronous' sends
# try:
# except KafkaError:
#     # Decide what to do if produce request failed...
#     log.exception()
#     pass
#
# # Successful result returns assigned partition and offset
# print (record_metadata.topic)
# print (record_metadata.partition)
# print (record_metadata.offset)
#
# # produce keyed messages to enable hashed partitioning
# producer.send('my-topic', key=b'foo', value=b'bar')
#
# # encode objects via msgpack
# producer = KafkaProducer(value_serializer=msgpack.dumps)
# producer.send('msgpack-topic', {'key': 'value'})
#
# # produce json messages
# producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
# producer.send('json-topic', {'key': 'value'})
#
# # produce asynchronously
# for _ in range(100):
#     producer.send('my-topic', b'msg')
#
# # block until all async messages are sent
# producer.flush()
#
# # configure multiple retries
# producer = KafkaProducer(retries=5)