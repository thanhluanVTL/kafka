import io, requests
import avro.schema
from kafka import KafkaConsumer
from avro.datafile import DataFileReader
from avro.io import DatumReader, BinaryDecoder

url = "http://localhost:8081/subjects/LUAN.inventory.customers-value/versions/1"
resp=requests.get(url).json()
print(resp)
print(type(resp))

schemas = resp["schema"]
print(schemas)
print(type(schemas))

# schemas =  '''
# {
#   "type": "record",
#   "name": "Envelope",
#   "namespace": "LUAN.inventory.customers",
#   "fields": [
#     {
#       "name": "before",
#       "type": [
#         "null",
#         {
#           "type": "record",
#           "name": "Value",
#           "fields": [
#             {
#               "name": "id",
#               "type": {
#                 "type": "int",
#                 "connect.default": 0
#               },
#               "default": 0
#             },
#             {
#               "name": "first_name",
#               "type": "string"
#             },
#             {
#               "name": "last_name",
#               "type": "string"
#             },
#             {
#               "name": "email",
#               "type": "string"
#             }
#           ],
#           "connect.name": "LUAN.inventory.customers.Value"
#         }
#       ],
#       "default": null
#     },
#     {
#       "name": "after",
#       "type": [
#         "null",
#         "Value"
#       ],
#       "default": null
#     },
#     {
#       "name": "source",
#       "type": {
#         "type": "record",
#         "name": "Source",
#         "namespace": "io.debezium.connector.postgresql",
#         "fields": [
#           {
#             "name": "version",
#             "type": "string"
#           },
#           {
#             "name": "connector",
#             "type": "string"
#           },
#           {
#             "name": "name",
#             "type": "string"
#           },
#           {
#             "name": "ts_ms",
#             "type": "long"
#           },
#           {
#             "name": "snapshot",
#             "type": [
#               {
#                 "type": "string",
#                 "connect.version": 1,
#                 "connect.parameters": {
#                   "allowed": "true,last,false,incremental"
#                 },
#                 "connect.default": "false",
#                 "connect.name": "io.debezium.data.Enum"
#               },
#               "null"
#             ],
#             "default": "false"
#           },
#           {
#             "name": "db",
#             "type": "string"
#           },
#           {
#             "name": "sequence",
#             "type": [
#               "null",
#               "string"
#             ],
#             "default": null
#           },
#           {
#             "name": "schema",
#             "type": "string"
#           },
#           {
#             "name": "table",
#             "type": "string"
#           },
#           {
#             "name": "txId",
#             "type": [
#               "null",
#               "long"
#             ],
#             "default": null
#           },
#           {
#             "name": "lsn",
#             "type": [
#               "null",
#               "long"
#             ],
#             "default": null
#           },
#           {
#             "name": "xmin",
#             "type": [
#               "null",
#               "long"
#             ],
#             "default": null
#           }
#         ],
#         "connect.name": "io.debezium.connector.postgresql.Source"
#       }
#     },
#     {
#       "name": "op",
#       "type": "string"
#     },
#     {
#       "name": "ts_ms",
#       "type": [
#         "null",
#         "long"
#       ],
#       "default": null
#     },
#     {
#       "name": "transaction",
#       "type": [
#         "null",
#         {
#           "type": "record",
#           "name": "ConnectDefault",
#           "namespace": "io.confluent.connect.avro",
#           "fields": [
#             {
#               "name": "id",
#               "type": "string"
#             },
#             {
#               "name": "total_order",
#               "type": "long"
#             },
#             {
#               "name": "data_collection_order",
#               "type": "long"
#             }
#           ]
#         }
#       ],
#       "default": null
#     }
#   ],
#   "connect.name": "LUAN.inventory.customers.Envelope"
# }
# '''


schema = avro.schema.parse(schemas)

consumer = KafkaConsumer(
    "LUAN.inventory.customers",
    bootstrap_servers=['192.168.1.250:9092'],
    # auto_offset_reset='latest',
    auto_offset_reset='earliest',
    # enable_auto_commit=True,
    # auto_commit_interval_ms=1000,
    # group_id='my-group',
    consumer_timeout_ms=10000
)

# schema = avro.schema.Parse(open("data_sources/EventRecord.avsc").read())
reader = DatumReader(schema)

def avro_decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(5)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict

for msg in consumer:
    msg_value = msg.value

    # bytes_reader = io.BytesIO(msg_value)
    # decoder = BinaryDecoder(bytes_reader)
    # print(decoder)
    # print(type(decoder))
    # reader = DatumReader(schema)
    # # reader = DatumReader()
    # message = reader.read(decoder)
    # print(message)

    a = avro_decode(msg_value=msg_value)
    print(a)