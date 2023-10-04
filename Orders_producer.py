import random
import logging
import schemas
import os
from dotenv import load_dotenv
from fastapi import FastAPI
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from models import Order
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

lo_topics: list[str] = []
load_dotenv(verbose=True)
app = FastAPI()
logging.basicConfig(filename='producerlogs.log', level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger()


class ProducerCallback:
    def __init__(self, rec):
        self.entity = rec

    def __call__(self, err_msg, msg_metadata):
        if err_msg:
            logger.error(f"Failed to send record '{self.entity}' to topic '{lo_topics[0]}'.")
        else:
            logger.info(f"""
            Successfully produced record '{self.entity}',
            at offset '{msg_metadata.offset()}',
            of partition '{msg_metadata.partition()}',
            in topic '{msg_metadata.topic()}',
            present in cluster '{os.environ['BOOTSTRAP_SERVERS']}'.
            """)


def defined_producer() -> SerializingProducer:
    schema_reg_client = SchemaRegistryClient({'url': os.environ['SCHEMA_REGISTRY_URL']})
    avro_value_serializer = AvroSerializer(schema_registry_client=schema_reg_client,
                                           schema_str=schemas.orders_schema,
                                           to_dict=lambda rec, ctx: rec.model_dump())
    return SerializingProducer({'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
                                'retries': 5,
                                'max.in.flight.requests.per.connection': 1,
                                'batch.size': 50000,
                                'linger.ms': 400,
                                'partitioner': 'murmur2_random',
                                'acks': 'all',
                                'enable.idempotence': 'true',
                                'key.serializer': StringSerializer('utf_8'),
                                'value.serializer': avro_value_serializer})


@app.get('/create/topic/{topic_name}')
def topic_creator(topic_name):
    lo_topics.clear()
    lo_topics.append(topic_name)
    client = AdminClient({'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS']})
    topic = NewTopic(topic=topic_name,
                     num_partitions=int(os.environ['TOPICS_PEOPLE_AVRO_PARTITIONS']),
                     replication_factor=int(os.environ['TOPICS_PEOPLE_AVRO_REPLICAS']),
                     config={'retention.ms': 172800000})
    config_update = ConfigResource(restype=ResourceType.TOPIC,
                                   name=topic_name,
                                   set_config={'retention.ms': 604800000})
    try:
        futures = client.create_topics([topic])
        for created_topic, future in futures.items():
            logger.info(f"Successfully created topic '{created_topic}'.")
            future.result()
    except Exception as e:
        logger.error(e)
    client.alter_configs([config_update])


@app.post('/produce/data', status_code=201, response_model=list[Order])
def record_producer():
    lo_records: list[Order] = []
    fake = Faker()
    producer = defined_producer()
    for order_id in range(10000, 100000):
        order = Order(id=order_id,
                      customer_id=int(random.choice(range(1000, 10000))),
                      product_id=int(random.choice(list(set(range(101, 111)) |
                                                        set(range(201, 211)) |
                                                        set(range(301, 311))))),
                      product_qty=int(random.choice(range(1, 100))),
                      created_ms=int(1000 * fake.date_time().time().second),
                      validity=random.choice([True, False, True, True]))
        lo_records.append(order)
        producer.produce(topic=lo_topics[0],
                         key=str(order.id),
                         value=order,
                         on_delivery=ProducerCallback(order))
    producer.flush()
    return lo_records
