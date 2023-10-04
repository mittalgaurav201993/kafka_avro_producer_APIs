import logging
import schemas
import os
from dotenv import load_dotenv
from fastapi import FastAPI
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from models import Revenue, OrderTotalAmount
from confluent_kafka import SerializingProducer, DeserializingConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer

lo_customers_orders: [dict] = []
so_customers: set[dict] = set()
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
                                           schema_str=schemas.revenue_schema,
                                           to_dict=lambda rec, ctx: rec.model_dump())
    return SerializingProducer({'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
                                'retries': 5,
                                'max.in.flight.requests.per.connection': 1,
                                'batch.size': 5000,
                                'linger.ms': 1000,
                                'partitioner': 'murmur2_random',
                                'acks': 'all',
                                'enable.idempotence': 'true',
                                'key.serializer': StringSerializer('utf_8'),
                                'value.serializer': avro_value_serializer})


def defined_consumer(data_model, consumer_grp) -> DeserializingConsumer:
    schema_reg_client = SchemaRegistryClient({'url': os.environ['SCHEMA_REGISTRY_URL']})
    avro_value_deserializer = AvroDeserializer(schema_registry_client=schema_reg_client,
                                               from_dict=lambda data, ctx: data_model(**data))
    return DeserializingConsumer({'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
                                  'key.deserializer': StringDeserializer('utf_8'),
                                  'value.deserializer': avro_value_deserializer,
                                  'group.id': consumer_grp,
                                  'enable.auto.commit': 'false'})


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


@app.post('/fetch/customers_revenue', status_code=201, response_model=list[Revenue])
async def revenue_producer():
    logger.info(f"""
    Consumer with group Id '{os.environ['ORDERS_WITH_AMOUNT_CONSUMER_GROUP']}'started and
    consuming from '{os.environ['ORDERS_WITH_AMOUNT_TOPIC_NAME']}' topic.""")
    print(f"""
    Consumer with group Id '{os.environ['ORDERS_WITH_AMOUNT_CONSUMER_GROUP']}'started and
    consuming from '{os.environ['ORDERS_WITH_AMOUNT_TOPIC_NAME']}' topic.""")

    lo_records: list[Revenue] = []
    producer = defined_producer()
    orders_with_amount_consumer_group = os.environ['ORDERS_WITH_AMOUNT_CONSUMER_GROUP']
    consumer = defined_consumer(OrderTotalAmount, orders_with_amount_consumer_group)

    topic_partitions = [TopicPartition(topic=os.environ['ORDERS_WITH_AMOUNT_TOPIC_NAME'], partition=0, offset=0),
                        TopicPartition(topic=os.environ['ORDERS_WITH_AMOUNT_TOPIC_NAME'], partition=1, offset=0),
                        TopicPartition(topic=os.environ['ORDERS_WITH_AMOUNT_TOPIC_NAME'], partition=2, offset=0)]
    consumer.assign(topic_partitions)
    for topic_partition in topic_partitions:
        consumer.seek(topic_partition)

    consuming = True
    while consuming:
        fetched_order = consumer.poll(timeout=5.0)
        if fetched_order is not None:
            so_customers.add({'id': fetched_order.value().customer_id,
                              'name': fetched_order.value().customer_name})
            lo_customers_orders.append({'id': fetched_order.value().customer_id,
                                        'each_order_amount': fetched_order.value().order_amount})
            consumer.commit(message=fetched_order)
        else:
            print(f"""
                - All orders with amount were fetched, thus getting 'fetched_order' as 'None'.
                - Added required order details to set and list, now exiting consumer.
                """)
            consuming = False
    for cust in so_customers:
        per_customer_revenue_total = 0
        for order in lo_customers_orders:
            if order['id'] == cust['id']:
                per_customer_revenue_total += order['each_order_amount']
        revenue = Revenue(cust_id=cust['id'],
                          cust_name=cust['name'],
                          order_amount_total=per_customer_revenue_total)
        producer.produce(topic=lo_topics[0],
                         key=str(revenue.cust_id),
                         value=revenue,
                         on_delivery=ProducerCallback(revenue))
        lo_records.append(revenue)
    producer.flush()
    return lo_records
