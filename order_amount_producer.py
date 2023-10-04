import logging
import schemas
import os
from dotenv import load_dotenv
from fastapi import FastAPI
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from models import OrderTotalAmount, Order, Product, Customer
from confluent_kafka import SerializingProducer, DeserializingConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer

lo_topics: list[str] = []
lo_products: list[dict] = []
lo_customers: list[dict] = []

load_dotenv(verbose=True)
app = FastAPI()
logger = logging.getLogger()
logging.basicConfig(filename='producerlogs.log', level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')


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
                                           schema_str=schemas.order_amount_schema,
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


@app.post('/fetch/valid_orders', status_code=201, response_model=list[OrderTotalAmount])
async def valid_order_producer():
    logger.info(f"""
    Consumer with group Ids '{[os.environ['VALID_ORDER_CONSUMER_GROUP'],
                               os.environ['PRODUCTS_CONSUMER_GROUP'],
                               os.environ['CUSTOMERS_CONSUMER_GROUP']]}'started and
    consuming from '{[os.environ['VALID_ORDER_TOPIC_NAME'],
                      os.environ['PRODUCTS_TOPIC_NAME'],
                      os.environ['CUSTOMERS_TOPIC_NAME']]}' topics.""")
    print(f"""
    Consumer with group Ids '{[os.environ['VALID_ORDER_CONSUMER_GROUP'],
                               os.environ['PRODUCTS_CONSUMER_GROUP'],
                               os.environ['CUSTOMERS_CONSUMER_GROUP']]}'started and
    consuming from '{[os.environ['VALID_ORDER_TOPIC_NAME'],
                      os.environ['PRODUCTS_TOPIC_NAME'],
                      os.environ['CUSTOMERS_TOPIC_NAME']]}' topics.""")

    lo_records: list[OrderTotalAmount] = []
    producer = defined_producer()

    valid_orders_consumer_group = os.environ['VALID_ORDER_CONSUMER_GROUP']
    products_consumer_group = os.environ['PRODUCTS_CONSUMER_GROUP']
    customers_consumer_group = os.environ['CUSTOMERS_CONSUMER_GROUP']

    valid_order_consumer = defined_consumer(Order, valid_orders_consumer_group)
    products_consumer = defined_consumer(Product, products_consumer_group)
    customers_consumer = defined_consumer(Customer, customers_consumer_group)

    valid_order_topic_partitions = [TopicPartition(topic=os.environ['VALID_ORDER_TOPIC_NAME'], partition=0, offset=0),
                                    TopicPartition(topic=os.environ['VALID_ORDER_TOPIC_NAME'], partition=1, offset=0),
                                    TopicPartition(topic=os.environ['VALID_ORDER_TOPIC_NAME'], partition=2, offset=0)]
    products_topic_partitions = [TopicPartition(topic=os.environ['PRODUCTS_TOPIC_NAME'], partition=0, offset=0),
                                 TopicPartition(topic=os.environ['PRODUCTS_TOPIC_NAME'], partition=1, offset=0),
                                 TopicPartition(topic=os.environ['PRODUCTS_TOPIC_NAME'], partition=2, offset=0)]
    customers_topic_partitions = [TopicPartition(topic=os.environ['CUSTOMERS_TOPIC_NAME'], partition=0, offset=0),
                                  TopicPartition(topic=os.environ['CUSTOMERS_TOPIC_NAME'], partition=1, offset=0),
                                  TopicPartition(topic=os.environ['CUSTOMERS_TOPIC_NAME'], partition=2, offset=0)]

    valid_order_consumer.assign(valid_order_topic_partitions)
    products_consumer.assign(products_topic_partitions)
    customers_consumer.assign(customers_topic_partitions)

    for valid_order_topic_partition in valid_order_topic_partitions:
        valid_order_consumer.seek(valid_order_topic_partition)
    for products_topic_partition in products_topic_partitions:
        products_consumer.seek(products_topic_partition)
    for customers_topic_partition in customers_topic_partitions:
        customers_consumer.seek(customers_topic_partition)

    consuming_products = True
    while consuming_products:
        fetched_product = products_consumer.poll(timeout=5.0)
        if fetched_product is not None:
            lo_products.append(fetched_product.value().model_dump())
            print(f"Successfully fetched product with ID: {fetched_product.value().id}")
            products_consumer.commit(message=fetched_product)
        else:
            print(f"""
            - All products were fetched, thus getting 'fetched_product' as 'None'.
            - Now exiting product_consumer and flushing products to 'lo_products'.
            """)
            consuming_products = False

    consuming_customers = True
    while consuming_customers:
        fetched_customer = customers_consumer.poll(timeout=5.0)
        if fetched_customer is not None:
            lo_customers.append(fetched_customer.value().model_dump())
            print(f"Successfully fetched customer with ID: {fetched_customer.value().id}")
            customers_consumer.commit(message=fetched_customer)
        else:
            print(f"""
                - All customers were fetched, thus getting 'fetched_customer' as 'None'.
                - Now exiting customer_consumer and flushing customers to 'lo_customers'.
                """)
            consuming_customers = False

    consuming_valid_orders = True
    while consuming_valid_orders:
        fetched_valid_order = valid_order_consumer.poll(timeout=5.0)
        if fetched_valid_order is not None:
            print(f"Successfully fetched valid_order with ID: {fetched_valid_order.value().id}")






            customers_consumer.commit(message=fetched_customer)
        else:
            print(f"""
                - All valid orders were fetched, thus getting 'fetched_valid_order' as 'None'.
                - Now exiting valid_order_consumer and flushing order amounts to 'Orders_with_amount_topic'.
                """)
            consuming_customers = False


    lo_records.append(valid_order)

    producer.flush()
    return lo_records
