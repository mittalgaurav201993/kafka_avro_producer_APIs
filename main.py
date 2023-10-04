import logging
import os
import schemas
from dotenv import load_dotenv
from fastapi import FastAPI
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from models import RecordModel
from commands import NumOfRecordsCommand

logging.basicConfig(filename='producerlogs.log',
                    level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger()
load_dotenv(verbose=True)
app = FastAPI()


def define_producer() -> SerializingProducer:
    schema_client = SchemaRegistryClient({'url': os.environ['SCHEMA_REGISTRY_URL']})
    value_avro_serializer = AvroSerializer(schema_registry_client=schema_client,
                                           schema_str=schemas.record_value_vn,
                                           to_dict=lambda record, ctx: record.dict())
    return SerializingProducer({'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
                                'linger.ms': 300,
                                'enable.idempotence': 'true',
                                'max.in.flight.requests.per.connection': 1,
                                'acks': 'all',
                                'key.serializer': StringSerializer('utf_8'),
                                'value.serializer': value_avro_serializer,
                                'partitioner': 'murmur2_random'})


class ProducerCallback:
    def __init__(self, rec):
        self.record = rec

    def __call__(self, err, msg_metadata):
        if err:
            logger.error(f"""
            Failed to produce record <'{self.record}'>,
            due to error message <{err}>.""")
        else:
            logger.info(f"""
            Successfully produced record '{self.record}',
            to topic '{msg_metadata.topic()}',
            in partition '{msg_metadata.partition()}',
            at offset '{msg_metadata.offset()}'.""")


@app.get('/create/topic')
async def topic_administration():
    client = AdminClient({'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS']})
    topic = NewTopic(topic=os.environ['TOPICS_PEOPLE_AVRO_TOPIC_NAME'],
                     num_partitions=int(os.environ['TOPICS_PEOPLE_AVRO_PARTITIONS']),
                     replication_factor=int(os.environ['TOPICS_PEOPLE_AVRO_REPLICAS']))
    try:
        futures = client.create_topics([topic])
        for topic_name, future in futures.items():
            future.result()
            logger.info(f"Created topic '{topic_name}'.")
    except Exception as e:
        logger.warning(e)


@app.post('/produce/data', status_code=201, response_model=list[RecordModel])
async def data_producer(cmd: NumOfRecordsCommand):
    records: list[RecordModel] = []
    record: RecordModel
    fake = Faker()
    producer = define_producer()

    for _ in range(cmd.records):
        record = RecordModel(ssn=fake.ssn(),
                             name=fake.name(),
                             citizenship_level=str(fake.date_of_birth().month),
                             address=fake.address(),
                             pancard=f"CHAPM{fake.date_of_birth().year}D",
                             dob=f"{fake.date_of_birth().day}-"
                                 f"{fake.date_of_birth().month}-"
                                 f"{fake.date_of_birth().year}"
                             )
        records.append(record)
        producer.produce(topic=os.environ['TOPICS_PEOPLE_AVRO_TOPIC_NAME'],
                         key=record.ssn,
                         value=record,
                         on_delivery=ProducerCallback(record))
    producer.flush()
    return records
