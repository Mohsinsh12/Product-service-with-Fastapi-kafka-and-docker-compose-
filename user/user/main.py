from typing import Annotated
from fastapi import Depends, FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from user import settings
from sqlmodel import SQLModel, Field,select
from user import product_pb2
from typing import Optional



class User(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    hashed_password: str



async def create_topic():
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await admin_client.start()
    topic_list = [NewTopic(name=settings.KAFKA_ORDER_TOPIC,
                           num_partitions=2, replication_factor=1)]
    try:
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{settings.KAFKA_ORDER_TOPIC}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{settings.KAFKA_ORDER_TOPIC}': {e}")
    finally:
        await admin_client.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    await create_topic()
    yield


app = FastAPI(lifespan=lifespan,
              title="FastAPI Producer Service...",
              version='1.0.0'
              )


@app.get('/')
async def root():
    return {"message": "Welcome to the Zia Mart User service"}


async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@app.post('/create_user', response_model=User)
def myha():
    ...
