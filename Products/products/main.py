from typing import Annotated
from fastapi import Depends, FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from products import settings
from sqlmodel import SQLModel, Field
from products import product_pb2
from typing import Optional



class Products (SQLModel):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price:float = Field()



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
    return {"message": "Welcome to the Zia Mart"}


async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@app.post('/create_product', response_model=Products)
async def Create_product(
    New_product:Products,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    pb_product = product_pb2.product()
    pb_product.id=New_product.id
    pb_product.name=New_product.name
    pb_product.price=New_product.price
    serialized_product = pb_product.SerializeToString()
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

    return New_product

# @app.get("/products/", response_model=list[AddProduct])
# def read_todos(session: Annotated[Session, Depends(get_session)]):
#         todos = session.exec(select(AddProduct)).all()
#         return todos