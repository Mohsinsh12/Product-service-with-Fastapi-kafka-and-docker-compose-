from typing import Annotated, Optional,List
from fastapi import Depends, FastAPI, HTTPException
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from products import settings
from sqlmodel import SQLModel, Field, Session, select, create_engine
from products import product_pb2
from products.product_pb2 import Operation 



class Products (SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price:float = Field()

connection_string=str(settings.DATABASE_URL)
engine = create_engine(connection_string, echo=True)

def get_session():
    with Session(engine) as session:
        yield session

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

async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@app.get('/')
async def root():
    return {"message": "Welcome to the Zia Mart"}


@app.get('/products', response_model=List[Products])
async def get_products(session: Annotated[Session, Depends(get_session)]):
    products = session.exec(select(Products)).all()
    return products

@app.get('/products/{product_id}', response_model=Products)
async def get_product(product_id: int, session: Annotated[Session, Depends(get_session)]):
    product = session.get(Products, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    else:
        return product
    
    

@app.post('/create_product', response_model=Products)
async def Create_product(
    New_product:Products,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    pb_product = product_pb2.product()
    pb_product.id=New_product.id
    pb_product.name=New_product.name
    pb_product.price=New_product.price
    pb_product.type = product_pb2.Operation.CREATE 


    serialized_product = pb_product.SerializeToString()
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

    return New_product

@app.delete('/products/')
async def delete_product(id: int, producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]):
    product_proto = product_pb2.product()
    product_proto.id = id
    product_proto.type = product_pb2.Operation.DELETE

    serialized_product = product_proto.SerializeToString()
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)

    return {"Product" : "Deleted"}