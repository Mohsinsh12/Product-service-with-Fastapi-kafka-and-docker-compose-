# main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaConsumer
from sqlmodel import SQLModel,select, Field, create_engine, Session
from consumer_service import settings, product_pb2
import asyncio
from typing import Optional
import logging
from consumer_service.product_pb2 import Operation 

# from product_pb2 import Operation 


class Products(SQLModel, table=True):
    id:Optional[int] = Field(default= None, primary_key= True)
    name: str = Field()
    price:float = Field()

# class User(SQLModel, table=True):
#     id: int = Field(default=None, primary_key=True)
#     email: str = Field(index=True, unique=True)
#     hashed_password: str

# class Token(SQLModel, table=True):
#     access_token: str
#     token_type: str
# 

connection_string=str(settings.DATABASE_URL)
engine = create_engine(connection_string, pool_recycle=300, echo=True)
def create_tables():
    SQLModel.metadata.create_all(engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_messages())
    print('Creating Tables')
    create_tables()
    print("Tables Created")
    yield
    task.cancel()
    await task


app = FastAPI(lifespan=lifespan, title="Kafka consumer with database", 
    version="0.0.01",)

async def consume_messages():
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        settings.KAFKA_ORDER_TOPIC,
        bootstrap_servers=settings.BOOTSTRAP_SERVER,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
        # session_timeout_ms=30000,  # Example: Increase to 30 seconds
        # max_poll_records=10,
    )
    # Start the consumer.
    await consumer.start()
    try:
        with Session(engine) as session:
        # Continuously listen for messages.
            async for msg in consumer:
                if msg.value is not None:
                    try:
                        product = product_pb2.product()
                        product.ParseFromString(msg.value)            
                        data_from_producer=Products(id=product.id, name= product.name, price=product.price)
                        
                        if product.type == product_pb2.Operation.CREATE:
                                session.add(data_from_producer)
                                session.commit()
                                session.refresh(data_from_producer)
                                print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')

                        elif product.type== product_pb2.Operation.DELETE:
                                product_to_delete = session.get(Products, product.id)
                                if product_to_delete:
                                    session.delete(product_to_delete)
                                    session.commit()
                                    logging.info(f"Deleted product with ID: {product.id}")
                                else:
                                    logging.warning(f"Product with ID {product.id} not found for deletion")
                        elif product.type== product_pb2.Operation.PUT:
                            ...

                    except (google.protobuf.message.DecodeError) as e:
                        logging.error(f"Error deserializing messages")
                else :
                    print("Msg has no value")
    except Exception as e: 
        logging.error(f"Error consuming messages: {e}")
    finally:
        # Ensure to close the consumer when done.
            await consumer.stop()
