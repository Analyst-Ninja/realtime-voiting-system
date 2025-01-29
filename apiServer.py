import os
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from confluent_kafka import Producer
import asyncio
from concurrent.futures import ThreadPoolExecutor

producer_configs = {
    'bootstrap.servers' : 'broker:29092',
    "client.id": "fastapi-producer"  
}

producer = Producer(producer_configs)


def sendVote(candidate : str):
    producer.produce(
        topic,
        value=str(candidate),
        callback = delivery_report
    )

    producer.flush()

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery Failed : {err.decode('utf-8')}")
    else:
        print(f"Message delivered to {msg.topic()} and {msg.value().decode('utf-8')} candidate")

topic = 'voteTopic'

app = FastAPI()

# Thread pool for background tasks
executor = ThreadPoolExecutor()

# Define a Pydantic model for the request body
class Item(BaseModel):
    name: str

@app.get("/")
def home_page():
    return {
        "message" : "Server Working"
    } 

@app.post("/sendToKafka/")
async def send_vote(item: Item):
    print(f"Candidate: {item.name}")
    try:
        # Run the sendVote function in a separate thread
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, sendVote, item.name)
        return f"Vote registered successfully for {item.name}"
    except Exception as e:
        return str(e)
    
if __name__ == "__main__":
    # Run the app on a custom port (e.g., 8080)
    uvicorn.run("apiServer:app", host="0.0.0.0", port=8000)