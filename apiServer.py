from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from confluent_kafka import Producer
import json

producer_configs = {
    'bootstrap.servers' : 'localhost:9092',
    "client.id": "fastapi-producer",
    "enable.idempotence": True    
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

@app.get("/")
def read_root():
    return {"message": "Welcome to my FastAPI server!"}

# Define a Pydantic model for the request body
class Item(BaseModel):
    name: str

@app.post("/sendToKafka/")
async def send_vote(item : Item):
    print(f"Candidate : {item.name}")
    try:
        sendVote(item.name)
        return f"Vote registered successfully for {item.name}"
    except Exception as e:
        return str(e)
    
if __name__ == "__main__":
    # Run the app on a custom port (e.g., 8080)
    uvicorn.run("apiServer:app", host="127.0.0.1", port=8000, reload=True)