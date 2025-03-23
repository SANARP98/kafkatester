from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from confluent_kafka import Producer, Consumer, TopicPartition
from typing import List, Dict
import uvicorn
import asyncio

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Kafka configuration
def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

# Kafka producer
def produce_message(topic: str, key: str, value: str, config: Dict):
    producer = Producer(config)
    producer.produce(topic, key=key.encode("utf-8"), value=value.encode("utf-8"))
    producer.flush()

# Kafka consumer for recent messages
def consume_messages(topic: str, config: Dict, limit: int = 10) -> List[Dict]:
    config["group.id"] = "fastapi-group-1"
    config["auto.offset.reset"] = "latest"
    consumer = Consumer(config)
    consumer.subscribe([topic])
    messages = []
    try:
        while len(messages) < limit:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                break
            key = msg.key().decode("utf-8") if msg.key() else ""
            value = msg.value().decode("utf-8") if msg.value() else ""
            messages.append({"key": key, "value": value})
            if len(messages) >= limit:
                break
    finally:
        consumer.close()
    return messages

# Kafka consumer for old messages
def consume_old_messages(topic: str, config: Dict, limit: int = 10) -> List[Dict]:
    config["group.id"] = "fastapi-group-old-1"
    config["auto.offset.reset"] = "earliest"
    consumer = Consumer(config)
    consumer.subscribe([topic])
    messages = []
    try:
        while len(messages) < limit:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                break
            key = msg.key().decode("utf-8") if msg.key() else ""
            value = msg.value().decode("utf-8") if msg.value() else ""
            messages.append({"key": key, "value": value})
            if len(messages) >= limit:
                break
    finally:
        consumer.close()
    return messages

# HTML form endpoint
@app.get("/", response_class=HTMLResponse)
async def get_form(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Produce endpoint
@app.post("/produce", response_class=HTMLResponse)
async def produce(request: Request, key: str = Form(...), value: str = Form(...)):
    config = read_config()
    topic = "topic_0"
    produce_message(topic, key, value, config)
    return templates.TemplateResponse("index.html", {"request": request, "message": f"Produced: {key} -> {value}"})

# Consume recent messages endpoint
@app.get("/consume", response_class=HTMLResponse)
async def consume(request: Request):
    config = read_config()
    topic = "topic_0"
    messages = consume_messages(topic, config)
    return templates.TemplateResponse("output.html", {"request": request, "messages": messages})

# Retrieve old messages endpoint
@app.get("/old-messages", response_class=HTMLResponse)
async def old_messages(request: Request):
    config = read_config()
    topic = "topic_0"
    messages = consume_old_messages(topic, config)
    return templates.TemplateResponse("output.html", {"request": request, "messages": messages})

# API endpoint for recent messages (JSON)
@app.get("/api/consume")
async def consume_api():
    config = read_config()
    topic = "topic_0"
    messages = consume_messages(topic, config)
    return {"messages": messages}

# API endpoint for old messages (JSON)
@app.get("/api/old-messages")
async def old_messages_api():
    config = read_config()
    topic = "topic_0"
    messages = consume_old_messages(topic, config)
    return {"messages": messages}

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)