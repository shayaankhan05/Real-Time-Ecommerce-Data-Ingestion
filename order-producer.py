import json
import time
import random
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()

project_name = 'magnetic-tenure-408314'
topic_name = 'order-topic'
topic_path = publisher.topic_path(project_name, topic_name)

def callable(future):
    try:
        message_id = future.result()
        print(f"Published message with ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")

def generate_mock_order_data(order_id):
    items = ["Laptop", "Phone", "Book", "Tablet", "Monitor"]
    addresses = ["123 Main St, City A, Country", "456 Elm St, City B, Country", "789 Oak St, City C, Country"]
    statuses = ["Shipped", "Pending", "Delivered", "Cancelled"]

    return {
        "order_id": order_id,
        "customer_id": random.randint(100, 1000),
        "item": random.choice(items),
        "quantity": random.randint(1, 10),
        "price": random.uniform(100, 1500),
        "shipping_address": random.choice(addresses),
        "order_status": random.choice(statuses),
        "creation_date": "2024-02-16"    
        }

order_id = 1
while True:
    mock_data = generate_mock_order_data(order_id)
    json_data = json.dumps(mock_data).encode('utf-8')

    try:
        future = publisher.publish(topic_path, data=json_data)
        future.add_done_callback(callable)
    except Exception as e:
        print(f"Exception encountered: {e}")
    
    time.sleep(2)

    order_id += 1
    while order_id >= 80:
        break



