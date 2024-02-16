from google.cloud import pubsub_v1
import json
import random
import time

publisher = pubsub_v1.PublisherClient()

project_name = 'magnetic-tenure-408314'
topic_name = 'payment-topic'
topic_path = publisher.topic_path(project_name, topic_name)

payment_methods = ["Credit Card", "Debit Card", "PayPal", "Google Pay", "Apple Pay"]

def callable(future):
    try:
        message_id = future.result()
        print(f"Message published with ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")

def generate_payment_mock_data(order_id):

    return {
        "payment_id": order_id + 1000,  # Starting from 1001 as in the example
        "order_id": order_id,
        "payment_method": random.choice(payment_methods),
        "card_last_four": str(order_id).zfill(4)[-4:],  # Just using order_id to make last 4 digits
        "payment_status": "Completed",
        "payment_datetime": f"2024-02-16T{str(order_id).zfill(2)}:01:30Z"  # Using order_id to vary the hour for variety
    }

for order_id in range(1, 501):
    data = generate_payment_mock_data(order_id)
    json_data = json.dumps(data).encode('utf-8')

    try:
        future = publisher.publish(topic_path, data=json_data)
        future.add_done_callback(callable)
        time.sleep(1)
    except Exception as e:
        print(f"Exception Occurred: {e}")