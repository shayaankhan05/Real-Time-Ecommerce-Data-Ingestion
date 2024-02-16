Summary: Real-time data integration in e-commerce is crucial for businesses to stay competitive and make informed decisions. This involves the seamless flow of data from various sources to a central system, allowing for quick analysis and action. In this scenario, we will explore the use of Google Cloud Platform (GCP) Pub/Sub, Python for producing and consuming messages, Dead Letter Queue (DLQ) for handling failures gracefully, and Cassandra for storing real-time data.

Tech Stack:
- GCP PubSub
- GCP IAM
- SQL
- Python
- Cassandra
- Docker
- Libraries(JSON, random, cassandra-driver, google-cloud-pubsub)

Workflow:

1. Message Producer (Python):
   
- 2 Producer code one for Orders Data and other for Payments Data
- Generates Mock e-commerce events.
- Uses the Google Cloud Pub/Sub Python client to publish messages.
- Sends messages to the respective Pub/Sub topic.

2. Pub/Sub Topic:
   
- Acts as a central communication hub.
- Receives and holds the incoming messages from the producer.
  
3. Message Consumer (Python):

- 2 Consumer code for 2 orders and payments data.
- Subscribes to the Pub/Sub topic using the Google Cloud Pub/Sub Python client.
- Defines a callback function to process incoming messages.
- Acknowledges successful message processing.
- A lookup function to check if order_id is there in for the same payment data, if not present then message is pushed to DLQ

4. Dead Letter Queue (DLQ):
   
- Handles messages that couldn't be processed successfully after multiple attempts.
- Payment data for which there is not order_id in Cassandra table.
- DLQ allows for further analysis and debugging of failed messages.

5. Cassandra Database (Docker):

- Utilizes Docker to set up a Cassandra instance for storing real-time e-commerce data.
- Provides high availability and scalability for the database.





