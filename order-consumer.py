import json
from google.cloud import pubsub_v1
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

subscriber = pubsub_v1.SubscriberClient()

project_name = 'magnetic-tenure-408314'
subscription_name = 'order-topic-sub'
subscription_path = subscriber.subscription_path(project_name, subscription_name)

def cassandra_connection():
    # Configuration
    CASSANDRA_NODES = ['127.0.0.1']  # Adjust if your Cassandra is hosted elsewhere or in a cluster
    CASSANDRA_PORT = 9042  # Default Cassandra port, adjust if needed
    KEYSPACE = 'ecom_store'
    
    # Authetication
    USERNAME = 'admin'
    PASSWORD = 'admin'
    auth_provider = PlainTextAuthProvider(username = USERNAME, password = PASSWORD)

    # Connection setup 
    cluster = Cluster(contact_points=CASSANDRA_NODES, port=CASSANDRA_PORT, auth_provider=auth_provider)

    # Session Setup
    session = cluster.connect(KEYSPACE)

    return cluster, session

cluster, session = cassandra_connection()

# Insert order data statement
insert_stmt = session.prepare("""
    INSERT INTO orders_payments_facts(order_id, customer_id, item, quantity, price, shipping_address, order_status, creation_date, payment_id, payment_method, card_last_four, payment_status, payment_datetime)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

def pull_messages():
    while True:
        ack_ids = []
        response = subscriber.pull(request={"subscription" : subscription_path, "max_messages" : 10})

        for received_message in response.received_messages:
            json_data = received_message.message.data.decode('utf-8')
            
            deserialized_data = json.loads(json_data)

            print(deserialized_data)

            cassandra_data = (
                deserialized_data.get("order_id"),
                deserialized_data.get("customer_id"),
                deserialized_data.get("item"),
                deserialized_data.get("quantity"),
                deserialized_data.get("price"),
                deserialized_data.get("shipping_address"),
                deserialized_data.get("order_status"),
                deserialized_data.get("creation_date"),
                None,
                None,
                None,
                None,
                None
            )

            session.execute(insert_stmt, cassandra_data)

            print("Data Inserted in Cassandra Table!!")

            ack_ids.append(received_message.ack_id)

            if ack_ids:
                subscriber.acknowledge(request={"subscription" : subscription_path, "ack_ids" : ack_ids})


if __name__ == "__main__":
    try:
        pull_messages()
    except KeyboardInterrupt:
        pass
    finally:
        cluster.shutdown()




