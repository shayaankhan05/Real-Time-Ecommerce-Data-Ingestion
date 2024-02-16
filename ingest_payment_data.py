import json
from google.cloud import pubsub_v1
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

project_name = 'magnetic-tenure-408314'
subscription_name = 'payment-topic-sub'
subscription_path = subscriber.subscription_path(project_name, subscription_name)
dlq_topic_path = publisher.topic_path(project_name, "dlq-payment-topic")

def cassandra_connection():
    CASSANDRA_NODES = ["127.0.0.1"]
    CASSANDRA_PORT = 9042
    KEYSPACE = 'ecom_store'

    # Authentication
    USERNAME = 'amin'
    PASSWORD = 'admin'
    auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)

    # Cluster Connection
    cluster = Cluster(contact_points=CASSANDRA_NODES, port=CASSANDRA_PORT, auth_provider=auth_provider)

    # Session Creation
    session = cluster.connect(KEYSPACE)

    return cluster, session

cluster, session = cassandra_connection()

def pull_messages():
    while True:
        ack_ids = []
        response = subscriber.pull(request={"subscription" : subscription_path, "max_messages" : 10})

        for received_message in response.received_messages:
            json_data = received_message.message.data.decode('utf-8')

            deserialized_data = json.loads(json_data)

            query = f"SELECT order_id FROM orders_payments_facts WHERE order_id = {deserialized_data.get('order_id')}"
            rows = session.execute(query)
            
            if rows.one():
                update_query = """
                    UPDATE orders_payments_facts
                    set payment_id = %s,
                        payment_method = %s,
                        card_last_four = %s,
                        payment_status = %s,
                        payment_datetime = %s
                    WHERE order_id = %s
                """
                
                payment_values = (
                    deserialized_data.get("payment_id"),
                    deserialized_data.get("payment_method"),
                    deserialized_data.get("card_last_four"),
                    deserialized_data.get("payment_status"),
                    deserialized_data.get("payment_datetime"),
                    deserialized_data.get('order_id')
                )

                session.execute(update_query, payment_values)

                print("Payment data updated in cassandra table!!")
            else:
                publisher.publish(dlq_topic_path, data=json.dumps(deserialized_data).encode('utf-8'))
                print("Data thrown in dlq because order_id not found -> ", deserialized_data)
            
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
