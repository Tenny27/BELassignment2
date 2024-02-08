import pika
import json
import sys
import psycopg2
import time
import threading
from datetime import datetime

DB_CONNECTION_PARAMS = {
    'host': 'localhost',
    'user': 'postgres',
    'password': '1967',
    'dbname': 'postgres'
}


def get_status(user_id, timestamp):
    try:
        connection = psycopg2.connect(**DB_CONNECTION_PARAMS)
        cursor = connection.cursor()
        sql_command = """
            SELECT status
            FROM main_table
            WHERE user_id = %s AND timestamp = %s;
        """

        cursor.execute(sql_command, (user_id, timestamp))
        result = cursor.fetchone()
        if result is not None:
            return result[0]
        else:
            print(f"No data found for User {user_id}, Timestamp {timestamp}")
            return None

    except psycopg2.DatabaseError as error:
        print(f"Error retrieving data from the database: {error}")
        return None

    finally:
        if connection:
            cursor.close()
            connection.close()


def update_in_postgres(user_id, timestamp, status):
    try:
        connection = psycopg2.connect(**DB_CONNECTION_PARAMS)
        cursor = connection.cursor()

        existing_status = get_status(user_id, timestamp)

        if existing_status == 0 and status == 1:
            status = min(existing_status, status)
        elif existing_status == -1 and status in [0, 1]:
            status = status
        elif existing_status in [0, 1] and status == -1:
            status = existing_status

        sql_command = """
            INSERT INTO main_table (user_id, timestamp, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, timestamp)
            DO UPDATE SET status = %s;
        """

        cursor.execute(sql_command, (user_id, timestamp, status, status))

        # Commit the changes
        connection.commit()
        print(f"Database updated successfully: User {user_id}, Timestamp {timestamp}, Final Status {status}")

    except psycopg2.DatabaseError as error:
        print(f"Error updating database: {error}")

    finally:
        if connection:
            cursor.close()
            connection.close()


def callback(ch, method, properties, body):
    message_data = json.loads(body)

    user_id = message_data.get('user_id')
    timestamp = message_data.get('timestamp')
    status = message_data.get('final_status')

    print(f"Received message: User {user_id}, Timestamp {timestamp}, Final Status {status}")
    update_in_postgres(user_id, timestamp, status)


def consume_messages():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='Main_queue', durable=True)
        channel.basic_consume(queue='Main_queue', on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)


def produce_messages():
    try:
        while True:
            for user_id in range(1, 11):
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
                message_data = {'user_id': user_id, 'timestamp': timestamp, 'status': None}
                message_body = json.dumps(message_data)

                connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
                channel = connection.channel()
                channel.queue_declare(queue='Main_queue', durable=True)
                channel.basic_publish(exchange='', routing_key='Main_queue', body=message_body)
                print(" [x] Sent Data")
                connection.close()

                time.sleep(60)  # Wait for 1 minute before sending the next message

    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)


if __name__ == '__main__':
    consumer_thread = threading.Thread(target=consume_messages)
    producer_thread = threading.Thread(target=produce_messages)

    consumer_thread.start()
    producer_thread.start()

    consumer_thread.join()
    producer_thread.join()
