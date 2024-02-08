import json
import threading
from datetime import datetime
import queue
import pika
import time


def run_program(user_id):
    status1 = queue.Queue()
    status2 = queue.Queue()

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    def main1(status_queue):
        s = simulation_program1(user_id, timestamp)
        status_queue.put(s)

    def simulation_program1(user_id, timestamp):
        status = int(input(f"Enter status for user {user_id} at {timestamp}: "))
        return status if status in [1, 0, -1] else None

    simulation1_thread = threading.Thread(target=main1, args=(status1,))
    simulation1_thread.start()

    def main2(status_queue):
        s = simulation_program2(user_id, timestamp)
        status_queue.put(s)

    def simulation_program2(user_id, timestamp):
        status = int(input(f"Enter status for user {user_id} at {timestamp}: "))
        return status if status in [1, 0, -1] else None

    simulation2_thread = threading.Thread(target=main2, args=(status2,))
    simulation2_thread.start()



    status1_value = status1.get()
    status2_value = status2.get()

    #def print_final_status(final_status):
     #   print("Final Status:", final_status)



    def message_producer1():
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        message_data = {'user_id': user_id, 'timestamp': timestamp, 'final_status': status1_value}
        message_body = json.dumps(message_data)
        channel.queue_declare(queue='Main_queue', durable=True)
        channel.basic_publish(exchange='', routing_key='Main_queue', body=message_body)
        print(" [x] Sent Data")
        connection.close()

    message_producer1()


    def message_producer2():
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        message_data = {'user_id': user_id, 'timestamp': timestamp, 'final_status': status2_value}
        message_body = json.dumps(message_data)
        channel.queue_declare(queue='Main_queue', durable=True)
        channel.basic_publish(exchange='', routing_key='Main_queue', body=message_body)
        print(" [x] Sent Data")
        connection.close()

    message_producer2()
# Continuous loop
while True:
    for user_id in range(1, 11):
        run_program(user_id)

    time.sleep(60)
