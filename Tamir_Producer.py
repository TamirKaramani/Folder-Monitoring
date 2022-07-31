# Tamir Karamani's Producer

# this python file manages the folder monitoring application (Producer)

# import useful libraries
import hashlib
import pika
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# defining the right parameters of the event handler
patterns = ["*"]
ignore_patterns = None
ignore_directories = False
case_sensitive = True
my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)


# Re-writing the built-in functions of the event handler, so we can combine them with RabbitMQ commands
# Function defining phase

# File creation event


def on_created(event):
    print(f"File was created at {event.src_path}")
    # Setting up the connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='MessageBusQueue')
    channel.basic_publish(exchange='', routing_key='MessageBusQueue',
                          body=f'A file was added - {event.src_path}'.encode())
    print(" [x] Sent a file update message to consumer")
    connection.close()


# File deletion event


def on_deleted(event):
    print(f"File was deleted at {event.src_path}!")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='MessageBusQueue')
    channel.basic_publish(exchange='', routing_key='MessageBusQueue',
                          body=f'A file was deleted - {event.src_path}'.encode())
    print(" [x] Sent a file delete message to consumer")
    connection.close()


# File modify event


def on_modified(event):
    if event.src_path != r"C:\Users\tamir\Desktop\TestDir\logs.txt":
        print(f"The following file has been modified: {event.src_path}")

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='MessageBusQueue')
        channel.basic_publish(exchange='', routing_key='MessageBusQueue',
                              body=f'A change detected - {event.src_path}'.encode())
        print(" [x] Sent 'A change detected'")
        connection.close()


# File move event


def on_moved(event):
    print(f"The file {event.src_path} has been moved to {event.dest_path}")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='MessageBusQueue')
    channel.basic_publish(exchange='', routing_key='MessageBusQueue',
                          body=f'A file moved from {event.src_path} to {event.dest_path}'.encode())
    print(" [x] Sent 'A file moved'")
    connection.close()


# Function rewriting phase


my_event_handler.on_created = on_created
my_event_handler.on_deleted = on_deleted
my_event_handler.on_modified = on_modified
my_event_handler.on_moved = on_moved

# Setting up the observer


# Setting up the target folder to monitor
path = r"C:\Users\tamir\Desktop\TestDir"
go_recursively = True
my_observer = Observer()
my_observer.schedule(my_event_handler, path, recursive=go_recursively)

my_observer.start()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    my_observer.stop()
    my_observer.join()
