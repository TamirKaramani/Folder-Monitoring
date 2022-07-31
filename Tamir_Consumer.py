# Tamir Karamani's Consumer

# this python file manages the Consumer

# Import useful libraries
import pika
import hashlib
import os
from datetime import datetime

# Setting up the queue to listen to
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='MessageBusQueue')

# Declare the initial DB dict and a dup dict that manages the duplications count
DB = {}
dup_dict = {}

# folder path - change the path to the folder you want to monitor path
DB_path = r"C:\Users\tamir\Desktop\TestDir"
# creation of the logs file
logs = open(DB_path + r'\\' + 'logs.txt', 'x')

# Creating a list of hushes for all files currently in the DB (the monitored folder)

# Iterate over all files currently in the folder and adding them to the hush DB dict
for path in os.listdir(DB_path):
    # check if current path is a file
    if os.path.isfile(os.path.join(DB_path, path)):
        with open(DB_path + r'\\' + path) as f:
            data = f.read().encode()

        md5hash = hashlib.md5(data).hexdigest()
        if md5hash not in DB.values():
            dup_dict[md5hash] = 1
            DB[DB_path + '\\' + path] = md5hash

# The basic consumer function. issues whenever a message is received from the producer


def callback(ch, method, properties, body):
    Decoded_Body = body.decode()
    print(" [x] Received - ", Decoded_Body)
    # In case a file was added:
    if Decoded_Body[0:16] == 'A file was added':
        with open(Decoded_Body[19:]) as new_file:
            new_data = new_file.read().encode()
        new_md5hash = hashlib.md5(new_data).hexdigest()

        if new_md5hash in DB:
            dup_dict[new_md5hash] += 1
            # working for files that end with 3 characters as in "txt" , "pdf" etc..
            os.rename(Decoded_Body[19:], Decoded_Body[19:-4] + f'_DUP_{dup_dict[new_md5hash]}' + Decoded_Body[-4:])
            print("There wasn't an update in the hash DB. duplicated file created")
        else:
            DB[Decoded_Body[19:]] = new_md5hash
            dup_dict[new_md5hash] = 1
            print("There was an update in the hash DB: ", DB)

    # In case a file was deleted:

    if Decoded_Body[0:18] == 'A file was deleted':
        print(f"The following hash was deleted from hash DB: {DB[Decoded_Body[21:]]}")
        DB.pop(Decoded_Body[21:])
        print(DB)

    # In case a change is detected:

    if Decoded_Body[0:17] == 'A change detected':
        with open(DB_path + r'\\' + 'logs.txt', 'a') as write_to_logs:
            write_to_logs.write(datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' - ' + Decoded_Body + '\n')

    # In case a movement or file rename was issued:

    if Decoded_Body[0:17] == 'A file moved from':
        with open(DB_path + r'\\' + 'logs.txt', 'a') as write_to_logs:
            write_to_logs.write(datetime.today().strftime('%Y-%m-%d %H:%M:%S') + ' - ' + Decoded_Body + '\n')

# Starting up the consumer


channel.basic_consume(queue='MessageBusQueue',
                      auto_ack=True,
                      on_message_callback=callback)

print(' [*] Waiting for messages')
channel.start_consuming()
