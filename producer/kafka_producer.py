from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random
import csv

fake=Faker()

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

####################
p=Producer({'bootstrap.servers':'broker:29092'})
print('Kafka Producer has been initiated...')
#####################
def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
        
#####################
def main():

    with open('italy_earthquakes.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        # Iterazione sulle righe del file CSV
        for row in reader:
            # Invio dei dati al topic di Kafka
            message = ','.join(row).encode('utf-8')
            p.poll(0.1)
            p.produce('messaggi', message,callback=receipt)
            p.flush()
            time.sleep(0.1)

        
        
if __name__ == '__main__':
    main()