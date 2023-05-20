from confluent_kafka import Consumer
import csv
import time
################
c=Consumer({'bootstrap.servers':'broker:29092','group.id':'python-consumer','auto.offset.reset':'earliest'})
print('Kafka Consumer has been initiated...')

print('Available topics to consume: ', c.list_topics().topics)
c.subscribe(['messaggi'])
################
def main():
    
    time.sleep(5)
    # Elenco delle righe del CSV
    rows = []

    empty_queue = False
    a=0
    while not empty_queue:
        msg=c.poll(1) #timeout
        if msg is None:
            a=a+1
            if a>10 :
                empty_queue = True
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data=msg.value().decode('utf-8')
        rows.append(data)
        print("STAMPA \n")
    c.close()
        
    with open('output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        for row in rows:
            writer.writerow([row])
            
if __name__ == '__main__':
    main()