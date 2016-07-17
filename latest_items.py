import threading
import redis
from time import sleep
import random

def data_genarator(thread_id, t1):
    r = redis.StrictRedis()
    for i in range(100):
        r.lpush('mylist', thread_id + '_' + str(i))
        r.ltrim('mylist', 0, 5)
        #sleep(0.01)
    print 'work for ' + thread_id + ' completed'


def producer(thread_id, t1):
    r = redis.StrictRedis()
    for i in range(100):
        r.lpush('mylist', thread_id + '_' + str(i))
        print 'Producer', thread_id, 'generated', str(i)
        #sleep for random time 0 - 1 seconds
        sleep(0.1 * random.random())
    print 'work completed by', thread_id

def consumer(thread_id, ret_processed_data):
    r = redis.StrictRedis()
    while True:
        data = r.rpop('mylist')
        if  data == None:
            print 'Nothing to consume... waiting for producer to generate data', thread_id
            data = r.brpop('mylist', 10)
            if data == None:
                print 'No data obtained after waiting for 10 secs', thread_id
                break
            else:
                print 'Consumer', thread_id, 'processed', str(data), 'using brpop'
                ret_processed_data.append(data[1])

        else:
            print 'Consumer', thread_id, 'processed', str(data)
            ret_processed_data.append(data)

        #sleep for random time 0 - 1 seconds
        sleep(0.1 * random.random())


def main():
    thread_list = ['thread'+str(i) for i in range(5)]
    threads = []
    consumer_data = [[] for i in thread_list]
    #start producers
    for i in range(len(thread_list)):
        t = threading.Thread(target = producer, args = (thread_list[i], 1))
        threads.append(t)
        t.start()

    
    #start consumers
    for i in range(len(thread_list)):
        t = threading.Thread(target = consumer, args = (thread_list[i], consumer_data[i]))
        threads.append(t)
        t.start()

    print 'Main - Nothing to do...'

    # wait for completion
    for t in threads:
        t.join()

    # check consistency
    total_processed = 0
    for i in range(len(consumer_data)):
        total_processed += len(consumer_data[i])
        print 'Thread', str(i), 'processed', len(consumer_data[i]), 'items'
        print consumer_data[i]
    print 'total_processed', total_processed



if __name__ == '__main__':
    main()