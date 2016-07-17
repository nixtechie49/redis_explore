import threading
import redis
from time import sleep

def producer(thread_id, t1):
    r = redis.StrictRedis()
    for i in range(100):
        r.lpush('mylist', thread_id + '_' + str(i))
        r.ltrim('mylist', 0, 5)
        #sleep(0.01)
    print 'work for ' + thread_id + ' completed'


def main():
    thread_list = ['thread'+str(i) for i in range(5)]
    for t in thread_list:
        t = threading.Thread(target = producer, args = (t, 1))
        t.start()

    r = redis.StrictRedis()
    for i in range(110):
        print r.lrange('mylist', 0, -1)
        print
        #sleep(0.01)
    print 'nothing more to do'





if __name__ == '__main__':
    main()