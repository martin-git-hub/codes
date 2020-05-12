import time
from stompest.config import StompConfig
from stompest.sync import Stomp
from stompest.error import StompConnectionError
import sys
import getopt, os
import os.path
import threading
import logging
from logging.handlers import RotatingFileHandler
from socket import error as SocketError
import subprocess
from subprocess import Popen,PIPE

def Write2File(ThreadName, LogLevel, Message):
    if LogLevel == "INFO":
        logger.setLevel(logging.INFO)
        handler.setLevel(logging.INFO)
        logger.info(str(ThreadName) + " : " + str(Message))
    if LogLevel == "ERROR":
        logger.setLevel(logging.ERROR)
        handler.setLevel(logging.ERROR)
        logger.error(str(ThreadName) + " : " + str(Message))
    if LogLevel == "DEBUG":
        logger.setLevel(logging.DEBUG)
        handler.setLevel(logging.DEBUG)
        logger.debug(str(ThreadName) + " : " + str(Message))

def CreateConnection():
    try:
        Write2File(threading.currentThread().getName(), "INFO", "Connecting to ActiveMQ Server.")
        client = Stomp(StompConfig("tcp://" + server + ":" + port, login = login, passcode = passcode, version = "1.2"))
        client.connect(versions = ["1.2"], host = vhost, heartBeats = (0, 60000))   #CONNECT
        subscription = client.subscribe(destination, {"ack": "client", "id":"0", "headers":{"activemq.prefetchSize": 1}})  #SUBSCRIB
        Write2File(threading.currentThread().getName(), "INFO", "Connection to ActiveMQ Server successfully established.")
    except:
        Write2File(threading.currentThread().getName(), "ERROR", str(sys.exc_info()))
        raise

    try:
        while(client.canRead(5)):
            Write2File(threading.currentThread().getName(), "DEBUG", "Setting LOCK.")
            container.acquire()
            ReceiveFrame(client)
        else:
            Write2File(threading.currentThread().getName(), "INFO", "Closing connection with ActiveMQ Server.")
            client.disconnect()
            if(container.acquire(blocking=False)):
                Write2File(threading.currentThread().getName(), "DEBUG", "Removing LOCK.")
                container.release()
    except:
        Write2File(threading.currentThread().getName(), "ERROR", str(sys.exc_info()))
        exit(1)

def ReceiveFrame(client):
    try:
        Write2File(threading.currentThread().getName(), "INFO", "Receiving from message queue.")
        frame = client.receiveFrame()
        Write2File(threading.currentThread().getName(), "DEBUG", "Removing LOCK.")
        container.release()
    except ConnectionResetError as error:
        Write2File(threading.currentThread().getName(), "ERROR",  "ConnectionResetError: " + str(error))
        raise
    except stompest.error.StompConnectionError as stomperror:
        Write2File(threading.currentThread().getName(), "ERROR", "StompConnectionError occured: " + str(stomperror))
        Write2File(threading.currentThread().getName(), "ERROR", "Closing connection with ActiveMQ Server.")
        client.disconnect()
        raise
    except:
        Write2File(threading.currentThread().getName(), "ERROR", "Can't handle message received, NACKing.")
        try:
            client.nack(frame)  #NACK
        except:
            Write2File(threading.currentThread().getName(), "ERROR", "Can't NACK the frame.")
            raise
        raise
    else:
        audiofile = frame.body.decode("utf-8")
        Write2File(threading.currentThread().getName(), "INFO", "Received frame: " + str(frame.body.decode("utf-8")))
        Write2File(threading.currentThread().getName(), "DEBUG", "Received frame header: " + str(frame.headers))
        if(os.path.exists(audiofile) == True):
            try:
                p = subprocess.run(["python", "nmf_converter_extra.py", audiofile], stdout=PIPE, stderr=PIPE, timeout=5)
                p.check_returncode()
            except Exception as error:
                Write2File(threading.currentThread().getName(), "ERROR", str(error))
                CreateRecords(client, audiofile.encode())
        else:
            Write2File(threading.currentThread().getName(), "Error", "File does not exist: " + str(audiofile))


        Write2File(threading.currentThread().getName(), "INFO", "ACKing received frame.")
        client.ack(frame)
        Write2File(threading.currentThread().getName(), "INFO", "Received frame ACKed.")
        return

def CreateRecords(client, audiofile):
    try:
        Write2File(threading.currentThread().getName(), "INFO", "File not processed. Creating record in queue FAILED.")
        client.send("/queue/FAILED", audiofile, headers={'persistent':'true'})
        Write2File(threading.currentThread().getName(), "INFO", "Record created.")
    except Exception as error:
        Write2File(threading.currentThread().getName(), "ERROR", str(error))

def create_threads(ThreadNum):
    count = 0
    while count < int(ThreadNum):
        ThreadName = "Thread-"+str(count)
        Write2File(ThreadName, "INFO", "Creating thread")
        try:
            threads.append(threading.Thread(name=str(ThreadName), target=CreateConnection))
            threads[count].start()
            Write2File(ThreadName, "INFO", "Thread started.")
        except:
            Write2File(ThreadName, "ERROR", "Unable to start thread.")
            raise
        count +=1

ThreadNum = int(sys.argv[1])
threads = []
server = "localhost"
port = "61613"
vhost = "localhost"
login = "admin"
passcode = "admin"
destination = "/queue/"+str(sys.argv[2])
global frame
global client
max_items = 1
container = threading.BoundedSemaphore(max_items)

# create a logger
logger = logging.getLogger("consumer")
# create a file handler
handler = RotatingFileHandler("consumer.log", mode='a', maxBytes=20971520, backupCount=900, encoding="utf-8", delay=False)
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(handler)
#Create configured threads
create_threads(ThreadNum)

