import time
import sys
import os
import stomp
import logging
from logging.handlers import RotatingFileHandler
#path = r'C:\Users\cer852\Pictures'
#path = r'/'
path = sys.argv[1]
queue = str(sys.argv[2])
messages = 1

def Write2File(LogLevel, Message):
    if LogLevel == "INFO":
        logger.setLevel(logging.INFO)
        handler.setLevel(logging.INFO)
        logger.info(str(Message))
    if LogLevel == "ERROR":
        logger.setLevel(logging.ERROR)
        handler.setLevel(logging.ERROR)
        logger.error(str(Message))

logger = logging.getLogger("producer")
handler = RotatingFileHandler("producer.log", mode='a', maxBytes=20971520, backupCount=900, encoding="utf-8", delay=False)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

user = os.getenv("ACTIVEMQ_USER") or "admin"
password = os.getenv("ACTIVEMQ_PASSWORD") or "password"
host = os.getenv("ACTIVEMQ_HOST") or "localhost"
port = os.getenv("ACTIVEMQ_PORT") or 61613

try:
    Write2File("INFO", "Connecting to ActiveMQ Server.")
    conn = stomp.Connection(host_and_ports = [(host, port)])
    conn.start()
    conn.connect(login=user,passcode=password)
    Write2File("INFO", "Connected.")
except:
    Write2File("ERROR", str(sys.exc_info()))
    exit(1)

for root, dirs, files in os.walk(path):
    for filename in files:
        destination = os.path.join(os.path.abspath(root), filename)
        try:
            Write2File("INFO", "Sending data: " + str(destination))
            conn.send(queue, destination, persistent='true')
            Write2File("INFO", "Data sent.")
        except:
            Write2File("ERROR", str(sys.exc_info()))
            conn.disconnect()
            exit(1)

conn.disconnect()
