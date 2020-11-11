import pika
import sys
import os
import fnmatch


def getFilesList(path, ext):
    logfiles = [os.path.join(dirpath, f)
                for dirpath, dirnames, files in os.walk(path)
                for f in fnmatch.filter(files, ext)]
    return logfiles


fileList = getFilesList(sys.argv[1], sys.argv[2])

if len(fileList) > 0:
    credentials = pika.PlainCredentials(sys.argv[3], sys.argv[4])
    connection = pika.BlockingConnection(pika.ConnectionParameters(sys.argv[5],
                                                                   5672,
                                                                   sys.argv[6],
                                                                   credentials))
    channel = connection.channel()
    channel.queue_declare(queue=sys.argv[6], durable=True)
    count = 0
    for i in range(0, len(fileList)):
        with open(fileList[i], "r", encoding="utf-8") as file:
            content = file.read()
            channel.basic_publish(exchange='', routing_key=sys.argv[6], body=content)
            count = count + 1
    print("download " + str(count) + " messages")
    connection.close()
else:
    print("not files for your request!")
