import Queue
import avro.schema
import time
import threading
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


schema = avro.schema.parse(open("log_schema.avsc").read())


class AvroAppender(threading.Thread):
    def __init__(self, file):
        threading.Thread.__init__(self)
        self.avro_writer = DataFileWriter(open(file, "w"), DatumWriter(), schema)
        self.queue = Queue.Queue()
        self.should_stop = False
        self.mutex = threading.Lock()
        self.start()


    def log_append(self, user, advertiser, **kwargs):
        if user is not None and advertiser is not None:
            record = dict(user=user, advertiser=advertiser)
            if kwargs["ip"]:
                record["ip"] = kwargs["ip"]
            if kwargs["agent"]:
                record["agent"] = kwargs["agent"]
            if kwargs["time"]:
                record["timestamp"] = float(kwargs["time"])
            else:
                record["timestamp"] = float(time.time())
            if kwargs["keywords"]:
                record["keywords"] = list(set([string.strip() for string in kwargs["keywords"].split(",")]))
            self.queue.put_nowait(record)
        else:
            print "Missing user"


    def close_appender(self):
        self.mutex.acquire()
        self.should_stop = True
        self.mutex.release()

    def run(self):
        while True:
            try:
                record = self.queue.get(False, 1000)
                self.avro_writer.append(record)
            except Queue.Empty:
                self.mutex.acquire()
                stop = self.should_stop
                self.mutex.release()
                if stop:
                    break
        self.avro_writer.close()

