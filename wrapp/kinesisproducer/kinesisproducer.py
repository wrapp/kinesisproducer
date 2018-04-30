"""
Imported and (heavily) modified from kiner https://github.com/bufferapp/kiner/blob/a4889e3/kiner/producer.py under the following license:

MIT License

Copyright (c) 2017 Buffer

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""
import boto3
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime
import threading
import time
import uuid
import atexit
import json
import base64
from abc import abstractmethod

try:
    from Queue import Queue
except ImportError:  # Py3
    from queue import Queue


LOG = logging.getLogger(__name__)


def encode_data(data, encoding='utf_8'):
    """Return bytes as-is, anything that can be serialized as json bytes,
    anything else as converted to string and encoded as bytes"""
    if isinstance(data, bytes):
        return data
    try:
        return json.dumps(data).encode(encoding)
    except:
        return str(data).encode(encoding)


class KinesisProducer(object):
    """Abstract base class for Kinesis producers (now Firehose, planned to be Streams as well)"""
    @abstractmethod
    def __init__(self, stream_name, batch_size,
                 batch_time, max_retries, threads,
                 kinesis_client):
        pass

    @abstractmethod
    def put_record(self, record): pass


class KinesisFirehoseProducer(KinesisProducer):
    """Basic Kinesis Producer adapted for Firehose
    Parameters
    ----------
    stream_name : string
        Name of the stream to send the records.
    batch_size : int
        Numbers of records to batch before flushing the queue.
    batch_time : int
        Maximum of seconds to wait before flushing the queue.
    max_retries: int
        Maximum number of times to retry the put operation.
    kinesis_client: boto3.client
        Kinesis Firehose client.
    Attributes
    ----------
    records : array
        Queue of formated records.
    pool: concurrent.futures.ThreadPoolExecutor
        Pool of threads handling client I/O.
    """

    def __init__(self, stream_name, batch_size=500,
                 batch_time=5, max_retries=5, threads=3,
                 kinesis_client=boto3.client('firehose')):
        self.stream_name = stream_name
        self.queue = Queue()
        self.batch_size = batch_size
        self.batch_time = batch_time
        self.max_retries = max_retries
        self.kinesis_client = kinesis_client
        self.pool = ThreadPoolExecutor(threads)
        self.last_flush = time.time()
        self.monitor_running = threading.Event()
        self.monitor_running.set()
        self.pool.submit(self.monitor)

        atexit.register(self.close)  # Make a best attempt to flush at exit

    def monitor(self):
        """Flushes the queue periodically."""
        while self.monitor_running.is_set():
            if time.time() - self.last_flush > self.batch_time:
                if not self.queue.empty():
                    LOG.debug("Queue Flush: time without flush exceeded")
                    self.flush_queue()
                    time.sleep(self.batch_time)
            time.sleep(.1)

    def put_record(self, record):
        """Add data to the record queue
        Parameters
        ----------
        data : dict
            Data to send.
        """
        self.queue.put(encode_data(record))

        # Flush the queue if it reaches the batch size
        if self.queue.qsize() >= self.batch_size:
            LOG.debug("Queue Flush: batch size reached")
            self.pool.submit(self.flush_queue)

    def close(self):
        """Flushes the queue and waits for the executor to finish."""
        LOG.info('Closing producer')
        self.flush_queue()
        self.monitor_running.clear()
        self.pool.shutdown()
        LOG.info('Producer closed')

    def flush_queue(self):
        """Grab all the current records in the queue and send them."""
        records = []

        while not self.queue.empty() and len(records) < self.batch_size:
            records.append(self.queue.get())

        if records:
            try:
                self.send_records(records)

            except Exception as e:
                LOG.exception("Exception when sending records")
            self.last_flush = time.time()

    def send_records(self, records, attempt=0):
        """Send records to the Kinesis Firehose.
        Falied records are sent again with an exponential backoff decay.
        Parameters
        ----------
        records : array
            Array of formated records to send.
        attempt: int
            Number of times the records have been sent without success.
        """

        # If we already tried more times than we wanted, save to a file
        if attempt > self.max_retries:
            LOG.warning('Writing {} records to file'.format(len(records)))
            with open('failed_records.dlq', 'at') as f:
                for r in records:
                    f.write(base64.b64encode(r))
                    f.write('\n')
            return

        # Sleep before retrying
        if attempt:
            time.sleep(2 ** attempt * .1)

        response = self.kinesis_client.put_record_batch(DeliveryStreamName=self.stream_name,
                                                        Records=[{'Data': record} for record in records])
        failed_record_count = response['FailedPutCount']

        # Grab failed records
        if failed_record_count:
            LOG.warning('Retrying failed records')
            failed_records = []
            for i, record in enumerate(response['RequestResponses']):
                if record.get('ErrorCode'):
                    # Index is always same in req and resp
                    failed_records.append(records[i])

            # Recursive call
            attempt += 1
            self.send_records(failed_records, attempt=attempt)
