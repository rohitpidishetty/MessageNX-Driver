import requests as req
from collections import deque
import time
import sys


"""
MessageNX - High-Performance Producer-Consumer Message Processing System

MessageNX is a scalable and efficient message processing library developed under
the NFRAC initiative. It provides a reliable mechanism for producers to push jobs
to isolated channels and for consumers to fetch, queue, and process these jobs
concurrently with configurable thresholds and timeouts.

Features:
- Asynchronous job fetching from multiple channels.
- Local queue management with configurable threshold to optimize processing.
- JSON-based message handling (using Python dict).
- Timeout and retry logic for robust connectivity with producers.
- Flexible processing via callback function to allow custom handling of jobs.

Designed for enterprise-level task orchestration where multiple
producer-consumer pairs need isolated channels but centralized management
of job metadata.

Author: Er. P. Rohit V. Acharya
Organization: NFRAC
Version: 1.0
Date: 2026-03-06
"""


class MessageNX:
    def __init__(
        self,
        channel="https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-consumer/listen",
        threshold=5,
        timeout=60,
    ):
        self.channel = channel
        self.threshold = threshold
        self.timeout = timeout
        self.localQueue = deque()
        self.previousTimeStamp = None
        self.producer_details = []
        self.channel_id = None

    def app_name(self, name):
        self.producer_details.append(name)
        return self

    def register(self):
        content = "".join(self.producer_details)
        res = req.get(
            f"https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-producer/id?content={content}"
        )

        if res.status_code == 409:
            print("ID already exists. Try a different combination.")
            sys.exit(1)
        elif res.status_code != 200:
            print("Error:", res.content.decode("utf-8"))
            return

        id = res.text

        print(
            f"{id} is the id registered for this particular app. Use this on consumer end, to read messages"
        )

        return id

    def produce(self, topic, id):
        res = req.post(
            url="https://messagenx-a5f0h8c5gehjd8ab.canadacentral-01.azurewebsites.net/message-nx-producer/listen",
            json={"channel": id, "topic": topic},
        )
        return res

    def set_threshold(self, threshold):
        self.threshold = threshold

    def set_channel(self, channel_id):
        self.channel_id = channel_id

    def consume(self, process_callback):
        if self.channel_id == None:
            print("Set a channel ID before consuming messages.")
            return
        while True:
            while len(self.localQueue) < self.threshold:
                res = req.get(url=self.channel + "?channel=" + self.channel_id)
                if res.status_code == 202:
                    self.localQueue.append(res.json())
                    self.previousTimeStamp = None
                else:
                    if self.previousTimeStamp == None:
                        self.previousTimeStamp = time.time()

                    if (
                        self.previousTimeStamp != None
                        and (time.time() - self.previousTimeStamp) > self.timeout
                    ):
                        self.previousTimeStamp = None
                        break
                    time.sleep(5)

            while len(self.localQueue) > 0:
                process_callback(self.localQueue.popleft())
