import os
import sys
from requests import Session
from time import sleep
import logging
from google.cloud import pubsub_v1
from concurrent import futures


class PublishTopic:
    def __init__(self):
        self.project_id = "sid-egen"
        self.topic_id = "dota-data"
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id,self.topic_id)
        self.publish_futures = []

    def getMatchData(self):
        url="https://api.opendota.com/api/publicMatches"
        sess = Session()
        raw_data = sess.get(url)
        if 200<=raw_data.status_code<400:
            logging.info("Data ingested")
            return raw_data.text
        else:
            raise Exception("Failed to fetch data")


    def get_callback(self, publish_future, data):
        def callback(publish_future):
            try:
                # Wait 60 seconds for the publish call to succeed.
                logging.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logging.error("Publishing data timed out.")

        return callback

    def pushToTopic(self,data):
        # When you publish a message, the client returns a future.
        publish_future = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(self.get_callback(publish_future, data))
        self.publish_futures.append(publish_future)

        # Wait for all the publish futures to resolve before exiting.
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        logging.info("Published message to Topic")

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO)
    
    serv = PublishTopic()
    for i in range(60):
        message=serv.getMatchData()
        serv.pushToTopic(message)
        