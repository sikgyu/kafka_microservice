import connexion
from connexion import NoContent

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from base import Base
from delivery_order import DeliveryOrder
from pickup_order import PickupOrder
import datetime
import yaml
import logging
import logging.config
import json
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
import time
import sys
import os

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())


with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)

# DB_ENGINE = create_engine("sqlite:///readings.sqlite")
DB_ENGINE = create_engine(
    f'mysql+pymysql://{app_config["datastore"]["user"]}:{app_config["datastore"]["password"]}@{app_config["datastore"]["hostname"]}:{app_config["datastore"]["port"]}/{app_config["datastore"]["db"]}')

Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)


def deliver_order_tracking(body):
    """ Receives a delivery order """

    session = DB_SESSION()

    bp = DeliveryOrder(body['total'],
                       body['items'],
                       body['deliveryOrderInfo']['remainingTime'],
                       body['deliveryOrderInfo']['address'],
                       body['deliveryOrderInfo']['driverName']
                       )

    session.add(bp)

    session.commit()
    session.close()

    logger.debug(
        f"Returned event Delivery {body['id']}")

    return NoContent, 201


def get_deliver_order_tracking(orderTime, orderTimeEnd):
    """ Gets delivery order """
    session = DB_SESSION()
    orderTime_datetime = datetime.datetime.strptime(
        orderTime, "%Y-%m-%d %H:%M:%S")
    orderTime_datetime_end = datetime.datetime.strptime(
        orderTimeEnd, "%Y-%m-%d %H:%M:%S")

    readings = session.query(DeliveryOrder).filter(
        and_(DeliveryOrder.orderTime >= orderTime_datetime,
         DeliveryOrder.orderTime < orderTime_datetime_end))
    results_list = []
    for reading in readings:
        results_list.append(reading.to_dict())
    session.close()

    logger.info("Query for delivery orders after %s returns %d results" %
                (orderTime, len(results_list)))
    return results_list, 200


def pickup_order_tracking(body):
    """ Receives a pickup order """
    session = DB_SESSION()

    bp = PickupOrder(body['total'],
                     body['items'],
                     body['pickupOrderInfo']['cookReady'],
                     body['pickupOrderInfo']['pickupPlace'],
                     )

    session.add(bp)

    session.commit()
    session.close()

    logger.debug(
        f"Returned event Pickup {body['id']}")

    return NoContent, 201


def get_pickup_order_tracking(orderTime, orderTimeEnd):
    """ Gets pickup orders """
    session = DB_SESSION()
    ordertime_datetime = datetime.datetime.strptime(
        orderTime, "%Y-%m-%d %H:%M:%S")
    orderTime_datetime_end = datetime.datetime.strptime(
        orderTimeEnd, "%Y-%m-%d %H:%M:%S")

    readings = session.query(PickupOrder).filter(
        and_(PickupOrder.orderTime >= ordertime_datetime, PickupOrder.orderTime < orderTime_datetime_end))
    results_list = []
    for reading in readings:
        results_list.append(reading.to_dict())
    session.close()

    logger.info("Query for pickup orders after %s returns %d results" %
                (orderTime, len(results_list)))
    return results_list, 200




def process_messages():
    """ Process event messages """
    hostname = "%s:%d" % (
        app_config["events"]["hostname"], app_config["events"]["port"])
    logger.info(f"processing messages from {hostname}.")
    
    client = None
    isConnected = False
    current_retry_count = 0
    retry_count = app_config["retries"]["number"]
    sleep_time = app_config["retries"]["sleep"]
    while not isConnected and current_retry_count < retry_count:
        try:
            logger.info(
                f"Try Connecting to Kafka... number of tries: {current_retry_count}")
            client = KafkaClient(hosts=hostname)
            isConnected = True
        except Exception as err:
            if err:
                logger.error("CONNECTION FAILED")
                time.sleep(sleep_time)
                current_retry_count += 1
    if not isConnected:
        logger.critical("CANNOT CONNECT TO KAFKA. EXITING...")
        sys.exit(0)
    topic = client.topics[str.encode(app_config["events"]["topic"])]

    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't
    # read all the old messages from the history in the message queue).
    consumer = topic.get_balanced_consumer(consumer_group=b'event_group',
                                            reset_offset_on_start=False,
                                            zookeeper_connect='zookeeper:2181',
                                            auto_commit_enable=True,
                                            auto_commit_interval_ms=100)
                                            # auto_offset_reset=OffsetType.LATEST)
    logger.info(f"consumer info : {consumer} ")
    logger.info(f"topic info : {topic} ")
    # This is blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg["payload"]
        if msg["type"] == "delivery_order":  # Change this to your event type
            # Store the event1 (i.e., the payload) to the DB
            deliver_order_tracking(payload)
        elif msg["type"] == "pickup_order":  # Change this to your event type
            # Store the event2 (i.e., the payload) to the DB
            pickup_order_tracking(payload)
        # Commit the new message as being read
        consumer.commit_offsets()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            base_path="/storage",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    logger.info(
        f"Connecting to DB, Hostname: {app_config['datastore']['hostname']}, Port: {app_config['datastore']['port']}")
    app.run(port=8090)
