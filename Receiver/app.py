"""
    import json => json_str = json.dumps(body, indent=2)
    fh = open("text.json", "w")
    fh.write(json_str)
    close the file    
    
    load test:
    Thread group 
    HTTP request
    result tree
    set variables
"""
import connexion
from connexion import NoContent
import os
import json
import requests
import yaml
import logging
import logging.config
from datetime import datetime
import json
from pykafka import KafkaClient
import time
import sys

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

count = 0
retry = app_config["retries"]["number"]
sleep_time = app_config["retries"]["sleep"]

while count < retry:
    try:
        logger.info("Connecting...")
        hostname = "%s: %d" % (
            app_config["events"]["hostname"], app_config["events"]["port"])
        client = KafkaClient(hosts=hostname)
        topic = client.topics[str.encode(app_config["events"]["topic"])]

        producer = topic.get_sync_producer()
        break
    except:
        logger.error("Lost connection. (%d)" % count)
        time.sleep(app_config["events"]["period_sec"])
        count += 1


def deliver_order_tracking(body):
    """ Receives a delivery order """

    logger.info(f"received event delivery {body['id']}")

    # this is a old version
    # headers = {"content_type": "application/json"}
    # response = requests.post(app_config['eventstore1']['url'],
    #                          json=body, headers=headers)

    msg = {"type": "delivery_order",
           "datetime":
           datetime.now().strftime(
               "%Y-%m-%d %H:%M:%S"),
           "payload": body}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    logger.info(
        f"Returned event delivery {body['id']} 201")
    logger.info(f"The message from delivery : {msg_str}")


    return NoContent, 201


def pickup_order_tracking(body):
    """ Receives a pickup order """
    logger.info(f"received event pickup {body['id']}")

    # this is old version
    # headers = {"content_type": "application/json"}
    # response = requests.post(app_config['eventstore2']['url'],
    #                          json=body, headers=headers)

    # client = KafkaClient(
    #     hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")
    # topic = client.topics[str.encode(app_config['events']['topic'])]
    msg = {"type": "pickup_order",
           "datetime":
           datetime.now().strftime(
               "%Y-%m-%d %H:%M:%S"),
           "payload": body}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    logger.info(
        f"Returned event pickup {body['id']} 201")
    logger.info(f"The message from pickup_order : {msg_str}")
    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            base_path="/receiver",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)
