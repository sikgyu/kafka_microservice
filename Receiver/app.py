import json
import connexion
from connexion import NoContent
import os
import requests
from pykafka import KafkaClient

import yaml
import logging.config
import datetime
import time


with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())


with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


retry = 10
count = 0

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
        time.sleep(app_config["event"]["period_sec"])
        count += 1


def student_account(body):
    """ Receives a creating free account event """
    username = body['username']
    logger.info(
        'Received event student account request with a unique id of {}'.format(username))

    hostname = "%s:%d" % (
        app_config["events"]["hostname"], app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    producer = topic.get_sync_producer()
    msg = {"type": "final_grade",
           "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
           "payload": body
           }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    # url = app_config['eventstore1']['url']
    # headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    # r = requests.post(url, data=json.dumps(body), headers=headers)

    logger.info('Received event student account request with a unique id of {} with 201'.format(
        username))
    return NoContent, 201


def instructor_account(body):
    """ Receives a creating premium account event """
    username = body['username']
    logger.info(
        'Received event instructor account request with a unique id of {}'.format(username))

    hostname = "%s:%d" % (
        app_config["events"]["hostname"], app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    producer = topic.get_sync_producer()

    msg = {"type": "classes",
           "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
           "payload": body
           }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    # url = app_config['eventstore2']['url']
    # headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    # r = requests.post(url, data=json.dumps(body), headers=headers)

    logger.info('Received event student account request with a unique id of {} with 201'.format(
        username))
    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            strict_validation=True,
            validate_responses=True)


if __name__ == "__main__":
    app.run(port=8080)
