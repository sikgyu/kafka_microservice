import connexion
import yaml
from connexion import NoContent
import logging
import logging.config
from base import Base
from student_account import StudentAccount
from instructor_account import InstructorAccount
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_
import datetime
import json
import time
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
import sys

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())
    DB_ENGINE = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(app_config['datastore']['user'],
                                                                      app_config['datastore']['password'],
                                                                      app_config['datastore']['hostname'],
                                                                      app_config['datastore']['port'],
                                                                      app_config['datastore']['db']))
    Base.metadata.bind = DB_ENGINE
    DB_SESSION = sessionmaker(bind=DB_ENGINE)

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)
    logger = logging.getLogger('basicLogger')
    logger.info('Connecting to DB. Hostname: {}, Port: {}'.format(app_config['datastore']['hostname'],
                                                                  app_config['datastore']['port']))


def student_account(body):
    """ Receives a student account log """

    session = DB_SESSION()

    sa = StudentAccount(body['fullname'],
                        body['username'],
                        body['password'],
                        body['final_grade'],
                        body['timestamp'])

    session.add(sa)

    session.commit()
    session.close()

    username = body['username']
    logger.debug(
        'Stored event student account request with a unique id of {}'.format(username))

    return NoContent, 201


def instructor_account(body):
    """ Receives an instructor account log """
    session = DB_SESSION()

    ia = InstructorAccount(body['fullname'],
                           body['username'],
                           body['password'],
                           body['classes']['math'],
                           body['classes']['python'],
                           body['timestamp'])

    session.add(ia)

    session.commit()
    session.close()

    username = body['username']
    logger.debug(
        'Stored event instructor account request with a unique id of {}'.format(username))

    return NoContent, 201


def get_student_account(start_timestamp, end_timestamp):
    """ Gets new student account events after the timestamp """
    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(
        start_timestamp, "%Y-%m-%dT%H:%M:%S")
    end_timestamp_datetime = datetime.datetime.strptime(
        end_timestamp, "%Y-%m-%dT%H:%M:%S")

    students = session.query(StudentAccount).filter(
        and_(StudentAccount.date_created >= start_timestamp_datetime,
             StudentAccount.date_created < end_timestamp_datetime))

    results_list = []
    for student in students:
        results_list.append(student.to_dict())
        session.close()

    logger.info("Query for Student Account events after %s returns %d results" %
                (start_timestamp_datetime, len(results_list)))

    return results_list, 200


def get_instructor_account(start_timestamp, end_timestamp):
    """ Gets new instructor account events after the timestamp """
    session = DB_SESSION()
    start_timestamp_datetime = datetime.datetime.strptime(
        start_timestamp, "%Y-%m-%dT%H:%M:%S")
    end_timestamp_datetime = datetime.datetime.strptime(
        end_timestamp, "%Y-%m-%dT%H:%M:%S")

    students = session.query(InstructorAccount).filter(
        and_(InstructorAccount.date_created >= start_timestamp_datetime,
             InstructorAccount.date_created < end_timestamp_datetime))

    results_list = []
    for instructor in students:
        results_list.append(instructor.to_dict())
        session.close()

    logger.info("Query for Instructor Account events after %s returns %d results" %
                (start_timestamp_datetime, len(results_list)))

    return results_list, 200


def process_messages():
    """ Process event messages """
    hostname = "%s:%d" % (
        app_config["events"]["hostname"], app_config["events"]["port"])
    logger.info("process messages.")
    count = 0
    retry = 10
    connected = False
    client = None

    while not connected and count < retry:
        try:
            logger.info("Connecting... (%d)" % count)
            client = KafkaClient(hosts=hostname)
            connected = True
        except:
            logger.error('lost connection. (%d)' % count)
            count += 1
            time.sleep(app_config["events"]["period_sec"])
    if not connected:
        logger.critical("CANNOT CONNECT TO KAFKA. EXITING...")
        sys.exit(0)

    topic = client.topics[str.encode(app_config["events"]["topic"])]
    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                                         reset_offset_on_start=False,
                                         auto_offset_reset=OffsetType.LATEST)
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg['payload']
        if msg["type"] == 'final_grade':
            student_account(payload)
        elif msg['type'] == 'classes':
            instructor_account(payload)
        consumer.commit_offsets()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", base_path="/storage",
            strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090)
