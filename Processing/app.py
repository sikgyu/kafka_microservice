import datetime
import logging
import logging.config
import os

import connexion
import json
from os import path
import requests
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS, cross_origin

from connexion import NoContent

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

# External Logging Configuration
with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)


def get_stats():
    logger.info("get stats request has been started")
    stats = {}
    if os.path.isfile(app_config["datastore"]["filename"]):
        f = open(app_config["datastore"]["filename"])
        data = f.read()
        f.close()

        data_stats = json.loads(data)

        if "num_score_events" in data_stats:
            stats["num_score_events"] = data_stats["num_score_events"]
        if "highest_score" in data_stats:
            stats["highest_score"] = data_stats["highest_score"]
        if "lowest_score" in data_stats:
            stats["lowest_score"] = data_stats["lowest_score"]
        if "num_in_events" in data_stats:
            stats["num_in_events"] = data_stats["num_in_events"]
        if "last_updated" in data_stats:
            stats["last_updated"] = data_stats["last_updated"]

        logger.info("Found valid stats")
        logger.debug(stats)
    else:
        return logger.error("Statistics Do Not Exist"), 404

    return stats, 200



def populate_stats():
    """ Periodically update stats """
    logger.info("Start Periodic Processing")
    if os.path.isfile(app_config["datastore"]["filename"]):
        with open('data.json', 'r') as f:
            data_config = json.load(f)
        logger.info("number of score: {}".format(data_config['num_score_events']))
        logger.info("highest score: {}".format(data_config['highest_score']))
        logger.info("lowest score: {}".format(data_config['lowest_score']))
    else:
        with open('data.json', 'w', encoding='utf-8') as f:
            data_config = {
                "num_score_events": 0,
                "highest_score": 0,
                "lowest_score": 100,
                "num_in_events": 0,
                "last_updated": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            }
            json.dump(data_config, f, ensure_ascii=False, indent=4)
        logger.info("number of score: {}".format(data_config['num_score_events']))
        logger.info("highest score: {}".format(data_config['highest_score']))
        logger.info("lowest score: {}".format(data_config['lowest_score']))
    logger.info("Current datetime : {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    last_updated = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    if "last_updated" in data_config:
        last_updated = data_config["last_updated"]
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(data_config, f, ensure_ascii=False, indent=4)

    current_updated = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    instructor_res = requests.get(app_config["eventstore"][
                                      "url"] + "/account/instructor?start_timestamp=" + last_updated + "&end_timestamp=" + current_updated)
    student_res = requests.get(app_config["eventstore"][
                                   "url"] + "/account/student?start_timestamp=" + last_updated + "&end_timestamp=" + current_updated)

    if instructor_res.status_code == 200:
        logger.info("instructor event has been received successfully")
        data_config["num_in_events"] += len(instructor_res.json())
    else:
        logger.error("receiving event error")

    if student_res.status_code == 200:
        logger.info("student event has been received successfully")
        data_config["num_score_events"] += len(student_res.json())
        for i in student_res.json():
            if data_config["highest_score"] < i["final_grade"]:
                data_config["highest_score"] = i["final_grade"]

        for i in student_res.json():
            if data_config["lowest_score"] > i["final_grade"]:
                data_config["lowest_score"] = i["final_grade"]
    else:
        logger.error("receiving events failed")

    data_config["last_updated"] = current_updated
    f = open(app_config["datastore"]["filename"], "w")
    f.write(json.dumps(data_config, indent=4))
    f.close()
    logger.debug("processing has been done")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            strict_validation=True,
            validate_responses=True)
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'
if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, use_reloader=False)
