import connexion
from connexion import NoContent

import requests
from datetime import datetime, timedelta
import yaml
import logging
import logging.config
import os
import json
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS, cross_origin
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


class Total:
    def __init__(self, total):
        self.total = total

    def add_to_total(self, sum):
        self.total = self.total + sum
        return self.total


total = Total(0)
stats = {}


def write_to_file(stats):
    with open(app_config['datastore']['filename'], "w") as f:
        json.dump(stats, f, indent=2)


with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)


def get_stats():
    """ Receives a order stats """
    logger.info("Starting request...")
    if (os.path.isfile(app_config['datastore']['filename'])):
        with open(app_config['datastore']['filename']) as f:
            data = f.read()
            py_data = json.loads(data)
            stats = py_data
    else:
        return "Statictics do not exist", 403

    logger.debug(stats)
    logger.info("Ending request...")

    return stats, 200


def populate_stats():
    """ Periodically update stats """
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Start Periodic Processing")
    logger.info("testing loggin")
    if (os.path.isfile(app_config['datastore']['filename'])):
        with open(app_config['datastore']['filename']) as f:
            data = f.read()
            py_data = json.loads(data)
            stats = py_data
    else:
        stats = {
            "num_delivery_orders": 0,
            "num_pickup_orders": 0,
            "max_total": 0,
            "num_drivers": 0,
            "last_updated": current_time,
        }

    # end_time = (datetime.now()+timedelta(seconds=5)
    #             ).strftime("%Y-%m-%d %H:%M:%S")
    print(stats)
    payload = {'orderTime': stats['last_updated'],
               'orderTimeEnd': current_time}
    d_url = f"{app_config['eventstore']['url']}/orders/delivery"
    response_delivery = requests.get(d_url, params=payload)

    if (response_delivery.status_code == 200):
        logger.info(
            f"number of deliveries: {len(response_delivery.json())} reponse 1 ")
    else:
        logger.info("400 Error from Get Delivery ")
        return 400

    delivery_data = response_delivery.json()

    p_url = f"{app_config['eventstore']['url']}/orders/pickup"
    response_pickup = requests.get(p_url, params=payload)

    if (response_pickup.status_code == 200):
        logger.info(f"number of pickups: {len(response_pickup.json())}")
    else:
        logger.info("400 Error from Get Pickup")
        return 400

    pickup_data = response_pickup.json()

    stats = {
        "num_delivery_orders": len(delivery_data) + stats['num_delivery_orders'],
        "num_pickup_orders": len(pickup_data) + stats['num_pickup_orders'],
        "max_total_delivery": total.add_to_total(sum([int(delivery["total"]) for delivery in delivery_data])),
        "num_drivers": list(set([delivery["deliveryOrderInfo"]["driverName"] for delivery in delivery_data])),
        "last_updated": current_time
    }
    write_to_file(stats)
    logger.debug(stats)

    logger.info("End Periodic Processing")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='')
if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    CORS(app.app)
    app.app.config['CORS_HEADERS'] = 'Content-Type'
app.add_api("openapi.yaml",
            base_path="/processing",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, use_reloader=False)
