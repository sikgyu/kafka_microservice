from pydantic import BaseModel
from dependency_injector import containers, providers
from applauncher.applauncher import Configuration
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import signal
from multiprocessing import Lock

logger = logging.getLogger("apscheduler-bundle")


class APSchedulerConfig(BaseModel):
    jobstores: dict = {}
    executors: dict = {}
    coalesce: bool = False
    max_instances: int = 3
    timezone: str = "UTC"

def build_apscheduler_config(config):
    apsconfig = {}
    for prop in ["jobstores", "executors"]:
        if isinstance(getattr(config, prop), dict) > 0:
            for prop_name, prop_config in getattr(config, prop).items():
                apsconfig[".".join(['apscheduler', prop, prop_name])] = prop_config
    apsconfig["apscheduler.job_defaults.coalesce"] = config.coalesce
    apsconfig["apscheduler.job_defaults.max_instances"] = config.max_instances
    apsconfig["apscheduler.timezone"] = config.timezone
    return apsconfig


def build_scheduler(scheduler_class, configuration):
    return scheduler_class(build_apscheduler_config(configuration.apscheduler))


def scheduler_resource(scheduler):
    logging.info("Starting scheduler")
    scheduler.start()
    yield scheduler
    logger.info("Shutting down scheduler")
    scheduler.shutdown()


class APSchedulerContainer(containers.DeclarativeContainer):
    config = providers.Dependency(instance_of=APSchedulerConfig)
    configuration = Configuration()

    background_scheduler = providers.Singleton(
        providers.Callable(
            build_scheduler,
            scheduler_class=BackgroundScheduler,
            configuration=configuration
        )
    )

    background_scheduler_resource = providers.Resource(
        scheduler_resource,
        scheduler=background_scheduler
    )


class APSchedulerBundle:
    def __init__(self):
        self.config_mapping = {
            "apscheduler": APSchedulerConfig
        }

        self.injection_bindings = {
            "apscheduler": APSchedulerContainer
        }

        self.lock = Lock()
        self.services = [
            ("apscheduler-lock", self.lock_function, [self.lock], {})
        ]

    def lock_function(self, lock):
        lock.acquire()
        signal.signal(signal.SIGINT, lambda _os_signal, _frame: lock.release())
        signal.signal(signal.SIGTERM, lambda _os_signal, _frame: lock.release())
        lock.acquire()
