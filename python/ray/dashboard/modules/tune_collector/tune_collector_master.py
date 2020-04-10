import asyncio
import copy
import datetime
import logging
import os

import aiohttp.web

try:
    from ray.tune.result import DEFAULT_RESULTS_DIR
    from ray.tune import Analysis
    from tensorboard import program
except ImportError:
    Analysis = None
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


@dashboard_utils.master(enable=Analysis is not None)
class TuneCollector:
    """Initialize collector worker thread.
    Args
        logdir (str): Directory path to save the status information of
                        jobs and trials.
        reload_interval (float): Interval(in s) of space between loading
                        data from logs
    """

    def __init__(self, redis_address, redis_password=None, logdir=DEFAULT_RESULTS_DIR, reload_interval=2.0):
        self._logdir = logdir
        self._trial_records = {}
        self._reload_interval = reload_interval
        self._available = False
        self._tensor_board_started = False

        os.makedirs(self._logdir, exist_ok=True)
        super().__init__()

    async def tune_info(self, req) -> aiohttp.web.Response:
        result = self._tune_info()
        return await dashboard_utils.json_response(result=result)

    def _tune_info(self):
        return {"trial_records": copy.deepcopy(self._trial_records)}

    async def tune_availability(self, req) -> aiohttp.web.Response:
        result = self._tune_availability()
        return await dashboard_utils.json_response(result=result)

    def _tune_availability(self):
        return {"available": self._available}

    async def run(self):
        while True:
            self.collect()
            await asyncio.sleep(self._reload_interval)

    def collect(self):
        """
        Collects and cleans data on the running Tune experiment from the
        Tune logs so that users can see this information in the front-end
        client
        """
        sub_dirs = os.listdir(self._logdir)
        job_names = filter(
                lambda d: os.path.isdir(os.path.join(self._logdir, d)), sub_dirs)

        self._trial_records = {}

        # search through all the sub_directories in log directory
        for job_name in job_names:
            analysis = Analysis(str(os.path.join(self._logdir, job_name)))
            df = analysis.dataframe()
            if len(df) == 0:
                continue

            # start TensorBoard server if not started yet
            if not self._tensor_board_started:
                tb = program.TensorBoard()
                tb.configure(argv=[None, "--logdir", self._logdir])
                tb.launch()
                self._tensor_board_started = True

            self._available = True

            # make sure that data will convert to JSON without error
            df["trial_id"] = df["trial_id"].astype(str)
            df = df.fillna(0)

            # convert df to python dict
            df = df.set_index("trial_id")
            trial_data = df.to_dict(orient="index")

            # clean data and update class attribute
            if len(trial_data) > 0:
                trial_data = self.clean_trials(trial_data, job_name)
                self._trial_records.update(trial_data)

    def clean_trials(self, trial_details, job_name):
        first_trial = trial_details[list(trial_details.keys())[0]]
        config_keys = []
        float_keys = []
        metric_keys = []

        # list of static attributes for trial
        default_names = [
            "logdir", "time_this_iter_s", "done", "episodes_total",
            "training_iteration", "timestamp", "timesteps_total",
            "experiment_id", "date", "timestamp", "time_total_s", "pid",
            "hostname", "node_ip", "time_since_restore",
            "timesteps_since_restore", "iterations_since_restore",
            "experiment_tag"
        ]

        # filter attributes into floats, metrics, and config variables
        for key, value in first_trial.items():
            if isinstance(value, float):
                float_keys.append(key)
            if str(key).startswith("config/"):
                config_keys.append(key)
            elif key not in default_names:
                metric_keys.append(key)

        # clean data into a form that front-end client can handle
        for trial, details in trial_details.items():
            ts = os.path.getctime(details["logdir"])
            formatted_time = datetime.datetime.fromtimestamp(ts).strftime(
                    "%Y-%m-%d %H:%M:%S")
            details["start_time"] = formatted_time
            details["params"] = {}
            details["metrics"] = {}

            # round all floats
            for key in float_keys:
                details[key] = round(details[key], 3)

            # group together config attributes
            for key in config_keys:
                new_name = key[7:]
                details["params"][new_name] = details[key]
                details.pop(key)

            # group together metric attributes
            for key in metric_keys:
                details["metrics"][key] = details[key]
                details.pop(key)

            if details["done"]:
                details["status"] = "TERMINATED"
            else:
                details["status"] = "RUNNING"
            details.pop("done")

            details["trial_id"] = trial
            details["job_id"] = job_name

        return trial_details
