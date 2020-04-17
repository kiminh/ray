import logging

import aiohttp.web

try:
    from ray.tune.result import DEFAULT_RESULTS_DIR
    from ray.tune import Analysis
    from tensorboard import program
except ImportError:
    Analysis = None
from ray.dashboard.modules.metrics_exporter.client import Exporter
from ray.dashboard.modules.metrics_exporter.client import MetricsExportClient
import ray.dashboard.utils as dashboard_utils
import ray.services
import ray.utils

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


# @dashboard_utils.master
class MetricsExporter:
    def __init__(self, redis_address, redis_password):
        pass

    def _setup_metrics_export(self):
        exporter = Exporter(self.dashboard_id, self.metrics_export_address,
                            self.dashboard_controller)
        self.metrics_export_client = MetricsExportClient(
                self.metrics_export_address, self.dashboard_controller,
                self.dashboard_id, exporter)

    def _start_exporting_metrics(self):
        result, error = self.metrics_export_client.start_exporting_metrics()
        if not result and error:
            url = ray.services.get_webui_url_from_redis(self.redis_client)
            error += (" Please reenable the metrics export by going to "
                      "the url: {}/api/metrics/enable".format(url))
            ray.utils.push_error_to_driver_through_redis(
                    self.redis_client, "metrics export failed", error)

    @routes.get("/api/metrics/enable")
    async def enable_export_metrics(self, req) -> aiohttp.web.Response:
        if self.metrics_export_client.enabled:
            return await dashboard_utils.json_response(
                    result={"url": None}, error="Already enabled")

        succeed, error = self.metrics_export_client.start_exporting_metrics()
        error_msg = "Failed to enable it. Error: {}".format(error)
        if not succeed:
            return await dashboard_utils.json_response(
                    result={"url": None}, error=error_msg)

        url = self.metrics_export_client.dashboard_url
        return await dashboard_utils.json_response(result={"url": url})

    @routes.get("/api/metrics/url")
    async def get_dashboard_address(self, req) -> aiohttp.web.Response:
        if not self.metrics_export_client.enabled:
            return await dashboard_utils.json_response(
                    result={"url": None},
                    error="Metrics exporting is not enabled.")

        url = self.metrics_export_client.dashboard_url
        return await dashboard_utils.json_response(result={"url": url})

    @routes.get("/metrics/redirect")
    async def redirect_to_dashboard(self, req) -> aiohttp.web.Response:
        if not self.metrics_export_client.enabled:
            return await dashboard_utils.json_response(
                    result={"url": None},
                    error="You should enable metrics export to use this endpoint.")

        raise aiohttp.web.HTTPFound(self.metrics_export_client.dashboard_url)

    async def run(self):
        pass
