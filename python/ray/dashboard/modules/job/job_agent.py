import asyncio
import logging
import os.path
import sys
from abc import abstractmethod

import aiohttp
import aioredis
import async_timeout

import ray
import ray.dashboard.modules.job.job_consts as job_consts
import ray.dashboard.modules.job.job_updater as job_updater
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import job_pb2
from ray.core.generated import job_pb2_grpc
from ray.experimental import set_resource
from ray.gcs_utils import JOB_RESOURCE_PREFIX
from ray.utils import hex_to_binary

logger = logging.getLogger(__name__)
DEBUG = False


class JobInfo:
    def __init__(self, job_info):
        self.job_info = job_info

    def url(self):
        return self.job_info["url"]

    def job_id(self):
        return self.job_info["id"]

    def driver_entry(self):
        return self.job_info["driver_entry"]

    def python_requirements_file(self):
        requirements = self.job_info.get("dependencies", {}).get("python", [])
        if not requirements:
            return None
        filename = job_consts.PYTHON_REQUIREMENTS_FILE.format(job_id=self.job_id())
        with open(filename, "w") as fp:
            fp.writelines(r.strip() + os.linesep for r in requirements)
        return filename


class JobProcessor:
    def __init__(self, job_info):
        self.job_info = job_info

    @staticmethod
    async def _run_cmd(cmd):
        proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)

        logger.info("Run cmd {}".format(repr(cmd)))
        stdout, stderr = await proc.communicate()
        if stdout:
            logger.info(stdout.decode("utf-8"))
        if stderr:
            logger.error(stderr.decode("utf-8"))
        if proc.returncode != 0:
            raise Exception("Run cmd {} exit with {}".format(repr(cmd), proc.returncode))

    @staticmethod
    def _get_current_python():
        return sys.executable

    @staticmethod
    def _get_virtualenv_python(virtualenv_path):
        return os.path.join(virtualenv_path, "bin/python")

    @abstractmethod
    async def run(self):
        pass


class DownloadPackage(JobProcessor):
    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self.http_session = http_session

    async def _download_package(self, url, filename):
        with async_timeout.timeout(job_consts.DOWNLOAD_TIMEOUT_SECONDS):
            async with self.http_session.get(url) as response:
                logger.info("Download %s to %s", url, filename)
                with open(filename, 'wb') as f:
                    while True:
                        chunk = await response.content.read(
                                job_consts.DOWNLOAD_RESOURCE_BUFFER_SIZE)
                        if not chunk:
                            break
                        f.write(chunk)

    async def _validate_zip_package(self, filename):
        validate_zip_cmd = "zip -T {}".format(filename)
        await self._run_cmd(validate_zip_cmd)

    async def _unzip_package(self, filename, path):
        unzip_cmd = "unzip -o -d {} {}".format(path, filename)
        await self._run_cmd(unzip_cmd)

    async def run(self):
        url = self.job_info.url()
        job_id = self.job_info.job_id()
        filename = job_consts.DOWNLOAD_PACKAGE.format(job_id=job_id)
        unzip_dir = job_consts.DOWNLOAD_PACKAGE_UNZIP_DIR.format(job_id=job_id)
        await self._download_package(url, filename)
        await self._unzip_package(filename, unzip_dir)


class PreparePythonEnviron(JobProcessor):
    async def _create_virtualenv(self, path):
        python = self._get_current_python()
        create_venv_cmd = "{} -m virtualenv --system-site-packages --no-download {}".format(python, path)
        await self._run_cmd(create_venv_cmd)

    async def _install_python_requirements(self, path, requirements_file):
        python = self._get_virtualenv_python(path)
        pypi = ""
        if job_consts.PYTHON_PACKAGE_INDEX:
            pypi = " -i {}".format(job_consts.PYTHON_PACKAGE_INDEX)
        pip_download_cmd = "{} -m pip download --destination-directory {}{} -r {}".format(
                python, job_consts.PYTHON_PIP_CACHE, pypi, requirements_file)
        pip_install_cmd = "{} -m pip install --no-index --find-links={}{} -r {}".format(
                python, job_consts.PYTHON_PIP_CACHE, pypi, requirements_file)
        await self._run_cmd(pip_download_cmd)
        await self._run_cmd(pip_install_cmd)

    async def run(self):
        job_id = self.job_info.job_id()
        requirements_file = self.job_info.python_requirements_file()
        virtualenv_path = job_consts.PYTHON_VIRTUAL_ENV_DIR.format(job_id=job_id)
        await self._create_virtualenv(virtualenv_path)
        if requirements_file:
            await self._install_python_requirements(virtualenv_path, requirements_file)


class StartPythonDriver(JobProcessor):
    _template = '''import sys
sys.path.append({import_path})
import ray
from ray.utils import hex_to_binary
ray.init(ignore_reinit_error=True,
         address={redis_address},
         redis_password={redis_password},
         load_code_from_local=True,
         job_id=ray.JobID({job_id}))
import {driver_entry}
{driver_entry}.main()
'''

    def __init__(self, job_info, redis_address, redis_password):
        super().__init__(job_info)
        self.redis_address = redis_address
        self.redis_password = redis_password

    def _gen_driver_code(self):
        job_id = self.job_info.job_id()
        package_dir = job_consts.DOWNLOAD_PACKAGE_UNZIP_DIR.format(job_id=job_id)
        driver_entry_file = job_consts.JOB_DRIVER_ENTRY_FILE.format(job_id=job_id)
        ip, port = self.redis_address
        driver_code = self._template.format(job_id=repr(hex_to_binary(job_id)),
                                            import_path=repr(package_dir),
                                            redis_address=repr(ip + ":" + str(port)),
                                            redis_password=repr(self.redis_password),
                                            driver_entry=self.job_info.driver_entry())
        with open(driver_entry_file, "w") as fp:
            fp.write(driver_code)
        return driver_entry_file

    @staticmethod
    async def _start_driver(cmd, stdout, stderr):
        proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=stdout,
                stderr=stderr)
        logger.info("Start driver cmd {} with pid {}".format(repr(cmd), proc.pid))

    async def run(self):
        job_id = self.job_info.job_id()
        virtualenv_path = job_consts.PYTHON_VIRTUAL_ENV_DIR.format(job_id=job_id)
        python = self._get_virtualenv_python(virtualenv_path)
        driver_file = self._gen_driver_code()
        driver_cmd = "{} -u {}".format(python, driver_file)
        stdout_file, stderr_file = ray.worker._global_node.new_log_files("driver_{}".format(job_id))
        await self._start_driver(driver_cmd, stdout_file, stderr_file)


@dashboard_utils.agent
class JobAgent(job_pb2_grpc.JobServiceServicer):
    def __init__(self, dashboard_agent):
        ip, port = dashboard_agent.redis_address
        ray.init(ignore_reinit_error=True,
                 log_to_driver=False,
                 address=ip + ":" + str(port),
                 redis_password=dashboard_agent.redis_password)
        loop = asyncio.get_event_loop()
        self.dashboard_agent = dashboard_agent
        self.http_session = aiohttp.ClientSession(loop=loop)
        self.aioredis_client = None
        self.job_queue = asyncio.Queue()
        self.submitted_jobs = set()
        self.failed_jobs = set()
        self.job_table = {}
        self.connected = False

    async def _prepare_job_environ(self, job_info):
        os.makedirs(job_consts.JOB_DIR.format(job_id=job_info.job_id()), exist_ok=True)
        for i in range(job_consts.JOB_RETRY_TIMES):
            try:
                concurrent_tasks = [DownloadPackage(job_info, self.http_session).run(),
                                    PreparePythonEnviron(job_info).run()]
                await asyncio.gather(*concurrent_tasks)
                break
            except Exception as ex:
                logger.exception(ex)
                await asyncio.sleep(job_consts.JOB_RETRY_INTERVAL_SECONDS)
        else:
            logger.error("Prepare job %s environment failed.", job_info.job_id())
            self.failed_jobs.add(job_info.job_id())
            return
        set_resource(JOB_RESOURCE_PREFIX + job_info.job_id().upper(), 1000)

    async def _load_all_jobs_from_store(self):
        all_job_ids = await job_updater.get_all_job_ids(self.aioredis_client)
        logger.info("Put %s jobs to queue.", len(all_job_ids))
        for job_id in all_job_ids:
            request = job_pb2.DispatchJobInfoRequest(job_id=job_id, start_driver=True)
            self._submit_job(request)

    def _submit_job(self, request):
        if request.job_id not in self.submitted_jobs:
            self.submitted_jobs.add(request.job_id)
            self.job_queue.put_nowait(request)

    async def DispatchJobInfo(self, request, context):
        if not self.connected:
            self.connected = True
            await self._load_all_jobs_from_store()
        self._submit_job(request)
        return job_pb2.DispatchJobInfoReply(
                job_id=request.job_id,
                status=job_pb2.JobStatus.OK)

    async def run(self, server):
        job_pb2_grpc.add_JobServiceServicer_to_server(
                self, server)
        self.aioredis_client = await aioredis.create_redis(
                address=self.dashboard_agent.redis_address,
                password=self.dashboard_agent.redis_password)

        if DEBUG:
            job_id = ray.JobID.from_int(
                    int(self.dashboard_agent.redis_client.incr("JobCounter")))
            test_job = {
                'id': job_id.hex(),
                'name': 'rayag_darknet',
                'owner': 'abc.xyz',
                'language': 'java',
                'url': 'http://arcos.oss-cn-hangzhou-zmf.aliyuncs.com/po/simple_job.zip',
                'driver_entry': 'simple_job',
                'driver_args': ['arg1', 'arg2'],
                'custom_config': {'k1': 'v1', 'k2': 'v2'},
                'jvm_options': '-Dabc=123 -Daaa=xxx',
                'dependencies': {
                    'python': ['aiohttp', 'click', 'colorama', 'filelock', 'google', 'grpcio', 'jsonschema',
                               'msgpack >= 0.6.0, < 1.0.0', 'numpy >= 1.16', 'protobuf >= 3.8.0', 'py-spy >= 0.2.0',
                               'pyyaml', 'redis >= 3.3.2']
                }
            }

            await job_updater.submit_job(self.aioredis_client, test_job)

        await self._load_all_jobs_from_store()
        while True:
            request = await self.job_queue.get()
            job_id = request.job_id
            job_info = await job_updater.get_job(self.aioredis_client, job_id)
            job_info = JobInfo(job_info)
            self.job_table[job_id] = job_info
            await self._prepare_job_environ(job_info)
            if request.start_driver:
                await StartPythonDriver(job_info,
                                        self.dashboard_agent.redis_address,
                                        self.dashboard_agent.redis_password).run()