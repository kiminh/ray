import asyncio
import json
import logging
import os.path
import sys
from abc import abstractmethod

import aiohttp
import aioredis
import async_timeout

import ray.dashboard.modules.job.job_consts as job_consts
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import job_pb2
from ray.core.generated import job_pb2_grpc
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
        python = self._get_python()
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

    @staticmethod
    def _get_python():
        return sys.executable

    @staticmethod
    def _get_virtualenv_python(path):
        return os.path.join(path, "bin/python")

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
         redis_address={redis_address},
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
        driver_code = self._template.format(job_id=repr(hex_to_binary(job_id)),
                                            import_path=repr(package_dir),
                                            redis_address=repr(self.redis_address),
                                            redis_password=repr(self.redis_password),
                                            driver_entry=self.job_info.driver_entry())
        with open(driver_entry_file, "w") as fp:
            fp.write(driver_code)

    async def run(self):
        self._gen_driver_code()


class JobAgentServer(job_pb2_grpc.JobServiceServicer):
    def __init__(self, dashboard_agent):
        loop = asyncio.get_event_loop()
        self.dashboard_agent = dashboard_agent
        self.http_session = aiohttp.ClientSession(loop=loop)
        self.job_queue = asyncio.Queue()
        self.submitted_jobs = set()
        self.failed_jobs = set()
        self.job_table = {}

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

    async def run(self):
        aioredis_client = await aioredis.create_redis(
                address=self.dashboard_agent.redis_address,
                password=self.dashboard_agent.redis_password)
        while True:
            request = await self.job_queue.get()
            job_id = request.job_id
            job_info = await aioredis_client.hget(job_consts.JOB_INFO_TABLE_NAME, job_id)
            job_info = JobInfo(json.loads(job_info))
            self.job_table[job_id] = job_info
            await self._prepare_job_environ(job_info)
            await StartPythonDriver(job_info,
                                    self.dashboard_agent.redis_address,
                                    self.dashboard_agent.redis_password).run()

    async def DispatchJobInfo(self, request, context):
        return job_pb2.DispatchJobInfoReply(
                job_id=request.job_id,
                status=job_pb2.JobStatus.OK)


@dashboard_utils.agent
class JobAgent:
    def __init__(self, dashboard_agent):
        """Initialize the JobAgent object."""
        self.dashboard_agent = dashboard_agent
        self.job_agent_server = JobAgentServer(dashboard_agent)

    async def run(self, server):
        job_pb2_grpc.add_JobServiceServicer_to_server(
                self.job_agent_server, server)

        if DEBUG:
            test_job = {
                'id': '0300',
                'name': 'rayag_darknet',
                'owner': 'abc.xyz',
                'language': 'java',
                'url': 'http://arcos.oss-cn-hangzhou-zmf.aliyuncs.com/tensorcache/tensorcache_service_app-0.4.9-py36-test.zip',
                'driver_entry': 'com.alipay.argh.Xxx',
                'driver_args': ['arg1', 'arg2'],
                'custom_config': {'k1': 'v1', 'k2': 'v2'},
                'jvm_options': '-Dabc=123 -Daaa=xxx',
                'dependencies': {
                    'python': ['aiohttp', 'click', 'colorama', 'filelock', 'google', 'grpcio', 'jsonschema',
                               'msgpack >= 0.6.0, < 1.0.0', 'numpy >= 1.16', 'protobuf >= 3.8.0', 'py-spy >= 0.2.0',
                               'pyyaml', 'redis >= 3.3.2']
                }
            }
            self.dashboard_agent.redis_client.hset(job_consts.JOB_INFO_TABLE_NAME, test_job["id"], json.dumps(test_job))
            self.job_agent_server.job_queue.put_nowait(
                    job_pb2.DispatchJobInfoRequest(job_id=test_job["id"], start_driver=False))
            await self.job_agent_server.run()
