import os

# Job constants
JOB_INFO_TABLE_NAME = "JOB_INFO_TABLE"
JOB_STATUS_TABLE_NAME = "JOB_STATUS_TABLE"
JOB_RETRY_INTERVAL_SECONDS = 10
JOB_RETRY_TIMES = 10
JOB_DIR = "/tmp/ray/job/{job_id}/"
JOB_DRIVER_ENTRY_FILE = os.path.join(JOB_DIR, "driver.py")
# Downloader constants
DOWNLOAD_RESOURCE_BUFFER_SIZE = 10 * 1024 * 1024  # 10MB
DOWNLOAD_TIMEOUT_SECONDS = 100  # 100 seconds
DOWNLOAD_PACKAGE = os.path.join(JOB_DIR, "package.zip")
DOWNLOAD_PACKAGE_UNZIP_DIR = os.path.join(JOB_DIR, "package")
# Python package constants
PYTHON_PIP_CACHE = "/tmp/ray/pipcache"
PYTHON_PACKAGE_INDEX = "https://pypi.antfin-inc.com/simple/"
PYTHON_VIRTUAL_ENV_DIR = os.path.join(JOB_DIR, "pyenv")
PYTHON_REQUIREMENTS_FILE = os.path.join(JOB_DIR, "requirements.txt")
