# Downloader constants
DOWNLOAD_RESOURCE_BUFFER_SIZE = 10 * 1024 * 1024  # 10MB
DOWNLOAD_TIMEOUT_SECONDS = 100  # 100 seconds
DOWNLOAD_PACKAGE = "/tmp/ray/{job_id}/package.zip"
DOWNLOAD_PACKAGE_UNZIP_DIR = "/tmp/ray/{job_id}/package"
# Python package constants
PYTHON_PIP_CACHE = "/tmp/ray/pipcache"
PYTHON_PACKAGE_INDEX = "https://pypi.antfin-inc.com/simple/"
PYTHON_VIRTUAL_ENV_DIR = "/tmp/ray/{job_id}/pyenv"
PYTHON_REQUIREMENTS_FILE = "/tmp/ray/{job_id}/requirements.txt"
# Job constants
JOB_TABLE_NAME = "job table"
JOB_RETRY_INTERVAL_SECONDS = 10
JOB_RETRY_TIMES = 10
JOB_DRIVER_ENTRY_FILE = "/tmp/ray{job_id}/driver.py"
