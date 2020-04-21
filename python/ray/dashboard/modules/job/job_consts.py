# 10MB
DOWNLOAD_RESOURCE_BUFFER_SIZE = 10 * 1024 * 1024
# 100 seconds
DOWNLOAD_TIMEOUT = 100
DOWNLOAD_FILE = "/tmp/ray/{job_id}/package.zip"
DOWNLOAD_FILE_UNZIP_DIR = "/tmp/ray/{job_id}/package"
PYTHON_PIP_CACHE = "/tmp/ray/pipcache"
PYTHON_PACKAGE_INDEX = "https://pypi.antfin-inc.com/simple/"
PYTHON_VIRTUAL_ENV_DIR = "/tmp/ray/{job_id}/pyenv"
PYTHON_REQUIREMENTS_FILE = "/tmp/ray/{job_id}/requirements.txt"

JOB_TABLE_NAME = "job table"
