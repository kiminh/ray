

class JobInfo:
    def __init__(self):
        self.id = None
        self.name = None
        self.owner = None
        self.language = None
        self.url = None
        self.driver_entry = None
        self.driver_args = []
        self.custom_config = {}
        self.jvm_options = None
        self.dependencies = {}

        # Fields below are execution fields.
        self.state = None
        self.driver_started = False
        self.assigned_node_name = None
        self.submitted_time = -1
        self.started_time = -1
        self.end_time = -1

    def to_dict(self):
        data = dict()
        data["id"] = self.id
        data["name"] = self.name
        data["owner"] = self.owner
        data["language"] = self.language
        data["url"] = self.url
        data["driver_entry"] = self.driver_entry
        data["driver_args"] = self.driver_args
        data["custom_config"] = self.custom_config
        data["jvm_options"] = self.jvm_options
        data["dependencies"] = self.dependencies
        data["state"] = self.state
        data["driver_started"] = self.driver_started
        data["assigned_node_name"] = self.assigned_node_name
        data["submitted_time"] = self.submitted_time
        data["started_time"] = self.started_time
        data["end_time"] = self.end_time
        return data

    @staticmethod
    def from_request(query):
        """Parse the job info from the given request."""
        job_info = JobInfo()
        job_info.name = query.get("name", "")
        job_info.owner = query.get("owner", "")
        job_info.language = query.get("language")
        job_info.url = query.get("url")
        job_info.driver_entry = query.get("driver_entry")
        job_info.driver_args = query.get("driver_args")
        job_info.custom_config = query.get("custom_config")
        job_info.jvm_options = query.get("custom_config", "")
        job_info.dependencies = query.get("dependencies")

        return job_info
