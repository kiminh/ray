from collections import defaultdict

# {ip address(str): raylet stats(dict)}
raylet_stats = {}

# {hostname(str): node stats(dict)}
node_stats = {}

# Mapping from IP address to PID to list of log lines
logs = defaultdict(lambda: defaultdict(list))

# Mapping from IP address to PID to list of error messages
errors = defaultdict(lambda: defaultdict(list))

# Mapping from actor id to actor table data
actors = {}
