import copy
import asyncio
import datetime
from queue import Queue
from collections import defaultdict
from collections.abc import MutableMapping
from aiohttp.signals import Signal


# TODO(fyrestone): use dataclass instead.
class Change:
    def __init__(self, owner=None, old=None, new=None):
        self.owner = owner
        self.old = old
        self.new = new


class NotifyQueue:
    _queue = asyncio.Queue()
    _signals = []

    @classmethod
    def register_signal(cls, sig):
        cls._signals.append(sig)

    @classmethod
    def freeze_signal(cls):
        for sig in cls._signals:
            sig.freeze()

    @classmethod
    def put(cls, co):
        cls._queue.put_nowait(co)

    @classmethod
    async def get(cls):
        return await cls._queue.get()


class Dict(MutableMapping):
    def __init__(self, *args, **kwargs):
        self._data = dict(*args, **kwargs)
        self._queue = Queue()
        self.signal = Signal(self)
        NotifyQueue.register_signal(self.signal)

    def __setitem__(self, key, value):
        old = self._data.pop(key, None)
        self._data[key] = value
        co = self.signal.send(Change(owner=self, old=old, new={key: value}))
        NotifyQueue.put(co)

    def __getitem__(self, item):
        return copy.deepcopy(self._data[item])

    def __delitem__(self, key):
        old = self._data.pop(key, None)
        del self._data[key]
        co = self.signal.send(Change(owner=self, old=old))
        NotifyQueue.put(co)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(copy.deepcopy(self._data))


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

agents = Dict()


# TODO(fyrestone): To be organized.
def calculate_log_counts():
    return {
        ip: {
            pid: len(logs_for_pid)
            for pid, logs_for_pid in logs_for_ip.items()
        }
        for ip, logs_for_ip in logs.items()
    }


def calculate_error_counts():
    return {
        ip: {
            pid: len(errors_for_pid)
            for pid, errors_for_pid in errors_for_ip.items()
        }
        for ip, errors_for_ip in errors.items()
    }


# TODO(fyrestone): Add to get node_stats data logic.
def purge_outdated_stats():
    def current(then, now):
        if (now - then) > 5:
            return False

        return True

    dt = datetime.datetime.utcnow()
    now = (dt - datetime.datetime(1970, 1, 1)).total_seconds()
    purged_node_stats = {
        k: v
        for k, v in node_stats.items() if current(v["now"], now)
    }
    node_stats.clear()
    node_stats.update(purged_node_stats)
