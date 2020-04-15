import asyncio
import logging
import inspect
import ray.utils as ray_utils
import ray.operation.modules.utils as module_utils

logger = logging.getLogger(__name__)


class OperationMaster:
    def __init__(self, redis_address, redis_password):
        self.redis_address = redis_address
        self.redis_password = redis_password
        self._modules, self._methods = self._load_modules()

    def _load_modules(self):
        """Load operation master modules."""
        agent_cls_list = module_utils.get_all_modules(module_utils.TYPE_MASTER)
        modules = []
        methods = {}
        for cls in agent_cls_list:
            logger.info("Load {} module: {}", module_utils.TYPE_MASTER, cls)
            c = cls(redis_address=self.redis_address,
                    redis_password=self.redis_password)
            modules.append(c)
            module_methods = inspect.getmembers(
                    c, predicate=ray_utils.is_function_or_method)
            for method_name, method in module_methods:
                if method_name in methods:
                    raise Exception("Conflict method {}".format(method_name))
                else:
                    methods[method_name] = method
        return modules, methods

    def __getattr__(self, item):
        try:
            return self._methods[item]
        except KeyError:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                    type(self).__name__, item))

    async def run(self):
        await asyncio.gather(*(m.run() for m in self._modules))
