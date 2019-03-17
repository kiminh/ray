import json
import pytest

import ray
from ray.tests.cluster_utils import Cluster


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def get_default_fixure_config():
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
    })
    return internal_config


def ray_start_with_parameter(request):
    internal_config = get_default_fixure_config()
    init_kargs = {
        "num_cpus": 1,
        "_internal_config": internal_config,
    }
    parameter = getattr(request, "param", {})
    init_kargs.update(parameter)
    # Start the Ray processes.
    address_info = ray.init(**init_kargs)
    return address_info


@pytest.fixture
def ray_start_regular(request):
    address_info = ray_start_with_parameter(request)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_2_cpus(request):
    parameter = getattr(request, "param", {})
    parameter['num_cpus'] = 2
    request.param = parameter
    address_info = ray_start_with_parameter(request)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def ray_start_cluster_with_parameter(request):
    internal_config = get_default_fixure_config()
    init_kargs = {
        "num_cpus": 1,
        "_internal_config": internal_config,
    }
    parameter = getattr(request, "param", {})
    if "num_nodes" in parameter:
        num_nodes = parameter["num_nodes"]
        del parameter["num_nodes"]
    else:
        num_nodes = 0
    initialize_head = False
    if "initialize_head" in parameter:
        initialize_head = parameter["initialize_head"]
        del parameter["initialize_head"]
    elif num_nodes > 0:
        initialize_head = True
    init_kargs.update(parameter)
    cluster = Cluster(
        initialize_head=initialize_head,
        connect=initialize_head,
        head_node_args=init_kargs)
    remote_node = []
    for _ in range(num_nodes):
        remote_node.append(cluster.add_node(**init_kargs))
    if num_nodes > 0:
        cluster.wait_for_nodes()
    return cluster, remote_node


@pytest.fixture
def ray_start_cluster(request):
    cluster, remote_node = ray_start_cluster_with_parameter(request)
    yield (cluster, remote_node)
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_start_cluster_init(request):
    parameter = getattr(request, "param", {})
    parameter['initialize_head'] = True
    request.param = parameter
    cluster, remote_node = ray_start_cluster_with_parameter(request)
    yield cluster, remote_node
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()
