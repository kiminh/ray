import copy

from controller.constants import raycluster_component, dash, raycluster_pod_type, head, raycluster_instance_name, dot


def new_pod(body, labels, pod_name, extension):
    name = body['metadata']['name']
    labels[raycluster_instance_name] = name
    labels[raycluster_component] = pod_name
    if extension['type'] == head:
        labels[raycluster_component] = name + dash + head
    labels[raycluster_pod_type] = extension['type']
    pod = {'apiVersion': 'v1', 'metadata': {'name': pod_name, 'labels': labels},
           'spec': {'containers': _create_container(body, extension, name)}}
    return pod


def _create_container(body, extension, name):
    containers = []
    container = {}
    image = body['spec']['images']['defaultImage']
    namespace = body['metadata']['namespace']
    container['image'] = image
    container['name'] = extension['type']
    container['env'] = extension['containerEnv']
    container['env'].append({'name': 'CLUSTER_NAME', 'value': name})
    container['env'].append({'name': 'NAMESPACE', 'value': namespace})
    container['env'].append({'name': 'RAY_HEAD_IP', 'value': name + dash + head + dot + extension['headServiceSuffix']})
    container['command'] = ["/bin/bash", "-c", "--"]
    container['args'] = [extension['command']]
    container['resources'] = extension['resources']
    container['ports'] = [{'name': 'redis', 'containerPort': 6379},
                          {'name': 'http-server', 'containerPort': 30021},
                          {'name': 'job-manager', 'containerPort': 30020}]
    containers.append(container)
    return containers


def build_pods(body):
    pods = []
    name = body['metadata']['name']
    extensions = body['spec']['extensions']
    for extension in extensions:
        replicas = extension['replicas']
        labels = extension['labels']
        for i in range(0, replicas):
            pod_name = name + dash + extension['groupName'] + dash + extension['type'] + dash + str(i)
            pod = new_pod(body, copy.deepcopy(labels), pod_name, extension)
            pods.append(pod)
    return pods
