#!/usr/bin/env python3
import logging

import kopf

from controller.constants import CRDRayCluster, raycluster_instance_name
from controller.sdk.pod import build_pods
from controller.sdk.service import new_service
from controller.util import k8s

logger = logging.getLogger(__name__)


@kopf.on.update(CRDRayCluster.GROUP, CRDRayCluster.VERSION, CRDRayCluster.PLURAL)
def update_fn(body, spec, **kwargs):
    # Object used to communicate with the API Server
    client = k8s.create_core_api_client()

    # Get info from RayCluster object
    name = body['metadata']['name']
    namespace = body['metadata']['namespace']

    pods = build_pods(body)
    desired_pods_name_list = []
    for pod in pods:
        kopf.adopt(pod, owner=body)
        desired_pods_name_list.append(pod['metadata']['name'])

    label_selector = "%s=%s" % (raycluster_instance_name, name)
    runtime_pods = k8s.get_pods(namespace, label_selector)
    runtime_pods_name_list = []
    for pod in runtime_pods.items:
        runtime_pods_name_list.append(pod.metadata.name)
    logger.info("Runtime Pod Name list:{}".format(runtime_pods_name_list))

    for pod in pods:
        if pod['metadata']['name'] not in runtime_pods_name_list:
            client.create_namespaced_pod(namespace=namespace, body=pod)

    pod_to_delete_name_list = list(set(runtime_pods_name_list) - set(desired_pods_name_list))
    logger.info("Pod to delete:{}".format(pod_to_delete_name_list))
    for pod_name in pod_to_delete_name_list:
        client.delete_namespaced_pod(name=pod_name, namespace=namespace)


@kopf.on.create(CRDRayCluster.GROUP, CRDRayCluster.VERSION, CRDRayCluster.PLURAL)
def create_fn(body, spec, **kwargs):
    # Object used to communicate with the API Server
    client = k8s.create_core_api_client()

    # Get info from RayCluster object
    name = body['metadata']['name']
    namespace = body['metadata']['namespace']

    pods = build_pods(body)
    for pod in pods:
        kopf.adopt(pod, owner=body)

    # Service template
    # Update templates based on Ray specification
    svc = new_service(body, name)

    # Make the Pod and Service the children of the Ray object
    kopf.adopt(svc, owner=body)

    # Create Service
    obj = client.create_namespaced_service(namespace=namespace, body=svc)
    logger.info("Service is created with msg:{}".format(obj))

    # Create Pod
    for pod in pods:
        obj = client.create_namespaced_pod(namespace=namespace, body=pod)
        logger.info("Pod is created in loop with msg:{}".format(obj))

    # Update status
    msg = f"Pod and Service created by Ray {name}"
    logger.info("Ray is created with msg:{}".format(msg))


@kopf.on.delete(CRDRayCluster.GROUP, CRDRayCluster.VERSION, CRDRayCluster.PLURAL)
def delete(body, **kwargs):
    msg = f"Ray {body['metadata']['name']} and its Pod / Service children deleted"
    logger.info("Delete is called with msg:{}".format(msg))
