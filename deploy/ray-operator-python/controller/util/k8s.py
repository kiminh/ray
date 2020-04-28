from kubernetes import client, config

memory_multipliers = {
    "k": 1000,
    "M": 1000 ** 2,
    "G": 1000 ** 3,
    "T": 1000 ** 4,
    "P": 1000 ** 5,
    "E": 1000 ** 6,
    "Ki": 1024,
    "Mi": 1024 ** 2,
    "Gi": 1024 ** 3,
    "Ti": 1024 ** 4,
    "Pi": 1024 ** 5,
    "Ei": 1024 ** 6,
}


def _load_k8s_config():
    is_load = False
    for load_fn in [config.load_kube_config, config.load_incluster_config]:
        try:
            load_fn()
            is_load = True
            break
        except Exception:
            continue

    if not is_load:
        raise RuntimeError("Failed to load kubeconfig!")


def create_core_api_client():
    """Create core api client."""
    _load_k8s_config()
    return client.CoreV1Api()


def _create_custom_object_api_object():
    """Create custom object api client."""
    _load_k8s_config()
    return client.CustomObjectsApi()


def create_custom_object(group, version, namespace, plural, body):
    """Create a namespaced custom object."""
    api_object = _create_custom_object_api_object()
    try:
        return api_object.create_namespaced_custom_object(group, version, namespace, plural, body)
    except Exception as e:
        raise Exception("Exception when creating a custom object: %s\n" % e)


def delete_custom_object(group, version, namespace, plural, name, grace_period_seconds=5,
                         propagation_policy="Background"):
    """Delete a namespaced custom object."""
    api_object = _create_custom_object_api_object()
    delete_options = client.V1DeleteOptions(grace_period_seconds=grace_period_seconds,
                                            propagation_policy=propagation_policy)
    try:
        return api_object.delete_namespaced_custom_object(group, version, namespace, plural, name, delete_options)
    except Exception as e:
        raise Exception("Exception when deleting a custom object: %s\n" % e)


def list_custom_objects(group, version, namespace, plural, label_selector):
    """List namespaced custom object."""
    api_object = _create_custom_object_api_object()
    try:
        return api_object.list_namespaced_custom_object(group, version, namespace, plural,
                                                        label_selector=label_selector)
    except Exception as e:
        raise Exception("Exception when listing CRDs: %s\n" % e)


def get_custom_object(group, version, namespace, plural, name):
    """Get a namespaced custom object."""
    api_object = _create_custom_object_api_object()
    try:
        api_object.get_namespaced_custom_object(group, version, namespace, plural, name)
    except Exception as e:
        raise Exception("Exception when getting a custom object: %s\n" % e)


def get_pods(namespace, label_selector):
    """List namespaced pods using the given label_selector."""
    api_client = create_core_api_client()
    try:
        return api_client.list_namespaced_pod(namespace, label_selector=label_selector)
    except Exception as e:
        raise Exception("Exception when getting pods: %s\n" % e)
