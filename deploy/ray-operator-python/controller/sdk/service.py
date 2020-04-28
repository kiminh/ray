from controller import constants
from controller.constants import head, dash


def new_service(body, name):
    # Get namespace from body
    namespace = body['metadata']['namespace']

    service = {'apiVersion': 'v1',
               'metadata': {'name': name + dash + head, 'namespace': namespace},
               'spec': {
                   'selector': {constants.raycluster_component: name + dash + head},
                   'type': 'ClusterIP',
                   'clusterIP': 'None'}
               }

    return service
