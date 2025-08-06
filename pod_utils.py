from typing import Set
from kubernetes import client

SYSTEM_NAMESPACES = {"kube-system", "kube-public", "kube-node-lease"}


def is_system_namespace(pod):
    return (pod.metadata.namespace or "") in SYSTEM_NAMESPACES


def is_daemonset_pod(pod):
    owners = pod.metadata.owner_references or []
    return any(owner.kind == "DaemonSet" for owner in owners)



def is_terminating(pod):
    return pod.metadata.deletion_timestamp is not None


def is_terminated_phase(phase):
    return phase in ("Succeeded", "Failed")



def should_skip_pod_for_scheduling(pod):
    if is_system_namespace(pod):
        return True
    if is_daemonset_pod(pod):
        return True

    return False