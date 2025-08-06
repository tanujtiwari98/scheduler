from dataclasses import dataclass
from typing import List, Set
from kubernetes import client
from pod_utils import is_system_namespace, is_daemonset_pod


@dataclass
class NodeStatus:
    name: str
    is_free: bool


class NodeDiscoverer:

    def __init__(self, v1: client.CoreV1Api):
        self.v1 = v1

    def get_nodes_with_status(self):

        all_nodes = self._list_nodes()
        used_nodes = self._nodes_with_active_pods()

        return [
            NodeStatus(name=n.metadata.name, is_free=(n.metadata.name not in used_nodes))
            for n in all_nodes
        ]


    def get_free_nodes(self):
        free = [node for node in self.get_nodes_with_status() if node.is_free]
        return free

    def count_free_nodes(self):
        return sum(1 for ns in self.get_nodes_with_status() if ns.is_free)

    def _list_nodes(self):
        return self.v1.list_node().items

    def _nodes_with_active_pods(self):
        pods = self.v1.list_pod_for_all_namespaces().items
        used = set()
        for p in pods:
            if not p.spec or not p.status:
                continue
            if is_system_namespace(p):
                continue
            phase = p.status.phase
            if phase not in ("Running", "Pending"):
                continue
            if is_daemonset_pod(p):
                continue
            if p.spec.node_name:
                used.add(p.spec.node_name)

        return used

