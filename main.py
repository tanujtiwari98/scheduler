import random
import json
from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException

from gang import PodGroupDiscoverer, GroupSelector
from node import NodeDiscoverer

class SchedulingError(Exception):
    pass

class NoNodesAvailableError(SchedulingError):
    pass

class InsufficientResourcesError(SchedulingError):
    pass


class Scheduler:
    def __init__(self, scheduler_name="foobar"):
        self.scheduler_name = scheduler_name
        self._load_config()
        self.v1 = client.CoreV1Api()
        self.watcher = watch.Watch()
        self.node_discovery = NodeDiscoverer(v1=self.v1)
        self.gang_manager = PodGroupDiscoverer(v1=self.v1)

    def _load_config(self):
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()

    def _get_group_id(self, pod):
        return (pod.metadata.annotations or {}).get("pod-group", "")

    def _preempt_for_group(self, group_id: str):
        current_group = self.gang_manager.get_group(group_id)
        if not current_group:
            raise InsufficientResourcesError(f"Group {group_id} not found")

        min_size = current_group.size
        groups = self.gang_manager.groups(GroupSelector(
            max_priority=current_group.priority - 1,
        ))
        groups = [group for group in groups if group.gang_id != group_id]

        available_capacity = sum(group.size for group in groups)
        if available_capacity < min_size:
            raise InsufficientResourcesError("Insufficient preemptible pods available")

        preempted = 0
        for group in groups:
            if group.gang_id == group_id:
                continue

            local_p = self.gang_manager.preempt_group(group.gang_id)
            if local_p != group.size:
                raise Exception('preempted partial group')

            preempted += group.size
            if preempted >= min_size:
                break

    def _select_node(self):
        free_nodes = self.node_discovery.get_free_nodes()
        if not free_nodes:
            raise NoNodesAvailableError("No available nodes")
        return random.choice(free_nodes).name

    def _bind_pod(self, pod_name, node_name, namespace):
        target = client.V1ObjectReference(api_version="v1", kind="Node", name=node_name)
        meta = client.V1ObjectMeta(name=pod_name, namespace=namespace)
        body = client.V1Binding(metadata=meta, target=target)

        if hasattr(self.v1, "create_namespaced_pod_binding"):
            self.v1.create_namespaced_pod_binding(name=pod_name, namespace=namespace, body=body)
        else:
            self.v1.create_namespaced_binding(namespace=namespace, body=body)


    def _is_schedulable(self, pod, event_type):
        return (event_type in ("ADDED", "MODIFIED") 
                and pod.status.phase == "Pending" 
                and pod.spec 
                and pod.spec.scheduler_name == self.scheduler_name 
                and not pod.spec.node_name)

    def _schedule_pod(self, pod):
        pod_name = pod.metadata.name
        namespace = pod.metadata.namespace or "default"

        try:
            node_name = self._select_node()
        except NoNodesAvailableError:
            group_id = self._get_group_id(pod)
            try:
                self._preempt_for_group(group_id)
                node_name = self._select_node()
            except (InsufficientResourcesError, NoNodesAvailableError) as e:
                print(f"Failed to schedule {pod_name}: {e}")
                return

        try:
            print(f"Binding {pod_name} -> {node_name}")
            self._bind_pod(pod_name, node_name, namespace)
        except ApiException as e:
            try:
                msg = json.loads(e.body).get("message", e.body)
            except (json.JSONDecodeError, AttributeError):
                msg = str(e)
            print(f"Bind failed for {pod_name}: {msg}")
        except Exception as e:
            print(f"Error binding {pod_name}: {e}")

    def run(self):
        print(f"Starting scheduler: {self.scheduler_name}")
        for event in self.watcher.stream(self.v1.list_pod_for_all_namespaces):
            pod = event["object"]
            if self._is_schedulable(pod, event["type"]):
                self._schedule_pod(pod)


if __name__ == "__main__":
    scheduler = Scheduler(scheduler_name="foobar")
    scheduler.run()
