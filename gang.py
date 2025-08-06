from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Iterable, Set
from kubernetes import client
from pod_utils import (
    is_terminating, is_terminated_phase,
    should_skip_pod_for_scheduling
)

DEFAULT_GROUP_ANNOTATION = "pod-group"
DEFAULT_PRIORITY_ANNOTATION = "priority"


@dataclass
class GroupSelector:
    max_priority: Optional[int] = None
    group_annotation: str = DEFAULT_GROUP_ANNOTATION
    priority_annotation: str = DEFAULT_PRIORITY_ANNOTATION
    allowed_statuses: Optional[Set[str]] = None


@dataclass
class PodGroup:
    gang_id: str
    pods: List[client.V1Pod] = field(default_factory=list)
    size: int = 0
    priority: int = 0


class PodGroupDiscoverer:
    def __init__(self, v1: client.CoreV1Api):
        self.v1 = v1

    def groups(self, selector):
        pods = self._list_pods()
        pods = self._filter_system_pods(pods)
        pods = self._filter_status_and_scheduler(pods, selector)
        pods = self._filter_priorities(pods, selector)
        groups = self._group_pods(pods, selector)
        groups = self._compute_group_priority(groups, selector)

        groups.sort(key=lambda g: (g.priority, -g.size))
        return groups

    def _list_pods(self):
        return self.v1.list_pod_for_all_namespaces().items

    def _filter_system_pods(self, pods):
        out = []
        for p in pods:
            if not self._should_skip_eviction(p):
                out.append(p)
        return out


    @staticmethod
    def _priority_of(p, priority_annotation):
        if p.spec and p.spec.priority is not None:
            return p.spec.priority
        ann = (p.metadata.annotations or {}).get(priority_annotation)
        if ann is not None:
            try:
                return int(ann)
            except ValueError:
                return 0
        return 0

    @staticmethod
    def _group_of(p, group_annotation):
        return (p.metadata.annotations or {}).get(group_annotation)

    def _filter_status_and_scheduler(self, pods, selector):
        out = []
        for p in pods:
            phase = getattr(p.status, "phase", None)
            if selector.allowed_statuses is None:
                if phase not in ("Succeeded", "Failed"):
                    out.append(p)
            else:
                if phase in selector.allowed_statuses:
                    out.append(p)
        return out

    def _filter_priorities(self, pods, selector):
        out = []
        for p in pods:
            prio = self._priority_of(p, selector.priority_annotation)
            if selector.max_priority is not None and prio > selector.max_priority:
                continue
            out.append(p)
        return out

    def _group_pods(self, pods, selector):
        groups = {}
        singles = []

        for p in pods:
            gid = self._group_of(p, selector.group_annotation)
            groups.setdefault(gid, []).append(p)

        agg = [PodGroup(gang_id=gid, pods=pl, size=len(pl)) for gid, pl in groups.items()]
        agg.extend(singles)
        return agg

    def _compute_group_priority(self, groups, selector):
        for g in groups:
            prios = [self._priority_of(p, selector.priority_annotation) for p in g.pods]
            g.priority = max(prios) if prios else 0
        return groups

    def get_group(self, gang_id):
        pods = self._list_pods()
        group_pods = []

        for p in pods:
            gid = self._group_of(p, DEFAULT_GROUP_ANNOTATION)
            if gid == gang_id:
                group_pods.append(p)

        if not group_pods:
            return None

        group = PodGroup(gang_id=gang_id, pods=group_pods, size=len(group_pods))
        prios = [self._priority_of(p, DEFAULT_PRIORITY_ANNOTATION) for p in group_pods]
        group.priority = max(prios) if prios else 0
        return group

    def _should_skip_eviction(self, p):
        return should_skip_pod_for_scheduling(p)

    def preempt_group(self, gang_id, grace_period_seconds=0, use_eviction=True):
        group = self.get_group(gang_id)
        if not group:
            return

        count = 0
        for pod in group.pods:
            phase = getattr(pod.status, "phase", None)
            if is_terminating(pod) or is_terminated_phase(phase):
                continue

            if self._should_skip_eviction(pod):
                continue

            namespace = pod.metadata.namespace
            name = pod.metadata.name

            if use_eviction:
                success = self._try_eviction(name, namespace, grace_period_seconds)
                if success:
                    count += 1

        return count


    def _try_eviction(self, name, namespace, grace_period_seconds):
        try:
            eviction = client.V1Eviction(
                metadata=client.V1ObjectMeta(name=name, namespace=namespace),
                delete_options=client.V1DeleteOptions(grace_period_seconds=grace_period_seconds)
            )
            self.v1.create_namespaced_pod_eviction(
                name=name,
                namespace=namespace,
                body=eviction
            )
            return True
        except Exception as e:
            return False