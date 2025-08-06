import unittest
from unittest.mock import Mock
from gang import PodGroupDiscoverer, GroupSelector, PodGroup
from kubernetes import client


class TestGangDiscovery(unittest.TestCase):
    def setUp(self):
        self.mock_v1 = Mock(spec=client.CoreV1Api)
        self.discoverer = PodGroupDiscoverer(self.mock_v1)

    def _create_mock_pod(self, name="pod", namespace="default", annotations=None, priority=None, phase="Running", priority_class_name=None):
        pod = Mock(spec=client.V1Pod)
        pod.metadata = Mock()
        pod.metadata.name = name
        pod.metadata.namespace = namespace
        pod.metadata.annotations = annotations or {}
        pod.metadata.owner_references = []
        
        pod.spec = Mock()
        pod.spec.priority = priority
        pod.spec.priority_class_name = priority_class_name
        
        pod.status = Mock()
        pod.status.phase = phase
        return pod

    def test_groups_basic_flow(self):
        pods = [
            self._create_mock_pod("pod1", annotations={"pod-group": "group-a"}, priority=10),
            self._create_mock_pod("pod2", annotations={"pod-group": "group-a"}, priority=20),
            self._create_mock_pod("system-pod", namespace="kube-system")
        ]
        
        mock_list = Mock()
        mock_list.items = pods
        self.mock_v1.list_pod_for_all_namespaces.return_value = mock_list
        
        result = self.discoverer.groups(GroupSelector())
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].gang_id, "group-a")
        self.assertEqual(result[0].size, 2)
        self.assertEqual(result[0].priority, 20)
        
    def test_get_group(self):
        pods = [
            self._create_mock_pod("pod1", annotations={"pod-group": "target"}, priority=10),
            self._create_mock_pod("pod2", annotations={"pod-group": "other"}, priority=20)
        ]
        
        mock_list = Mock()
        mock_list.items = pods
        self.mock_v1.list_pod_for_all_namespaces.return_value = mock_list
        
        result = self.discoverer.get_group("target")
        
        self.assertIsNotNone(result)
        self.assertEqual(result.gang_id, "target")
        self.assertEqual(result.size, 1)
        self.assertEqual(result.priority, 10)

    def test_should_skip_eviction(self):
        pod = self._create_mock_pod(namespace="kube-system")
        self.assertTrue(self.discoverer._should_skip_eviction(pod))
        pod = self._create_mock_pod()
        self.assertFalse(self.discoverer._should_skip_eviction(pod))

