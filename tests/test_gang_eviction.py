import unittest
from unittest.mock import Mock, patch
from gang import PodGroupDiscoverer, PodGroup
from kubernetes import client


class TestGangEviction(unittest.TestCase):
    def setUp(self):
        self.mock_v1 = Mock(spec=client.CoreV1Api)
        self.discoverer = PodGroupDiscoverer(self.mock_v1)

    def _create_mock_pod(self, name="pod", namespace="default", phase="Running"):
        pod = Mock(spec=client.V1Pod)
        pod.metadata = Mock()
        pod.metadata.name = name
        pod.metadata.namespace = namespace
        pod.metadata.deletion_timestamp = None
        pod.metadata.owner_references = []
        pod.metadata.annotations = {}
        
        pod.spec = Mock()
        pod.spec.priority_class_name = None
        
        pod.status = Mock()
        pod.status.phase = phase
        return pod

    def test_try_eviction_success(self):
        self.mock_v1.create_namespaced_pod_eviction.return_value = None
        
        result = self.discoverer._try_eviction("pod", "default", 30)
        
        self.assertTrue(result)
        call_args = self.mock_v1.create_namespaced_pod_eviction.call_args
        self.assertEqual(call_args[1]['name'], "pod")
        self.assertEqual(call_args[1]['namespace'], "default")

    def test_try_eviction_pdb_violation(self):
        api_exception = client.exceptions.ApiException(status=429)
        self.mock_v1.create_namespaced_pod_eviction.side_effect = api_exception
        
        result = self.discoverer._try_eviction("pod", "default", 30)
        
        self.assertFalse(result)


    @patch.object(PodGroupDiscoverer, 'get_group')
    @patch.object(PodGroupDiscoverer, '_try_eviction')
    def test_preempt_group_success(self, mock_evict, mock_get_group):
        pods = [self._create_mock_pod("pod1"), self._create_mock_pod("pod2")]
        mock_get_group.return_value = PodGroup("group", pods=pods, size=2)
        mock_evict.return_value = True
        
        result = self.discoverer.preempt_group("group")
        
        self.assertEqual(result, 2)
        self.assertEqual(mock_evict.call_count, 2)

    @patch.object(PodGroupDiscoverer, 'get_group')
    def test_preempt_group_no_group(self, mock_get_group):
        mock_get_group.return_value = None
        
        result = self.discoverer.preempt_group("missing")
        
        self.assertIsNone(result)
